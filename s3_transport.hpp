#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "rcMisc.h"
#include "transport/transport.hpp"

#include "json.hpp"

#include <string>
#include <vector>
#include <stdio.h>
#include <iostream>

#include <libs3.h>

// TODO 
//    1) Create member variable for part_size
//    2) Create member variable for buffer pointer
//    3) On second call to send(), just reset the buffer pointer

// **** just simulate irods::error ****

namespace irods 
{
    class error {
        public:
            error() {}
            error(bool _status, long long code, std::string _message) {
                status = _status;
            }
            bool ok() const {
                return status;
            }
        private:
            bool status;
            
    };  // end class error
} // end namespace irods

namespace irods::experimental::io::s3_transport
{

    const size_t S3_DEFAULT_RETRY_WAIT_SEC = 1;
    const size_t S3_DEFAULT_RETRY_COUNT = 1;

    /*typedef struct S3Auth {
        char accessKeyId[MAX_NAME_LEN];
        char secretAccessKey[MAX_NAME_LEN];
    } s3Auth_t;*/
    
    // Returns timestamp in usec for delta-t comparisons
    static unsigned long long usNow() {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        unsigned long long us = (tv.tv_sec) * 1000000LL + tv.tv_usec;
        return us;
    } // end usNow

    typedef struct s3Stat
    {
        char key[MAX_NAME_LEN];
        rodsLong_t size;
        time_t lastModified;
    } s3Stat_t;
    
    typedef struct callback_data
    {
        std::vector<char_type*> *buffer_list_ptr;
        std::vector<size_t> *buffer_size_list_ptr;
     
        char_type *buffer_ptr; 
        size_t buffer_size; 
         
        rodsLong_t contentLength, originalContentLength;
        S3Status status;
        int keyCount;
        s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */
    
        S3BucketContext *pCtx; /* To enable more detailed error messages */
    } callback_data_t;

    typedef struct upload_manager
    {
        char *upload_id;    /* Returned from S3 on MP begin */
        char **etags;       /* Each upload part's MD5 */
    
        /* Below used for the upload completion command, need to send in XML */
        char *xml;
        long remaining;
        long offset;
    
        S3BucketContext *pCtx; /* To enable more detailed error messages */
    
        S3Status status;
    } upload_manager_t;

    typedef struct multipart_data
    {
        int seq;                       /* Sequence number, i.e. which part */
        int mode;                      /* PUT or COPY */
        S3BucketContext *pSrcCtx;      /* Source bucket context, ignored in a PUT */
        const char *srcKey;            /* Source key, ignored in a PUT */
        callback_data put_object_data; /* File being uploaded */
        upload_manager_t *manager;     /* To update w/the MD5 returned */
    
        S3Status status;
        bool enable_md5;
        bool server_encrypt;
    } multipart_data_t;


    static int putObjectDataCallback(
        int libs3_buffer_size,
        char *libs3_buffer,
        void *callbackData)
    {
        // keep reading a bufferSize bytes from buffer.
        // if buffer is empty, get another from the circular_buffer
        callback_data_t *data = (callback_data_t *) callbackData;

        // if data->buffer_ptr is nullptr, grab next one
        if (nullptr == data->buffer_ptr) {
            data->buffer_ptr = data->buffer_list_ptr->pop_front();
            data->buffer_size = data->buffer_size_ptr->pop_front();
        }


    
        // if we've exhausted our current buffer, read the next buffer from the circular_buffer
        while (0 == data->buffer_size) {
            upload_page_t page;
            circular_buffer_instance_ptr->pop_front(page);
    
            // if we get a terminate there are no more bytes to be read
            if (page.terminate_flag) {
                return 0;
            }
            //data->original_bytes_ptr = data->bytes = page.buffer;
            data->buffer_size = page.buffer_size;
        }

    // bufferSize is the size of *buffer provided by libs3
    // data->buffer_size is the size of data->bytes we set up

    auto length = libs3_buffer_size > data->buffer_size ? data->buffer_size : libs3_buffer_size;
    memcpy(libs3_buffer, data->bytes, length);

    data->buffer_size -= length;
    data->bytes += length;

    return length;
} // end putObjectDataCallback

    /* Upload data from the part, use the plain callback_data reader */
    static int mpuPartPutDataCB (
        int bufferSize,
        char *buffer,
        void *callbackData)
    {
        return putObjectDataCallback( bufferSize, buffer, 
                                      &((multipart_data_t*)callbackData)->put_object_data );
    } // end mpuPartPutDataCB
    
    static S3Status mpuPartRespPropCB (
        const S3ResponseProperties *properties,
        void *callbackData)
    {
        multipart_data_t *data = (multipart_data_t *)callbackData;
    
        int seq = data->seq;
        const char *etag = properties->eTag;
        if (etag) {
            data->manager->etags[seq - 1] = strdup(etag);
        } else {
            data->manager->etags[seq - 1] = strdup("");
        }
    
        return S3StatusOK;
    } // end mpuPartRespPropCB
    
    static void mpuPartRespCompCB (
        S3Status status,
        const S3ErrorDetails *error,
        void *callbackData)
    {
        multipart_data_t *data = (multipart_data_t *)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, &(data->status) );
        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuPartRespCompCB
    S3STSDate s3GetSTSDate()
    {
        return S3STSAmzOnly;
    } // end s3GetSTSDate
    
    S3Protocol s3GetProto()
    {
        return S3ProtocolHTTP;
    } // end s3GetProto
    
    bool s3GetEnableMD5 ()
    {
        return false;
    } // end s3GetEnableMD5
    
    bool s3GetEnableMultiPartUpload ()
    {
        return true;
    } // end s3GetEnableMultiPartUpload
    
    static bool s3GetServerEncrypt ()
    {
        return false;
    } // end s3GetServerEncrypt

    template <typename CharT>
    class s3_transport : public transport<CharT>
    {
    public:
        // clang-format off
        using char_type   = typename transport<CharT>::char_type;
        using traits_type = typename transport<CharT>::traits_type;
        using int_type    = typename traits_type::int_type;
        using pos_type    = typename traits_type::pos_type;
        using off_type    = typename traits_type::off_type;
        // clang-format on

    private:
        // clang-format off
        inline static constexpr auto uninitialized_file_descriptor = -1;
        inline static constexpr auto minimum_valid_file_descriptor = 3;

        // Errors
        inline static constexpr auto translation_error             = -1;
        inline static const     auto seek_error                    = pos_type{off_type{-1}};

        // clang-format on

    public:
        explicit s3_transport(rxComm& _comm, 
                              int _sequence,
                              size_t _part_size,
                              const std::string& _s3_hostname, 
                              const std::string& _s3_bucket_name, 
                              const std::string& _object_key, 
                              const std::string& _access_key, 
                              const std::string& _secret_access_key, 
                              bool _debug_flag = false)
            : transport<CharT>{}
            , comm_{&_comm}
            , sequence_{_sequence}
            , part_size_{_part_size}
            , s3_hostname_{_s3_hostname}
            , s3_bucket_name_{_s3_bucket_name}
            , object_key_{_object_key}
            , access_key_{_access_key}
            , secret_access_key_{_secret_access_key}
            , debug_flag_{_debug_flag}
            , fd_{uninitialized_file_descriptor}
            , fd_info_{}
        {
        }

        bool open(const irods::experimental::filesystem::path& _p,
                  std::ios_base::openmode _mode) override
        {
            return !is_open()
                ? open_impl(_p, _mode, [](auto&) {})
                : false;
        }

        bool open(const irods::experimental::filesystem::path& _p,
                  int _replica_number,
                  std::ios_base::openmode _mode) override
        {
            if (is_open()) {
                return false;
            }

            return open_impl(_p, _mode, [_replica_number](auto& _input) {
                const auto replica = std::to_string(_replica_number);
                addKeyVal(&_input.condInput, REPL_NUM_KW, replica.c_str());
            });
        }

        bool open(const irods::experimental::filesystem::path& _p,
                  const std::string& _resource_name,
                  std::ios_base::openmode _mode) override
        {
            if (is_open()) {
                return false;
            }

            return open_impl(_p, _mode, [&_resource_name](auto& _input) {
                addKeyVal(&_input.condInput, RESC_NAME_KW, _resource_name.c_str());
                //addKeyVal(&_input.condInput, RESC_HIER_STR_KW, _resource_name.c_str());
            });
        }

        bool close(const on_close_success* _on_close_success = nullptr) override
        {
            using json = nlohmann::json;

            json json_input{
                {"update_catalog", true},
                {"file_descriptor", fd_},
                {"file_descriptor_info", fd_info_},
                {"metadata", json::array()},
                {"acl", json::array()}
            };

            if (_on_close_success) {
                json_input["update_catalog"] = _on_close_success->update_catalog;
                json_input["metadata"] = _on_close_success->metadata_ops;
                json_input["acl"] = _on_close_success->acl_ops;
            }

            const auto json_string = json_input.dump();

#ifdef IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
            const auto ec = rs_sync_with_physical_object(comm_, json_string.c_str());
#else
            const auto ec = rc_sync_with_physical_object(comm_, json_string.c_str());
#endif // IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API

            if (ec != 0) {
                return false;
            }

            fd_ = uninitialized_file_descriptor;

            return true;
        }

        std::streamsize receive(char_type* _buffer, std::streamsize _buffer_size) override
        {
            openedDataObjInp_t input{};

            input.l1descInx = fd_;
            input.len = _buffer_size;

            bytesBuf_t output{};

            output.len = input.len;
            output.buf = _buffer;

            std::cout << "Received: " << output.buf << std::endl;
        }

        std::streamsize send(const char_type* _buffer, std::streamsize _buffer_size) override
        {

            static bool first_run = true;

            // add the buffer and size to the lists
            buffer_list_.push_back(_buffer);
            buffer_size_list_.put_back(_buffer_size);

            // If this is not the first run, we have already set up the callback mechanism.  Allow that to continue.
            if (!first_run) {
                return;
            }

            first_run = false;

            // add the buffer and size to the lists
            buffer_list_.push_back(_buffer);
            buffer_size_list_.put_back(_buffer_size);

            S3BucketContext bucketContext;
            bucketContext.hostName = s3_hostname_.c_str();
            bucketContext.bucketName = s3_bucket_name_.c_str();
            bucketContext.accessKeyId = access_key_.c_str();
            bucketContext.secretAccessKey = secret_access_key_.c_str();

            // ADDED this
            irods::error result;
            std::stringstream msg;
            S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };
        
            size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
            size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
        
            multipart_data_t partData{};

            int seq = sequence_;

            size_t retry_cnt = 0;
            do {
                // Work on a local copy of the structure in case an error occurs in the middle
                // of an upload.  If we updated in-place, on a retry the part would start
                // at the wrong offset and length.
                partData = mpu_data_;
                partData.seq = seq;
                partData.put_object_data.pCtx = &bucketContext;
                partData.put_object_data.buffer_list_ptr = &buffer_list_;
                partData.put_object_data.buffer_size_list_ptr = &buffer_size_list_;
        
                // Each part is floor(file_size/thread_count) long.  The last thread has the extra bytes.
                /*if (thread_number == thread_count -1) {
                    partData.put_object_data.contentLength = file_size - (thread_count-1) * (file_size / thread_count);
                } else {
                    partData.put_object_data.contentLength = file_size / thread_count;
                }*/

                partData.put_object_data.contentLength = _part_size;
        
                if (debug_flag_) {
                    msg.str( std::string() ); // Clear
                    msg << "Multipart:  Start part " << (int)seq << ", key \"" << object_key_ << "\", uploadid \"" << mpu_upload_id_ << 
                        ", len " << (int)partData.put_object_data.contentLength;
                    printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
                    msg.str( std::string() ); // Clear
                }
        
                S3PutProperties *putProps = NULL;
                putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
                putProps->md5 = nullptr;
                if ( putProps && partData.enable_md5 ) {
                    // jjames - not sure how to do MD5 piecewise 
                    //putProps->md5 = s3CalcMD5( partData.put_object_data.fd, partData.put_object_data.offset, partData.put_object_data.contentLength, resource_name );
                }
                putProps->expires = -1;
                unsigned long long usStart = usNow();
        
                if (debug_flag_) {
                    printf("%s:%d (%s) S3_upload_part (ctx, key, props, handler, %d, uploadId, %ld, 0, partData)\n", 
                            __FILE__, __LINE__, __FUNCTION__, seq, partData.put_object_data.contentLength);
                }
                S3_upload_part(&bucketContext, object_key_.c_str(), putProps, &putObjectHandler, seq, mpu_upload_id_, 
                        partData.put_object_data.contentLength, 0, &partData);
                if (debug_flag_) {
                    printf("%s:%d (%s) S3_upload_part returned\n", __FILE__, __LINE__, __FUNCTION__);
                }
        
        
                unsigned long long usEnd = usNow();
                //double bw = (mpu_data_[seq-1].put_object_data.contentLength / (1024.0 * 1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                // Clear up the S3PutProperties, if it exists
                if (putProps) {
                    if (putProps->md5) free( (char*)putProps->md5 );
                    free( putProps );
                }
                if (debug_flag_) {
                    msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                    printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
                }
                if (partData.status != S3StatusOK) s3_sleep( retry_wait, 0 );
            } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && (++retry_cnt < retry_count_limit));
            if (partData.status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << "[resource_name=" << resource_name() << "] " << __FUNCTION__ << " - Error putting the S3 object: \"" 
                    << object_key_ << "\"" << " part " << seq;
                if(partData.status >= 0) {
                    msg << " - \"" << S3_get_status_name(partData.status) << "\"";
                }
                result = ERROR( S3_PUT_ERROR, msg.str() );
                printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
                g_mpuResult = result;
            }
            //return rxDataObjWrite(comm_, &input, &input_buffer);
            return _buffer_size;

        }

        pos_type seekpos(off_type _offset, std::ios_base::seekdir _dir) override
        {
            if (!is_open()) {
                return seek_error;
            }

            openedDataObjInp_t input{};

            input.l1descInx = fd_;
            input.offset = _offset;

            switch (_dir) {
                case std::ios_base::beg:
                    input.whence = SEEK_SET;
                    break;

                case std::ios_base::cur:
                    input.whence = SEEK_CUR;
                    break;

                case std::ios_base::end:
                    input.whence = SEEK_END;
                    break;

                default:
                    return seek_error;
            }

            fileLseekOut_t* output{};

            if (const auto ec = rxDataObjLseek(comm_, &input, &output); ec < 0) {
                return seek_error;
            }

            return output->offset;
        }

        bool is_open() const noexcept override
        {
            return fd_ >= minimum_valid_file_descriptor;
        }

        int file_descriptor() const noexcept override
        {
            return fd_;
        }

        std::string resource_name() const override
        {
            return fd_info_["data_object_info"]["resource_name"].template get<std::string>();
        }

        std::string resource_hierarchy() const override
        {
            return fd_info_["data_object_info"]["resource_hierarchy"].template get<std::string>();
        }

        int replica_number() const override
        {
            return fd_info_["data_object_info"]["replica_number"].template get<int>();
        }

    private:
        int make_open_flags(std::ios_base::openmode _mode) noexcept
        {
            using std::ios_base;

            const auto m = _mode & ~(ios_base::ate | ios_base::binary);

            if (ios_base::in == m) {
                return O_RDONLY;
            }
            else if (ios_base::out == m || (ios_base::out | ios_base::trunc) == m) {
                return O_CREAT | O_WRONLY | O_TRUNC;
            }
            else if (ios_base::app == m || (ios_base::out | ios_base::app) == m) {
                return O_CREAT | O_WRONLY | O_APPEND;
            }
            else if ((ios_base::out | ios_base::in) == m) {
                return O_CREAT | O_RDWR;
            }
            else if ((ios_base::out | ios_base::in | ios_base::trunc) == m) {
                return O_CREAT | O_RDWR | O_TRUNC;
            }
            else if ((ios_base::out | ios_base::in | ios_base::app) == m ||
                     (ios_base::in | ios_base::app) == m)
            {
                return O_CREAT | O_RDWR | O_APPEND | O_TRUNC;
            }

            return translation_error;
        }

        bool seek_to_end_if_required(std::ios_base::openmode _mode)
        {
            if (std::ios_base::ate & _mode) {
                if (seek_error == seekpos(0, std::ios_base::end)) {
                    return false;
                }
            }

            return true;
        }

        template <typename Function>
        bool open_impl(const filesystem::path& _p, std::ios_base::openmode _mode, Function _func)
        {
            const auto flags = make_open_flags(_mode);

            if (flags == translation_error) {
                return false;
            }

            dataObjInp_t input{};

            input.createMode = 0600;
            input.openFlags = flags;
            rstrcpy(input.objPath, _p.c_str(), sizeof(input.objPath));

            _func(input);

            //const auto fd = rxDataObjOpen(comm_, &input);
            const auto fd = file_descriptor_counter_++;

            if (fd < minimum_valid_file_descriptor) {
                return false;
            }

            fd_ = fd;

            if (!seek_to_end_if_required(_mode)) {
                close();
                return false;
            }

            // TODO Calling rxDataObjOpen() should just return the file descriptor information
            // to avoid an additional network call.
            if (!capture_file_descriptor_info()) {
                close();
                return false;
            }

            return true;
        }

        bool capture_file_descriptor_info()
        {
            using json = nlohmann::json;

            const auto json_input = json{{"fd", fd_}}.dump();
            char* json_output{};

            // reads L1desc table

#ifdef IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
            const auto ec = rs_get_file_descriptor_info(comm_, json_input.c_str(), &json_output);
#else
            const auto ec = rc_get_file_descriptor_info(comm_, json_input.c_str(), &json_output);
#endif

            if (ec != 0) {
                return false;
            }

            try {
                fd_info_ = json::parse(json_output);
            }
            catch (const json::parse_error& e) {
                return false;
            }

            return true;
        }

        irods::error complete_multipart_upload(const std::string& _object_key,
                                               const int totalSeq,
                                               S3BucketContext& bucketContext,
                                               upload_manager_t& manager,
                                               S3PutProperties*& putProps)
        {
        
            irods::error result = SUCCESS();
        
            std::stringstream msg;
            unsigned int retry_cnt = 0;
            size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
            size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
        
            if (g_mpuResult.ok()) { // If someone aborted, don't complete...
                if (debug_flag) {
                    msg.str( std::string() ); // Clear
                    msg << "Multipart:  Completing key \"" << _object_key.c_str() << "\"";
                    printf( "%s\n", msg.str().c_str() );
                }
        
                int i;
                strcpy(manager.xml, "<CompleteMultipartUpload>\n");
                manager.remaining = strlen(manager.xml);
                char buf[256];
                int n;
                for ( i = 0; i < totalSeq; i++ ) {
                    n = snprintf( buf, 256, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n", i + 1, manager.etags[i] );
                    strcpy( manager.xml+manager.remaining, buf );
                    manager.remaining += n;
                }
                strcat( manager.xml + manager.remaining, "</CompleteMultipartUpload>\n" );
                manager.remaining += strlen( manager.xml+manager.remaining );
                int manager_remaining = manager.remaining;
                manager.offset = 0;
                retry_cnt = 0;
                S3MultipartCommitHandler commit_handler = { {mpuCommitRespPropCB, mpuCommitRespCompCB }, mpuCommitXmlCB, NULL };
                do {
                    // On partial error, need to restart XML send from the beginning
                    manager.remaining = manager_remaining;
                    manager.offset = 0;
                    std::string hostname = s3GetHostname();
                    bucketContext.hostName = hostname.c_str(); 
                    manager.pCtx = &bucketContext;
                    S3_complete_multipart_upload(&bucketContext, _object_key.c_str(), &commit_handler, manager.upload_id, 
                            manager.remaining, NULL, &manager);
                    if (debug_flag) {
                        printf("%s:%d (%s) [manager.status=%s]\n", __FILE__, __LINE__, __FUNCTION__, S3_get_status_name(manager.status));
                    }
                    if (manager.status != S3StatusOK) s3_sleep( retry_wait, 0 );
                } while ((manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < retry_count_limit));
                if (manager.status != S3StatusOK) {
                    msg.str( std::string() ); // Clear
                    msg << __FUNCTION__ << " - Error putting the S3 object: \"" << _object_key << "\"";
                    if(manager.status >= 0) {
                        msg << " - \"" << S3_get_status_name( manager.status ) << "\"";
                    }
                    g_mpuResult = ERROR( S3_PUT_ERROR, msg.str() );
                }
            }
            if ( !g_mpuResult.ok() && manager.upload_id ) {
                // Someone aborted after we started, delete the partial object on S3
                printf("Cancelling multipart upload\n");
                mpuCancel( &bucketContext, _object_key.c_str(), manager.upload_id);
                // Return the error
                result = g_mpuResult;
            }
            // Clean up memory
            if (manager.xml) free(manager.xml);
            if (manager.upload_id) free(manager.upload_id);
            for (int i=0; manager.etags && i<totalSeq; i++) {
                if (manager.etags[i]) free(manager.etags[i]);
            }
            if (manager.etags) free(manager.etags);
            if (mpu_data_) free(mpu_data_);
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) {
                    //free( (char*)putProps->md5 );
                }
                free( putProps );
            }
        
            return result;
        } // end complete_multipart_upload


        int sequence_;       // for multiparts, the creator must specify which sequence is being used
        size_t part_size_;   // for multiparts, the creator must specify the part size
        rxComm* comm_;
        int fd_;
        nlohmann::json fd_info_;
        std::string s3_hostname_;
        std::string s3_bucket_name_;
        std::string object_key_;
        std::string access_key_;
        std::string secret_access_key_;
        bool debug_flag_;
        multipart_data_t mpu_data_;
        std::string mpu_upload_id_;

        std::vector<char_type*> buffer_list_;
        std::vector<size_t> buffer_size_list_;

        // TODO do i need a file descriptor?
        inline static int file_descriptor_counter_ = minimum_valid_file_descriptor;

    }; // s3_transport

    using default_transport = s3_transport<char>;
} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

