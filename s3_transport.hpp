#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "rcMisc.h"
#include "transport/transport.hpp"
#include "circular_buffer.hpp"

#include "json.hpp"

#include <string>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <mutex>
#include <condition_variable>

#include <libs3.h>

// TODO 
//    1) Create member variable for part_size
//    2) Create member variable for buffer pointer
//    3) On second call to send(), just reset the buffer pointer

namespace irods::experimental::io::s3_transport
{
    struct upload_page_t {
       char *buffer;
       size_t buffer_size;
       bool terminate_flag;
    }; 

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
        char *bytes;               // pointer to last entry on circular_buffer
        size_t buffer_size;
        irods::experimental::circular_buffer<upload_page_t> *circular_buffer_instance_ptr;
        rodsLong_t contentLength, originalContentLength;
        size_t bytes_written;
        S3Status status;
        int keyCount;
        s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */
        S3BucketContext *pCtx; /* To enable more detailed error messages */
        s3_transport *s3_transport_ptr;
    } callback_data_t;

//    typedef struct callback_data
//    {
//        std::vector<char_type*> *buffer_list_ptr;
//        std::vector<size_t> *buffer_size_list_ptr;
//     
//        char_type *buffer_ptr; 
//        size_t buffer_size; 
//         
//        rodsLong_t contentLength, originalContentLength;
//        S3Status status;
//        int keyCount;
//        s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */
//    
//        S3BucketContext *pCtx; /* To enable more detailed error messages */
//    } callback_data_t;

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

    static void StoreAndLogStatus (
        S3Status status,
        const S3ErrorDetails *error,
        const char *function,
        const S3BucketContext *pCtx,
        S3Status *pStatus,
        bool debug_flag = false )
    {
        int i;
    
        *pStatus = status;
        if( status != S3StatusOK ) {
            printf( "  S3Status: [%s] - %d\n", S3_get_status_name( status ), (int) status );
            printf( "    S3Host: %s\n", pCtx->hostName );
        }
    
        if (debug_flag) {
            if (status != S3StatusOK && function )
                printf( "  Function: %s\n", function );
            if (error && error->message)
                printf( "  Message: %s\n", error->message);
            if (error && error->resource)
                printf( "  Resource: %s\n", error->resource);
            if (error && error->furtherDetails)
                printf( "  Further Details: %s\n", error->furtherDetails);
            if (error && error->extraDetailsCount) {
                printf( "%s", "  Extra Details:\n");
    
                for (i = 0; i < error->extraDetailsCount; i++) {
                    printf( "    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
                }
            }
        }
    }  // end StoreAndLogStatus

    static int putObjectDataCallback(
        int libs3_buffer_size,
        char *libs3_buffer,
        void *callbackData)
    {
        // keep reading a bufferSize bytes from buffer.
        // if buffer is empty, get another from the circular_buffer
        callback_data_t *data = (callback_data_t *) callbackData;
        irods::experimental::circular_buffer<upload_page_t> *circular_buffer_instance_ptr = 
            data->circular_buffer_instance_ptr;

        // if we've already written the expected number of bytes, just return 0 and notify 
        // s3_transport of completion 
        if (data->bytes_written == data->contentLength) {
            std::lock_guard<std::mutex> lck(data.s3_transport_ptr->finished_part_upload_mutex_);
            data.s3_transport_ptr->upload_part_finished_flag_ = true;
            data.s3_transport_ptr->upload_part_finished_condition_variable_.notify_all();
            return 0;
        }

        // if we've exhausted our current buffer, read the next buffer from the circular_buffer
        while (0 == data->buffer_size) {
            upload_page_t page;
            circular_buffer_instance_ptr->pop_front(page);

            delete[] data->original_bytes_ptr;

            // TODO
            data->bytes_written +=

            // if we get a terminate there are no more bytes to be read
            if (data->bytes_written == data->contentLength) {

                std::lock_guard<std::mutex> lck(data.s3_transport_ptr->finished_part_upload_mutex_);
                data.s3_transport_ptr->upload_part_finished_flag_ = true;
                data.s3_transport_ptr->upload_part_finished_condition_variable_.notify_all();
                return 0;
            }
            data->original_bytes_ptr = data->bytes = page.buffer;
            data->buffer_size = page.buffer_size;
        }

        // bufferSize is the size of *buffer provided by libs3
        // data->buffer_size is the size of data->bytes we set up

        auto length = libs3_buffer_size > data->buffer_size ? data->buffer_size : libs3_buffer_size;
        memcpy(libs3_buffer, data->bytes, length);

        data->buffer_size -= length;
        data->bytes += length;

        data->bytes_written += length;

        // if we've now written the expected number of bytes, 
        if (data->bytes_written == data->contentLength) {
            std::lock_guard<std::mutex> lck(data.s3_transport_ptr->finished_part_upload_mutex_);
            data.s3_transport_ptr->upload_part_finished_flag_ = true;
            data.s3_transport_ptr->upload_part_finished_condition_variable_.notify_all();
            return 0;
        }

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
        void *callbackData, 
        bool debug_flag = false )
    {
        multipart_data_t *data = (multipart_data_t *)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, &(data->status), debug_flag );
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


    int initiate_multipart_upload(S3BucketContext *bucketContextPtr,
                                             size_t fileSize,
                                             const std::string& objectKey,
                                             size_t totalParts,
                                             upload_manager_t& manager,
                                             S3PutProperties*& putProps,
                                             )
    {
    
        int cache_fd = -1;
        int err_status = 0;
        size_t retry_cnt    = 0;
        bool enable_md5 = s3GetEnableMD5 ();
        bool server_encrypt = s3GetServerEncrypt ();
        std::stringstream msg;
    
        std::string resource_name = get_resource_name();
    
        size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
        size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
    
        putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
        putProps->md5 = nullptr;
        if ( putProps && enable_md5 )
            putProps->md5 = s3CalcMD5( cache_fd, 0, fileSize, get_resource_name() );
        if ( putProps && server_encrypt )
            putProps->useServerSideEncryption = true;
        putProps->expires = -1;
    
        // Multi-part upload or copy
        memset(&manager, 0, sizeof(manager));
    
        manager.upload_id = NULL;
        manager.remaining = 0;
        manager.offset  = 0;
        manager.xml = NULL;
    
        msg.str( std::string() ); // Clear
    
        long seq;
        size_t totalParts = thread_count;
    
        multipart_data_t partData;
        int partContentLength = 0;
   
        callback_data_t data();
        bzero(&data, sizeof(data));
        data.contentLength = data.originalContentLength = fileSize;
        data.s3_transport = this;
    
        // Allocate all dynamic storage now, so we don't start a job we can't finish later
        manager.etags = (char**)calloc(sizeof(char*) * totalParts, 1);
        if (!manager.etags) {
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            return SYS_MALLOC_ERR;
        }
        g_mpuData = (multipart_data_t*)calloc(total_parts, sizeof(multipart_data_t));
        if (!g_mpuData) {
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            free(manager.etags);
            return SYS_MALLOC_ERR;
        }
        // Maximum XML completion length with extra space for the <complete...></complete...> tag
        manager.xml = (char *)malloc((total_parts+2) * 256);
        if (manager.xml == NULL) {
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            free(g_mpuData);
            free(manager.etags);
            return SYS_MALLOC_ERR;
        }
    
        retry_cnt = 0;
        // These expect a upload_manager_t* as cbdata
        S3MultipartInitialHandler mpuInitialHandler = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };
        do {
            std::string hostname = s3GetHostname();
            manager.pCtx = bucketContextPtr;
            S3_initiate_multipart(bucketContextPtr, objectKey.c_str(), putProps, &mpuInitialHandler, NULL, &manager);
            if (manager.status != S3StatusOK) s3_sleep( retry_wait, 0 );
        } while ( (manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < retry_count_limit));
        if (manager.upload_id == NULL || manager.status != S3StatusOK) {
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            return S3_PUT_ERROR;
        }
    
        g_mpuUploadId = manager.upload_id;
        g_mpuKey = objectKey.c_str();
        for(seq = 1; seq <= total_parts; seq ++) {
            memset(&partData, 0, sizeof(partData));
            partData.manager = &manager;
            partData.seq = seq;
            partData.put_object_data = data;
            partData.enable_md5 = s3GetEnableMD5();
            partData.server_encrypt = s3GetServerEncrypt();
            g_mpuData[seq-1] = partData;
            data.contentLength -= partContentLength;
        }
    
        unsigned long long usStart = usNow();
    
        manager.remaining = 0;
        manager.offset  = 0;
    
        return 0;
    
    } // end initiate_multipart_upload

    int complete_multipart_upload(const std::string& objectKey,
                                           const int totalSeq,
                                           S3BucketContext& bucketContext,
                                           upload_manager_t& manager,
                                           S3PutProperties*& putProps, 
                                           bool debug_flag = false)
    {
    
        std::stringstream msg;
        unsigned int retry_cnt = 0;
        size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
        size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
    
        if (g_mpuResult.ok()) { // If someone aborted, don't complete...
            if (debug_flag) {
                msg.str( std::string() ); // Clear
                msg << "Multipart:  Completing key \"" << objectKey.c_str() << "\"";
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
                S3_complete_multipart_upload(&bucketContext, objectKey.c_str(), &commit_handler, manager.upload_id, 
                        manager.remaining, NULL, &manager);
                if (debug_flag) {
                    printf("%s:%d (%s) [manager.status=%s]\n", __FILE__, __LINE__, __FUNCTION__, S3_get_status_name(manager.status));
                }
                if (manager.status != S3StatusOK) s3_sleep( retry_wait, 0 );
            } while ((manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < retry_count_limit));
            if (manager.status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << __FUNCTION__ << " - Error putting the S3 object: \"" << objectKey << "\"";
                if(manager.status >= 0) {
                    msg << " - \"" << S3_get_status_name( manager.status ) << "\"";
                }
                g_mpuResult = ERROR( S3_PUT_ERROR, msg.str() );
            }
        }
        if ( !g_mpuResult.ok() && manager.upload_id ) {
            // Someone aborted after we started, delete the partial object on S3
            printf("Cancelling multipart upload\n");
            mpuCancel( &bucketContext, objectKey.c_str(), manager.upload_id);
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
                              int _total_parts,
                              const std::string& _object_key,
                              S3BucketContext& _bucket_context,
                              upload_manager_t& _upload_manager,
                              std::mutex& _initiate_multipart_mtx,
                              std::condition_variable& _initiate_multipart_condition_variable,
                              bool& _initiate_multipart_done_flag,
                              bool _initiate_multipart,
                              bool _debug_flag = false)



            : transport<CharT>{}
            , comm_{&_comm}
            , sequence_{_sequence}
            , part_size_{_part_size}
            , total_parts{_total_parts}
            , object_key_{_object_key}
            , bucket_context_{_bucket_context}
            , upload_manager_{_upload_manager}

            , initiate_multipart_mtx_{_initiate_multipart_mtx}
            , initiate_multipart_condition_variable_{_initiate_multipart_condition_variable}
            , initiate_multipart_done_flag_{_initiate_multipart_done_flag}

            , initiate_multipart_{_initiate_multipart}
            , debug_flag_{_debug_flag}
            , fd_{uninitialized_file_descriptor}
            , fd_info_{}
            , call_s3_upload_part_{true}
            , begin_part_upload_thread_ptr_{nullptr}
            , upload_part_finished_flag_{false}
        {
        }

        ~s3_connector() {
            if (begin_part_upload_thread_ptr_) {
                wait_until_upload_part_finished();
                begin_part_upload_thread_ptr_ -> join();
                delete begin_part_upload_thread_ptr_;
            }
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

            if (initiate_multipart_) {
                {
                    std::lock_guard<std::mutex> lock(initiate_multipart_mtx_); 

                    // send initiate message to S3
                    int ret = initiate_multipart_upload(&bucket_context_, file_size_, object_key_, total_parts_, upload_manager_, putProps); 
                    if (ret < 0) {
                        // TODO what to return on error
                        return ret;

                    }
                    initiate_multipart_done_flag_ = true;
                }
                initiate_multipart_condition_variable_.notify_all();
            } else {
                std::unique_lock<std::mutex> lock(initiate_multipart_mtx_); 
                initiate_multipart_condition_variable_.wait(lock, [this]{ return initiate_multipart_done_flag_; });
            }


            // Put the buffer on the circular buffer.  
            // We must copy the buffer because it will persist after send returns.
            char *copied_buffer = new char[_buffer_size];
            memcpy(copied_buffer, _buffer, _buffer_size);
            circular_buffer_instance_ptr->push_back({copied_buffer, buffer_size});

            // only call s3_upload_part once for this thread / part 
            if (call_s3_upload_part_) {

                call_s3_upload_part_ = false;

                std::stringstream msg;
                S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };
            
                size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
                size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
            
                multipart_data_t partData{};
    
                size_t retry_cnt = 0;
                do {
                    // Work on a local copy of the structure in case an error occurs in the middle
                    // of an upload.  If we updated in-place, on a retry the part would start
                    // at the wrong offset and length.
                    partData = mpu_data_;
                    partData.seq = sequence_;
                    partData.put_object_data.pCtx = &bucket_context_;
                    partData.put_object_data.original_bytes_ptr = partData.put_object_data.bytes = page.buffer;
            
                    partData.put_object_data.contentLength = part_size_;
            
                    if (debug_flag_) {
                        std::stringstream msg;
                        msg << "Multipart:  Start part " << (int)sequence_ << ", key \"" << object_key_ << "\", uploadid \"" << mpu_upload_id_ << 
                            ", len " << (int)partData.put_object_data.contentLength;
                        printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
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
                                __FILE__, __LINE__, __FUNCTION__, sequence_, partData.put_object_data.contentLength);
                    }

                    // this has to go in the background because we are going to have multiple calls send() to fill up the data 
                    begin_part_upload_thread_ptr_ = new std::thread(S3_upload_part, &bucket_context_, object_key_.c_str(), putProps, &putObjectHandler, sequence_, mpu_upload_id_, part_size_, 0, &partData);

                    // TODO how to handle errors
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
                        std::stringstream msg;
                        msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                        printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
                    }
                    if (partData.status != S3StatusOK) s3_sleep( retry_wait, 0 );
                } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && (++retry_cnt < retry_count_limit));
                if (partData.status != S3StatusOK) {
                    return S3_PUT_ERROR;
                }
            }

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

        wait_until_upload_part_finished() {
            std::unique_lock<std::mutex> lck(finished_part_upload_mutex_);
            upload_part_finished_condition_variable_.wait(lck, [this] { return upload_part_finished_flag_; });
        }

        int sequence_;       // for multiparts, the creator must specify which sequence is being used
        size_t part_size_;   // for multiparts, the creator must specify the part size
        rxComm* comm_;
        int fd_;
        nlohmann::json fd_info_;
        size_t part_size_,
        int total_parts_,
        std::string object_key_;

        S3BucketContext& bucket_context_;
        upload_manager_t& upload_manager_;
        std::mutex& initiate_multipart_mtx_;          
        std::condition_variable& initiate_multipart_condition_variable_;
        bool& initiate_multipart_done_flag_;

        bool initiate_multipart_;
        bool debug_flag_;
        multipart_data_t mpu_data_;
        std::string mpu_upload_id_;
        bool call_s3_upload_part_;

        std::vector<char_type*> buffer_list_;
        std::vector<size_t> buffer_size_list_;

        irods::experimental::circular_buffer<upload_page_t> circular_buffer_instance_(1);
        std::thread *begin_part_upload_thread_ptr_;

        // these are
        bool upload_part_finished_flag_;
        std::condition_variable upload_part_finished_condition_variable_;
        std::mutex finished_part_upload_mutex_;

        // TODO do i need a file descriptor?
        inline static int file_descriptor_counter_ = minimum_valid_file_descriptor;

    }; // s3_transport

    using default_transport = s3_transport<char>;
} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

