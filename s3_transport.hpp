#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "circular_buffer.hpp"

#include <rcMisc.h>
#include <transport/transport.hpp>
#include <fileLseek.h>
#include <rs_get_file_descriptor_info.hpp>

#include "json.hpp"

#include <string>
#include <thread>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>

#include <libs3.h>

namespace irods::experimental::io
{

    void print_bucket_context(S3BucketContext& bucket_context) {
        printf("BucketContext: [hostName=%s] [bucketName=%s][protocol=%d]"
               "[uriStyle=%d][accessKeyId=%s][secretAccessKey=%s][securityToken=%s][stsDate=%d]\n",
               bucket_context.hostName == nullptr ? "" : bucket_context.hostName,
               bucket_context.bucketName == nullptr ? "" : bucket_context.bucketName,
               bucket_context.protocol,
               bucket_context.uriStyle,
               bucket_context.accessKeyId == nullptr ? "" : bucket_context.accessKeyId,
               bucket_context.secretAccessKey == nullptr ? "" : bucket_context.secretAccessKey,
               bucket_context.securityToken == nullptr ? "" : bucket_context.securityToken,
               bucket_context.stsDate);
    }

    struct upload_page_t {
       char *buffer;
       size_t buffer_size;
       bool terminate_flag;
    };

    const size_t S3_DEFAULT_RETRY_WAIT_SEC = 1;
    const size_t S3_DEFAULT_RETRY_COUNT = 1;

    // Returns timestamp in usec for delta-t comparisons
    static unsigned long long usNow() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
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
        char *original_bytes_ptr;  // set to the current buffer, used so that it can be deleted
        char *bytes;               // a pointer to the current offset in the buffer
        size_t buffer_size;
        irods::experimental::circular_buffer<upload_page_t> *circular_buffer_ptr;
        rodsLong_t contentLength, originalContentLength;
        size_t bytes_written;
        S3Status status;
        int keyCount;
        s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */
        S3BucketContext *pCtx; /* To enable more detailed error messages */
        bool debug_flag;
    } callback_data_t;

    // Shared info between all s3_transport objects for a single data object.
    typedef struct upload_manager
    {
        upload_manager(std::mutex& multipart_mutex_, std::condition_variable& multipart_condition_variable_)
            : multipart_mutex{multipart_mutex_}
            , multipart_condition_variable{multipart_condition_variable_}
            , multipart_upload_flag(false)
            , multipart_download_flag(false)
            , count_parts_complete(0)
            , upload_id{""}
            , pCtx{nullptr}
            , xml{""}
            , last_error(0)
        {
        }

        int increment_count_parts_complete() {

            std::lock_guard<std::mutex> lg(multipart_mutex);
            return ++count_parts_complete;
        }

        int get_count_parts_complete() {

            std::lock_guard<std::mutex> lg(multipart_mutex);
            return count_parts_complete;
        }

        std::mutex&              multipart_mutex;   /* Used to protect the members as this struct is meant
                                                       to be shared among multiple threads. */

        std::condition_variable& multipart_condition_variable;  /* Used to coordinate multipart initiation and
                                                                   completion actions.  */

        bool                     initiate_multipart_done_flag;
        bool                     multipart_upload_flag;
        bool                     multipart_download_flag;
        int                      count_parts_complete;

        std::string              upload_id;         /* Returned from S3 on MP begin */
        std::vector<std::string> etags;
        S3BucketContext          *pCtx;             /* To enable more detailed error messages */

        int                      last_error;        /* holds the last error detected on part upload/download */

        /* Below used for the upload completion command, need to send in XML */
        std::string              xml;
        long                     remaining;
        long                     offset;
        bool                     debug_flag;

        S3Status                 status;            /* status returned by libs3 */
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
        bool debug_flag;
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
        if( debug_flag || status != S3StatusOK ) {
            printf( "  S3Status: [%s] - %d\n", S3_get_status_name( status ), (int) status );
            printf( "    S3Host: %s\n", pCtx->hostName );
        }

        if ((debug_flag || status != S3StatusOK) && function )
            printf( "  Function: %s\n", function );
        if ((debug_flag || error) && error->message)
            printf( "  Message: %s\n", error->message);
        if ((debug_flag || error) && error->resource)
            printf( "  Resource: %s\n", error->resource);
        if ((debug_flag || error) && error->furtherDetails)
            printf( "  Further Details: %s\n", error->furtherDetails);
        if ((debug_flag || error) && error->extraDetailsCount) {
            printf( "%s", "  Extra Details:\n");

            for (i = 0; i < error->extraDetailsCount; i++) {
                printf( "    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
            }
        }
    }  // end StoreAndLogStatus

    static S3Status mpuInitXmlCB (
        const char* upload_id,
        void *callbackData )
    {
        upload_manager_t *manager = (upload_manager_t *)callbackData;
        manager->upload_id = upload_id;
        return S3StatusOK;
    } // end mpuInitXmlCB

    static S3Status mpuInitRespPropCB (
        const S3ResponseProperties *properties,
        void *callbackData)
    {
        return S3StatusOK;
    } // end mpuInitRespPropCB

    static void mpuInitRespCompCB (
        S3Status status,
        const S3ErrorDetails *error,
        void *callbackData)
    {
        upload_manager_t *data = (upload_manager_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status), data->debug_flag );
    } // end mpuInitRespCompCB


    /* Uploading the multipart completion XML from our buffer */
    static int mpuCommitXmlCB (
        int bufferSize,
        char *buffer,
        void *callbackData )
    {
        upload_manager_t *manager = (upload_manager_t *)callbackData;
        long ret = 0;
        if (manager->remaining) {
            int toRead = ((manager->remaining > bufferSize) ?
                          bufferSize : manager->remaining);
            memcpy(buffer, manager->xml.c_str() + manager->offset, toRead);
            ret = toRead;
        }
        manager->remaining -= ret;
        manager->offset += ret;

        return (int)ret;
    } // end mpuCommitXmlCB

    static S3Status mpuCommitRespPropCB (
        const S3ResponseProperties *properties,
        void *callbackData)
    {
        return S3StatusOK;
    } // end mpuCommitRespPropCB

    static void mpuCommitRespCompCB (
        S3Status status,
        const S3ErrorDetails *error,
        void *callbackData)
    {
        upload_manager_t *data = (upload_manager_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status), data->debug_flag );
        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuCommitRespCompCB

    static int putObjectDataCallback(
        int libs3_buffer_size,
        char *libs3_buffer,
        void *callbackData)
    {
        // keep reading a bufferSize bytes from buffer.
        // if buffer is empty, get another from the circular_buffer
        callback_data_t *data = (callback_data_t *) callbackData;
        irods::experimental::circular_buffer<upload_page_t> *circular_buffer_ptr =
            data->circular_buffer_ptr;

        // if we've already written the expected number of bytes, just return 0 which will
        // trigger the completion
        if (data->bytes_written == data->contentLength) {
            return 0;
        }

        // if we've exhausted our current buffer, read the next buffer from the circular_buffer
        while (0 == data->buffer_size) {
            upload_page_t page;
            circular_buffer_ptr->pop_front(page);
            delete[] data->original_bytes_ptr;
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
            data->manager->etags[seq - 1] = etag;
        } else {
            data->manager->etags[seq - 1] = "";
        }

        return S3StatusOK;
    } // end mpuPartRespPropCB

    static void mpuPartRespCompCB (
        S3Status status,
        const S3ErrorDetails *error,
        void *callbackData)
    {
        multipart_data_t *data = (multipart_data_t *)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, &(data->status), data->debug_flag);

        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuPartRespCompCB


    static S3Status mpuCancelRespPropCB (
        const S3ResponseProperties *properties,
        void *callbackData)
    {
        return S3StatusOK;
    } // mpuCancelRespPropCB

    // S3_abort_multipart_upload() does not allow a callbackData parameter, so pass the
    // final operation status using this global.
    static S3Status g_mpuCancelRespCompCB_status = S3StatusOK;
    static S3BucketContext *g_mpuCancelRespCompCB_pCtx = nullptr;
    static void mpuCancelRespCompCB (
        S3Status status,
        const S3ErrorDetails *error,
        void *callbackData)
    {
        S3Status *pStatus = (S3Status*)&g_mpuCancelRespCompCB_status;
        StoreAndLogStatus( status, error, __FUNCTION__, g_mpuCancelRespCompCB_pCtx, pStatus, false );
        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuCancelRespCompCB

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


    // Sleep for *at least* the given time, plus some up to 1s additional
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(
        int _s,
        int _ms ) {
        // We're the only user of libc rand(), so if we mutex around calls we can
        // use the thread-unsafe rand() safely and randomly...if this is changed
        // in the future, need to use rand_r and init a static seed in this function
        static std::mutex randMutex;
        randMutex.lock();
        int random = rand();
        randMutex.unlock();
        int addl = (int)(((double)random / (double)RAND_MAX) * 1000.0); // Add up to 1000 ms (1 sec)
        useconds_t us = ( _s * 1000000 ) + ( (_ms + addl) * 1000 );
        usleep( us );
    } // end s3_sleep


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

        explicit s3_transport(int _sequence,
                              size_t _part_size,
                              int _total_parts,
                              size_t _object_size,
                              size_t _retry_count_limit,
                              size_t _retry_wait,
                              const std::string& _hostname,
                              const std::string& _bucket_name,
                              const std::string& _access_key,
                              const std::string& _secret_access_key,
                              upload_manager_t& _upload_manager,
                              bool _initiate_multipart,
                              bool _debug_flag = false)



            : transport<CharT>{}
            , sequence_{_sequence}
            , part_size_{_part_size}
            , total_parts_{_total_parts}
            , object_size_{_object_size}
            , retry_count_limit_{_retry_count_limit}
            , retry_wait_{_retry_wait}
            , hostname_{_hostname}
            , bucket_name_{_bucket_name}
            , access_key_{_access_key}
            , secret_access_key_{_secret_access_key}
            , upload_manager_{_upload_manager}
            , initiate_multipart_{_initiate_multipart}
            , debug_flag_{_debug_flag}
            , fd_{uninitialized_file_descriptor}
            , fd_info_{}
            , call_s3_upload_part_flag_{true}
            , begin_part_upload_thread_ptr_{nullptr}
            , circular_buffer_(1)
        {

            memset(&mpu_data_, 0, sizeof(mpu_data_));
            mpu_data_.manager = &upload_manager_;
            mpu_data_.seq = sequence_;
            mpu_data_.enable_md5 = s3GetEnableMD5();
            mpu_data_.server_encrypt = s3GetServerEncrypt();

            memset(&put_props_, 0, sizeof(put_props_));

            upload_manager_.debug_flag = debug_flag_;
            mpu_data_.debug_flag = debug_flag_;

            bucket_context_.hostName        = hostname_.c_str();
            bucket_context_.bucketName      = bucket_name_.c_str();
            bucket_context_.accessKeyId     = access_key_.c_str();
            bucket_context_.secretAccessKey = secret_access_key_.c_str();

            // TODO pass in as args
            bucket_context_.protocol         = S3ProtocolHTTP;
            bucket_context_.stsDate          = S3STSAmzOnly;
            bucket_context_.uriStyle         = S3UriStylePath;
        }

        ~s3_transport() {

            if (begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_ -> join();
                delete begin_part_upload_thread_ptr_;
            }
            if (put_props_.md5) free( (char*)put_props_.md5 );
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
            if (begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_->join();
                begin_part_upload_thread_ptr_ = nullptr;
            }

            fd_ = uninitialized_file_descriptor;

            // last one will close
            int parts_complete = upload_manager_.get_count_parts_complete();

            if (parts_complete == total_parts_  && upload_manager_.last_error == 0) {

                int ret = complete_multipart_upload();

                if (ret < 0) {
                    return false;
                }
            }

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
            return output.len;
        }

        std::streamsize send(const char_type* _buffer, std::streamsize _buffer_size) override
        {

            if (initiate_multipart_) {
                {
                    std::lock_guard<std::mutex> lock(upload_manager_.multipart_mutex);

                    // send initiate message to S3
                    int ret = initiate_multipart_upload();

                    if (ret < 0) {
                        // TODO what to return on error
                        upload_manager_.last_error = ret;
                        upload_manager_.multipart_condition_variable.notify_all();
                        return ret;

                    }
                    upload_manager_.multipart_upload_flag = true;
                    upload_manager_.initiate_multipart_done_flag = true;
                }
                upload_manager_.multipart_condition_variable.notify_all();
            } else {
                // TODO this assumes we're doing multipart, pass in a multipart cutoff size
                std::unique_lock<std::mutex> lock(upload_manager_.multipart_mutex);
                upload_manager_.multipart_condition_variable.wait(lock, [this] {
                        return upload_manager_.initiate_multipart_done_flag || upload_manager_.last_error < 0; });

                // if we got an error on initiation, don't complete just return the error 
                if (upload_manager_.last_error < 0) {
                    return upload_manager_.last_error;
                }
            }


            // Put the buffer on the circular buffer.
            // We must copy the buffer because it will persist after send returns.
            char *copied_buffer = new char[_buffer_size];
            memcpy(copied_buffer, _buffer, _buffer_size);
            circular_buffer_.push_back({copied_buffer, static_cast<size_t>(_buffer_size)});

            if (debug_flag_) {
                printf("%s:%d (%s) [part=%d] wrote buffer of size %ld\n", __FILE__, __LINE__, __FUNCTION__,
                        sequence_, _buffer_size);
            }

            // only call s3_upload_part once for this thread / part
            if (call_s3_upload_part_flag_) {

                call_s3_upload_part_flag_ = false;

                // this has to go in the background because we are going to have multiple calls send() to fill up the data
                // and this doesn't return until all the data is sent
                begin_part_upload_thread_ptr_ = new std::thread(&s3_transport::call_s3_upload_part, this);

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

            return 0;
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
            //return fd_info_["data_object_info"]["resource_name"].template get<std::string>();
            return "";
        }

        std::string resource_hierarchy() const override
        {
            //return fd_info_["data_object_info"]["resource_hierarchy"].template get<std::string>();
            return "";
        }

        int replica_number() const override
        {

            //return fd_info_["data_object_info"]["replica_number"].template get<int>();
            return 0;
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

            object_key_ = _p.string();
            const auto flags = make_open_flags(_mode);

            if (flags == translation_error) {
                return false;
            }

            dataObjInp_t input{};

            input.createMode = 0600;
            input.openFlags = flags;
            rstrcpy(input.objPath, _p.c_str(), sizeof(input.objPath));

            _func(input);

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
//            using json = nlohmann::json;
//
//            const auto json_input = json{{"fd", fd_}}.dump();
//            char* json_output{};

            // reads L1desc table

//#ifdef IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
//            const auto ec = rs_get_file_descriptor_info(comm_, json_input.c_str(), &json_output);
//#else
//            const auto ec = rc_get_file_descriptor_info(comm_, json_input.c_str(), &json_output);
//#endif
//
//            if (ec != 0) {
//                return false;
//            }
//
//            try {
//                fd_info_ = json::parse(json_output);
//            }
//            catch (const json::parse_error& e) {
//                return false;
//            }

            return true;
        }

        int initiate_multipart_upload()
        {
            int flags = S3_INIT_ALL;

            // TODO signature version as an argument
            flags |= S3_INIT_SIGNATURE_V4;

            int status = S3_initialize( "s3", flags, bucket_context_.hostName );
            if (status != S3StatusOK) {
                fprintf(stderr, "S3_initialize returned error\n");
                return S3_PUT_ERROR;
            }

            int cache_fd = -1;
            int err_status = 0;
            size_t retry_cnt    = 0;
            bool enable_md5 = s3GetEnableMD5 ();
            put_props_.useServerSideEncryption = s3GetServerEncrypt ();
            std::stringstream msg;

            put_props_.md5 = nullptr;
            //if ( enable_md5 )
            //    putProps.md5 = s3CalcMD5( cache_fd, 0, object_size, resource_name() );
            put_props_.expires = -1;

            upload_manager_.upload_id = "";
            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;
            upload_manager_.xml = "";

            msg.str( std::string() ); // Clear

            int partContentLength = 0;

            callback_data_t data;
            memset(&data, 0, sizeof(data));
            data.contentLength = data.originalContentLength = object_size_;
            data.debug_flag = debug_flag_;

            // Allocate all dynamic storage now, so we don't start a job we can't finish later
            //manager.etags = (char**)calloc(sizeof(char*) * totalParts, 1);
            try {
                upload_manager_.etags.resize(total_parts_);
            } catch (std::bad_alloc& ba) {
                return SYS_MALLOC_ERR;   // not really malloc but close enough
            }

            retry_cnt = 0;
            // These expect a upload_manager_t* as cbdata
            S3MultipartInitialHandler mpuInitialHandler = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };

            do {
                upload_manager_.pCtx = &bucket_context_;
                print_bucket_context(bucket_context_);
                if (debug_flag_) {
                    printf("%s:%d (%s) [part=%d] call S3_initiate_multipart\n", __FILE__, __LINE__, __FUNCTION__, sequence_);
                }
                S3_initiate_multipart(&bucket_context_, object_key_.c_str(), &put_props_, &mpuInitialHandler, 
                        nullptr, &upload_manager_);

                if (upload_manager_.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
            } while ( (upload_manager_.status != S3StatusOK) && S3_status_is_retryable(upload_manager_.status) 
                    && ( ++retry_cnt < retry_count_limit_));

            if ("" == upload_manager_.upload_id || upload_manager_.status != S3StatusOK) {

                return S3_PUT_ERROR;
            }

            if (debug_flag_) {
                printf("%s:%d (%s) [part=%d] S3_initiate_multipart returned.  Upload ID = %s\n", 
                        __FILE__, __LINE__, __FUNCTION__, sequence_, upload_manager_.upload_id.c_str());
            }
            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;

            return 0;

        } // end initiate_multipart_upload

        void mpuCancel()
        {
            S3AbortMultipartUploadHandler abortHandler = { { mpuCancelRespPropCB, mpuCancelRespCompCB } };
            std::stringstream msg;
            S3Status status;

            if (debug_flag_) {
                msg << "Cancelling multipart upload: key=\""
                    << object_key_ << "\", upload_id=\"" << upload_manager_.upload_id << "\"";
                printf( "%s\n", msg.str().c_str() );
            }
            g_mpuCancelRespCompCB_status = S3StatusOK;
            g_mpuCancelRespCompCB_pCtx = &bucket_context_;
            S3_abort_multipart_upload(&bucket_context_, object_key_.c_str(), upload_manager_.upload_id.c_str(), &abortHandler);
            status = g_mpuCancelRespCompCB_status;
            if (status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << "] " << __FUNCTION__
                    << " - Error cancelling the multipart upload of S3 object: \"" << object_key_ << "\"";
                if (status >= 0) {
                    msg << " - \"" << S3_get_status_name(status) << "\"";
                }
                printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
            }
        } // end mpuCancel


        int complete_multipart_upload()
        {

            int result;
            std::stringstream msg;
            unsigned int retry_cnt = 0;

            std::stringstream xml("");

            if (0 == upload_manager_.last_error) { // If someone aborted, don't complete...
                if (debug_flag_) {
                    msg.str( std::string() ); // Clear
                    msg << "Multipart:  Completing key \"" << object_key_.c_str() << "\"";
                    printf( "%s\n", msg.str().c_str() );
                }

                int i;
                xml << "<CompleteMultipartUpload>\n";
                char buf[256];
                int n;
                for ( i = 0; i < total_parts_; i++ ) {
                    xml << "<Part><PartNumber>";
                    xml << (i + 1);
                    xml << "</PartNumber><ETag>";
                    xml << upload_manager_.etags[i];
                    xml << "</ETag></Part>";
                }
                xml << "</CompleteMultipartUpload>\n";

                int manager_remaining = xml.str().size(); 
                upload_manager_.offset = 0;
                retry_cnt = 0;
                S3MultipartCommitHandler commit_handler = { {mpuCommitRespPropCB, mpuCommitRespCompCB }, mpuCommitXmlCB, nullptr };
                do {
                    // On partial error, need to restart XML send from the beginning
                    upload_manager_.remaining = manager_remaining;
                    upload_manager_.xml = xml.str().c_str();

                    upload_manager_.offset = 0;
                    upload_manager_.pCtx = &bucket_context_;
                    S3_complete_multipart_upload(&bucket_context_, object_key_.c_str(), &commit_handler, 
                            upload_manager_.upload_id.c_str(), upload_manager_.remaining, nullptr, &upload_manager_);
                    if (debug_flag_) {
                        printf("%s:%d (%s) [manager.status=%s]\n", __FILE__, __LINE__, __FUNCTION__,
                                S3_get_status_name(upload_manager_.status));
                    }
                    if (upload_manager_.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
                } while ((upload_manager_.status != S3StatusOK) &&
                        S3_status_is_retryable(upload_manager_.status) &&
                        ( ++retry_cnt < retry_count_limit_));

                if (upload_manager_.status != S3StatusOK) {
                    msg.str( std::string() ); // Clear
                    msg << __FUNCTION__ << " - Error putting the S3 object: \"" << object_key_ << "\"";
                    if(upload_manager_.status >= 0) {
                        msg << " - \"" << S3_get_status_name( upload_manager_.status ) << "\"";
                    }
                    return S3_PUT_ERROR;
                }
            }
            if (0 > upload_manager_.last_error && "" != upload_manager_.upload_id ) {

                // Someone aborted after we started, delete the partial object on S3
                printf("Cancelling multipart upload\n");
                mpuCancel();

                // Return the error
                result = upload_manager_.last_error;
            }

            return result;
        } // end complete_multipart_upload

        // this function is called in the background in a separate thread
        void call_s3_upload_part() {


            std::stringstream msg;
            S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };

            multipart_data_t partData{};
            upload_page_t page;

            // read the first page
            if (debug_flag_) {
                printf("%s:%d (%s) waiting to read\n", __FILE__, __LINE__, __FUNCTION__);
            }
            circular_buffer_.pop_front(page);
            if (debug_flag_) {
                printf("%s:%d (%s) read page [buffer=%p][buffer_size=%zu][terminate_flag=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, page.buffer, page.buffer_size, page.terminate_flag);
            }

            size_t retry_cnt = 0;

            do {

                // Work on a local copy of the structure in case an error occurs in the middle
                // of an upload.  If we updated in-place, on a retry the part would start
                // at the wrong offset and length.
                partData = mpu_data_;
                partData.debug_flag = debug_flag_;
                partData.seq = sequence_;
                partData.put_object_data.pCtx = &bucket_context_;
                partData.put_object_data.original_bytes_ptr = partData.put_object_data.bytes = page.buffer;
                partData.put_object_data.circular_buffer_ptr = &(s3_transport::circular_buffer_);
                partData.put_object_data.buffer_size = page.buffer_size;

                partData.put_object_data.contentLength = part_size_;

                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  Start part " << (int)sequence_ << ", key \"" << object_key_ << "\", uploadid \"" 
                        << upload_manager_.upload_id << "\", len " << (int)partData.put_object_data.contentLength;
                    printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
                }

                put_props_.md5 = nullptr;
                //S3PutProperties putProps;
                //if ( partData.enable_md5 ) {
                //    // jjames - not sure how to do MD5 piecewise
                //    putProps->md5 = s3CalcMD5( partData.put_object_data.fd, partData.put_object_data.offset, 
                //    partData.put_object_data.contentLength, resource_name );
                //}
                put_props_.expires = -1;
                unsigned long long usStart = usNow();

                if (debug_flag_) {
                    printf("%s:%d (%s) S3_upload_part (ctx, key, props, handler, %d, uploadId, %lld, 0, partData)\n",
                            __FILE__, __LINE__, __FUNCTION__, sequence_, partData.put_object_data.contentLength);
                }

                S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props_, &putObjectHandler, sequence_, 
                        upload_manager_.upload_id.c_str(), part_size_, 0, &partData);

                //upload_manager_.count_parts_complete++;
                int parts_complete = upload_manager_.increment_count_parts_complete();
 
                if (debug_flag_) {
                    printf("%s:%d (%s) S3_upload_part returned. [count_parts_complete=%d]\n", __FILE__, __LINE__, __FUNCTION__,
                           upload_manager_.count_parts_complete );
                }

                unsigned long long usEnd = usNow();
                //double bw = (mpu_data_[seq-1].put_object_data.contentLength / (1024.0 * 1024.0)) / 
                //   ( (usEnd - usStart) / 1000000.0 );
                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                    printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, msg.str().c_str() );
                }
                if (partData.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
            } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && 
                    (++retry_cnt < retry_count_limit_));

            if (partData.status != S3StatusOK) {
                upload_manager_.last_error = S3_PUT_ERROR;
            }
        }

        int               sequence_;       // for multiparts, the creator must specify which sequence is being used
        size_t            part_size_;      // for multiparts, the creator must specify the part size
        int               fd_;
        nlohmann::json    fd_info_;
        int               total_parts_;
        size_t            object_size_;
        size_t            retry_count_limit_;
        size_t            retry_wait_;
        std::string       hostname_;
        std::string       bucket_name_;
        std::string       access_key_;
        std::string       secret_access_key_;
        std::string       object_key_;
        S3BucketContext   bucket_context_;
        upload_manager_t& upload_manager_;

        bool              initiate_multipart_;
        bool              debug_flag_;
        multipart_data_t  mpu_data_;
        bool              call_s3_upload_part_flag_;
        S3PutProperties   put_props_;

        irods::experimental::circular_buffer<upload_page_t>
                          circular_buffer_;
        std::thread       *begin_part_upload_thread_ptr_;

        // TODO do i need a file descriptor?
        inline static int file_descriptor_counter_ = minimum_valid_file_descriptor;

    }; // s3_transport

    using default_transport = s3_transport<char>;
} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

