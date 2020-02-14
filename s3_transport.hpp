#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "circular_buffer.hpp"

// iRODS includes
#include <rcMisc.h>
#include <transport/transport.hpp>
#include <fileLseek.h>
#include <rs_get_file_descriptor_info.hpp>

// misc includes
#include "json.hpp"
#include <libs3.h>

// stdlib includes
#include <string>
#include <thread>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>

// boost includes
#include <boost/algorithm/string/predicate.hpp>

// boost includes
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/list.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/algorithm/string.hpp>


namespace irods::experimental::io
{

    namespace bi = boost::interprocess;

    class scoped_lock_test {
    public:

        scoped_lock_test(const std::string& _mutex_name,
                    const char *_file,
                    int _line,
                    const char *_function,
                    int _object_identifier)
            : mutex_name{_mutex_name}
            , file{_file}
            , line{_line}
            , function{_function}
            , named_mtx{nullptr}
            , lock{nullptr}
            , object_identifier{_object_identifier}
        {
            if (file != nullptr && function != nullptr) {
                printf("%s:%d (%s) [[%d]] ---LOCK--- waiting for lock\n", file, line, function, object_identifier);
            }

            named_mtx = new bi::named_mutex(bi::open_or_create, mutex_name.c_str());
            lock = new bi::scoped_lock<bi::named_mutex>(*named_mtx);

            if (file != nullptr && function != nullptr) {
                printf("%s:%d (%s) [[%d]] ---LOCK--- acquired lock\n", file, line, function, object_identifier);
            }
        }

        ~scoped_lock_test()
        {
            if (file != nullptr && function != nullptr) {
                printf("%s:%d (%s) [[%d]] ---LOCK--- releasing lock\n", file, line, function, object_identifier);
            }

            delete lock;
            delete named_mtx;

        }
    private:

        std::string mutex_name;
        bi::named_mutex *named_mtx;
        bi::scoped_lock<bi::named_mutex> *lock;
        const char *file;
        int line;
        const char *function;
        int object_identifier;
    };


    typedef bi::managed_shared_memory::segment_manager           segment_manager_t;
    typedef boost::container::scoped_allocator_adaptor<
                    bi::allocator<void, segment_manager_t> >     void_allocator;
    typedef bi::allocator<int, segment_manager_t>                int_allocator;
    typedef bi::allocator<char, segment_manager_t>               char_allocator;
    typedef bi::vector<int, int_allocator>                       shm_int_vector_t;
    typedef bi::basic_string<char, std::char_traits<char>,
            char_allocator>                                      shm_char_string_t;
    typedef bi::allocator<shm_char_string_t, segment_manager_t>  char_string_allocator;
    typedef bi::vector<shm_char_string_t, char_string_allocator> shm_string_vector_t;

    // data that needs to be shared among different processes
    struct multipart_shared_data_t
    {
        multipart_shared_data_t(const void_allocator &allocator)
            : file_open_cntr(0)
            , upload_id(allocator)
            , download_id(allocator)
            , etags(allocator)
            , last_error(0)
            , cache_file_downloaded_flag{false}
        {}

        int                      file_open_cntr;
        shm_char_string_t        upload_id;
        shm_char_string_t        download_id;
        shm_string_vector_t      etags;
        int                      last_error;
        bool                     cache_file_downloaded_flag;
    };


    typedef bi::allocator<multipart_shared_data_t,
            segment_manager_t>                                   multipart_shared_data_allocator;
    typedef std::pair<const shm_char_string_t,
            multipart_shared_data_t>   map_value_type;
    /*typedef bi::allocator<map_value_type,
            segment_manager_t>                                   map_value_type_allocator;
    typedef bi::map<shm_char_string_t, multipart_shared_data_t,
            std::less<shm_char_string_t>,
            map_value_type_allocator>                            multipart_shared_data_t;*/

    void print_bucket_context(S3BucketContext& bucket_context)
    {
        printf("BucketContext: [hostName=%s] [bucketName=%s][protocol=%d]"
               "[uriStyle=%d][accessKeyId=%s][secretAccessKey=%s]"
               "[securityToken=%s][stsDate=%d]\n",
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
       char   *buffer;
       size_t buffer_size;
       bool   terminate_flag;
    };

    typedef struct s3Stat
    {
        char       key[MAX_NAME_LEN];
        rodsLong_t size;
        time_t     lastModified;
    } s3Stat_t;

    typedef struct callback_data
    {
        char              *original_bytes_ptr;  /* set to the current buffer, used so that it can be deleted */
        char              *bytes;               /* a pointer to the current offset in the buffer */
        size_t            buffer_size;
        irods::experimental::circular_buffer<upload_page_t>
                          *circular_buffer_ptr;
        rodsLong_t        contentLength;
        rodsLong_t        originalContentLength;
        size_t            bytes_written;
        S3Status          status;
        int               keyCount;
        s3Stat_t          s3Stat;               /* should be a pointer if keyCount > 1 */
        S3BucketContext   *pCtx;                /* To enable more detailed error messages */
        bool              debug_flag;
        int               object_identifier;
    } callback_data_t;

    // callback data for reads
    typedef struct callback_data_read
    {
        // used when writing directly to buffer
        char            *output_buffer;
        size_t          output_buffer_size;

        // used when writing to cache file
        int             fd;

        long            offset;       /* For multiple upload */
        rodsLong_t      contentLength;
        rodsLong_t      originalContentLength;
        S3Status        status;
        int             keyCount;
        s3Stat_t        s3Stat;    /* should be a pointer if keyCount > 1 */

        S3BucketContext *pCtx; /* To enable more detailed error messages */
    } callback_data_read_t;

    typedef struct multirange_data
    {
        int                   seq;
        callback_data_read_t  get_object_data;
        S3Status              status;
        S3BucketContext       *pCtx; // To enable more detailed error messages
        bool                  debug_flag;
    } multirange_data_t;

    const size_t S3_DEFAULT_RETRY_WAIT_SEC = 1;
    const size_t S3_DEFAULT_RETRY_COUNT = 1;

    // Returns timestamp in usec for delta-t comparisons
    static unsigned long long usNow()
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        unsigned long long us = (tv.tv_sec) * 1000000LL + tv.tv_usec;
        return us;
    } // end usNow


    typedef struct upload_manager
    {
        upload_manager()
            : pCtx{nullptr}
            , xml{""}
        {
        }

        S3BucketContext          *pCtx;             /* To enable more detailed error messages */

        /* Below used for the upload completion command, need to send in XML */
        std::string              xml;
        long                     remaining;
        long                     offset;
        bool                     debug_flag;
        S3Status                 status;            /* status returned by libs3 */
        std::string              object_key;
    } upload_manager_t;

    typedef struct multipart_data
    {
        int              seq;                /* Sequence number, i.e. which part */
        int              mode;               /* PUT or COPY */
        S3BucketContext  *pSrcCtx;           /* Source bucket context, ignored in a PUT */
        const char       *srcKey;            /* Source key, ignored in a PUT */
        callback_data_t  put_object_data;    /* File being uploaded */
        upload_manager_t *manager;           /* To update w/the MD5 returned */
        S3Status         status;
        bool             enable_md5;
        bool             server_encrypt;
        bool             debug_flag;
        int              object_identifier;
    } multipart_data_t;

    void StoreAndLogStatus (S3Status status,
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
                printf( "    %s: %s\n", error->extraDetails[i].name,
                        error->extraDetails[i].value);
            }
        }
    }  // end StoreAndLogStatus

    S3Status mpuInitXmlCB (const char* upload_id,
                           void *callbackData )
    {
        upload_manager_t *manager = (upload_manager_t *)callbackData;

        // upload upload_id in shared memory
        std::string& object_key = manager->object_key;

        std::string shared_memory_name =  object_key + "-shm";
        bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

        void_allocator alloc_inst(segment.get_segment_manager());

        // no need to lock as this should already be locked
        multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
        shared_data->upload_id = upload_id;

        return S3StatusOK;
    } // end mpuInitXmlCB

    S3Status mpuInitRespPropCB (const S3ResponseProperties *properties,
                                void *callbackData)
    {
        return S3StatusOK;
    } // end mpuInitRespPropCB

    void mpuInitRespCompCB (S3Status status,
                            const S3ErrorDetails *error,
                            void *callbackData)
    {
        upload_manager_t *data = (upload_manager_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx,
                &(data->status), data->debug_flag );
    } // end mpuInitRespCompCB


    /* Uploading the multipart completion XML from our buffer */
    int mpuCommitXmlCB (int bufferSize,
                        char *buffer,
                        void *callbackData)
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

    S3Status mpuCommitRespPropCB (const S3ResponseProperties *properties,
                                  void *callbackData)
    {
        return S3StatusOK;
    } // end mpuCommitRespPropCB

    void mpuCommitRespCompCB (S3Status status,
                              const S3ErrorDetails *error,
                              void *callbackData)
    {
        upload_manager_t *data = (upload_manager_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx,
                &(data->status), data->debug_flag );
        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuCommitRespCompCB

    S3Status getObjectDataCallback(int libs3_buffer_size,
                                   const char *libs3_buffer,
                                   void *callbackData)
    {
        callback_data_read_t *cb = (callback_data_read_t *)callbackData;

        if (cb->output_buffer == nullptr) {

            // writing output to cache file

            ssize_t wrote = pwrite(cb->fd, libs3_buffer, libs3_buffer_size, cb->offset);
            if (wrote>0) cb->offset += wrote;

            return ((wrote < (ssize_t) libs3_buffer_size) ?
                    S3StatusAbortedByCallback : S3StatusOK);
        } else {

            // writing to buffer

            int bytes_to_write = libs3_buffer_size; //cb->offset + bufferSize > cb->output_buffer_size 
                    //? cb->output_buffer_size - cb->offset 
                    //: bufferSize;

//printf("%s:%d (%s) [[%d]] [bytes_to_write=%d][cb->offset=%lu][cb->output_buffer_size=%lu]\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
//        bytes_to_write, cb->offset, cb->output_buffer_size);

            memcpy(cb->output_buffer + cb->offset, libs3_buffer, bytes_to_write);

            cb->offset += bytes_to_write;

            /*return ((bytes_to_write < (ssize_t) bufferSize) ?
                    S3StatusAbortedByCallback : S3StatusOK);*/
            return S3StatusOK;
        }
    }

    S3Status mrdRangeGetDataCB(int bufferSize,
                               const char *buffer,
                               void *callbackData)
    {
        multirange_data_t *data = (multirange_data_t*)callbackData;
        return getObjectDataCallback( bufferSize, buffer, &(data->get_object_data) );
    }

    S3Status mrdRangeRespPropCB(const S3ResponseProperties *properties,
                                void *callbackData)
    {
        // Don't need to do anything here
        return S3StatusOK;
    }

    void mrdRangeRespCompCB (S3Status status,
                             const S3ErrorDetails *error,
                             void *callbackData)
    {
        multirange_data_t *data = (multirange_data_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status), 
                data->debug_flag );
        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    }

    int putObjectDataCallback(int libs3_buffer_size,
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

            // read the first page
            if (data->debug_flag) {
                printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__, data->object_identifier);
            }
            circular_buffer_ptr->pop_front(page);
            if (data->debug_flag) {
                printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%zu][terminate_flag=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, data->object_identifier, page.buffer, page.buffer_size,
                        page.terminate_flag);
            }
            delete[] data->original_bytes_ptr;
            data->original_bytes_ptr = data->bytes = page.buffer;
            data->buffer_size = page.buffer_size;
        }

        auto length = libs3_buffer_size > data->buffer_size
            ? data->buffer_size
            : libs3_buffer_size;

        memcpy(libs3_buffer, data->bytes, length);

        data->buffer_size -= length;
        data->bytes += length;

        data->bytes_written += length;

        return length;
    } // end putObjectDataCallback

    /* Upload data from the part, use the plain callback_data reader */
    int mpuPartPutDataCB (int bufferSize,
                          char *buffer,
                          void *callbackData)
    {
        return putObjectDataCallback( bufferSize, buffer,
                                      &((multipart_data_t*)callbackData)->put_object_data );
    } // end mpuPartPutDataCB

    S3Status mpuPartRespPropCB (const S3ResponseProperties *properties,
                                void *callbackData)
    {
        // update etag for this object

        multipart_data_t *data = (multipart_data_t *)callbackData;

        std::string& object_key = data->manager->object_key;

        std::string shared_memory_name =  object_key + "-shm";
        bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);
        void_allocator alloc_inst(segment.get_segment_manager());

        // lock for update
        std::string mtx_name = object_key + "-mtx";
        scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__, data->object_identifier);

        int object_identifier = data->object_identifier;
        multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);

        int seq = data->seq;
        const char *etag = properties->eTag;

        // Update the etags vector.  It should be sized large enough
        // to not require a resize but resize if necessary.
        if (seq > shared_data->etags.size()) {
            try {
                shared_data->etags.resize(seq, shm_char_string_t("", alloc_inst));
            } catch (std::bad_alloc& ba) {
                return S3StatusOutOfMemory;
            }
        }

        if (etag) {
            shared_data->etags[seq - 1] = etag;
        } else {
            shared_data->etags[seq - 1] = "";
        }

        return S3StatusOK;
    } // end mpuPartRespPropCB

    void mpuPartRespCompCB (S3Status status,
                            const S3ErrorDetails *error,
                            void *callbackData)
    {
        multipart_data_t *data = (multipart_data_t *)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx,
                &(data->status), data->debug_flag);

        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuPartRespCompCB


    S3Status mpuCancelRespPropCB (const S3ResponseProperties *properties,
                                  void *callbackData)
    {
        return S3StatusOK;
    } // mpuCancelRespPropCB

    // S3_abort_multipart_upload() does not allow a callbackData parameter, so pass the
    // final operation status using this global.
    // TODO do something different
    static S3Status g_mpuCancelRespCompCB_status = S3StatusOK;
    static S3BucketContext *g_mpuCancelRespCompCB_pCtx = nullptr;

    void mpuCancelRespCompCB (S3Status status,
                              const S3ErrorDetails *error,
                              void *callbackData)
    {
        S3Status *pStatus = (S3Status*)&g_mpuCancelRespCompCB_status;
        StoreAndLogStatus( status, error, __FUNCTION__, g_mpuCancelRespCompCB_pCtx,
                pStatus, false );
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

    bool s3GetServerEncrypt ()
    {
        return false;
    } // end s3GetServerEncrypt


    // Sleep for *at least* the given time, plus some up to 1s additional
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(int _s,
                  int _ms )
    {
        // We're the only user of libc rand(), so if we mutex around calls we can
        // use the thread-unsafe rand() safely and randomly...if this is changed
        // in the future, need to use rand_r and init a static seed in this function
        static std::mutex randMutex;
        randMutex.lock();
        int random = rand();
        randMutex.unlock();
        // Add up to 1000 ms (1 sec)
        int addl = (int)(((double)random / (double)RAND_MAX) * 1000.0);
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

        explicit s3_transport(size_t _object_size,
                              int _number_of_transfer_threads,     // only used when doing full file upload/download via cache
                                                                   // otherwise it is controlled by iRODS
                              size_t _retry_count_limit,
                              size_t _retry_wait,
                              const std::string& _hostname,
                              const std::string& _bucket_name,
                              const std::string& _access_key,
                              const std::string& _secret_access_key,
                              bool _multipart_flag,
                              const std::string& _s3_signature_version_str,
                              const std::string& _s3_protocol_str = "http",
                              const std::string& _s3_sts_data_str = "amz",
                              bool _debug_flag = false,
                              int _object_identifier = 0  // just for debug purposes
                              )

            : transport<CharT>{}
            , object_size_{_object_size}
            , number_of_transfer_threads_{_number_of_transfer_threads}
            , retry_count_limit_{_retry_count_limit}
            , retry_wait_{_retry_wait}
            , hostname_{_hostname}
            , bucket_name_{_bucket_name}
            , access_key_{_access_key}
            , secret_access_key_{_secret_access_key}
            , multipart_flag_{_multipart_flag}
            , cache_fd_{0}
            , debug_flag_{_debug_flag}
            , fd_{uninitialized_file_descriptor}
            , fd_info_{}
            , call_s3_upload_part_flag_{true}
            , call_s3_download_part_flag_{true}
            , begin_part_upload_thread_ptr_{nullptr}
            , circular_buffer_{1}
            , mode_{0}
            , file_offset_{0}
            , download_to_cache_{false}
            , use_cache_{false}
            , object_must_exist_{false}
            , object_identifier_{_object_identifier}
        {


            memset(&mpu_data_, 0, sizeof(mpu_data_));
            mpu_data_.manager = &upload_manager_;
            mpu_data_.enable_md5 = s3GetEnableMD5();
            mpu_data_.server_encrypt = s3GetServerEncrypt();
            mpu_data_.object_identifier = object_identifier_;

            memset(&put_props_, 0, sizeof(put_props_));

            upload_manager_.debug_flag = debug_flag_;
            mpu_data_.debug_flag = debug_flag_;

            bucket_context_.hostName        = hostname_.c_str();
            bucket_context_.bucketName      = bucket_name_.c_str();
            bucket_context_.accessKeyId     = access_key_.c_str();
            bucket_context_.secretAccessKey = secret_access_key_.c_str();

            if (_s3_signature_version_str == "4"
                    || boost::iequals(_s3_signature_version_str, "V4")) {
                s3_signature_version_       = S3SignatureV4;
            } else {
                s3_signature_version_       = S3SignatureV2;
            }

            if (boost::iequals(_s3_protocol_str, "http")) {
                bucket_context_.protocol    = S3ProtocolHTTP;
            } else {
                bucket_context_.protocol    = S3ProtocolHTTPS;
            }

            if (boost::iequals(_s3_sts_data_str, "date")) {
                bucket_context_.stsDate     = S3STSDateOnly;
            } else if (boost::iequals(_s3_sts_data_str, "both")) {
                bucket_context_.stsDate     = S3STSAmzAndDate;
            } else {
                bucket_context_.stsDate     = S3STSAmzOnly;
            }

            bucket_context_.uriStyle        = S3UriStylePath;

        }

        ~s3_transport() {

            if (begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_ -> join();
                delete begin_part_upload_thread_ptr_;
            }

            if (put_props_.md5) free( (char*)put_props_.md5 );
        }

        bool object_exists_in_s3() {
            // TODO
            return false;
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

            if (!is_open()) {
                return false;
            }


            fd_ = uninitialized_file_descriptor;

            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);

            int open_flags = populate_open_mode_flags();

            // if it is a full multpart upload, wait for the upload to complete
            if ( multipart_flag_ && (O_CREAT | O_WRONLY | O_TRUNC) == open_flags ) {

                // This was a full multipart upload w/o cache.


                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] wait for join of upload thread\n", 
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_);
                }

                // upload was in background.  wait for it to complete.
                if (begin_part_upload_thread_ptr_) {
                    begin_part_upload_thread_ptr_->join();
                    begin_part_upload_thread_ptr_ = nullptr;
                }

                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] join for part\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_);
                }
            }
   
            // read the open_count
            std::string mtx_name = object_key_ + "-mtx";
            scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__, object_identifier_);
            
            int file_open_cntr = --(shared_data->file_open_cntr);
            if (file_open_cntr == 0) {

                if (multipart_flag_ && (O_CREAT | O_WRONLY | O_TRUNC) == open_flags) {
                    if (0 > complete_multipart_upload()) {
                        remove_shared_memory();
                        return false;
                    }
                }

                remove_shared_memory();
            }

            // each process must initialize and deinitiatize.
            if (--s3_initialized_counter_ == 0) {
                S3_deinitialize();
            }

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] [file_open_cntr=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                        file_open_cntr);
            }

            return true;
        }

        std::streamsize receive(char_type* _buffer,
                                std::streamsize _buffer_size) override
        {
            // just get what is asked for
            s3_download_part_worker_routine(_buffer, _buffer_size);
            return _buffer_size;
        }

        std::streamsize send(const char_type* _buffer,
                             std::streamsize _buffer_size) override
        {

            // Put the buffer on the circular buffer.
            // We must copy the buffer because it will persist after send returns.
            char *copied_buffer = new char[_buffer_size];
            memcpy(copied_buffer, _buffer, _buffer_size);
            circular_buffer_.push_back({copied_buffer, static_cast<size_t>(_buffer_size)});

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] wrote buffer of size %ld\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, _buffer_size);
            }

            begin_part_upload_thread_ptr_ = new std::thread(&s3_transport::s3_upload_part_worker_routine, this);
            

            return _buffer_size;

        }

        pos_type seekpos(off_type _offset,
                         std::ios_base::seekdir _dir) override
        {
            if (!is_open()) {
                return seek_error;
            }

            if (use_cache_) {

                // we are using a cache file so just seek on it

                int return_val;
                switch (_dir) {
                    case std::ios_base::beg:
                        return lseek(cache_fd_, _offset, SEEK_SET);

                    case std::ios_base::cur:
                        return lseek(cache_fd_, _offset, SEEK_CUR);

                    case std::ios_base::end:
                        return lseek(cache_fd_, _offset, SEEK_END);

                    default:
                        return seek_error;
                }

            } else {

                switch (_dir) {
                    case std::ios_base::beg:
                        file_offset_ = _offset;
                        break;

                    case std::ios_base::cur:
                        file_offset_ = file_offset_ + _offset;
                        break;

                    case std::ios_base::end:
                        file_offset_ = object_size_ + _offset;
                        break;

                    default:
                        return seek_error;
                }
                return file_offset_;
            }

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

        void remove_shared_memory() {

            std::string shared_memory_name =  object_key_ + "-shm";
            std::string mtx_name = object_key_ + "-mtx";

            bi::shared_memory_object::remove(shared_memory_name.c_str());
            bi::named_mutex::remove(mtx_name.c_str());
        }

        int populate_open_mode_flags() noexcept
        {
            using std::ios_base;

            const auto m = mode_ & ~(ios_base::ate | ios_base::binary);

            if (ios_base::in == m) {
                download_to_cache_ = false;
                use_cache_ = false;
                object_must_exist_ = true;
                return O_RDONLY;
            }
            else if (ios_base::out == m || (ios_base::out | ios_base::trunc) == m) {
                download_to_cache_ = false;
                use_cache_ = false;
                object_must_exist_ = false;
                return O_CREAT | O_WRONLY | O_TRUNC;
            }
            else if (ios_base::app == m || (ios_base::out | ios_base::app) == m) {
                download_to_cache_ = true;
                use_cache_ = true;
                object_must_exist_ = false;
                return O_CREAT | O_WRONLY | O_APPEND;
            }
            else if ((ios_base::out | ios_base::in) == m) {
                download_to_cache_ = true;
                use_cache_ = true;
                object_must_exist_ = true;
                return O_RDWR;
            }
            else if ((ios_base::out | ios_base::in | ios_base::trunc) == m) {
                download_to_cache_ = false;
                use_cache_ = true;
                object_must_exist_ = false;
                return O_CREAT | O_RDWR | O_TRUNC;
            }
            else if ((ios_base::out | ios_base::in | ios_base::app) == m ||
                     (ios_base::in | ios_base::app) == m)
            {
                download_to_cache_ = true;
                use_cache_ = true;
                object_must_exist_ = false;
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
        bool open_impl(const filesystem::path& _p,
                       std::ios_base::openmode _mode,
                       Function _func)
        {

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] [_mode & in = %d][_mode & out = %d][_mode & trunc = %d][_mode & app = %d]\n",
                    __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                    (_mode & std::ios_base::in) == std::ios_base::in,
                    (_mode & std::ios_base::out) == std::ios_base::out,
                    (_mode & std::ios_base::trunc) == std::ios_base::trunc,
                    (_mode & std::ios_base::app) == std::ios_base::out);
            }

            object_key_ = _p.string();
            upload_manager_.object_key = object_key_;

            mode_ = _mode;

            bool object_exists = object_exists_in_s3();
            int open_flags = populate_open_mode_flags();
            if (open_flags == translation_error) {
                printf("%s:%d (%s) [[%d]] Invalid open mode detected.\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_);
                return false;
            }

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] [multipart_flag_ = %d][use_cache_ = %d]"
                    "[O_WRONLY = %d][O_RDONLY = %d]\n",
                    __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                    multipart_flag_,
                    use_cache_,
                    (open_flags & O_ACCMODE) == O_WRONLY,
                    (open_flags & O_ACCMODE) == O_RDONLY);
            }

// Don't have stat done yet so don't check this for now
//            if (object_must_exist_ && !object_exists) {
//                printf("%s:%d (%s) [[%d]] Object does not exist and open mode requires it to exist.\n",
//                        __FILE__, __LINE__, __FUNCTION__);
//                return false;
//            }


            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());

            multipart_shared_data_t *shared_data = segment.find_or_construct <multipart_shared_data_t>
                ("SharedData")(alloc_inst);

            // lock shared data for updates
            std::string mtx_name = object_key_ + "-mtx";
            scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__, object_identifier_);

            ++(shared_data->file_open_cntr);

            // each process must intitialize S3 
            if (s3_initialized_counter_ == 0) {

                int flags = S3_INIT_ALL;

                if (s3_signature_version_ == S3SignatureV4) {
                    flags |= S3_INIT_SIGNATURE_V4;
                }

                int status = S3_initialize( "s3", flags, bucket_context_.hostName );
                if (status != S3StatusOK) {
                    fprintf(stderr, "S3_initialize returned error\n");
                    return false;
                }

                ++s3_initialized_counter_;
            }
                
            std::string cache_file =  "/tmp" + object_key_ + "-cache";

            if (object_exists && download_to_cache_) {

                std::string cache_file =  "/tmp" + object_key_ + "-cache";

                if (!shared_data->cache_file_downloaded_flag) {

                    // go ahead and download the object to cache file
                    cache_fd_ = open(cache_file.c_str(), O_RDONLY);

                    size_t part_size = object_size_ / number_of_transfer_threads_;

                    unsigned long long usStart = usNow(); std::list<std::thread*> threads;
                    for (int thr_id=0; thr_id < number_of_transfer_threads_; thr_id++) {
 
                        // TODO not complete and not tested.  
                        // we still have a lock here so if any of the threads do a lock
                        // we will have a deadlock

                        size_t buffer_size;
                        if (thr_id == number_of_transfer_threads_ - 1) {
                            buffer_size = part_size + (object_size_ - part_size * number_of_transfer_threads_);
                        } else {
                            buffer_size = part_size;
                        }

                        std::thread *thisThread = new std::thread(&s3_transport::s3_download_part_worker_routine, 
                                this, nullptr, buffer_size);
                        threads.push_back(thisThread);

                    }

                    // Wait for threads to finish
                    while (!threads.empty()) {
                        std::thread *thisThread = threads.front();
                        thisThread->join();
                        delete thisThread;
                        threads.pop_front();
                    }
                    shared_data->cache_file_downloaded_flag = true;
                }
            }

            if ( open_flags == (O_CREAT | O_WRONLY | O_TRUNC) ) {


                // this is a full file upload - do not use cache

                if (multipart_flag_) {

                    // first one in initiates the multipart (everyone has same lock)
                    if (shared_data->last_error >= 0 && shared_data->file_open_cntr == 1) {

                        // send initiate message to S3
                        int ret = initiate_multipart_upload();

                        if (ret < 0) {
                            printf("%s:%d (%s) [[%d]] open returning false [last_error=%d]\n",
                                    __FILE__, __LINE__, __FUNCTION__, object_identifier_, ret);
                            shared_data->last_error = ret;
                            return false;
                        }
                    } else {
                        if (shared_data->last_error < 0) {
                            printf("%s:%d (%s) [[%d]] open returning false [last_error=%d]\n",
                                    __FILE__, __LINE__, __FUNCTION__, object_identifier_, shared_data->last_error);
                            return false;
                        }
                    }
                } else {
                    // TODO - non multipart upload
                }
            } else if (open_flags == O_RDONLY) {

                // this is read only - do not use cached
                // nothing to be done on open() in this case

            }

            const auto fd = file_descriptor_counter_++;

            if (fd < minimum_valid_file_descriptor) {
                return false;
            }

            fd_ = fd;

            if (!seek_to_end_if_required(_mode)) {
                close();
                return false;
            }

            return true;
        }

        int initiate_multipart_upload()
        {

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

            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;
            upload_manager_.xml = "";

            msg.str( std::string() ); // Clear

            int partContentLength = 0;

            callback_data_t data;
            memset(&data, 0, sizeof(data));
            data.contentLength = data.originalContentLength = object_size_;
            data.debug_flag = debug_flag_;
            data.object_identifier = object_identifier_;

            // read shared memory entry for this key (shmem already locked here)
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);

            /*try {
                shared_data->etags.resize(number_of_transfer_threads_, shm_char_string_t("", alloc_inst));
            } catch (std::bad_alloc& ba) {
                return SYS_MALLOC_ERR;   // not really malloc but close enough
            }*/

            retry_cnt = 0;
            // These expect a upload_manager_t* as cbdata
            S3MultipartInitialHandler mpuInitialHandler
                = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };

            do {
                upload_manager_.pCtx = &bucket_context_;
                print_bucket_context(bucket_context_);
                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] call S3_initiate_multipart [object_key=%s]\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_, object_key_.c_str());
                }
                S3_initiate_multipart(&bucket_context_, object_key_.c_str(),
                        &put_props_, &mpuInitialHandler, nullptr, &upload_manager_);

                if (upload_manager_.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
            } while ( (upload_manager_.status != S3StatusOK)
                    && S3_status_is_retryable(upload_manager_.status)
                    && ( ++retry_cnt < retry_count_limit_));

            if ("" == shared_data->upload_id || upload_manager_.status != S3StatusOK) {
                return S3_PUT_ERROR;
            }

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] S3_initiate_multipart returned.  Upload ID = %s\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, shared_data->upload_id.c_str());
            }
            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;

            return 0;

        } // end initiate_multipart_upload

        void mpuCancel()
        {
            // read upload_id from shmem
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            std::string upload_id = shared_data->upload_id.c_str();

            S3AbortMultipartUploadHandler abortHandler
                = { { mpuCancelRespPropCB, mpuCancelRespCompCB } };

            std::stringstream msg;
            S3Status status;

            if (debug_flag_) {
                msg << "Cancelling multipart upload: key=\""
                    << object_key_ << "\", upload_id=\"" << upload_id << "\"";
                printf( "%s\n", msg.str().c_str() );
            }
            g_mpuCancelRespCompCB_status = S3StatusOK;
            g_mpuCancelRespCompCB_pCtx = &bucket_context_;
            S3_abort_multipart_upload(&bucket_context_, object_key_.c_str(),
                    upload_id.c_str(), &abortHandler);
            status = g_mpuCancelRespCompCB_status;
            if (status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << "] " << __FUNCTION__
                    << " - Error cancelling the multipart upload of S3 object: \""
                    << object_key_ << "\"";
                if (status >= 0) {
                    msg << " - \"" << S3_get_status_name(status) << "\"";
                }
                printf( "%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                        msg.str().c_str() );
            }
        } // end mpuCancel


        int complete_multipart_upload()
        {

            int result;
            std::stringstream msg;
            unsigned int retry_cnt = 0;

            std::stringstream xml("");

            // read upload_id from shmem
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            std::string upload_id = shared_data->upload_id.c_str();

            if (0 == shared_data->last_error) { // If someone aborted, don't complete...
                if (debug_flag_) {
                    msg.str( std::string() ); // Clear
                    msg << "Multipart:  Completing key \"" << object_key_.c_str() << "\" Upload ID \""
                        << upload_id << "\"";
                    printf( "%s\n", msg.str().c_str() );
                }

                int i;
                xml << "<CompleteMultipartUpload>\n";
                char buf[256];
                int n;
                for ( i = 0; i < shared_data->etags.size(); i++ ) {
                    xml << "<Part><PartNumber>";
                    xml << (i + 1);
                    xml << "</PartNumber><ETag>";
                    xml << shared_data->etags[i];
                    xml << "</ETag></Part>";
                }
                xml << "</CompleteMultipartUpload>\n";

                int manager_remaining = xml.str().size();
                upload_manager_.offset = 0;
                retry_cnt = 0;
                S3MultipartCommitHandler commit_handler
                    = { {mpuCommitRespPropCB, mpuCommitRespCompCB }, mpuCommitXmlCB, nullptr };
                do {
                    // On partial error, need to restart XML send from the beginning
                    upload_manager_.remaining = manager_remaining;
                    upload_manager_.xml = xml.str().c_str();

                    upload_manager_.offset = 0;
                    upload_manager_.pCtx = &bucket_context_;
                    S3_complete_multipart_upload(&bucket_context_, object_key_.c_str(),
                            &commit_handler, upload_id.c_str(),
                            upload_manager_.remaining, nullptr, &upload_manager_);
                    if (debug_flag_) {
                        printf("%s:%d (%s) [[%d]] [manager.status=%s]\n", __FILE__, __LINE__,
                                __FUNCTION__, object_identifier_, S3_get_status_name(upload_manager_.status));
                    }
                    if (upload_manager_.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
                } while ((upload_manager_.status != S3StatusOK) &&
                        S3_status_is_retryable(upload_manager_.status) &&
                        ( ++retry_cnt < retry_count_limit_));

                if (upload_manager_.status != S3StatusOK) {
                    msg.str( std::string() ); // Clear
                    msg << __FUNCTION__ << " - Error putting the S3 object: \""
                        << object_key_ << "\"";
                    if(upload_manager_.status >= 0) {
                        msg << " - \"" << S3_get_status_name( upload_manager_.status ) << "\"";
                    }
                    return S3_PUT_ERROR;
                }
            }
            if (0 > shared_data->last_error && "" != shared_data->upload_id ) {

                // Someone aborted after we started, delete the partial object on S3
                printf("Cancelling multipart upload\n");
                mpuCancel();

                // Return the error
                result = shared_data->last_error;
            }

            return result;
        } // end complete_multipart_upload

        // this function is called in the background in a separate thread
        void s3_download_part_worker_routine(char *buffer, size_t length)
        {

            // read upload_id from shmem
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            std::string download_id = shared_data->download_id.c_str();

            std::stringstream msg;

            size_t retry_cnt = 0;
            multirange_data_t rangeData;
            do {

                S3GetObjectHandler getObjectHandler = { {mrdRangeRespPropCB, mrdRangeRespCompCB }, mrdRangeGetDataCB};

                // we are writing to cache file

                rangeData.pCtx = &bucket_context_;

                // if writing to cache file, the output_buffer will be nullptr
                // if writing directly to output_buffer, the fd will not be used
                rangeData.get_object_data.contentLength = length;
                rangeData.get_object_data.offset = 0;
                rangeData.get_object_data.fd = cache_fd_;
                rangeData.get_object_data.output_buffer = buffer;
                rangeData.get_object_data.output_buffer_size = length;
                rangeData.debug_flag = debug_flag_;


                if (debug_flag_) {
                    msg.str( std::string() ); // Clear
                    msg << "Multirange:  Start range key \"" << object_key_ << "\", offset "
                        << (long)rangeData.get_object_data.offset << ", len " 
                        << (int)rangeData.get_object_data.contentLength;
                    printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_, 
                            msg.str().c_str());
                }

                unsigned long long usStart = usNow();
                print_bucket_context(bucket_context_);
                S3_get_object( &bucket_context_, object_key_.c_str(), NULL, 
                        file_offset_,
                        //rangeData.get_object_data.offset,
                        rangeData.get_object_data.contentLength, 0, 
                        &getObjectHandler, &rangeData );

                if (debug_flag_) {
                    unsigned long long usEnd = usNow();
                    double bw = (rangeData.get_object_data.contentLength / (1024.0*1024.0)) /
                        ( (usEnd - usStart) / 1000000.0 );
                    msg << " -- END -- BW=" << bw << " MB/s";
                    printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_, msg.str().c_str());
                }

                if (rangeData.status != S3StatusOK) s3_sleep( retry_wait_, 0 );

            } while ((rangeData.status != S3StatusOK) && S3_status_is_retryable(rangeData.status) 
                    && (++retry_cnt < retry_count_limit_));

            if (rangeData.status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << " - Error getting the S3 object: \"" << object_key_ << " ";
                if (rangeData.status >= 0) {
                    msg << " - \"" << S3_get_status_name( rangeData.status ) << "\"";
                }
                printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_, msg.str().c_str());
            }

            if (rangeData.status != S3StatusOK) {
                shared_data->last_error = S3_GET_ERROR;
            }

        } // end s3_download_part_worker_routine

        // this function is called in the background in a separate thread
        void s3_upload_part_worker_routine() {

            // read upload_id from shmem
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            std::string upload_id = shared_data->upload_id.c_str();

            std::stringstream msg;
            S3PutObjectHandler putObjectHandler
                = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };

            multipart_data_t partData{};
            upload_page_t page;

            // read the first page
            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_);
            }
            circular_buffer_.pop_front(page);
            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%zu][terminate_flag=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, page.buffer, page.buffer_size,
                        page.terminate_flag);
            }

            size_t retry_cnt = 0;

            // determine the sequence number from the offset, file size, and buffer size
            // the last page might be larger so doing a little trick to handle that case (second term)
            int sequence = (file_offset_ / page.buffer_size) + (file_offset_ % page.buffer_size == 0 ? 0 : 1) + 1;

            // estimate the size and resize the etags vector
            int number_of_parts = object_size_ / page.buffer_size;
            number_of_parts = number_of_parts < sequence ? sequence : number_of_parts;
      
            // resize the etags vector if necessary 
            {  
                std::string mtx_name = object_key_ + "-mtx";
                scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__, object_identifier_);
                if (number_of_parts > shared_data->etags.size()) {
                    try {
                        shared_data->etags.resize(number_of_parts, shm_char_string_t("", alloc_inst));
                    } catch (std::bad_alloc& ba) {
                        shared_data->last_error = SYS_MALLOC_ERR;
                    }
                }
            }

            do {

                // Work on a local copy of the structure in case an error occurs in the middle
                // of an upload.  If we updated in-place, on a retry the part would start
                // at the wrong offset and length.
                partData = mpu_data_;
                partData.debug_flag = debug_flag_;
                partData.seq = sequence;
                partData.put_object_data.pCtx = &bucket_context_;
                partData.put_object_data.original_bytes_ptr
                    = partData.put_object_data.bytes = page.buffer;
                partData.put_object_data.circular_buffer_ptr = &(s3_transport::circular_buffer_);
                partData.put_object_data.buffer_size = partData.put_object_data.contentLength = page.buffer_size;
                partData.put_object_data.object_identifier = object_identifier_;

                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  Start part " << (int)sequence << ", key \""
                        << object_key_ << "\", uploadid \"" << upload_id
                        << "\", len " << (int)partData.put_object_data.contentLength;
                    printf( "%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str() );
                }

                put_props_.md5 = nullptr;
                //S3PutProperties putProps;
                //if ( partData.enable_md5 ) {
                //    // jjames - not sure how to do MD5 piecewise
                //    putProps->md5 = s3CalcMD5( partData.put_object_data.fd,
                //    partData.put_object_data.offset,
                //    partData.put_object_data.contentLength, resource_name );
                //}
                put_props_.expires = -1;
                unsigned long long usStart = usNow();

                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] S3_upload_part (ctx, key, props, handler, %d, "
                           "uploadId, %lld, 0, partData)\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                           sequence, partData.put_object_data.contentLength);
                }

                S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props_,
                        &putObjectHandler, sequence, upload_id.c_str(),
                        page.buffer_size, 0, &partData);

                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] S3_upload_part returned [part=%d].\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_, sequence);
                }

                unsigned long long usEnd = usNow();
                //double bw = (mpu_data_[seq-1].put_object_data.contentLength /
                //   (1024.0 * 1024.0)) /
                //   ( (usEnd - usStart) / 1000000.0 );
                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                    printf( "%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str() );
                }
                if (partData.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
            } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) &&
                    (++retry_cnt < retry_count_limit_));

            if (partData.status != S3StatusOK) {
                shared_data->last_error = S3_PUT_ERROR;
            }
        }

        int                       fd_;
        nlohmann::json            fd_info_;
        size_t                    object_size_;
        int                       number_of_transfer_threads_;
        size_t                    retry_count_limit_;
        size_t                    retry_wait_;
        std::string               hostname_;
        std::string               bucket_name_;
        std::string               access_key_;
        std::string               secret_access_key_;
        std::string               object_key_;
        S3BucketContext           bucket_context_;
        upload_manager_t          upload_manager_;
        S3SignatureVersion        s3_signature_version_;

        bool                      debug_flag_;
        multipart_data_t          mpu_data_;
        bool                      call_s3_upload_part_flag_;
        bool                      call_s3_download_part_flag_;
        S3PutProperties           put_props_;

        irods::experimental::circular_buffer<upload_page_t>
                                  circular_buffer_;
        std::thread               *begin_part_upload_thread_ptr_;

        std::ios_base::openmode   mode_;
        size_t                    file_offset_;
        bool                      multipart_flag_;
        int                       cache_fd_;

        // operational modes based on input flags
        bool                      download_to_cache_;
        bool                      use_cache_;
        bool                      object_must_exist_;

        // just for debugging purposes
        int                       object_identifier_;

        inline static int file_descriptor_counter_ = minimum_valid_file_descriptor;

        // this counter keeps track of whether this process has initialized
        // S3.  If we are in a multithreaded / single process environment the
        // initialization will run only once.  If we are in a multiprocess
        // environment it will run multiple times.
        inline static int         s3_initialized_counter_ = 0;

    }; // s3_transport

    using default_transport = s3_transport<char>;
} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

