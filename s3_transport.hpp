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
                    const char *_function)
            : mutex_name{_mutex_name}
            , file{_file}
            , line{_line} 
            , function{_function}
            , named_mtx{nullptr}
            , lock{nullptr}
        {
            if (file != nullptr && function != nullptr) { 
                printf("%s:%d (%s) ---LOCK--- waiting for lock\n", file, line, function);
            }

            named_mtx = new bi::named_mutex(bi::open_or_create, mutex_name.c_str());
            lock = new bi::scoped_lock<bi::named_mutex>(*named_mtx);

            if (file != nullptr && function != nullptr) { 
                printf("%s:%d (%s) ---LOCK--- acquired lock\n", file, line, function);
            }
        }

        ~scoped_lock_test() 
        {
            if (file != nullptr && function != nullptr) { 
                printf("%s:%d (%s) ---LOCK--- releasing lock\n", file, line, function);
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
        {} 
    
        int                      file_open_cntr;
        shm_char_string_t        upload_id;
        shm_char_string_t        download_id;
        shm_string_vector_t      etags;
        int                      last_error;
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
    } callback_data_t;

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
        callback_data    put_object_data;    /* File being uploaded */
        upload_manager_t *manager;           /* To update w/the MD5 returned */
        S3Status         status;
        bool             enable_md5;
        bool             server_encrypt;
        bool             debug_flag;
    } multipart_data_t;

    static void StoreAndLogStatus (S3Status status, 
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

    static S3Status mpuInitXmlCB (const char* upload_id, 
                                  void *callbackData )
    {
        upload_manager_t *manager = (upload_manager_t *)callbackData;

        // upload upload_id in shared memory
        std::string& object_key = manager->object_key;

        std::string shared_memory_name =  object_key + "-shm";
        bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

        void_allocator alloc_inst(segment.get_segment_manager());
        shm_char_string_t key_str(object_key.c_str(), alloc_inst);

        // no need to lock as this should already be locked

        multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
        shared_data->upload_id = upload_id;

        return S3StatusOK;
    } // end mpuInitXmlCB

    static S3Status mpuInitRespPropCB (const S3ResponseProperties *properties, 
                                       void *callbackData)
    {
        return S3StatusOK;
    } // end mpuInitRespPropCB

    static void mpuInitRespCompCB (S3Status status, 
                                   const S3ErrorDetails *error, 
                                   void *callbackData)
    {
        upload_manager_t *data = (upload_manager_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, 
                &(data->status), data->debug_flag );
    } // end mpuInitRespCompCB


    /* Uploading the multipart completion XML from our buffer */
    static int mpuCommitXmlCB (int bufferSize, 
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

    static S3Status mpuCommitRespPropCB (const S3ResponseProperties *properties, 
                                         void *callbackData)
    {
        return S3StatusOK;
    } // end mpuCommitRespPropCB

    static void mpuCommitRespCompCB (S3Status status, 
                                     const S3ErrorDetails *error, 
                                     void *callbackData)
    {
        upload_manager_t *data = (upload_manager_t*)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, 
                &(data->status), data->debug_flag );
        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuCommitRespCompCB

    /*static S3Status getObjectDataCallback(int bufferSize, 
                                          const char *buffer, 
                                          void *callbackData)
    {
        callback_data_t *cb = (callback_data_t *)callbackData;
        irods::plugin_property_map *prop_map_ptr = cb ? cb->prop_map_ptr : nullptr;
        std::string resource_name = prop_map_ptr != nullptr ? get_resource_name(*prop_map_ptr) : "";
    
        irods::error result = ASSERT_ERROR(bufferSize != 0 && buffer != NULL && callbackData != NULL,
                                           SYS_INVALID_INPUT_PARAM, "[resource_name=%s] Invalid input parameter.", resource_name.c_str() );
        if(!result.ok()) {
            irods::log(result);
        }
    
        ssize_t wrote = pwrite(cb->fd, buffer, bufferSize, cb->offset);
        if (wrote>0) cb->offset += wrote;
    
    #ifdef ERROR_INJECT
        g_error_mutex.lock();
        g_werr++;
        if (g_werr == g_werr_idx) {
            rodsLog(LOG_ERROR, "[resource_name=%s] Injecting a PWRITE error during S3 callback", resource_name.c_str() );
            g_error_mutex.unlock();
            return S3StatusAbortedByCallback;
        }
        g_error_mutex.unlock();
    #endif
    
        return ((wrote < (ssize_t) bufferSize) ?
                S3StatusAbortedByCallback : S3StatusOK);
    }*/


    static int putObjectDataCallback(int libs3_buffer_size, 
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
    static int mpuPartPutDataCB (int bufferSize,
                                 char *buffer,
                                 void *callbackData)
    {
        return putObjectDataCallback( bufferSize, buffer,
                                      &((multipart_data_t*)callbackData)->put_object_data );
    } // end mpuPartPutDataCB

    static S3Status mpuPartRespPropCB (const S3ResponseProperties *properties, 
                                       void *callbackData)
    {
        // update etag for this object
        
        multipart_data_t *data = (multipart_data_t *)callbackData;

        std::string& object_key = data->manager->object_key;

        std::string shared_memory_name =  object_key + "-shm";
        bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);
        void_allocator alloc_inst(segment.get_segment_manager());
        shm_char_string_t key_str(object_key.c_str(), alloc_inst);

        // lock for update
        std::string mtx_name = object_key + "-mtx";
        //bi::named_mutex named_mtx{bi::open_or_create, mtx_name.c_str()};
        //bi::scoped_lock<bi::named_mutex> lock{named_mtx};
        scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__);

        multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);

        int seq = data->seq;
        const char *etag = properties->eTag;

        // update etags vector in shmem
        if (etag) {
            shared_data->etags[seq - 1] = etag;
        } else {
            shared_data->etags[seq - 1] = "";
        }

        return S3StatusOK;
    } // end mpuPartRespPropCB

    static void mpuPartRespCompCB (S3Status status,
                                   const S3ErrorDetails *error,
                                   void *callbackData)
    {
        multipart_data_t *data = (multipart_data_t *)callbackData;
        StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, 
                &(data->status), data->debug_flag);

        // Don't change the global error, we may want to retry at a higher level.
        // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
    } // end mpuPartRespCompCB


    static S3Status mpuCancelRespPropCB (const S3ResponseProperties *properties, 
                                         void *callbackData)
    {
        return S3StatusOK;
    } // mpuCancelRespPropCB

    // S3_abort_multipart_upload() does not allow a callbackData parameter, so pass the
    // final operation status using this global.
    static S3Status g_mpuCancelRespCompCB_status = S3StatusOK;
    static S3BucketContext *g_mpuCancelRespCompCB_pCtx = nullptr;

    static void mpuCancelRespCompCB (S3Status status, 
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

    static bool s3GetServerEncrypt ()
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
                              bool _multipart_flag,
                              const std::string& _s3_signature_version_str,
                              const std::string& _s3_protocol_str = "http",
                              const std::string& _s3_sts_data_str = "amz",
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
            , multipart_flag_{_multipart_flag}
            , debug_flag_{_debug_flag}
            , fd_{uninitialized_file_descriptor}
            , fd_info_{}
            , call_s3_upload_part_flag_{true}
            , call_s3_download_part_flag_{true}
            , begin_part_upload_thread_ptr_{nullptr}
            , begin_part_download_thread_ptr_{nullptr}
            , circular_buffer_{1}
            , mode_(0)
        {


            memset(&mpu_data_, 0, sizeof(mpu_data_));
            mpu_data_.manager = &upload_manager_;

            mpu_data_.seq = sequence_;
            mpu_data_.enable_md5 = s3GetEnableMD5();
            mpu_data_.server_encrypt = s3GetServerEncrypt();

            memset(&put_props_, 0, sizeof(put_props_));

            upload_manager_.debug_flag = debug_flag_;

            // TODO calculate this
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
            if (begin_part_download_thread_ptr_) {
                begin_part_download_thread_ptr_ -> join();
                delete begin_part_download_thread_ptr_;
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
            if (!is_open()) {
                return false;
            }

            if (multipart_flag_ && (mode_ & std::ios_base::out)) {

                std::string shared_memory_name =  object_key_ + "-shm";
                bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

                void_allocator alloc_inst(segment.get_segment_manager());
                shm_char_string_t key_str(object_key_.c_str(), alloc_inst);
                multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                    ("SharedData")(alloc_inst);

                if (debug_flag_) {
                    printf("%s:%d (%s) [multipart_flag=true]\n", 
                            __FILE__, __LINE__, __FUNCTION__);
                }

                // upload was in background.  wait for it to complete.
                if (begin_part_upload_thread_ptr_) {
                    begin_part_upload_thread_ptr_->join();
                    begin_part_upload_thread_ptr_ = nullptr;
                }

                if (debug_flag_) {
                    printf("%s:%d (%s) join for part %d\n", 
                            __FILE__, __LINE__, __FUNCTION__, sequence_);
                }

                fd_ = uninitialized_file_descriptor;

                {
                    // lock for update
                    std::string mtx_name = object_key_ + "-mtx";
                    //bi::named_mutex named_mtx{bi::open_or_create, mtx_name.c_str()};
                    //bi::scoped_lock<bi::named_mutex> lock{named_mtx};
                    scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__);

                    // read number of opens for this key 
                    int open_count = --(shared_data->file_open_cntr);

                    if (debug_flag_) {
                        printf("%s:%d (%s) [file_open_cntr=%d]\n", 
                                __FILE__, __LINE__, __FUNCTION__,
                                open_count);
                    }

                    if (open_count == 0) {

                        int ret = complete_multipart_upload();

                        if (ret < 0) {
                            return false;
                        }


                        bool remove_flag= bi::shared_memory_object::remove(shared_memory_name.c_str());
                        if (!remove_flag) {
                            fprintf(stderr, "%s:%d (%s) Failed to remove shared memory %s\n", 
                                    __FILE__, __LINE__, __FUNCTION__, shared_memory_name.c_str());
                        }

                        remove_flag = bi::named_mutex::remove(mtx_name.c_str());
                        if (!remove_flag) {
                            fprintf(stderr, "%s:%d (%s) Failed to remove shared mutex %s\n", 
                                    __FILE__, __LINE__, __FUNCTION__, mtx_name.c_str());
                        }
                    }
                }
            } else if (multipart_flag_ && (mode_ & std::ios_base::in)) {

                /*if (debug_flag_) {
                    printf("%s:%d (%s) [multipart_flag=true]\n", 
                            __FILE__, __LINE__, __FUNCTION__);
                }

                // download was in background.  wait for it to complete.
                if (begin_part_download_thread_ptr_) {
                    begin_part_download_thread_ptr_->join();
                    begin_part_download_thread_ptr_ = nullptr;
                }

                if (debug_flag_) {
                    printf("%s:%d (%s) join for part %d\n", 
                            __FILE__, __LINE__, __FUNCTION__, sequence_);
                }

                fd_ = uninitialized_file_descriptor;

                {
                    // lock for update
                    std::string mtx_name = object_key_ + "-mtx";
                    //bi::named_mutex named_mtx{bi::open_or_create, mtx_name.c_str()};
                    //bi::scoped_lock<bi::named_mutex> lock{named_mtx};
                    scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__);

                    // read number of opens for this key 
                    int open_count = --(shared_data->file_open_cntr);

                    if (debug_flag_) {
                        printf("%s:%d (%s) [file_open_cntr=%d]\n", 
                                __FILE__, __LINE__, __FUNCTION__,
                                open_count);
                    }

                    if (open_count == 0) {

                        int ret = complete_multipart_download();

                        if (ret < 0) {
                            return false;
                        }


                        bool remove_flag= bi::shared_memory_object::remove(shared_memory_name.c_str());
                        if (!remove_flag) {
                            fprintf(stderr, "%s:%d (%s) Failed to remove shared memory %s\n", 
                                    __FILE__, __LINE__, __FUNCTION__, shared_memory_name.c_str());
                        }

                        remove_flag = bi::named_mutex::remove(mtx_name.c_str());
                        if (!remove_flag) {
                            fprintf(stderr, "%s:%d (%s) Failed to remove shared mutex %s\n", 
                                    __FILE__, __LINE__, __FUNCTION__, mtx_name.c_str());
                        }
                    }
                }*/
            }

            return true;
        }

        std::streamsize receive(char_type* _buffer, 
                                std::streamsize _buffer_size) override
        {
            // TODO right now this assumes a full file get before filling the buffer
            /*openedDataObjInp_t input{};

            input.l1descInx = fd_;
            input.len = _buffer_size;

            bytesBuf_t output{};

            output.len = input.len;
            output.buf = _buffer;

            // only call s3_upload_part once for this thread / part
            if (call_s3_download_part_flag_) {

                call_s3_download_part_flag_ = false;

                // this has to go in the background because we are going to have multiple calls receive() to fill up the data
                // and this doesn't return until all the data is sent
                begin_part_upload_thread_ptr_ = 
                    new std::thread(&s3_transport::s3_download_part_worker_routine, this);

            }

            // wait for download to fill cache file


            std::cout << "Received: " << output.buf << std::endl;
            return output.len;*/
            return 0;
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
                printf("%s:%d (%s) [part=%d] wrote buffer of size %ld\n", 
                        __FILE__, __LINE__, __FUNCTION__, sequence_, _buffer_size);
            }

            // only call s3_upload_part once for this thread / part
            if (call_s3_upload_part_flag_) {

                call_s3_upload_part_flag_ = false;

                // this has to go in the background because we are going to have multiple calls send() to fill up the data
                // and this doesn't return until all the data is sent
                begin_part_upload_thread_ptr_ = 
                    new std::thread(&s3_transport::s3_upload_part_worker_routine, this);

            }

            return _buffer_size;

        }

        pos_type seekpos(off_type _offset, 
                         std::ios_base::seekdir _dir) override
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
        bool open_impl(const filesystem::path& _p, 
                       std::ios_base::openmode _mode, 
                       Function _func)
        {
            object_key_ = _p.string();
            upload_manager_.object_key = object_key_;

            mode_ = _mode;

            if (multipart_flag_) {
                
                std::string shared_memory_name =  object_key_ + "-shm";
                bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

                void_allocator alloc_inst(segment.get_segment_manager());
                shm_char_string_t key_str(object_key_.c_str(), alloc_inst);

                multipart_shared_data_t *shared_data = segment.find_or_construct <multipart_shared_data_t> 
                    ("SharedData")(alloc_inst);

                // first one end calls initiate, all else waits
                std::string mtx_name = object_key_ + "-mtx";
                //bi::named_mutex named_mtx{bi::open_or_create, mtx_name.c_str()};
                //bi::scoped_lock<bi::named_mutex> lock{named_mtx};
                scoped_lock_test sl(mtx_name, __FILE__, __LINE__, __FUNCTION__);

                if (mode_ & std::ios_base::out)
                {
                    if (debug_flag_) {
                        printf("%s:%d (%s) [multipart_flag=true]\n", __FILE__, __LINE__, 
                                __FUNCTION__);
                    }

                    ++(shared_data->file_open_cntr);

                    if (debug_flag_) {
                        printf("%s:%d (%s) [file_open_cntr=%d]\n", __FILE__, __LINE__, __FUNCTION__,
                                shared_data->file_open_cntr);
                    }

                    // first one in initiates the multipart
                    if (shared_data->last_error >= 0 && shared_data->file_open_cntr == 1) {

                        // send initiate message to S3
                        int ret = initiate_multipart_upload();

                        if (ret < 0) {
                            shared_data->last_error = ret;
                            return ret;
                        }
                    } else {
                        if (shared_data->last_error < 0) {
                            return shared_data->last_error;
                        }
                    }
                } else if (mode_ & std::ios_base::in) {

                }
            }

            const auto flags = make_open_flags(_mode);

            if (flags == translation_error) {
                return false;
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

            if (s3_signature_version_ == S3SignatureV4) {
                flags |= S3_INIT_SIGNATURE_V4;
            }

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

            upload_manager_.remaining = 0;
            upload_manager_.offset  = 0;
            upload_manager_.xml = "";

            msg.str( std::string() ); // Clear

            int partContentLength = 0;

            callback_data_t data;
            memset(&data, 0, sizeof(data));
            data.contentLength = data.originalContentLength = object_size_;
            data.debug_flag = debug_flag_;

            // read shared memory entry for this key (shmem already locked here)
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            shm_char_string_t key_str(object_key_.c_str(), alloc_inst);
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            // Allocate all dynamic storage now, so we don't start a job we can't finish later
            //manager.etags = (char**)calloc(sizeof(char*) * totalParts, 1);
            try {
                shared_data->etags.resize(total_parts_, shm_char_string_t("", alloc_inst));
            } catch (std::bad_alloc& ba) {
                return SYS_MALLOC_ERR;   // not really malloc but close enough
            }

            retry_cnt = 0;
            // These expect a upload_manager_t* as cbdata
            S3MultipartInitialHandler mpuInitialHandler 
                = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };

            do {
                upload_manager_.pCtx = &bucket_context_;
                print_bucket_context(bucket_context_);
                if (debug_flag_) {
                    printf("%s:%d (%s) [part=%d] call S3_initiate_multipart [object_key=%s]\n",
                            __FILE__, __LINE__, __FUNCTION__, sequence_, object_key_.c_str());
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
                printf("%s:%d (%s) [part=%d] S3_initiate_multipart returned.  Upload ID = %s\n",
                        __FILE__, __LINE__, __FUNCTION__, sequence_, 
                        shared_data->upload_id.c_str());
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
            shm_char_string_t key_str(object_key_.c_str(), alloc_inst);
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
                printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, 
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
            shm_char_string_t key_str(object_key_.c_str(), alloc_inst);
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            std::string upload_id = shared_data->upload_id.c_str();

            if (0 == shared_data->last_error) { // If someone aborted, don't complete...
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
                        printf("%s:%d (%s) [manager.status=%s]\n", __FILE__, __LINE__, 
                                __FUNCTION__, S3_get_status_name(upload_manager_.status));
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
        /*void s3_download_part_worker_routine() 
        {
            // read upload_id from shmem
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            shm_char_string_t key_str(object_key_.c_str(), alloc_inst);
            multipart_shared_data_t *shared_data = segment.find_or_construct<multipart_shared_data_t>
                ("SharedData")(alloc_inst);
            std::string download_id = shared_data->download_id.c_str();


            S3BucketContext bucketContext = *((S3BucketContext*)bucketContextParam);
            irods::plugin_property_map _prop_map = *((irods::plugin_property_map*)pluginPropertyMapParam);

            std::string resource_name = get_resource_name(_prop_map);

            irods::error result;
            std::stringstream msg;
            S3GetObjectHandler getObjectHandler = { {mrdRangeRespPropCB, mrdRangeRespCompCB }, mrdRangeGetDataCB };

            size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
            _prop_map.get<size_t>(s3_retry_count_size_t, retry_count_limit);

            size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
            _prop_map.get<size_t>(s3_wait_time_sec_size_t, retry_wait);

            // Will break out when no work detected 
            while (1) {
                // Pointer is probably safe w/o mutex, but JIK...
                g_mrdLock.lock();
                bool ok = (g_mrdResult.ok());
                g_mrdLock.unlock();
                if (!ok) break;

                int seq;
                g_mrdLock.lock();
                if (g_mrdNext >= g_mrdLast) {
                    g_mrdLock.unlock();
                    break;
                }
                seq = g_mrdNext + 1;
                g_mrdNext++;
                g_mrdLock.unlock();

                size_t retry_cnt = 0;
                multirange_data_t rangeData;
                do {
                    // Work on a local copy of the structure in case an error occurs in the middle
                    // of an upload.  If we updated in-place, on a retry the part would start
                    // at the wrong offset and length.
                    rangeData = g_mrdData[seq-1];
                    rangeData.pCtx = &bucketContext;
                    rangeData.prop_map_ptr = &_prop_map;

                    msg.str( std::string() ); // Clear
                    msg << "Multirange:  Start range " << (int)seq << ", key \"" << g_mrdKey << "\", offset "
                        << (long)rangeData.get_object_data.offset << ", len " << (int)rangeData.get_object_data.contentLength;
                    rodsLog( LOG_DEBUG, msg.str().c_str() );

                    unsigned long long usStart = usNow();
                    std::string&& hostname = s3GetHostname(_prop_map);
                    bucketContext.hostName = hostname.c_str(); // Safe to do, this is a local copy of the data structure
                    S3_get_object( &bucketContext, g_mrdKey, NULL, rangeData.get_object_data.offset,
                                   rangeData.get_object_data.contentLength, 0, &getObjectHandler, &rangeData );
                    unsigned long long usEnd = usNow();
                    double bw = (g_mrdData[seq-1].get_object_data.contentLength / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
                    msg << " -- END -- BW=" << bw << " MB/s";
                    rodsLog( LOG_DEBUG, msg.str().c_str() );
                    if (rangeData.status != S3StatusOK) s3_sleep( retry_wait, 0 );
                } while ((rangeData.status != S3StatusOK) && S3_status_is_retryable(rangeData.status) && (++retry_cnt < retry_count_limit));
                if (rangeData.status != S3StatusOK) {
                    msg.str( std::string() ); // Clear
                    msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ 
                        << " - Error getting the S3 object: \"" << g_mrdKey << "\" range " << seq;
                    if (rangeData.status >= 0) {
                        msg << " - \"" << S3_get_status_name( rangeData.status ) << "\"";
                    }
                    result = ERROR( S3_GET_ERROR, msg.str() );
                    rodsLog( LOG_ERROR, msg.str().c_str() );
                    g_mrdLock.lock();
                    g_mrdResult = result;
                    g_mrdLock.unlock();
                }
            }

        } // end call_s3_download_part*/

        // this function is called in the background in a separate thread
        void s3_upload_part_worker_routine() {

            // read upload_id from shmem
            std::string shared_memory_name =  object_key_ + "-shm";
            bi::managed_shared_memory segment(bi::open_or_create, shared_memory_name.c_str(), 65536);

            void_allocator alloc_inst(segment.get_segment_manager());
            shm_char_string_t key_str(object_key_.c_str(), alloc_inst);
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
                printf("%s:%d (%s) waiting to read\n", __FILE__, __LINE__, __FUNCTION__);
            }
            circular_buffer_.pop_front(page);
            if (debug_flag_) {
                printf("%s:%d (%s) read page [buffer=%p][buffer_size=%zu][terminate_flag=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, page.buffer, page.buffer_size, 
                        page.terminate_flag);
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
                partData.put_object_data.original_bytes_ptr 
                    = partData.put_object_data.bytes = page.buffer;
                partData.put_object_data.circular_buffer_ptr = &(s3_transport::circular_buffer_);
                partData.put_object_data.buffer_size = page.buffer_size;

                partData.put_object_data.contentLength = part_size_;

                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  Start part " << (int)sequence_ << ", key \"" 
                        << object_key_ << "\", uploadid \"" << upload_id 
                        << "\", len " << (int)partData.put_object_data.contentLength;
                    printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, 
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
                    printf("%s:%d (%s) S3_upload_part (ctx, key, props, handler, %d, "
                           "uploadId, %lld, 0, partData)\n", __FILE__, __LINE__, __FUNCTION__, 
                           sequence_, partData.put_object_data.contentLength);
                }

                S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props_, 
                        &putObjectHandler, sequence_, upload_id.c_str(), 
                        part_size_, 0, &partData);

                if (debug_flag_) {
                    printf("%s:%d (%s) S3_upload_part returned [part=%d].\n", 
                            __FILE__, __LINE__, __FUNCTION__, sequence_);
                }

                unsigned long long usEnd = usNow();
                //double bw = (mpu_data_[seq-1].put_object_data.contentLength /
                //   (1024.0 * 1024.0)) /
                //   ( (usEnd - usStart) / 1000000.0 );
                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                    printf( "%s:%d (%s) %s\n", __FILE__, __LINE__, __FUNCTION__, 
                            msg.str().c_str() );
                }
                if (partData.status != S3StatusOK) s3_sleep( retry_wait_, 0 );
            } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) &&
                    (++retry_cnt < retry_count_limit_));

            if (partData.status != S3StatusOK) {
                shared_data->last_error = S3_PUT_ERROR;
            }
        }

        int                       sequence_;
        size_t                    part_size_;
        int                       fd_;
        nlohmann::json            fd_info_;
        int                       total_parts_;
        size_t                    object_size_;
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
        std::thread               *begin_part_download_thread_ptr_;

        std::ios_base::openmode   mode_;
        bool                      multipart_flag_;
    
        // TODO do i need a file descriptor?
        inline static int file_descriptor_counter_ = minimum_valid_file_descriptor;

    }; // s3_transport

    using default_transport = s3_transport<char>;
} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

