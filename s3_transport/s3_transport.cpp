#include "circular_buffer.hpp"

// iRODS includes
#include <rcMisc.h>
#include <transport/transport.hpp>
#include <fileLseek.h>
#include <rs_get_file_descriptor_info.hpp>

// misc includes
#include "json.hpp"
#include <libs3.h>

// stdlib and misc includes
#include <string>
#include <thread>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>
#include <time.h>

// boost includes
#include <boost/algorithm/string/predicate.hpp>
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
#include <boost/filesystem.hpp>

// local includes
#include "hashed_managed_shared_memory_object.hpp"
#include "s3_multipart_shared_data.hpp"
#include "s3_transport.hpp"


namespace irods::experimental::io::s3_transport
{

    void print_bucket_context(const libs3_types::bucket_context& bucket_context)
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

    void store_and_log_status( libs3_types::status status,
                               const libs3_types::error_details *error,
                               const std::string& function,
                               const libs3_types::bucket_context& saved_bucket_context,
                               libs3_types::status& pStatus,
                               bool debug_flag = false )
    {

        pStatus = status;
        if( debug_flag || status != S3StatusOK ) {
            printf( "  libs3_types::status: [%s] - %d\n", S3_get_status_name( status ), static_cast<int>(status) );
            printf( "    S3Host: %s\n", saved_bucket_context.hostName );
        }

        if (debug_flag || status != S3StatusOK)
            printf( "  Function: %s\n", function.c_str() );
        if ((debug_flag || error) && error->message)
            printf( "  Message: %s\n", error->message);
        if ((debug_flag || error) && error->resource)
            printf( "  Resource: %s\n", error->resource);
        if ((debug_flag || error) && error->furtherDetails)
            printf( "  Further Details: %s\n", error->furtherDetails);
        if ((debug_flag || error) && error->extraDetailsCount) {
            printf( "%s", "  Extra Details:\n");

            for (int i = 0; i < error->extraDetailsCount; i++) {
                printf( "    %s: %s\n", error->extraDetails[i].name,
                        error->extraDetails[i].value);
            }
        }
    }  // end store_and_log_status

    // Returns timestamp in usec for delta-t comparisons
    // uint64_t provides plenty of headroom
    uint64_t get_time_in_microseconds()
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return (tv.tv_sec) * 1000000LL + tv.tv_usec;
    } // end get_time_in_microseconds

    // Sleep for *at least* the given time, plus some up to 1s additional
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(int _s,
                  int _ms )
    {
        // We're the only user of libc rand(), so if we mutex around calls we can
        // use the thread-unsafe rand() safely and randomly...if this is changed
        // in the future, need to use rand_r and init a static seed in this function
        static std::mutex rand_mutex;
        rand_mutex.lock();
        int random = rand();
        rand_mutex.unlock();
        // Add up to 1000 ms (1 sec)
        int addl = static_cast<int>((static_cast<double>(random) / static_cast<double>(RAND_MAX)) * 1000.0);
        useconds_t us = ( _s * 1000000 ) + ( (_ms + addl) * 1000 );
        usleep( us );
    } // end s3_sleep

    namespace s3_head_object_callback
    {
        libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                    void *callback_data)
        {
            return S3StatusOK;
        }

        void on_response_complete (libs3_types::status status,
                                   const libs3_types::error_details *error,
                                   void *callback_data)
        {
            data_for_head_callback *data = (data_for_head_callback*)callback_data;
            store_and_log_status( status, error, __FUNCTION__, data->bucket_context,
                    data->status, data->debug_flag );
        }


    }

    namespace s3_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data )
            {
                using hashed_named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::hashed_named_shared_memory_object
                    <shared_data::multipart_shared_data>;
                // upload upload_id in shared memory
                // no need to shared_memory_lock as this should already be locked

                // upload upload_id in shared memory
                upload_manager *manager = (upload_manager *)callback_data;

                // upload upload_id in shared memory
                // upload upload_id in shared memory
                std::string& object_key = manager->object_key;
                auto shared_memory_name =  object_key + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

                // upload upload_id in shared memory
                hashed_named_shared_memory_object shm_obj{shared_memory_name,
                    constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                    constants::MAX_S3_SHMEM_SIZE};

                // upload upload_id in shared memory - already locked here
                shm_obj.exec([upload_id](auto& data) {
                    data.upload_id = upload_id;
                });

                // upload upload_id in shared memory
                return S3StatusOK;
            } // end on_response

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                return S3StatusOK;
            } // end on_response_properties

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context,
                        data->status, data->debug_flag );
            } // end on_response_complete

        } // end namespace initialization_callback

        // Uploading the multipart completion XML from our buffer
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data)
            {
                upload_manager *manager = (upload_manager *)callback_data;
                long ret = 0;
                if (manager->remaining) {
                    int to_read_count = ((manager->remaining > buffer_size) ?
                                  buffer_size : manager->remaining);
                    memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                    ret = to_read_count;
                }
                manager->remaining -= ret;
                manager->offset += ret;

                return static_cast<int>(ret);
            } // end commit

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return S3StatusOK;
            } // end response_properties

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context,
                        data->status, data->debug_flag );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion


        } // end namespace commit_callback


        namespace cancel_callback
        {
            libs3_types::status g_response_completion_status = S3StatusOK;
            libs3_types::bucket_context *g_response_completion_saved_bucket_context = nullptr;

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return S3StatusOK;
            } // response_properties

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                store_and_log_status( status, error, __FUNCTION__, *g_response_completion_saved_bucket_context,
                        g_response_completion_status, false );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace cancel_callback



    } // end namespace s3_multipart_upload

    namespace s3_multipart_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data )
            {
                using hashed_named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::hashed_named_shared_memory_object
                    <shared_data::multipart_shared_data>;
                // upload upload_id in shared memory
                // no need to shared_memory_lock as this should already be locked

                // upload upload_id in shared memory
                upload_manager *manager = (upload_manager *)callback_data;

                // upload upload_id in shared memory
                // upload upload_id in shared memory
                std::string& object_key = manager->object_key;
                auto shared_memory_name =  object_key + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

                // upload upload_id in shared memory
                hashed_named_shared_memory_object shm_obj{shared_memory_name,
                    constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                    constants::MAX_S3_SHMEM_SIZE};

                // upload upload_id in shared memory - already locked here
                shm_obj.exec([upload_id](auto& data) {
                    data.upload_id = upload_id;
                });

                // upload upload_id in shared memory
                return S3StatusOK;
            } // end on_response

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                return S3StatusOK;
            } // end on_response_properties

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context,
                        data->status, data->debug_flag );
            } // end on_response_complete

        } // end namespace initialization_callback

        // Uploading the multipart completion XML from our buffer
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data)
            {
                upload_manager *manager = (upload_manager *)callback_data;
                long ret = 0;
                if (manager->remaining) {
                    int to_read_count = ((manager->remaining > buffer_size) ?
                                  buffer_size : manager->remaining);
                    memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                    ret = to_read_count;
                }
                manager->remaining -= ret;
                manager->offset += ret;

                return static_cast<int>(ret);
            } // end commit

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return S3StatusOK;
            } // end response_properties

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context,
                        data->status, data->debug_flag );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion


        } // end namespace commit_callback


        namespace cancel_callback
        {
            libs3_types::status g_response_completion_status = S3StatusOK;
            libs3_types::bucket_context *g_response_completion_saved_bucket_context = nullptr;

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return S3StatusOK;
            } // response_properties

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                store_and_log_status( status, error, __FUNCTION__, *g_response_completion_saved_bucket_context,
                        g_response_completion_status, false );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace cancel_callback



    } // end namespace s3_multipart_upload


}
