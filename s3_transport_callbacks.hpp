#ifndef S3_TRANSPORT_CALLBACKS_HPP
#define S3_TRANSPORT_CALLBACKS_HPP

#include "circular_buffer.hpp"


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
#include "managed_shared_memory_object.hpp"
#include "s3_multipart_shared_data.hpp"
#include "s3_transport_types.hpp"
//#include "s3_transport_util.hpp"

namespace irods::experimental::io::s3_transport
{


    template <typename buffer_type>
    class callback_for_read_from_s3_base
    {

        public:

            using char_type   = typename buffer_type::value_type;

            callback_for_read_from_s3_base(libs3_types::bucket_context& _saved_bucket_context)
                : saved_bucket_context{_saved_bucket_context}
                , sequence{0}
                , offset{0}
                , content_length{0}
            {}

            virtual libs3_types::status callback_implementation(int libs3_buffer_size,
                                                                const char_type *libs3_buffer) = 0;

            static libs3_types::status invoke_callback(int libs3_buffer_size,
                                                       const char_type *libs3_buffer,
                                                       void *callback_data)
            {
                callback_for_read_from_s3_base *data =
                    (callback_for_read_from_s3_base*)callback_data;
                return data->callback_implementation(libs3_buffer_size, libs3_buffer);
            }

            static libs3_types::status on_response_properties(const libs3_types::response_properties *properties,
                                                              void *callback_data)
            {
                // Don't need to do anything here
                return S3StatusOK;
            }

            static void on_response_completion (libs3_types::status status,
                                                      const libs3_types::error_details *error,
                                                      void *callback_data)
            {
                callback_for_read_from_s3_base *data = (callback_for_read_from_s3_base*)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context, data->status,
                        data->debug_flag );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            }

            virtual ~callback_for_read_from_s3_base() {};

            int                  sequence;

            long                 offset;       /* For multiple upload */
            uint64_t             content_length;
            libs3_types::status  status;
            libs3_types::bucket_context&     saved_bucket_context; /* To enable more detailed error messages */
            bool                 debug_flag;
    };

    template <typename buffer_type>
    class callback_for_read_from_s3_to_cache : public callback_for_read_from_s3_base<buffer_type>
    {

        public:

            using char_type   = typename buffer_type::value_type;

            callback_for_read_from_s3_to_cache(libs3_types::bucket_context& _saved_bucket_context)
                : callback_for_read_from_s3_base<buffer_type>{_saved_bucket_context}
            {}

            libs3_types::status callback_implementation(int libs3_buffer_size,
                                                        const char_type *libs3_buffer)
            {

                // writing output to cache file

                auto wrote = pwrite(cache_fd, libs3_buffer, libs3_buffer_size, this->offset);
                if (wrote>0) this->offset += wrote;

                return ((wrote < static_cast<decltype(wrote)>(libs3_buffer_size)) ?
                        S3StatusAbortedByCallback : S3StatusOK);

            }

            ~callback_for_read_from_s3_to_cache() {};

            void set_cache_fd(int fd)
            {
                cache_fd = fd;
            }

        private:

            int cache_fd;

    };

    template <typename buffer_type>
    class callback_for_read_from_s3_to_buffer : public callback_for_read_from_s3_base<buffer_type>
    {

        public:

            using char_type   = typename buffer_type::value_type;

            callback_for_read_from_s3_to_buffer(libs3_types::bucket_context& _saved_bucket_context)
                : callback_for_read_from_s3_base<buffer_type>{_saved_bucket_context}
            {}

            libs3_types::status callback_implementation(int libs3_buffer_size,
                                                        const char_type *libs3_buffer)
            {
                // writing to buffer

                // TODO type?
                auto bytes_to_write = this->offset + libs3_buffer_size > output_buffer_size
                    ? output_buffer_size - this->offset
                    : libs3_buffer_size;

                memcpy(output_buffer + this->offset, libs3_buffer, bytes_to_write);

                this->offset += bytes_to_write;

                return ((bytes_to_write < static_cast<ssize_t>(libs3_buffer_size)) ?
                        S3StatusAbortedByCallback : S3StatusOK);

            }

            ~callback_for_read_from_s3_to_buffer() {};

            void set_output_buffer_size(uint64_t size)
            {
                output_buffer_size = size;
            }

            void set_output_buffer(char_type *buffer)
            {
                output_buffer = buffer;
            }

        private:

            char_type       *output_buffer;
            uint64_t        output_buffer_size;

    };

    namespace s3_multipart_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data );

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data);

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data);
        } // end namespace initialization_callback


        /* Uploading the multipart completion XML from our buffer */
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data);

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data);

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data);

        } // end namespace commit_callback


        namespace part_transport_callback
        {
            template <typename buffer_type>
            int on_put_data (int libs3_buffer_size,
                          libs3_types::buffer_type libs3_buffer,
                          void *callback_data)
            {
                data_for_write_callback<buffer_type>& data =
                    (static_cast<multipart_data<buffer_type>*>(callback_data))->put_object_data;

                // keep reading a buffer_size bytes from buffer.
                // if buffer is empty, get another from the circular_buffer
                irods::experimental::circular_buffer<upload_page<buffer_type>>& circular_buffer =
                    data.circular_buffer;

                // if we've already written the expected number of bytes, just return 0 which will
                // trigger the completion
                if (data.bytes_written >= data.content_length) {
                    return 0;
                }

                // if we've exhausted our current buffer, read the next buffer from the circular_buffer
                while (data.offset >= data.buffer.size()) {

                    upload_page<buffer_type> page;

                    // read the first page
                    if (data.debug_flag) {
                        printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__,
                                data.object_identifier);
                    }
                    circular_buffer.pop_front(page);
                    if (data.debug_flag) {
                        printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%lu][terminate_flag=%d]\n",
                                __FILE__, __LINE__, __FUNCTION__, data.object_identifier, page.buffer.data(),
                                page.buffer.size(), page.terminate_flag);
                    }
                    data.buffer = page.buffer;
                    data.offset = 0;
                }

                auto remaining_transport_buffer_size = data.buffer.size() - data.offset;

                bool libs3_buffer_larger_than_remaining_transport_buffer = libs3_buffer_size
                    > remaining_transport_buffer_size;

                auto length = libs3_buffer_larger_than_remaining_transport_buffer
                    ? remaining_transport_buffer_size
                    : libs3_buffer_size;

                memcpy(libs3_buffer, data.buffer.data() + data.offset, length);

                data.offset += length;
                data.bytes_written += length;

                return length;
            } // end on_put_data


            template <typename buffer_type>
            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                // update etag for this object

                namespace bi = boost::interprocess;
                namespace types = shared_data::interprocess_types;

                using named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::named_shared_memory_object
                    <shared_data::multipart_shared_data>;

                multipart_data<buffer_type> *data = (multipart_data<buffer_type> *)callback_data;

                const auto& object_key = static_cast<upload_manager&>(data->manager).object_key;

                auto shared_memory_name =  object_key + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

                named_shared_memory_object shm_obj{shared_memory_name,
                    constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                    constants::MAX_S3_SHMEM_SIZE};

                auto sequence = data->sequence;

                return shm_obj.atomic_exec([properties, sequence, &shm_obj](auto& data) {

                    const char *etag = properties->eTag;

                    // Update the etags vector.  It should be sized large enough
                    // to not require a resize but resize if necessary.
                    if (sequence > data.etags.size()) {
                        try {
                            data.etags.resize(sequence, types::shm_char_string("", shm_obj.get_allocator()));
                        } catch (std::bad_alloc& ba) {
                            return S3StatusOutOfMemory;
                        }
                    }

                    if (etag) {
                        data.etags[sequence - 1] = etag;
                    } else {
                        data.etags[sequence - 1] = "";
                    }

                    return S3StatusOK;
                });

            } // end response_properties

            template <typename buffer_type>
            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                multipart_data<buffer_type> *data = (multipart_data<buffer_type> *)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->put_object_data.saved_bucket_context,
                        data->status, data->debug_flag);

                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace part_transport_callback


        namespace cancel_callback
        {
            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data);

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            extern libs3_types::status g_response_completion_status;
            extern libs3_types::bucket_context *g_response_completion_saved_bucket_context;

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data);
        } // end namespace cancel_callback

    } // end namespace s3_multipart_upload

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_CALLBACKS_HPP

