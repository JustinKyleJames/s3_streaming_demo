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
#include <fstream>

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

            callback_for_read_from_s3_base(libs3_types::bucket_context& _saved_bucket_context)
                : saved_bucket_context{_saved_bucket_context}
                , offset{0}
                , content_length{0}
            {}

            virtual libs3_types::status callback_implementation(int libs3_buffer_size,
                                                                const libs3_types::char_type *libs3_buffer) = 0;

            static libs3_types::status invoke_callback(int libs3_buffer_size,
                                                       const libs3_types::char_type *libs3_buffer,
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

            uint64_t                     offset;       /* For multiple upload */
            uint64_t                     content_length;
            libs3_types::status          status;
            libs3_types::bucket_context& saved_bucket_context; /* To enable more detailed error messages */
            bool                         debug_flag;
    };

    template <typename buffer_type>
    class callback_for_read_from_s3_to_cache : public callback_for_read_from_s3_base<buffer_type>
    {

        public:

            callback_for_read_from_s3_to_cache(libs3_types::bucket_context& _saved_bucket_context)
                : callback_for_read_from_s3_base<buffer_type>{_saved_bucket_context}
            {}

            libs3_types::status callback_implementation(int libs3_buffer_size,
                                                        const libs3_types::char_type *libs3_buffer)
            {

                if (!cache_fstream.is_open()) {
                    cache_fstream.open(filename.c_str(), std::ios_base::out);
                }

                // writing output to cache file
                cache_fstream.seekp(this->offset);
                cache_fstream.write(libs3_buffer, libs3_buffer_size);

                auto wrote = static_cast<uint64_t>(cache_fstream.tellp()) - this->offset;
                if (wrote>0) this->offset += wrote;

                return ((wrote < static_cast<decltype(wrote)>(libs3_buffer_size)) ?
                        S3StatusAbortedByCallback : S3StatusOK);

            }

            ~callback_for_read_from_s3_to_cache() {};

            void set_and_open_cache_file(std::string& f)
            {
                filename = f;
                cache_fstream.open(filename.c_str(), std::ios_base::out);
            }

        private:

            std::string   filename;
            std::ofstream cache_fstream;

    };

    template <typename buffer_type>
    class callback_for_read_from_s3_to_buffer : public callback_for_read_from_s3_base<buffer_type>
    {

        public:

            using output_char_type   = typename buffer_type::value_type;

            callback_for_read_from_s3_to_buffer(libs3_types::bucket_context& _saved_bucket_context)
                : callback_for_read_from_s3_base<buffer_type>{_saved_bucket_context}
            {}

            libs3_types::status callback_implementation(int libs3_buffer_size,
                                                        const libs3_types::char_type *libs3_buffer)
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

            void set_output_buffer(output_char_type *buffer)
            {
                output_buffer = buffer;
            }

        private:

            output_char_type *output_buffer;
            uint64_t         output_buffer_size;

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


        template <typename buffer_type>
        class callback_for_write_to_s3_base
        {

            public:

                callback_for_write_to_s3_base(libs3_types::bucket_context& _saved_bucket_context,
                                              upload_manager& _manager)
                    : saved_bucket_context{_saved_bucket_context}
                    , manager{_manager}
                    , sequence{0}
                    , offset{0}
                    , content_length{0}
                    , enable_md5{false}
                    , server_encrypt{false}
                    , object_identifier{0}
                    , debug_flag{false}
                    , object_key{}
                {}


                virtual int callback_implementation(int libs3_buffer_size,
                                                    libs3_types::char_type *libs3_buffer) = 0;

                static int invoke_callback(int libs3_buffer_size,
                                           libs3_types::char_type *libs3_buffer,
                                           void *callback_data)
                {
                    callback_for_write_to_s3_base *data =
                        (callback_for_write_to_s3_base*)callback_data;
                    return data->callback_implementation(libs3_buffer_size, libs3_buffer);
                }

                static libs3_types::status on_response_properties(const libs3_types::response_properties *properties,
                                                                  void *callback_data)
                {
                    namespace bi = boost::interprocess;
                    namespace types = shared_data::interprocess_types;

                    callback_for_write_to_s3_base *callback_for_write_to_s3_base_data
                        = static_cast<callback_for_write_to_s3_base*>(callback_data);

                    using named_shared_memory_object =
                        irods::experimental::interprocess::shared_memory::named_shared_memory_object
                        <shared_data::multipart_shared_data>;

                    const auto& object_key = callback_for_write_to_s3_base_data->object_key;

                    auto shared_memory_name =  object_key + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

                    named_shared_memory_object shm_obj{shared_memory_name,
                        constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                        constants::MAX_S3_SHMEM_SIZE};

                    return shm_obj.atomic_exec([properties,
                            &callback_for_write_to_s3_base_data,
                            &shm_obj](auto& data) {

                        const char *etag = properties->eTag;

                        // Update the etags vector.  It should be sized large enough
                        // to not require a resize but resize if necessary.
                        if (callback_for_write_to_s3_base_data->sequence > data.etags.size()) {
                            try {
                                data.etags.resize(callback_for_write_to_s3_base_data->sequence,
                                        types::shm_char_string("", shm_obj.get_allocator()));
                            } catch (std::bad_alloc& ba) {
                                return S3StatusOutOfMemory;
                            }
                        }

                        if (etag) {
                            data.etags[callback_for_write_to_s3_base_data->sequence - 1] = etag;
                        } else {
                            data.etags[callback_for_write_to_s3_base_data->sequence - 1] = "";
                        }

                        return S3StatusOK;
                    });
                }

                static void on_response_completion (libs3_types::status status,
                                                    const libs3_types::error_details *error,
                                                    void *callback_data)
                {
                    callback_for_write_to_s3_base *data =
                        (callback_for_write_to_s3_base*)callback_data;
                    store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context,
                            data->status, data->debug_flag);

                }

                virtual ~callback_for_write_to_s3_base() {};

                //std::reference_wrapper<upload_manager> manager;
                libs3_types::status          status;
                bool                         enable_md5;
                bool                         server_encrypt;
                int                          object_identifier;
                time_t                       shared_memory_timeout_in_seconds;
                std::string                  object_key;

                int                          sequence;
                uint64_t                     offset;       // For multiple upload
                uint64_t                     content_length;
                libs3_types::bucket_context& saved_bucket_context; // To enable more detailed error messages
                bool                         debug_flag;

                upload_manager&              manager;

                uint64_t                     bytes_written;

        };

        template <typename buffer_type>
        class callback_for_write_from_cache_to_s3 : public callback_for_write_to_s3_base<buffer_type>
        {

            public:

                callback_for_write_from_cache_to_s3(libs3_types::bucket_context& _saved_bucket_context,
                                                    upload_manager& _manager)
                    : callback_for_write_to_s3_base<buffer_type>{_saved_bucket_context, _manager}
                {}

                int callback_implementation(int libs3_buffer_size,
                                            libs3_types::buffer_type libs3_buffer)
                {

                    if (!cache_fstream.is_open()) {
                        cache_fstream.open(filename.c_str(), std::ios_base::in);
                    }

                    // writing cache file to s3
                    cache_fstream.seekg(this->offset);
                    cache_fstream.read(libs3_buffer, libs3_buffer_size);

                    auto read = static_cast<uint64_t>(cache_fstream.tellg()) - this->offset;
                    if (read > 0) this->offset += read;

                    return read;

                }

                ~callback_for_write_from_cache_to_s3() {};

                void set_and_open_cache_file(std::string& f)
                {
                    filename = f;
                    cache_fstream.open(filename.c_str(), std::ios_base::in);
                }

            private:

                std::string   filename;
                std::ifstream cache_fstream;

        };

        template <typename buffer_type>
        class callback_for_write_from_buffer_to_s3 : public callback_for_write_to_s3_base<buffer_type>
        {

            public:

                using circular_buffer_type = irods::experimental::circular_buffer<upload_page<buffer_type>>;
                using output_char_type   = typename buffer_type::value_type;

                callback_for_write_from_buffer_to_s3(libs3_types::bucket_context& _saved_bucket_context,
                                                     upload_manager& _manager,
                                                     circular_buffer_type& _circular_buffer)
                    : callback_for_write_to_s3_base<buffer_type>{_saved_bucket_context, _manager}
                    , circular_buffer{_circular_buffer}
                {}

                int callback_implementation(int libs3_buffer_size,
                                            libs3_types::buffer_type libs3_buffer)
                {

                    // if we've already written the expected number of bytes, just return 0 which will
                    // trigger the completion
                    if (bytes_written >= this->content_length) {
                        return 0;
                    }

                    // if we've exhausted our current buffer, read the next buffer from the circular_buffer
                    while (this->offset >= buffer.size()) {

                        upload_page<buffer_type> page;

                        // read the first page
                        if (this->debug_flag) {
                            printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__,
                                    this->object_identifier);
                        }
                        circular_buffer.pop_front(page);
                        if (this->debug_flag) {
                            printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%lu][terminate_flag=%d]\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->object_identifier, page.buffer.data(),
                                    page.buffer.size(), page.terminate_flag);
                        }
                        buffer = page.buffer;
                        this->offset = 0;
                    }

                    auto remaining_transport_buffer_size = buffer.size() - this->offset;

                    bool libs3_buffer_larger_than_remaining_transport_buffer = libs3_buffer_size
                        > remaining_transport_buffer_size;

                    auto length = libs3_buffer_larger_than_remaining_transport_buffer
                        ? remaining_transport_buffer_size
                        : libs3_buffer_size;

                    memcpy(libs3_buffer, buffer.data() + this->offset, length);

                    this->offset += length;
                    bytes_written += length;

                    return length;

                }

                ~callback_for_write_from_buffer_to_s3() {};

                buffer_type buffer;
                irods::experimental::circular_buffer<upload_page<buffer_type>>& circular_buffer;
                uint64_t bytes_written;

        };

        namespace part_transport_callback
        {

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
