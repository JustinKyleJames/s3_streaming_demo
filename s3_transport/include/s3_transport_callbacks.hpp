#ifndef S3_TRANSPORT_CALLBACKS_HPP
#define S3_TRANSPORT_CALLBACKS_HPP

// stdlib and misc includes
#include <string>
#include <thread>
#include <vector>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>
#include <ctime>
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
#include "circular_buffer.hpp"
#include "managed_shared_memory_object.hpp"
#include "s3_multipart_shared_data.hpp"
#include "s3_transport_types.hpp"

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
                , thread_identifier{0}
                , bytes_read_from_s3{0}
                , shmem_key{}
                , shared_memory_timeout_in_seconds{constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS}
                , callback_counter{0}
            {}

            virtual libs3_types::status callback_implementation(int libs3_buffer_size,
                                                                const libs3_types::char_type *libs3_buffer) = 0;

            static libs3_types::status invoke_callback(int libs3_buffer_size,
                                                       const libs3_types::char_type *libs3_buffer,
                                                       void *callback_data)
            {
                using named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::named_shared_memory_object
                    <shared_data::multipart_shared_data>;

                callback_for_read_from_s3_base *data =
                    (callback_for_read_from_s3_base*)callback_data;

                // just touch shmem so we know we are active
                if (data->callback_counter++ % 10000 == 0) {
                    auto shmem_key =  data->shmem_key;
                    auto shared_memory_timeout_in_seconds = data->shared_memory_timeout_in_seconds;

                    named_shared_memory_object shm_obj{shmem_key,
                        shared_memory_timeout_in_seconds,
                        constants::MAX_S3_SHMEM_SIZE};
                }

                return data->callback_implementation(libs3_buffer_size, libs3_buffer);
            }

            static libs3_types::status on_response_properties(const libs3_types::response_properties *properties,
                                                              void *callback_data)
            {
                // Don't need to do anything here
                return libs3_types::status_ok;
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

            libs3_types::bucket_context& saved_bucket_context; /* To enable more detailed error messages */
            uint64_t                     offset;       /* For multiple upload */
            uint64_t                     content_length;
            int                          thread_identifier;
            uint64_t                     bytes_read_from_s3;
            std::string                  shmem_key;
            time_t                       shared_memory_timeout_in_seconds;

            // Counter incremented each data callback.  Every Nth iteration touch shared memory
            // so that we know the process didn't die and leave shared memory corrupted
            int                          callback_counter;

            libs3_types::status          status;
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
                assert(libs3_buffer_size >= 0);

                if (!cache_fstream.is_open()) {
                    cache_fstream.open(filename.c_str(), std::ios_base::out);
                }

                if (!cache_fstream) {
                    fprintf(stderr, "%s:%d (%s) [[%d]] could not open cache file\n",
                            __FILE__, __LINE__, __FUNCTION__, this->thread_identifier);
                    return S3StatusAbortedByCallback;
                }

                // writing output to cache file
                cache_fstream.seekp(this->offset);
                cache_fstream.write(libs3_buffer, libs3_buffer_size);

                auto wrote = static_cast<uint64_t>(cache_fstream.tellp()) - this->offset;
                if (wrote>0) {
                    this->offset += wrote;
                    this->bytes_read_from_s3 += wrote;
                }

                return ((wrote < static_cast<decltype(wrote)>(libs3_buffer_size)) ?
                        S3StatusAbortedByCallback : libs3_types::status_ok);

            }

            ~callback_for_read_from_s3_to_cache() {
                if (cache_fstream.is_open()) {
                    cache_fstream.close();
                }
            };

            void set_and_open_cache_file(std::string& f)
            {
                filename = f;
                cache_fstream.open(filename.c_str(), std::ios_base::out);
                if (!cache_fstream) {
                    fprintf(stderr, "%s:%d (%s) [[%d]] could not open cache file\n",
                            __FILE__, __LINE__, __FUNCTION__, this->thread_identifier);
                }
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
                assert(libs3_buffer_size >= 0);

                // writing to buffer

                uint64_t bytes_to_write = this->offset + libs3_buffer_size > output_buffer_size
                    ? output_buffer_size - this->offset
                    : libs3_buffer_size;

                memcpy(output_buffer + this->offset, libs3_buffer, bytes_to_write);

                this->offset += bytes_to_write;
                this->bytes_read_from_s3 += bytes_to_write;

                return ((bytes_to_write < static_cast<uint64_t>(libs3_buffer_size)) ?
                        S3StatusAbortedByCallback : libs3_types::status_ok);

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

    namespace s3_head_object_callback
    {
        libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                    void *callback_data);

        void on_response_complete (libs3_types::status status,
                                       const libs3_types::error_details *error,
                                       void *callback_data);
    }

    namespace s3_upload
    {

        template <typename buffer_type>
        class callback_for_write_to_s3_base
        {

            public:

                callback_for_write_to_s3_base(libs3_types::bucket_context& _saved_bucket_context,
                                              upload_manager& _manager)
                    : enable_md5{false}
                    , server_encrypt{false}
                    , thread_identifier{0}
                    , object_key{}
                    , shmem_key{}
                    , shared_memory_timeout_in_seconds{constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS}
                    , offset{0}
                    , content_length{0}
                    , saved_bucket_context{_saved_bucket_context}
                    , debug_flag{false}
                    , manager{_manager}
                    , bytes_written{0}
                    , callback_counter{0}
                {}


                virtual int callback_implementation(int libs3_buffer_size,
                                                    libs3_types::char_type *libs3_buffer) = 0;

                static int invoke_callback(int libs3_buffer_size,
                                           libs3_types::char_type *libs3_buffer,
                                           void *callback_data)
                {

                    using named_shared_memory_object =
                        irods::experimental::interprocess::shared_memory::named_shared_memory_object
                        <shared_data::multipart_shared_data>;

                    callback_for_write_to_s3_base *data =
                        (callback_for_write_to_s3_base*)callback_data;

                    // just touch shmem so we know we are active
                    if (data->callback_counter++ % 10000 == 0) {
                        auto shmem_key =  data->shmem_key;
                        auto shared_memory_timeout_in_seconds = data->shared_memory_timeout_in_seconds;

                        named_shared_memory_object shm_obj{shmem_key,
                            shared_memory_timeout_in_seconds,
                            constants::MAX_S3_SHMEM_SIZE};
                    }

                    return data->callback_implementation(libs3_buffer_size, libs3_buffer);
                }

                static libs3_types::status on_response_properties(const libs3_types::response_properties *properties,
                                                                  void *callback_data)
                {
                    return libs3_types::status_ok;
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

                libs3_types::status          status;
                bool                         enable_md5;
                bool                         server_encrypt;
                int                          thread_identifier;
                std::string                  object_key;
                std::string                  shmem_key;
                time_t                       shared_memory_timeout_in_seconds;

                uint64_t                     offset;
                uint64_t                     content_length;
                libs3_types::bucket_context& saved_bucket_context; // To enable more detailed error messages
                bool                         debug_flag;
                upload_manager&              manager;
                uint64_t                     bytes_written;

                // Counter incremented each data callback.  Every Nth iteration touch shared memory
                // so that we know the process didn't die and leave shared memory corrupted
                int                          callback_counter;

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

                    assert(libs3_buffer_size >= 0);

                    if (!cache_fstream.is_open()) {
                        cache_fstream.open(filename.c_str(), std::ios_base::in);
                    }

                    if (!cache_fstream) {
                        fprintf(stderr, "%s:%d (%s) [[%d]] could not open cache file\n",
                                __FILE__, __LINE__, __FUNCTION__, this->thread_identifier);
                        return S3StatusAbortedByCallback;
                    }

                    // writing cache file to s3 buffer
                    uint64_t length_to_read_from_cache = this->content_length - this->bytes_written
                        > static_cast<uint64_t>(libs3_buffer_size)
                        ? static_cast<uint64_t>(libs3_buffer_size)
                        : this->content_length - this->bytes_written;

                    cache_fstream.seekg(this->offset);
                    cache_fstream.read(libs3_buffer, length_to_read_from_cache);

                    auto bytes_read_from_cache = static_cast<uint64_t>(cache_fstream.tellg()) - this->offset;
                    if (bytes_read_from_cache > 0) {
                        this->offset += bytes_read_from_cache;
                        this->bytes_written += bytes_read_from_cache;
                    }

                    return bytes_read_from_cache;

                }

                ~callback_for_write_from_cache_to_s3() {
                    if (cache_fstream.is_open()) {
                        cache_fstream.close();
                    }
                };

                void set_and_open_cache_file(std::string& f)
                {
                    filename = f;
                    cache_fstream.open(filename.c_str(), std::ios_base::in);
                    if (!cache_fstream) {
                        fprintf(stderr, "%s:%d (%s) [[%d]] could not open cache file\n",
                                __FILE__, __LINE__, __FUNCTION__, this->thread_identifier);
                    }
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

                    assert(libs3_buffer_size >= 0);

                    // if we've already written the expected number of bytes, just return 0 which will
                    // trigger the completion
                    if (this->bytes_written >= this->content_length) {
                        return 0;
                    }

                    // if we've exhausted our current buffer, read the next buffer from the circular_buffer
                    while (this->offset >= buffer.size()) {

                        upload_page<buffer_type> page;

                        // read the first page
                        if (this->debug_flag) {
                            printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__,
                                    this->thread_identifier);
                        }
                        circular_buffer.pop_front(page);
                        if (this->debug_flag) {
                            printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%lu][terminate_flag=%d]\n",
                                    __FILE__, __LINE__, __FUNCTION__, this->thread_identifier, page.buffer.data(),
                                    page.buffer.size(), page.terminate_flag);
                        }
                        buffer = page.buffer;
                        this->offset = 0;
                    }

                    auto remaining_transport_buffer_size = buffer.size() - this->offset;

                    bool libs3_buffer_larger_than_remaining_transport_buffer = static_cast<uint64_t>(libs3_buffer_size)
                        > remaining_transport_buffer_size;

                    uint64_t length = libs3_buffer_larger_than_remaining_transport_buffer
                        ? remaining_transport_buffer_size
                        : libs3_buffer_size;

                    memcpy(libs3_buffer, buffer.data() + this->offset, length);

                    this->offset += length;
                    this->bytes_written += length;

                    return length;

                }

                ~callback_for_write_from_buffer_to_s3() {};

                buffer_type buffer;
                irods::experimental::circular_buffer<upload_page<buffer_type>>& circular_buffer;

        };

    } // end namespace s3_upload

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
                    : enable_md5{false}
                    , server_encrypt{false}
                    , thread_identifier{0}
                    , shared_memory_timeout_in_seconds{constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS}
                    , object_key{}
                    , shmem_key{}
                    , sequence{0}
                    , offset{0}
                    , content_length{0}
                    , saved_bucket_context{_saved_bucket_context}
                    , debug_flag{false}
                    , manager{_manager}
                    , bytes_written{0}
                    , callback_counter{0}
                {}


                virtual int callback_implementation(int libs3_buffer_size,
                                                    libs3_types::char_type *libs3_buffer) = 0;

                static int invoke_callback(int libs3_buffer_size,
                                           libs3_types::char_type *libs3_buffer,
                                           void *callback_data)
                {

                    using named_shared_memory_object =
                        irods::experimental::interprocess::shared_memory::named_shared_memory_object
                        <shared_data::multipart_shared_data>;

                    callback_for_write_to_s3_base *data =
                        (callback_for_write_to_s3_base*)callback_data;

                    // just touch shmem so we know we are active
                    if (data->callback_counter++ % 10000 == 0) {
                        auto shmem_key =  data->shmem_key;
                        auto shared_memory_timeout_in_seconds = data->shared_memory_timeout_in_seconds;

                        named_shared_memory_object shm_obj{shmem_key,
                            shared_memory_timeout_in_seconds,
                            constants::MAX_S3_SHMEM_SIZE};
                    }

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

                    auto shmem_key =  callback_for_write_to_s3_base_data->shmem_key;
                    auto shared_memory_timeout_in_seconds =
                        callback_for_write_to_s3_base_data->shared_memory_timeout_in_seconds;

                    named_shared_memory_object shm_obj{shmem_key,
                        shared_memory_timeout_in_seconds,
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

                        return libs3_types::status_ok;
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
                int                          thread_identifier;
                time_t                       shared_memory_timeout_in_seconds;
                std::string                  object_key;
                std::string                  shmem_key;

                unsigned long                sequence;
                uint64_t                     offset;       // For multiple upload
                uint64_t                     content_length;
                libs3_types::bucket_context& saved_bucket_context; // To enable more detailed error messages
                bool                         debug_flag;

                upload_manager&              manager;

                uint64_t                     bytes_written;


                // Counter incremented each data callback.  Every Nth iteration touch shared memory
                // so that we know the process didn't die and leave shared memory corrupted
                int                          callback_counter;

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

                    assert(libs3_buffer_size >= 0);

                    if (!cache_fstream.is_open()) {
                        cache_fstream.open(filename.c_str(), std::ios_base::in);
                    }

                    if (!cache_fstream) {
                        fprintf(stderr, "%s:%d (%s) [[%d]] could not open cache file\n",
                                __FILE__, __LINE__, __FUNCTION__, this->thread_identifier);
                        return 0;
                    }

                    // writing cache file to s3 buffer
                    uint64_t length_to_read_from_cache = this->content_length - this->bytes_written
                        > static_cast<uint64_t>(libs3_buffer_size)
                        ? static_cast<uint64_t>(libs3_buffer_size)
                        : this->content_length - this->bytes_written;

                    cache_fstream.seekg(this->offset);
                    cache_fstream.read(libs3_buffer, length_to_read_from_cache);

                    auto bytes_read_from_cache = static_cast<uint64_t>(cache_fstream.tellg()) - this->offset;
                    if (bytes_read_from_cache > 0) {
                        this->offset += bytes_read_from_cache;
                        this->bytes_written += bytes_read_from_cache;
                    }


                    return bytes_read_from_cache;

                }

                ~callback_for_write_from_cache_to_s3() {
                    if (cache_fstream.is_open()) {
                        cache_fstream.close();
                    }
                };

                void set_and_open_cache_file(std::string& f)
                {
                    filename = f;
                    cache_fstream.open(filename.c_str(), std::ios_base::in);
                    if (!cache_fstream) {
                        fprintf(stderr, "%s:%d (%s) [[%d]] could not open cache file\n",
                                __FILE__, __LINE__, __FUNCTION__, this->thread_identifier);
                    }
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

                    assert(libs3_buffer_size >= 0);

                    // if we've already written the expected number of bytes, just return 0 which will
                    // trigger the completion
                    if (this->bytes_written >= this->content_length) {
                        return 0;
                    }

                    // if we've exhausted our current buffer, read the next buffer from the circular_buffer
                    while (this->offset >= buffer.size()) {

                        upload_page<buffer_type> page;

                        // read the first page
                        if (this->debug_flag) {
                            printf("%s:%d (%s) [[%d]] waiting to read\n",
                                    __FILE__, __LINE__, __FUNCTION__,
                                    this->thread_identifier);
                        }
                        circular_buffer.pop_front(page);
                        if (this->debug_flag) {
                            printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%lu]"
                                    "[terminate_flag=%d]\n", __FILE__, __LINE__, __FUNCTION__,
                                    this->thread_identifier, page.buffer.data(),
                                    page.buffer.size(), page.terminate_flag);
                        }
                        buffer = page.buffer;
                        this->offset = 0;
                    }

                    auto remaining_transport_buffer_size = buffer.size() - this->offset;

                    bool libs3_buffer_larger_than_remaining_transport_buffer = static_cast<unsigned long>(libs3_buffer_size)
                        > remaining_transport_buffer_size;

                    uint64_t length = libs3_buffer_larger_than_remaining_transport_buffer
                        ? remaining_transport_buffer_size
                        : libs3_buffer_size;

                    memcpy(libs3_buffer, buffer.data() + this->offset, length);

                    this->offset += length;
                    this->bytes_written += length;

                    return length;

                }

                ~callback_for_write_from_buffer_to_s3() {};

                buffer_type buffer;
                irods::experimental::circular_buffer<upload_page<buffer_type>>& circular_buffer;

        };

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

