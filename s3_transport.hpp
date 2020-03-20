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
#include "shared_memory_object.hpp"
#include "s3_multipart_shared_data.hpp"

namespace irods::experimental::io::s3_transport
{

    struct libs3_types
    {
        using status = S3Status;
    };

    struct constants
    {

        static const uint64_t    ETAG_SIZE{34};
        static const uint64_t    UPLOAD_ID_SIZE{128};
        static const uint64_t    MAX_S3_SHMEM_SIZE{sizeof(shared_data::multipart_shared_data) +
                                                   10000 * (ETAG_SIZE + 1) +
                                                   UPLOAD_ID_SIZE + 1};

        static const int         DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS{900};
        static const std::string MULTIPART_SHARED_MEMORY_EXTENSION;
    };

    void print_bucket_context(const S3BucketContext& bucket_context);
    void store_and_log_status( libs3_types::status status,
                               const S3ErrorDetails *error,
                               const std::string& function,
                               const S3BucketContext& saved_bucket_context,
                               libs3_types::status& pStatus,
                               bool debug_flag);

    // Returns timestamp in usec for delta-t comparisons
    unsigned long long get_time_in_microseconds();

    template <typename buffer_type>
    struct upload_page
    {
       buffer_type        buffer;
       bool               terminate_flag;
    };

    // TODO how is this used?  remove?  keep for now until stat is implemented
    struct s3_stat
    {
        std::string key;
        uint64_t    size;
        time_t      last_modified;
    };


    template <typename buffer_type>
    struct data_for_write_callback
    {
        data_for_write_callback(S3BucketContext& _saved_bucket_context,
                                irods::experimental::circular_buffer<upload_page<buffer_type>>& _circular_buffer)
            : saved_bucket_context{_saved_bucket_context}
            , circular_buffer{_circular_buffer}
            , content_length{0}
            , offset{0}
            , bytes_written{0}
        {}

        buffer_type         buffer;
        uint64_t            offset;

        irods::experimental::circular_buffer<upload_page<buffer_type>>&
                            circular_buffer;

        uint64_t            content_length;
        uint64_t            bytes_written;
        libs3_types::status status;

        S3BucketContext&    saved_bucket_context;   /* To enable more detailed error messages */
        bool                debug_flag;
        int                 object_identifier;
    };

    template <typename buffer_type>
    class callback_for_read_from_s3
    {

        public:

            using char_type   = typename buffer_type::value_type;

            callback_for_read_from_s3(S3BucketContext& _saved_bucket_context)
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
                callback_for_read_from_s3 *data = (callback_for_read_from_s3*)callback_data;
                return data->callback_implementation(libs3_buffer_size, libs3_buffer);
            }

            static libs3_types::status response_properties_callback(const S3ResponseProperties *properties,
                                                         void *callback_data)
            {
                // Don't need to do anything here
                return S3StatusOK;
            }

            static void response_completion_callback (libs3_types::status status,
                                                      const S3ErrorDetails *error,
                                                      void *callback_data)
            {
                callback_for_read_from_s3 *data = (callback_for_read_from_s3*)callback_data;
                store_and_log_status( status, error, __FUNCTION__, data->saved_bucket_context, data->status,
                        data->debug_flag );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            }

            virtual ~callback_for_read_from_s3() {};

            int                  sequence;

            long                 offset;       /* For multiple upload */
            uint64_t             content_length;
            libs3_types::status  status;
            S3BucketContext&     saved_bucket_context; /* To enable more detailed error messages */
            bool                 debug_flag;
    };

    template <typename buffer_type>
    class callback_for_read_from_s3_to_cache : public callback_for_read_from_s3<buffer_type>
    {

        public:

            using char_type   = typename buffer_type::value_type;

            callback_for_read_from_s3_to_cache(S3BucketContext& _saved_bucket_context)
                : callback_for_read_from_s3<buffer_type>{_saved_bucket_context}
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
    class callback_for_read_from_buffer : public callback_for_read_from_s3<buffer_type>
    {

        public:

            using char_type   = typename buffer_type::value_type;

            callback_for_read_from_buffer(S3BucketContext& _saved_bucket_context)
                : callback_for_read_from_s3<buffer_type>{_saved_bucket_context}
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

            ~callback_for_read_from_buffer() {};

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


    struct upload_manager
    {
        upload_manager(S3BucketContext& _saved_bucket_context)
            : saved_bucket_context{_saved_bucket_context}
            , remaining{0}
            , offset{0}
            , xml{""}
        {
        }

        S3BucketContext           saved_bucket_context;             /* To enable more detailed error messages */

        /* Below used for the upload completion command, need to send in XML */
        std::string              xml;

        // TODO derive types
        uint64_t                 remaining;
        uint64_t                 offset;
        bool                     debug_flag;
        libs3_types::status      status;            /* status returned by libs3 */
        std::string              object_key;
        time_t                   shared_memory_timeout_in_seconds;
    };

    template <typename buffer_type>
    struct multipart_data
    {
        multipart_data(upload_manager& _manager,
                       S3BucketContext& _bucket_context,
                       irods::experimental::circular_buffer<upload_page<buffer_type>>& _circular_buffer)
            : manager{_manager}
            , put_object_data{_bucket_context, _circular_buffer}
            , sequence{0}
            , status{S3StatusOK}
            , enable_md5{false}
            , server_encrypt{false}
            , debug_flag{false}
            , object_identifier{0}
            , shared_memory_timeout_in_seconds{constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS}
        {}


        int                                    sequence;           // sequence or part number
        int                                    mode;               // PUT or COPY
        data_for_write_callback<buffer_type>   put_object_data;
        std::reference_wrapper<upload_manager> manager;
        libs3_types::status                    status;
        bool                                   enable_md5;
        bool                                   server_encrypt;
        bool                                   debug_flag;
        int                                    object_identifier;
        time_t                                 shared_memory_timeout_in_seconds;
    };

    namespace s3_multipart_upload
    {
        using libs3_char_type   = char;
        using libs3_buffer_type = libs3_char_type*;

        namespace initialization_callback
        {

            libs3_types::status response (const libs3_char_type* upload_id,
                                          void *callback_data );

            libs3_types::status response_properties (const S3ResponseProperties *properties,
                                                     void *callback_data);

            void response_complete (libs3_types::status status,
                                    const S3ErrorDetails *error,
                                    void *callback_data);
        } // end namespace initialization_callback


        /* Uploading the multipart completion XML from our buffer */
        namespace commit_callback
        {
            int response (int buffer_size,
                          libs3_buffer_type buffer,
                          void *callback_data);

            libs3_types::status response_properties (const S3ResponseProperties *properties,
                                                     void *callback_data);

            void response_completion (libs3_types::status status,
                                      const S3ErrorDetails *error,
                                      void *callback_data);

        } // end namespace commit_callback


        namespace part_transport_callback
        {
            template <typename buffer_type>
            int put_data (int libs3_buffer_size,
                          libs3_buffer_type libs3_buffer,
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
            } // end put_data


            template <typename buffer_type>
            libs3_types::status response_properties (const S3ResponseProperties *properties,
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
            void response_completion (libs3_types::status status,
                                      const S3ErrorDetails *error,
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
            libs3_types::status response_properties (const S3ResponseProperties *properties,
                                                     void *callback_data);

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            extern libs3_types::status g_response_completion_status;
            extern S3BucketContext *g_response_completion_saved_bucket_context;

            void response_completion (libs3_types::status status,
                                      const S3ErrorDetails *error,
                                      void *callback_data);
        } // end namespace cancel_callback
    } // end namespace s3_multipart_upload

    // Sleep for *at least* the given time, plus some up to 1s additional
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(int _s,
                  int _ms );

    struct config
    {

        config()
            : object_size{0}
            , number_of_transfer_threads{1}
            , part_size{1000}
            , retry_count_limit{3}
            , retry_wait_seconds{3}
            , hostname{"s3.amazonaws.com"}
            , bucket_name{""}
            , access_key{""}
            , secret_access_key{""}
            , multipart_flag{true}
            , shared_memory_timeout_in_seconds{constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS}
            , enable_md5_flag{false}
            , server_encrypt_flag{false}
            , s3_signature_version_str{"v4"}
            , s3_protocol_str{"http"}
            , s3_sts_date_str{"amz"}
            , cache_directory{"/tmp"}
            , debug_flag{false}
            , object_identifier{0}  // just for debug purposes
        {}

        uint64_t     object_size;
        int          number_of_transfer_threads; // only used when doing full file upload/download via cache
                                                       // otherwise it is controlled by iRODS
        uint64_t     part_size;                  // only used when doing a multipart upload
        int          retry_count_limit;
        int          retry_wait_seconds;
        std::string  hostname;
        std::string  bucket_name;
        std::string  access_key;
        std::string  secret_access_key;
        bool         multipart_flag;
        time_t       shared_memory_timeout_in_seconds;
        bool         enable_md5_flag;
        bool         server_encrypt_flag;
        std::string  s3_signature_version_str;
        std::string  s3_protocol_str;
        std::string  s3_sts_date_str;
        std::string  cache_directory;
        bool         debug_flag;
        int          object_identifier;          // just for debug purposes
    };

    template <typename CharT>
    class s3_transport : public transport<CharT>
    {
    public:

        // clang-format off
        using char_type   = typename transport<CharT>::char_type;
        using buffer_type = std::vector<char_type>;
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

        explicit s3_transport(const config& _config)

            : transport<CharT>{}
            , object_size_{_config.object_size}
            , number_of_transfer_threads_{_config.number_of_transfer_threads}
            , part_size_{_config.part_size}
            , retry_count_limit_{_config.retry_count_limit}
            , retry_wait_seconds_{_config.retry_wait_seconds}
            , hostname_{_config.hostname}
            , bucket_name_{_config.bucket_name}
            , access_key_{_config.access_key}
            , secret_access_key_{_config.secret_access_key}
            , multipart_flag_{_config.multipart_flag}
            , shared_memory_timeout_in_seconds_{_config.shared_memory_timeout_in_seconds}
            , enable_md5_flag_{_config.enable_md5_flag}
            , server_encrypt_flag_{_config.server_encrypt_flag}
            , cache_directory_{_config.cache_directory}
            , debug_flag_{_config.debug_flag}
            , cache_fd_{0}
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
            , object_identifier_{_config.object_identifier}
            , upload_manager_{bucket_context_}
            , mpu_data_{upload_manager_, bucket_context_, circular_buffer_}
            , put_props_{}
            , bucket_context_{}
        {

            mpu_data_.enable_md5 = enable_md5_flag_;
            mpu_data_.server_encrypt = server_encrypt_flag_;
            mpu_data_.object_identifier = object_identifier_;
            mpu_data_.shared_memory_timeout_in_seconds = shared_memory_timeout_in_seconds_;

            upload_manager_.debug_flag = debug_flag_;
            upload_manager_.shared_memory_timeout_in_seconds = shared_memory_timeout_in_seconds_;
            mpu_data_.debug_flag = debug_flag_;

            bucket_context_.hostName        = hostname_.c_str();
            bucket_context_.bucketName      = bucket_name_.c_str();
            bucket_context_.accessKeyId     = access_key_.c_str();
            bucket_context_.secretAccessKey = secret_access_key_.c_str();

            if (_config.s3_signature_version_str == "4"
                    || boost::iequals(_config.s3_signature_version_str, "V4")) {
                s3_signature_version_       = S3SignatureV4;
            } else {
                s3_signature_version_       = S3SignatureV2;
            }

            if (boost::iequals(_config.s3_protocol_str, "http")) {
                bucket_context_.protocol    = S3ProtocolHTTP;
            } else {
                bucket_context_.protocol    = S3ProtocolHTTPS;
            }

            if (boost::iequals(_config.s3_sts_date_str, "date")) {
                bucket_context_.stsDate     = S3STSDateOnly;
            } else if (boost::iequals(_config.s3_sts_date_str, "both")) {
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

            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            if (!is_open()) {
                return false;
            }


            fd_ = uninitialized_file_descriptor;

            int open_flags = populate_open_mode_flags();

            if ( is_full_multipart_upload(open_flags) ) {

                // This was a full multipart upload, wait for the upload to complete

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

            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                constants::MAX_S3_SHMEM_SIZE};

            bool success_flag = shm_obj.atomic_exec([this, &shm_obj, open_flags](auto& data) {

                int file_open_counter = --(data.file_open_counter);

                if (file_open_counter == 0) {

                    if (use_cache_) {

                        namespace bf = boost::filesystem;

                        // Flush the cache file to S3.

                        bf::path cache_file =  bf::path(cache_directory_) / bf::path(object_key_ + "-cache");

                        // go ahead and upload the object
                        cache_fd_ = open(cache_file.string().c_str(), O_RDONLY);
                        off_t cache_file_size = lseek(cache_fd_, 0, SEEK_END);

                        uint64_t part_size = cache_file_size / number_of_transfer_threads_;

                        unsigned long long usStart = get_time_in_microseconds();

                        flush_cache_file(part_size);


                    } else if ( is_full_multipart_upload(open_flags) ) {
                        if (0 > complete_multipart_upload()) {
                            // error - remove shmem object
                            shm_obj.remove();
                            return false;
                        }
                    }

                    if (debug_flag_) {
                        printf("%s:%d (%s) [[%d]] [file_open_counter=%d]\n",
                                __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                                file_open_counter);
                    }

                    // remove shmem object
                    shm_obj.remove();

                }

                return true;

            });

            if (!success_flag) {
                return success_flag;
            }

            // each process must initialize and deinitiatize.
            if (--s3_initialized_counter_ == 0) {
                S3_deinitialize();
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
            buffer_type copied_buffer(_buffer, _buffer + _buffer_size);
            circular_buffer_.push_back({copied_buffer});

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] wrote buffer of size %ld\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, _buffer_size);
            }

            // if we haven't already started an upload thread, start it
            if (!begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_ = new std::thread(&s3_transport::s3_upload_part_worker_routine, this);
            }


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

        void flush_cache_file(uint64_t part_size) {

            std::list<std::thread*> cache_flush_thread_list;

            auto cache_flush_oper =  [this, part_size](int thr_id) {

                        uint64_t buffer_size;
                        if (thr_id == number_of_transfer_threads_ - 1) {
                            buffer_size = part_size + (object_size_ - part_size * number_of_transfer_threads_);
                        } else {
                            buffer_size = part_size;
                        }

                        // TODO
                    };

            create_and_run_threads( cache_flush_thread_list, number_of_transfer_threads_,  cache_flush_oper );
        }

        void wait_for_threads_to_finish( std::list<std::thread*>& thread_list ) {

            while (!thread_list.empty()) {
                std::thread *thisThread = thread_list.front();
                thisThread->join();
                delete thisThread;
                thread_list.pop_front();
            }

        }

        void create_and_run_threads( std::list<std::thread*>& thread_list,
                                     int number_of_threads,
                                     std::function<void(int)> op) {

            for (int thr_id=0; thr_id < number_of_threads; thr_id++) {

                std::thread *thisThread = new std::thread(op, thr_id);

                thread_list.push_back(thisThread);
            }
            return;
        }

        bool is_full_multipart_upload(int open_flags) {
            return ( multipart_flag_ && is_full_upload(open_flags) );
        }

        bool is_full_upload(int open_flags) {
            return ( (O_CREAT | O_WRONLY | O_TRUNC) == open_flags );
        }

        /*void remove_shared_memory()
        {
            namespace bi = boost::interprocess;

            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;
            std::string mtx_name = object_key_ + "-mtx";

            bi::shared_memory_object::remove(shared_memory_name.c_str());
            bi::named_mutex::remove(mtx_name.c_str());
        }  // end remove_shared_memory*/

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
        }  // end populate_mode_flags

        bool seek_to_end_if_required(std::ios_base::openmode _mode)
        {
            if (std::ios_base::ate & _mode) {
                if (seek_error == seekpos(0, std::ios_base::end)) {
                    return false;
                }
            }

            return true;
        }  // end seek_to_end_if_required

        template <typename Function>
        bool open_impl(const filesystem::path& _p,
                       std::ios_base::openmode _mode,
                       Function _func)
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] [_mode & in = %d][_mode & out = %d]"
                    "[_mode & trunc = %d][_mode & app = %d]\n",
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
                printf("%s:%d (%s) [[%d]] Invalid open mode detected.\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_);
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


            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                constants::MAX_S3_SHMEM_SIZE};

            // only allow open to run one at a time for this object
            bi::interprocess_recursive_mutex* file_open_mutex =
                shm_obj.atomic_exec([](auto& data)
            {
                        return &(data.file_open_mutex);

            });
            bi::scoped_lock file_open_lock(*file_open_mutex);

            auto file_open_counter = shm_obj.atomic_exec([](auto& data) {
                return ++(data.file_open_counter);
            });

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

            }

            ++s3_initialized_counter_;

            if (object_exists && download_to_cache_) {

                namespace bf = boost::filesystem;

                // TODO not yet tested

                // TODO not sure how boost::filesystem will handle this if object_key_ has a directory structure in it. test.
                bf::path cache_file =  bf::path(cache_directory_) / bf::path(object_key_ + "-cache");

                bool cache_file_download_started_flag = shm_obj.atomic_exec([](auto& data) {
                    bool return_val = data.cache_file_download_started_flag;
                    data.cache_file_download_started_flag = true;
                    return return_val;
                });


                if (!cache_file_download_started_flag) {

                    // go ahead and download the object to cache file
                    cache_fd_ = open(cache_file.string().c_str(), O_WRONLY);

                    uint64_t part_size = object_size_ / number_of_transfer_threads_;

                    unsigned long long usStart = get_time_in_microseconds();
                    std::list<std::thread*> threads;
                    for (int thr_id=0; thr_id < number_of_transfer_threads_; thr_id++) {

                        uint64_t this_part_size;
                        if (thr_id == number_of_transfer_threads_ - 1) {
                            this_part_size = part_size + (object_size_ - part_size * number_of_transfer_threads_);
                        } else {
                            this_part_size = part_size;
                        }

                        std::thread *this_thread= new std::thread(&s3_transport::s3_download_part_worker_routine,
                                this, nullptr, this_part_size);
                        threads.push_back(this_thread);

                    }

                    // Wait for threads to finish
                    while (!threads.empty()) {
                        std::thread *this_thread= threads.front();
                        this_thread->join();
                        delete this_thread;
                        threads.pop_front();
                    }

                    shm_obj.atomic_exec([](auto& data) {
                        data.cache_file_download_completed_flag = true;
                    });

                }

                // download has started so we must wait until it completes before continuing
                // TODO figure out a better way to do this than a busy wait
                //      can't use std::condition_variable because of the multiprocess case
                //      get it working and then fix it
                while (!(shm_obj.atomic_exec([](auto& data) { return data.cache_file_download_completed_flag; }))) {
                    sleep(1);
                }

            }

            if ( is_full_upload(open_flags) ) {

                // this is a full file upload - do not use cache

                if ( is_full_multipart_upload(open_flags) ) {

                    auto last_irods_error_code = shm_obj.atomic_exec([](auto& data) {
                        return data.last_irods_error_code;
                    });

                    // first one in initiates the multipart (everyone has same shared_memory_lock)
                    if (last_irods_error_code >= 0 && file_open_counter == 1) {

                        // send initiate message to S3
                        int ret = shm_obj.atomic_exec([this](auto& data) {
                            return initiate_multipart_upload();
                        });

                        // send initiate message to S3
                        //int ret = initiate_multipart_upload();

                        if (ret < 0) {
                            printf("%s:%d (%s) [[%d]] open returning false [last_irods_error_code=%d]\n",
                                    __FILE__, __LINE__, __FUNCTION__, object_identifier_, ret);

                            // update the last error
                            shm_obj.atomic_exec([ret](auto& data) {
                                data.last_irods_error_code = ret;
                            });

                            return false;
                        }
                    } else {
                        if (last_irods_error_code < 0) {
                            printf("%s:%d (%s) [[%d]] open returning false [last_irods_error_code=%d]\n",
                                    __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                                    last_irods_error_code);
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

            if (!seek_to_end_if_required(mode_)) {
                close();
                return false;
            }

            return true;


        }  // end open_impl

        int initiate_multipart_upload()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            int cache_fd = -1;
            int err_status = 0;
            int retry_cnt    = 0;
            bool enable_md5 = enable_md5_flag_;
            put_props_.useServerSideEncryption = server_encrypt_flag_;
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

            data_for_write_callback data{bucket_context_, circular_buffer_};
            memset(&data, 0, sizeof(data));
            data.debug_flag = debug_flag_;
            data.object_identifier = object_identifier_;

            // read shared memory entry for this key
            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                shared_memory_timeout_in_seconds_,
                constants::MAX_S3_SHMEM_SIZE};

            // no lock here as it is already locked
            return shm_obj.exec([this, &retry_cnt](auto& data) {

                retry_cnt = 0;

                // These expect a upload_manager* as cbdata
                S3MultipartInitialHandler mpu_initial_handler
                    = { { s3_multipart_upload::initialization_callback::response_properties,
                          s3_multipart_upload::initialization_callback::response_complete },
                        s3_multipart_upload::initialization_callback::response };

                do {
                    print_bucket_context(bucket_context_);
                    if (debug_flag_) {
                        printf("%s:%d (%s) [[%d]] call S3_initiate_multipart [object_key=%s]\n",
                                __FILE__, __LINE__, __FUNCTION__, object_identifier_, object_key_.c_str());
                    }
                    S3_initiate_multipart(&bucket_context_, object_key_.c_str(),
                            &put_props_, &mpu_initial_handler, nullptr, &upload_manager_);

                    if (upload_manager_.status != S3StatusOK) {
                        s3_sleep( retry_wait_seconds_, 0 );
                    }

                } while ( (upload_manager_.status != S3StatusOK)
                        && S3_status_is_retryable(upload_manager_.status)
                        && ( ++retry_cnt < retry_count_limit_));

                if ("" == data.upload_id || upload_manager_.status != S3StatusOK) {
                    return S3_PUT_ERROR;
                }

                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] S3_initiate_multipart returned.  Upload ID = %s\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            data.upload_id.c_str());
                }
                upload_manager_.remaining = 0;
                upload_manager_.offset  = 0;

                return static_cast<IRODS_ERROR_ENUM>(0);
            });

        } // end initiate_multipart_upload

        void mpu_cancel()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            // read shared memory entry for this key
            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                shared_memory_timeout_in_seconds_,
                constants::MAX_S3_SHMEM_SIZE};

            // read upload_id from shared_memory
            // no lock here as it is already locked
            std::string upload_id = shm_obj.exec([](auto& data) {
                return data.upload_id.c_str();
            });

            S3AbortMultipartUploadHandler abort_handler
                = { { s3_multipart_upload::cancel_callback::response_properties,
                      s3_multipart_upload::cancel_callback::response_completion } };

            std::stringstream msg;
            libs3_types::status status;

            if (debug_flag_) {
                msg << "Cancelling multipart upload: key=\""
                    << object_key_ << "\", upload_id=\"" << upload_id << "\"";
                printf( "%s\n", msg.str().c_str() );
            }
            s3_multipart_upload::cancel_callback::g_response_completion_status = S3StatusOK;
            s3_multipart_upload::cancel_callback::g_response_completion_saved_bucket_context = &bucket_context_;
            S3_abort_multipart_upload(&bucket_context_, object_key_.c_str(),
                    upload_id.c_str(), &abort_handler);
            status = s3_multipart_upload::cancel_callback::g_response_completion_status;
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
        } // end mpu_cancel


        int complete_multipart_upload()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                shared_memory_timeout_in_seconds_,
                constants::MAX_S3_SHMEM_SIZE};

            // no lock here as it is already locked
            int result = shm_obj.exec([this](auto& data) {

                std::stringstream msg;
                unsigned int retry_cnt = 0;

                std::stringstream xml("");

                std::string upload_id  = data.upload_id.c_str();

                if (0 == data.last_irods_error_code) { // If someone aborted, don't complete...
                    if (debug_flag_) {
                        msg.str( std::string() ); // Clear
                        msg << "Multipart:  Completing key \"" << object_key_.c_str() << "\" Upload ID \""
                            << upload_id << "\"";
                        printf( "%s\n", msg.str().c_str() );
                    }

                    int i;
                    xml << "<CompleteMultipartUpload>\n";
                    char_type buf[256];
                    int n;
                    for ( i = 0; i < data.etags.size(); i++ ) {
                        xml << "<Part><PartNumber>";
                        xml << (i + 1);
                        xml << "</PartNumber><ETag>";
                        xml << data.etags[i];
                        xml << "</ETag></Part>";
                    }
                    xml << "</CompleteMultipartUpload>\n";

                    int manager_remaining = xml.str().size();
                    upload_manager_.offset = 0;
                    retry_cnt = 0;
                    S3MultipartCommitHandler commit_handler
                        = { {s3_multipart_upload::commit_callback::response_properties,
                             s3_multipart_upload::commit_callback::response_completion },
                            s3_multipart_upload::commit_callback::response, nullptr };
                    do {
                        // On partial error, need to restart XML send from the beginning
                        upload_manager_.remaining = manager_remaining;
                        upload_manager_.xml = xml.str().c_str();

                        upload_manager_.offset = 0;
                        S3_complete_multipart_upload(&bucket_context_, object_key_.c_str(),
                                &commit_handler, upload_id.c_str(),
                                upload_manager_.remaining, nullptr, &upload_manager_);
                        if (debug_flag_) {
                            printf("%s:%d (%s) [[%d]] [manager.status=%s]\n", __FILE__, __LINE__,
                                    __FUNCTION__, object_identifier_, S3_get_status_name(upload_manager_.status));
                        }
                        if (upload_manager_.status != S3StatusOK) s3_sleep( retry_wait_seconds_, 0 );
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
                if (0 > data.last_irods_error_code && "" != data.upload_id ) {

                    // Someone aborted after we started, delete the partial object on S3
                    printf("Cancelling multipart upload\n");
                    mpu_cancel();

                    // Return the error
                    return static_cast<IRODS_ERROR_ENUM>(data.last_irods_error_code);
                }

                return static_cast<IRODS_ERROR_ENUM>(0);

            });

            return result;
        } // end complete_multipart_upload

        // this function is called in the background in a separate thread
        void s3_download_part_worker_routine(char_type *buffer, uint64_t length)
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            std::stringstream msg;

            int retry_cnt = 0;

            std::shared_ptr<callback_for_read_from_s3<buffer_type>> read_callback_data;

            do {

                S3GetObjectHandler get_object_handler = {
                    {
                        callback_for_read_from_s3<buffer_type>::response_properties_callback,
                        callback_for_read_from_s3<buffer_type>::response_completion_callback
                    },
                    callback_for_read_from_s3<buffer_type>::invoke_callback
                };

                if (buffer == nullptr) {
                    read_callback_data.reset(new callback_for_read_from_s3_to_cache<buffer_type>
                            (bucket_context_));
                    static_cast<callback_for_read_from_s3_to_cache<buffer_type>*>
                        (read_callback_data.get())->set_cache_fd(cache_fd_);
                } else {
                    read_callback_data.reset(new callback_for_read_from_buffer<buffer_type>(bucket_context_));
                    static_cast<callback_for_read_from_buffer<buffer_type>*>(read_callback_data.get())
                        ->set_output_buffer(buffer);
                    static_cast<callback_for_read_from_buffer<buffer_type>*>(read_callback_data.get())
                        ->set_output_buffer_size(length);

                }
                read_callback_data->content_length = length;
                read_callback_data->offset = 0;
                read_callback_data->debug_flag = debug_flag_;

                if (debug_flag_) {
                    msg.str( std::string() ); // Clear
                    msg << "Multirange:  Start range key \"" << object_key_ << "\", offset "
                        << static_cast<long>(read_callback_data->offset) << ", len "
                        << static_cast<int>(read_callback_data->content_length);
                    printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str());
                }

                unsigned long long usStart = get_time_in_microseconds();
                print_bucket_context(bucket_context_);
                S3_get_object( &bucket_context_, object_key_.c_str(), NULL,
                        file_offset_,
                        read_callback_data->content_length, 0,
                        &get_object_handler, read_callback_data.get() );

                if (debug_flag_) {
                    unsigned long long usEnd = get_time_in_microseconds();
                    double bw = (read_callback_data->content_length / (1024.0*1024.0)) /
                        ( (usEnd - usStart) / 1000000.0 );
                    msg << " -- END -- BW=" << bw << " MB/s";
                    printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                            object_identifier_, msg.str().c_str());
                }

                if (read_callback_data->status != S3StatusOK) s3_sleep( retry_wait_seconds_, 0 );

            } while ((read_callback_data->status != S3StatusOK)
                    && S3_status_is_retryable(read_callback_data->status)
                    && (++retry_cnt < retry_count_limit_));

            if (read_callback_data->status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << " - Error getting the S3 object: \"" << object_key_ << " ";
                if (read_callback_data->status >= 0) {
                    msg << " - \"" << S3_get_status_name( read_callback_data->status ) << "\"";
                }
                printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                        object_identifier_, msg.str().c_str());
            }

            if (read_callback_data->status != S3StatusOK) {

                // update the last error in shmem

                auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;
                named_shared_memory_object shm_obj{shared_memory_name,
                    shared_memory_timeout_in_seconds_,
                    constants::MAX_S3_SHMEM_SIZE};

                shm_obj.atomic_exec([](auto& data) {
                    data.last_irods_error_code = S3_GET_ERROR;
                });
            }

        } // end s3_download_part_worker_routine

        // this function is called in the background in a separate thread
        void s3_upload_part_worker_routine()
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <shared_data::multipart_shared_data>;

            // read upload_id from shmem
            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                constants::MAX_S3_SHMEM_SIZE};

            std::string upload_id =  shm_obj.atomic_exec([](auto& data) {
                return data.upload_id.c_str();
            });

            std::stringstream msg;
            S3PutObjectHandler putObjectHandler
                = { {s3_multipart_upload::part_transport_callback::response_properties<buffer_type>,
                     s3_multipart_upload::part_transport_callback::response_completion<buffer_type> },
                    &s3_multipart_upload::part_transport_callback::put_data<buffer_type> };

            multipart_data partData{upload_manager_, bucket_context_, circular_buffer_};
            upload_page<buffer_type> page;

            // read the first page
            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_);
            }
            circular_buffer_.pop_front(page);
            if (debug_flag_) {
                printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%lu][terminate_flag=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, page.buffer.data(), page.buffer.size(),
                        page.terminate_flag);
            }

            int retry_cnt = 0;

            // determine the sequence number from the offset, file size, and buffer size
            // the last page might be larger so doing a little trick to handle that case (second term)
            int sequence = (file_offset_ / part_size_) + (file_offset_ % part_size_ == 0 ? 0 : 1) + 1;

            // estimate the size and resize the etags vector
            int number_of_parts = object_size_ / part_size_;
            number_of_parts = number_of_parts < sequence ? sequence : number_of_parts;

            // resize the etags vector if necessary
            shm_obj.atomic_exec([number_of_parts, &shm_obj](auto& data) {

                if (number_of_parts > data.etags.size()) {
                    try {
                        data.etags.resize(number_of_parts, types::shm_char_string("", shm_obj.get_allocator()));
                    } catch (std::bad_alloc& ba) {
                        data.last_irods_error_code = SYS_MALLOC_ERR;
                    }
                }

            });

            do {

                // Work on a local copy of the structure in case an error occurs in the middle
                // of an upload.  If we updated in-place, on a retry the part would start
                // at the wrong offset and length.
                partData.mode = mpu_data_.mode;
                partData.status = mpu_data_.status;
                partData.enable_md5 = mpu_data_.enable_md5;
                partData.server_encrypt = mpu_data_.server_encrypt;
                partData.object_identifier = mpu_data_.object_identifier;
                partData.debug_flag = debug_flag_;
                partData.sequence = sequence;
                partData.shared_memory_timeout_in_seconds = shared_memory_timeout_in_seconds_;
                partData.put_object_data.saved_bucket_context = bucket_context_;
                partData.put_object_data.buffer = page.buffer;
                partData.put_object_data.content_length = part_size_;
                partData.put_object_data.object_identifier = object_identifier_;

                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  Start part " << static_cast<int>(sequence) << ", key \""
                        << object_key_ << "\", uploadid \"" << upload_id
                        << "\", len " << static_cast<int>(partData.put_object_data.content_length);
                    printf( "%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str() );
                }

                put_props_.md5 = nullptr;
                //S3PutProperties putProps;
                //if ( partData.enable_md5 ) {
                //    // jjames - not sure how to do MD5 piecewise
                //    putProps->md5 = s3CalcMD5( partData.put_object_data.fd,
                //    partData.put_object_data.offset,
                //    partData.put_object_data.content_length, resource_name );
                //}
                put_props_.expires = -1;
                unsigned long long usStart = get_time_in_microseconds();

                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] S3_upload_part (ctx, key, props, handler, %d, "
                           "uploadId, %lu, 0, partData)\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                           sequence, partData.put_object_data.content_length);
                }

                S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props_,
                        &putObjectHandler, sequence, upload_id.c_str(),
                        part_size_, 0, &partData);

                if (debug_flag_) {
                    printf("%s:%d (%s) [[%d]] S3_upload_part returned [part=%d].\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_, sequence);
                }

                unsigned long long usEnd = get_time_in_microseconds();
                //double bw = (mpu_data_[sequence-1].put_object_data.content_length /
                //   (1024.0 * 1024.0)) /
                //   ( (usEnd - usStart) / 1000000.0 );
                if (debug_flag_) {
                    std::stringstream msg;
                    msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                    printf( "%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str() );
                }
                if (partData.status != S3StatusOK) s3_sleep( retry_wait_seconds_, 0 );
            } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) &&
                    (++retry_cnt < retry_count_limit_));

            if (partData.status != S3StatusOK) {

                shm_obj.atomic_exec([](auto& data) {
                    data.last_irods_error_code = S3_PUT_ERROR;
                });
            }
        }

        int                        fd_;
        nlohmann::json             fd_info_;
        uint64_t                   object_size_;
        int                        number_of_transfer_threads_;
        uint64_t                   part_size_;
        int                        retry_count_limit_;
        int                        retry_wait_seconds_;
        std::string                hostname_;
        std::string                bucket_name_;
        std::string                access_key_;
        std::string                secret_access_key_;
        std::string                object_key_;
        S3BucketContext            bucket_context_;
        upload_manager             upload_manager_;
        S3SignatureVersion         s3_signature_version_;

        time_t                     shared_memory_timeout_in_seconds_;
        bool                       enable_md5_flag_;
        bool                       server_encrypt_flag_;

        bool                       debug_flag_;
        multipart_data<buffer_type> mpu_data_;
        bool                       call_s3_upload_part_flag_;
        bool                       call_s3_download_part_flag_;
        S3PutProperties            put_props_;

        irods::experimental::circular_buffer<upload_page<buffer_type>>
                                   circular_buffer_;
        std::thread                *begin_part_upload_thread_ptr_;

        std::ios_base::openmode    mode_;
        off_t                      file_offset_;
        bool                       multipart_flag_;
        int                        cache_fd_;
        std::string                cache_directory_;

        // operational modes based on input flags
        bool                       download_to_cache_;
        bool                       use_cache_;
        bool                       object_must_exist_;

        // just for debugging purposes
        int                        object_identifier_;

        inline static int          file_descriptor_counter_ = minimum_valid_file_descriptor;

        // this counter keeps track of whether this process has initialized
        // S3.  If we are in a multithreaded / single process environment the
        // initialization will run only once.  If we are in a multiprocess
        // environment it will run multiple times.
        inline static int          s3_initialized_counter_ = 0;

    }; // s3_transport

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

