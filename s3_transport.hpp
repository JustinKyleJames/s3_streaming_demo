#ifndef S3_TRANSPORT_HPP
#define S3_TRANSPORT_HPP

#include "circular_buffer.hpp"

// iRODS includes
#include <rcMisc.h>
#include <transport/transport.hpp>
#include <fileLseek.h>
#include <rs_get_file_descriptor_info.hpp>
#include <thread_pool.hpp>

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
#include "managed_shared_memory_object.hpp"
#include "s3_multipart_shared_data.hpp"
#include "s3_transport_types.hpp"
#include "s3_transport_util.hpp"
#include "s3_transport_callbacks.hpp"

namespace irods::experimental::io::s3_transport
{

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

        using named_shared_memory_object =
            irods::experimental::interprocess::shared_memory::named_shared_memory_object
            <shared_data::multipart_shared_data>;

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
            , config_{_config}
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

            mpu_data_.enable_md5 = config_.enable_md5_flag;
            mpu_data_.server_encrypt = config_.server_encrypt_flag;
            mpu_data_.object_identifier = object_identifier_;
            mpu_data_.shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;

            upload_manager_.debug_flag = config_.debug_flag;
            upload_manager_.shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;
            mpu_data_.debug_flag = config_.debug_flag;

            bucket_context_.hostName        = config_.hostname.c_str();
            bucket_context_.bucketName      = config_.bucket_name.c_str();
            bucket_context_.accessKeyId     = config_.access_key.c_str();
            bucket_context_.secretAccessKey = config_.secret_access_key.c_str();

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
                begin_part_upload_thread_ptr_ = nullptr;
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

            if (!is_open()) {
                return false;
            }


            fd_ = uninitialized_file_descriptor;

            int open_flags = populate_open_mode_flags();

            if ( is_full_multipart_upload(open_flags) ) {

                // This was a full multipart upload, wait for the upload to complete

                if (config_.debug_flag) {
                    printf("%s:%d (%s) [[%d]] wait for join of upload thread\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_);
                }

                // upload was in background.  wait for it to complete.
                if (begin_part_upload_thread_ptr_) {
                    begin_part_upload_thread_ptr_->join();
                    begin_part_upload_thread_ptr_ = nullptr;
                }

                if (config_.debug_flag) {
                    printf("%s:%d (%s) [[%d]] join for part\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_);
                }
            }

            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                constants::DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                constants::MAX_S3_SHMEM_SIZE};

            int file_open_counter;

            // do close processing if # files open = 0
            //  - for multipart upload send the complete message
            //  - if using a cache file flush the cache and delete cache file
            bool close_processing_success_flag = shm_obj.atomic_exec([this, &file_open_counter, &shm_obj, open_flags](auto& data) {

                file_open_counter = data.file_open_counter > 0 ? --(data.file_open_counter) : 0;

                if (file_open_counter == 0) {

                    if (use_cache_) {

                        flush_cache_file(shm_obj);


                    } else if ( is_full_multipart_upload(open_flags) ) {
                        if (0 > complete_multipart_upload()) {
                            // error - remove shmem object
                            shm_obj.remove();
                            return false;
                        }
                    }

                    if (config_.debug_flag) {
                        printf("%s:%d (%s) [[%d]] [file_open_counter=%d]\n",
                                __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                                file_open_counter);
                    }

                }

                return true;

            });

            // TODO is this opening up to a race condition around file_open_counter and shm_obj?
            if (file_open_counter == 0) {
                shm_obj.remove();
            }

            if (!close_processing_success_flag) {
                return close_processing_success_flag;
            }

            // each process must initialize and deinitiatize.
            {
                std::lock_guard<std::mutex> lock(s3_initialized_counter_mutex_);
                if (--s3_initialized_counter_ == 0) {
                    S3_deinitialize();
                }
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

            if (config_.debug_flag) {
                printf("%s:%d (%s) [[%d]] wrote buffer of size %ld\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, _buffer_size);
            }

            // if config_.part_size is 0 then bail
            if (0 == config_.part_size) {
                return 0;
            }

            // if we haven't already started an upload thread, start it
            if (!begin_part_upload_thread_ptr_) {
                begin_part_upload_thread_ptr_ = std::make_unique<std::thread>(&s3_transport::s3_upload_part_worker_routine, this);
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
                    cache_fstream_.seekg(_offset, _dir);
                    return cache_fstream_.tellg();
                    /*case std::ios_base::beg:
                        cache_fstream_.seekg(offset);
                        return tellg();

                    case std::ios_base::cur:
                        cache_fstream_.seekg(offset, std::ios_base::cur);
                        return tellg();
                        //return lseek(cache_fd_, _offset, SEEK_CUR);

                    case std::ios_base::end:
                        cache_fstream_.seekg(offset, std::ios_base::end);
                        return tellg();
                        //return lseek(cache_fd_, _offset, SEEK_END);*/

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
                        file_offset_ = config_.object_size + _offset;
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

        bool perform_multipart_upload(named_shared_memory_object& shm_obj, const int& file_open_counter)
        {
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

            return true;
        }

        void download_object_to_cache(named_shared_memory_object& shm_obj)
        {

            namespace bf = boost::filesystem;

            // TODO not yet tested

            // TODO not sure how boost::filesystem will handle this if object_key_ has a directory structure in it. test.
            bf::path cache_file =  bf::path(config_.cache_directory) / bf::path(object_key_ + "-cache");
            cache_file_path_ = cache_file.string();

            bool cache_file_download_started_flag = shm_obj.atomic_exec([](auto& data) {
                bool return_val = data.cache_file_download_started_flag;
                data.cache_file_download_started_flag = true;
                return return_val;
            });


            if (!cache_file_download_started_flag) {

                // go ahead and download the object to cache file

                uint64_t part_size = config_.object_size / config_.number_of_transfer_threads;

                unsigned long long usStart = get_time_in_microseconds();

                irods::thread_pool threads{config_.number_of_transfer_threads};

                /*std::list<std::thread*> threads;
                for (int thr_id=0; thr_id < config_.number_of_transfer_threads; thr_id++) {

                    uint64_t this_part_size;
                    if (thr_id == config_.number_of_transfer_threads - 1) {
                        this_part_size = part_size + (config_.object_size - part_size * config_.number_of_transfer_threads);
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
                }*/

                {
                    int thread_cntr = 0;
                    std::mutex thread_cntr_mutex;

                    irods::thread_pool threads{config_.number_of_transfer_threads};
                    irods::thread_pool::post(threads, [this, part_size, &thread_cntr, &thread_cntr_mutex] () {

                            int thr_id;
                            {
                                std::lock_guard<std::mutex> lock(thread_cntr_mutex);
                                thr_id = thread_cntr++;
                            }

                            off_t this_part_offset = part_size * thr_id;
                            uint64_t this_part_size;
                            if (thr_id == config_.number_of_transfer_threads - 1) {
                                this_part_size = part_size + (config_.object_size - part_size * config_.number_of_transfer_threads);
                            } else {
                                this_part_size = part_size;
                            }
                            this->s3_download_part_worker_routine(nullptr, this_part_size, this_part_offset);
                    });

                    shm_obj.atomic_exec([](auto& data) {
                        data.cache_file_download_completed_flag = true;
                    });
                }
            }

            // download has started so we must wait until it completes before continuing
            // TODO figure out a better way to do this than a busy wait
            //      can't use std::condition_variable because of the multiprocess case
            //      get it working and then fix it
            while (!(shm_obj.atomic_exec([](auto& data) { return data.cache_file_download_completed_flag; }))) {
                sleep(1);
            }
        }

        void flush_cache_file(named_shared_memory_object& shm_obj) {

            namespace bf = boost::filesystem;

            // Flush the cache file to S3.

            bf::path cache_file =  bf::path(config_.cache_directory) / bf::path(object_key_ + "-cache");
            cache_file_path_ = cache_file.string();

            // calculate the part size
            std::ifstream ifs;
            ifs.open(cache_file_path_.c_str(), std::ios::out);
            ifs.seekg(0, std::ios_base::end);
            off_t cache_file_size = ifs.tellg();
            uint64_t part_size = cache_file_size / config_.number_of_transfer_threads;
            ifs.close();



            {
                int thread_cntr = 0;
                std::mutex thread_cntr_mutex;

                irods::thread_pool cache_flush_threads{config_.number_of_transfer_threads};
                irods::thread_pool::post(cache_flush_threads, [this, part_size, &thread_cntr, &thread_cntr_mutex]() {

                        int thr_id;
                        {
                            std::lock_guard<std::mutex> lock(thread_cntr_mutex);
                            thr_id = thread_cntr++;
                        }

                        uint64_t buffer_size;
                        if (thr_id == config_.number_of_transfer_threads - 1) {
                            buffer_size = part_size + (config_.object_size - part_size * config_.number_of_transfer_threads);
                        } else {
                            buffer_size = part_size;
                        }

                        // TODO
                    });
            }

            shm_obj.atomic_exec([](auto& data) {
                data.cache_file_download_completed_flag = true;
            });

        }

        bool is_full_multipart_upload(int open_flags) {
            return ( config_.multipart_flag && is_full_upload(open_flags) );
        }

        bool is_full_upload(int open_flags) {
            return ( (O_CREAT | O_WRONLY | O_TRUNC) == open_flags );
        }

        // This populates the following flags based on the open mode (mode_).
        //   - download_to_cache_ - Set to true iff:
        //                             * the object is open for writing (optionally reading)
        //                               and the object is not being truncated
        //   - use_cache_         - Set to true iff one of the following applies:
        //                             * download_to_cache_ is true
        //                             * open for reading and writing with the truncate flag
        //   - object_must_exist_ - See table "Action if file does not exist" in the table at the
        //                          following link: https://en.cppreference.com/w/cpp/io/basic_filebuf/open
        //
        // Return value - The translation of the stream open modes to posix open modes.
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
                return O_CREAT | O_RDWR | O_APPEND;
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

            if (config_.debug_flag) {
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

            if (config_.debug_flag) {
                printf("%s:%d (%s) [[%d]] [config_.multipart_flag = %d][use_cache_ = %d]"
                    "[O_WRONLY = %d][O_RDONLY = %d]\n",
                    __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                    config_.multipart_flag,
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
            // note: locked from file_open_lock so no need to lock
            // with s3_initialized_counter_mutex_
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
                download_object_to_cache(shm_obj);
            }

            if ( is_full_upload(open_flags) ) {

                // this is a full file upload - do not use cache

                if ( is_full_multipart_upload(open_flags) ) {

                    bool multipart_upload_success = perform_multipart_upload(shm_obj, file_open_counter);
                    if (!multipart_upload_success) {
                        return false;
                    }

                } else {
                    // TODO - non multipart upload
                }
            } else if (open_flags == O_RDONLY) {

                // this is read only - do not use cache
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

            int cache_fd = -1;
            int err_status = 0;
            int retry_cnt    = 0;
            bool enable_md5 = config_.enable_md5_flag;
            put_props_.useServerSideEncryption = config_.server_encrypt_flag;
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
            data.debug_flag = config_.debug_flag;
            data.object_identifier = object_identifier_;

            // read shared memory entry for this key
            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // no lock here as it is already locked
            return shm_obj.exec([this, &retry_cnt](auto& data) {

                retry_cnt = 0;

                // These expect a upload_manager* as cbdata
                S3MultipartInitialHandler mpu_initial_handler
                    = { { s3_multipart_upload::initialization_callback::on_response_properties,
                          s3_multipart_upload::initialization_callback::on_response_complete },
                        s3_multipart_upload::initialization_callback::on_response };

                do {
                    print_bucket_context(bucket_context_);
                    if (config_.debug_flag) {
                        printf("%s:%d (%s) [[%d]] call S3_initiate_multipart [object_key=%s]\n",
                                __FILE__, __LINE__, __FUNCTION__, object_identifier_, object_key_.c_str());
                    }
                    S3_initiate_multipart(&bucket_context_, object_key_.c_str(),
                            &put_props_, &mpu_initial_handler, nullptr, &upload_manager_);

                    if (upload_manager_.status != S3StatusOK) {
                        s3_sleep( config_.retry_wait_seconds, 0 );
                    }

                } while ( (upload_manager_.status != S3StatusOK)
                        && S3_status_is_retryable(upload_manager_.status)
                        && ( ++retry_cnt < config_.retry_count_limit));

                if ("" == data.upload_id || upload_manager_.status != S3StatusOK) {
                    return S3_PUT_ERROR;
                }

                if (config_.debug_flag) {
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

            // read shared memory entry for this key
            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // read upload_id from shared_memory
            // no lock here as it is already locked
            std::string upload_id = shm_obj.exec([](auto& data) {
                return data.upload_id.c_str();
            });

            S3AbortMultipartUploadHandler abort_handler
                = { { s3_multipart_upload::cancel_callback::on_response_properties,
                      s3_multipart_upload::cancel_callback::on_response_completion } };

            std::stringstream msg;
            libs3_types::status status;

            if (config_.debug_flag) {
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

            auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;

            named_shared_memory_object shm_obj{shared_memory_name,
                config_.shared_memory_timeout_in_seconds,
                constants::MAX_S3_SHMEM_SIZE};

            // no lock here as it is already locked
            int result = shm_obj.exec([this](auto& data) {

                std::stringstream msg;
                unsigned int retry_cnt = 0;

                std::stringstream xml("");

                std::string upload_id  = data.upload_id.c_str();

                if (0 == data.last_irods_error_code) { // If someone aborted, don't complete...
                    if (config_.debug_flag) {
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
                        = { {s3_multipart_upload::commit_callback::on_response_properties,
                             s3_multipart_upload::commit_callback::on_response_completion },
                            s3_multipart_upload::commit_callback::on_response, nullptr };
                    do {
                        // On partial error, need to restart XML send from the beginning
                        upload_manager_.remaining = manager_remaining;
                        upload_manager_.xml = xml.str().c_str();

                        upload_manager_.offset = 0;
                        S3_complete_multipart_upload(&bucket_context_, object_key_.c_str(),
                                &commit_handler, upload_id.c_str(),
                                upload_manager_.remaining, nullptr, &upload_manager_);
                        if (config_.debug_flag) {
                            printf("%s:%d (%s) [[%d]] [manager.status=%s]\n", __FILE__, __LINE__,
                                    __FUNCTION__, object_identifier_, S3_get_status_name(upload_manager_.status));
                        }
                        if (upload_manager_.status != S3StatusOK) s3_sleep( config_.retry_wait_seconds, 0 );
                    } while ((upload_manager_.status != S3StatusOK) &&
                            S3_status_is_retryable(upload_manager_.status) &&
                            ( ++retry_cnt < config_.retry_count_limit));

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

        // download the part from the S3 object
        //   input:
        //     buffer - If not null the downloaded part is written to buffer.  Buffer must have
        //              length reserved.  If buffer is null the download is written to the cache file.
        //     length - The length to be downloaded.
        //     offset - If provided this is the offset of the object that is being downloaded.  If not
        //              provided the current offset (file_offset_) is used.
        void s3_download_part_worker_routine(char_type *buffer, uint64_t length, off_t offset = -1)
        {
            namespace bi = boost::interprocess;
            namespace types = shared_data::interprocess_types;

            std::stringstream msg;

            int retry_cnt = 0;

            if (0 > offset) {
                offset = file_offset_;
            }

            std::shared_ptr<callback_for_read_from_s3_base<buffer_type>> read_callback;

            do {

                S3GetObjectHandler get_object_handler = {
                    {
                        callback_for_read_from_s3_base<buffer_type>::on_response_properties,
                        callback_for_read_from_s3_base<buffer_type>::on_response_completion
                    },
                    callback_for_read_from_s3_base<buffer_type>::invoke_callback
                };

                if (buffer == nullptr) {
                    // Download to cache
                    read_callback.reset(new callback_for_read_from_s3_to_cache<buffer_type>
                            (bucket_context_));
                    static_cast<callback_for_read_from_s3_to_cache<buffer_type>*>
                        (read_callback.get())->set_and_open_cache_file(cache_file_path_);
                } else {
                    // Download to buffer
                    read_callback.reset(new callback_for_read_from_s3_to_buffer<buffer_type>(bucket_context_));
                    static_cast<callback_for_read_from_s3_to_buffer<buffer_type>*>(read_callback.get())
                        ->set_output_buffer(buffer);
                    static_cast<callback_for_read_from_s3_to_buffer<buffer_type>*>(read_callback.get())
                        ->set_output_buffer_size(length);

                }
                read_callback->content_length = length;
                read_callback->offset = 0;
                read_callback->debug_flag = config_.debug_flag;

                if (config_.debug_flag) {
                    msg.str( std::string() ); // Clear
                    msg << "Multirange:  Start range key \"" << object_key_ << "\", offset "
                        << static_cast<long>(read_callback->offset) << ", len "
                        << static_cast<int>(read_callback->content_length);
                    printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str());
                }

                unsigned long long usStart = get_time_in_microseconds();
                print_bucket_context(bucket_context_);
                S3_get_object( &bucket_context_, object_key_.c_str(), NULL,
                        offset, read_callback->content_length, 0,
                        &get_object_handler, read_callback.get() );

                if (config_.debug_flag) {
                    unsigned long long usEnd = get_time_in_microseconds();
                    double bw = (read_callback->content_length / (1024.0*1024.0)) /
                        ( (usEnd - usStart) / 1000000.0 );
                    msg << " -- END -- BW=" << bw << " MB/s";
                    printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                            object_identifier_, msg.str().c_str());
                }

                if (read_callback->status != S3StatusOK) s3_sleep( config_.retry_wait_seconds, 0 );

            } while ((read_callback->status != S3StatusOK)
                    && S3_status_is_retryable(read_callback->status)
                    && (++retry_cnt < config_.retry_count_limit));

            if (read_callback->status != S3StatusOK) {
                msg.str( std::string() ); // Clear
                msg << " - Error getting the S3 object: \"" << object_key_ << " ";
                if (read_callback->status >= 0) {
                    msg << " - \"" << S3_get_status_name( read_callback->status ) << "\"";
                }
                printf("%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__,
                        object_identifier_, msg.str().c_str());
            }

            if (read_callback->status != S3StatusOK) {

                // update the last error in shmem

                auto shared_memory_name =  object_key_ + constants::MULTIPART_SHARED_MEMORY_EXTENSION;
                named_shared_memory_object shm_obj{shared_memory_name,
                    config_.shared_memory_timeout_in_seconds,
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
                = { {s3_multipart_upload::part_transport_callback::on_response_properties<buffer_type>,
                     s3_multipart_upload::part_transport_callback::on_response_completion<buffer_type> },
                    &s3_multipart_upload::part_transport_callback::on_put_data<buffer_type> };

            multipart_data partData{upload_manager_, bucket_context_, circular_buffer_};
            upload_page<buffer_type> page;

            // read the first page
            if (config_.debug_flag) {
                printf("%s:%d (%s) [[%d]] waiting to read\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_);
            }
            circular_buffer_.pop_front(page);
            if (config_.debug_flag) {
                printf("%s:%d (%s) [[%d]] read page [buffer=%p][buffer_size=%lu][terminate_flag=%d]\n",
                        __FILE__, __LINE__, __FUNCTION__, object_identifier_, page.buffer.data(), page.buffer.size(),
                        page.terminate_flag);
            }

            int retry_cnt = 0;

            // determine the sequence number from the offset, file size, and buffer size
            // the last page might be larger so doing a little trick to handle that case (second term)
            //  Note:  We bailed early if config_.part_size == 0
            int sequence = (file_offset_ / config_.part_size) + (file_offset_ % config_.part_size == 0 ? 0 : 1) + 1;

            // estimate the size and resize the etags vector
            int number_of_parts = config_.object_size / config_.part_size;
            number_of_parts = number_of_parts < sequence ? sequence : number_of_parts;

            // resize the etags vector if necessary
            int resize_error = shm_obj.atomic_exec([number_of_parts, &shm_obj](auto& data) {

                if (number_of_parts > data.etags.size()) {
                    try {
                        data.etags.resize(number_of_parts, types::shm_char_string("", shm_obj.get_allocator()));
                    } catch (std::bad_alloc& ba) {
                        data.last_irods_error_code = SYS_MALLOC_ERR;
                        return true;
                    }
                }

                return false;

            });

            if (resize_error) {
                printf("Error on reallocation of etags buffer in shared memory.");
                return;
            }


            do {

                // Work on a local copy of the structure in case an error occurs in the middle
                // of an upload.  If we updated in-place, on a retry the part would start
                // at the wrong offset and length.
                partData.mode = mpu_data_.mode;
                partData.status = mpu_data_.status;
                partData.enable_md5 = mpu_data_.enable_md5;
                partData.server_encrypt = mpu_data_.server_encrypt;
                partData.object_identifier = mpu_data_.object_identifier;
                partData.debug_flag = config_.debug_flag;
                partData.sequence = sequence;
                partData.shared_memory_timeout_in_seconds = config_.shared_memory_timeout_in_seconds;
                partData.put_object_data.saved_bucket_context = bucket_context_;
                partData.put_object_data.buffer = page.buffer;
                partData.put_object_data.content_length = config_.part_size;
                partData.put_object_data.object_identifier = object_identifier_;

                if (config_.debug_flag) {
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

                if (config_.debug_flag) {
                    printf("%s:%d (%s) [[%d]] S3_upload_part (ctx, key, props, handler, %d, "
                           "uploadId, %lu, 0, partData)\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                           sequence, partData.put_object_data.content_length);
                }

                S3_upload_part(&bucket_context_, object_key_.c_str(), &put_props_,
                        &putObjectHandler, sequence, upload_id.c_str(),
                        config_.part_size, 0, &partData);

                if (config_.debug_flag) {
                    printf("%s:%d (%s) [[%d]] S3_upload_part returned [part=%d].\n",
                            __FILE__, __LINE__, __FUNCTION__, object_identifier_, sequence);
                }

                unsigned long long usEnd = get_time_in_microseconds();
                //double bw = (mpu_data_[sequence-1].put_object_data.content_length /
                //   (1024.0 * 1024.0)) /
                //   ( (usEnd - usStart) / 1000000.0 );
                if (config_.debug_flag) {
                    std::stringstream msg;
                    msg << "Multipart:  -- END -- BW=";// << bw << " MB/s";
                    printf( "%s:%d (%s) [[%d]] %s\n", __FILE__, __LINE__, __FUNCTION__, object_identifier_,
                            msg.str().c_str() );
                }
                if (partData.status != S3StatusOK) s3_sleep( config_.retry_wait_seconds, 0 );
            } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) &&
                    (++retry_cnt < config_.retry_count_limit));

            if (partData.status != S3StatusOK) {

                shm_obj.atomic_exec([](auto& data) {
                    data.last_irods_error_code = S3_PUT_ERROR;
                });
            }
        }

        const config&                config_;
            /*, config_.object_size{_config.object_size}
            , config_.number_of_transfer_threads{_config.number_of_transfer_threads}
            , config_.part_size{_config.part_size}
            , config_.retry_count_limit{_config.retry_count_limit}
            , config_.retry_wait_seconds{_config.retry_wait_seconds}
            , config_.hostname{_config.hostname}
            , config_.bucket_name{_config.bucket_name}
            , config_.access_key{_config.access_key}
            , config_.secret_access_key{_config.secret_access_key}
            , config_.multipart_flag{_config.multipart_flag}
            , config_.shared_memory_timeout_in_seconds{_config.shared_memory_timeout_in_seconds}
            , config_.enable_md5_flag{_config.enable_md5_flag}
            , config_.server_encrypt_flag{_config.server_encrypt_flag}
            , config_.cache_directory{_config.cache_directory}
            , config_.debug_flag{_config.debug_flag}*/

        int                          fd_;
        nlohmann::json               fd_info_;
        std::string                  object_key_;
        libs3_types::bucket_context  bucket_context_;
        upload_manager               upload_manager_;
        S3SignatureVersion           s3_signature_version_;

        multipart_data<buffer_type>  mpu_data_;
        bool                         call_s3_upload_part_flag_;
        bool                         call_s3_download_part_flag_;
        S3PutProperties              put_props_;

        irods::experimental::circular_buffer<upload_page<buffer_type>>
                                     circular_buffer_;
        std::unique_ptr<std::thread> begin_part_upload_thread_ptr_;

        std::ios_base::openmode      mode_;
        off_t                        file_offset_;

        std::string                  cache_file_path_;
        std::fstream                 cache_fstream_;
        int                          cache_fd_;

        // operational modes based on input flags
        bool                         download_to_cache_;
        bool                         use_cache_;
        bool                         object_must_exist_;

        // just for debugging purposes
        int                          object_identifier_;

        inline static int            file_descriptor_counter_ = minimum_valid_file_descriptor;

        // this counter keeps track of whether this process has initialized
        // S3.  If we are in a multithreaded / single process environment the
        // initialization will run only once.  If we are in a multiprocess
        // environment it will run multiple times.
        inline static int            s3_initialized_counter_ = 0;
        inline static std::mutex     s3_initialized_counter_mutex_;

    }; // s3_transport

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_HPP

