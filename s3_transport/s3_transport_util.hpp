#ifndef S3_TRANSPORT_UTIL_HPP
#define S3_TRANSPORT_UTIL_HPP

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
#include <cstdio>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>
#include <ctime>

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
#include "s3_multipart_shared_data.hpp"

#include "s3_transport_types.hpp"

namespace irods::experimental::io::s3_transport
{

    struct constants
    {

        static const uint64_t           ETAG_SIZE{34};
        static const uint64_t           UPLOAD_ID_SIZE{128};
        static const uint64_t           MAX_S3_SHMEM_SIZE{sizeof(shared_data::multipart_shared_data) +
                                                          10000 * (ETAG_SIZE + 1) +
                                                          UPLOAD_ID_SIZE + 1};

        static const int                DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS{900};
        inline static const std::string SHARED_MEMORY_KEY_PREFIX{"s3-shm-"};
    };

    void print_bucket_context( const libs3_types::bucket_context& bucket_context );

    void store_and_log_status( libs3_types::status status,
                               const libs3_types::error_details *error,
                               const std::string& function,
                               const libs3_types::bucket_context& saved_bucket_context,
                               libs3_types::status& pStatus,
                               bool debug_flag);

    // Returns timestamp in usec for delta-t comparisons
    auto get_time_in_microseconds() -> uint64_t;

    // Sleep for *at least* the given time, plus some up to 1s additional
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(int _s,
                  int _ms );

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


    struct upload_manager
    {
        upload_manager(libs3_types::bucket_context& _saved_bucket_context)
            : saved_bucket_context{_saved_bucket_context}
            , remaining{0}
            , offset{0}
            , xml{""}
        {
        }

        libs3_types::bucket_context           saved_bucket_context;             /* To enable more detailed error messages */

        /* Below used for the upload completion command, need to send in XML */
        std::string              xml;

        // TODO derive types
        uint64_t                 remaining;
        uint64_t                 offset;
        bool                     debug_flag;
        libs3_types::status      status;            /* status returned by libs3 */
        std::string              object_key;
        std::string              shmem_key;
        time_t                   shared_memory_timeout_in_seconds;
    };

    template <typename buffer_type>
    struct data_for_write_callback
    {
        data_for_write_callback(libs3_types::bucket_context& _saved_bucket_context,
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

        libs3_types::bucket_context&
                            saved_bucket_context;   // To enable more detailed error messages
        bool                debug_flag;
        int                 thread_identifier;
    };

    template <typename buffer_type>
    struct multipart_data
    {
        multipart_data(upload_manager& _manager,
                       libs3_types::bucket_context& _bucket_context,
                       irods::experimental::circular_buffer<upload_page<buffer_type>>& _circular_buffer)
            : manager{_manager}
            , put_object_data{_bucket_context, _circular_buffer}
            , sequence{0}
            , status{S3StatusOK}
            , enable_md5{false}
            , server_encrypt{false}
            , debug_flag{false}
            , thread_identifier{0}
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
        int                                    thread_identifier;
        time_t                                 shared_memory_timeout_in_seconds;
    };


    struct data_for_head_callback
    {
        data_for_head_callback(libs3_types::bucket_context& _bucket_context, bool _debug_flag = false)
            : last_modified{0}
            , content_length{0}
            , status{S3StatusOK}
            , debug_flag{_debug_flag}
            , bucket_context{_bucket_context}
        {}

        time_t                             last_modified;
        uint64_t                           content_length;
        libs3_types::status                status;
        bool                               debug_flag;
        libs3_types::bucket_context&       bucket_context;
    };

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_UTIL_HPP
