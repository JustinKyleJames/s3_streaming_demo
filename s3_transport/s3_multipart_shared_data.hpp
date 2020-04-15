#ifndef S3_MULTIPART_TEST_SHARED_DATA
#define S3_MULTIPART_TEST_SHARED_DATA

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/list.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/container/scoped_allocator.hpp>

#include "s3_transport_types.hpp"

namespace irods::experimental::io::s3_transport::shared_data
{

    namespace interprocess_types
    {

        namespace bi = boost::interprocess;

        using segment_manager       = bi::managed_shared_memory::segment_manager;
        using void_allocator        = boost::container::scoped_allocator_adaptor
                                      <bi::allocator<void, segment_manager> >;
        using int_allocator         = bi::allocator<int, segment_manager>;
        using char_allocator        = bi::allocator<char, segment_manager>;
        using shm_int_vector        = bi::vector<int, int_allocator>;
        using shm_char_string       = bi::basic_string<char, std::char_traits<char>,
                                      char_allocator>;
        using char_string_allocator = bi::allocator<shm_char_string, segment_manager>;
        using shm_string_vector     = bi::vector<shm_char_string, char_string_allocator>;
    }

    // data that needs to be shared among different processes
    struct multipart_shared_data
    {
        using interprocess_recursive_mutex = boost::interprocess::interprocess_recursive_mutex;
        using error_codes = irods::experimental::io::s3_transport::error_codes;

        multipart_shared_data(const interprocess_types::void_allocator &allocator)
            : file_open_counter{0}
            , upload_id{allocator}
            , etags{allocator}
            , last_error_code{error_codes::SUCCESS}
            , cache_file_download_started_flag{false}
            , cache_file_download_completed_flag{false}
            , ref_count{0}
        {}

        void reset_fields()
        {
            file_open_counter = 0;
            upload_id = "";
            etags.clear();
            last_error_code = error_codes::SUCCESS;
            cache_file_download_started_flag = false;
            cache_file_download_completed_flag = false;
        }

        int                                   file_open_counter;
        interprocess_types::shm_char_string   upload_id;
        interprocess_types::shm_string_vector etags;
        error_codes                           last_error_code;
        bool                                  cache_file_download_started_flag;
        bool                                  cache_file_download_completed_flag;
        int                                   ref_count;

        interprocess_recursive_mutex file_open_close_mutex;

    };

}



#endif // S3_MULTIPART_TEST_SHARED_DATA
