#include "catch.hpp"

#include "s3_transport.hpp"
#include "s3_transport_util.hpp"
#include "s3_multipart_shared_data.hpp"
#include <filesystem/filesystem.hpp>
#include <dstream.hpp>
#include <mutex>
#include <condition_variable>
//#include <jansson.h>
#include <fstream>
#include <thread>
#include <chrono>
#include <sys/wait.h>
#include <stdexcept>
#include <cstdio>

#include <string_view>

using odstream            = irods::experimental::io::odstream;
using idstream            = irods::experimental::io::idstream;
using dstream             = irods::experimental::io::dstream;
using s3_transport        = irods::experimental::io::s3_transport::s3_transport<char>;
using s3_transport_config = irods::experimental::io::s3_transport::config;

namespace fs = irods::experimental::filesystem;
namespace io = irods::experimental::io;

std::string keyfile = "/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair";
std::string hostname = "s3.amazonaws.com";
std::string bucket_name = "justinkylejames1";

void read_keys(const std::string& keyfile, std::string& access_key, std::string& secret_access_key)
{
    // open and read keyfile
    std::ifstream key_ifs;

    key_ifs.open(keyfile.c_str());
    if (!key_ifs.good()) {
        throw std::invalid_argument("could not open provided keyfile");
    }

    if (!std::getline(key_ifs, access_key)) {
        throw std::invalid_argument("could not read access key from provided keyfile");
    }
    if (!std::getline(key_ifs, secret_access_key)) {
        throw std::invalid_argument("could not read secret key from provided keyfile");
    }
}

void upload_stage_and_cleanup(const std::string& bucket_name, const std::string& filename,
        const std::string& object_prefix)
{
    // clean up from a previous test, ignore errors
    std::stringstream ss;
    ss << "aws --endpoint-url http://" << hostname << " s3 rm s3://" << bucket_name
        << "/" << object_prefix << filename;
    system(ss.str().c_str());

    ss.str("");
    ss << filename << ".downloaded";
    remove(ss.str().c_str());
}

void download_stage_and_cleanup(const std::string& bucket_name, const std::string& filename, const std::string& object_prefix)
{
    // stage file to s3 and cleanup from previous tests
    std::stringstream ss;
    ss << "aws --endpoint-url http://" << hostname << " s3 cp " << filename << " s3://" << bucket_name << "/" << object_prefix
       << filename;
    system(ss.str().c_str());

    ss.str("");
    ss << filename << ".downloaded";
    remove(ss.str().c_str());
}

void read_write_stage_and_cleanup(const std::string& bucket_name, const std::string& filename, const std::string& object_prefix)
{

    // stage the file to s3 and cleanup
    std::stringstream ss;
    ss << "aws --endpoint-url http://" << hostname << " s3 cp " << filename << " s3://" << bucket_name << "/" << object_prefix
       << filename;
    system(ss.str().c_str());

    std::stringstream filename_ss;
    filename_ss << filename << ".downloaded";
    std::string downloaded_filename = filename_ss.str();
    filename_ss.str("");;
    filename_ss << filename << ".comparison";
    std::string comparison_filename = filename_ss.str();

    remove(downloaded_filename.c_str());

    ss.str("");
    ss << "cp " << filename << " " << comparison_filename;
    system(ss.str().c_str());
}

void check_upload_results(const std::string& bucket_name, const std::string& filename, const std::string& object_prefix)
{
    // download the file and compare (using s3 client with system calls for now)
    std::stringstream ss;
    ss << "aws --endpoint-url http://" << hostname << " s3 cp s3://" << bucket_name << "/" << object_prefix
       << filename << " " << filename << ".downloaded";
    int download_return_val = system(ss.str().c_str());

    REQUIRE(0 == download_return_val);

    ss.str("");
    ss << "cmp -s " << filename << " " << filename << ".downloaded";
    int cmp_return_val = system(ss.str().c_str());

    REQUIRE(0 == cmp_return_val);
}

void check_download_results(const std::string& bucket_name, const std::string& filename, const std::string& object_prefix)
{
    // compare the downloaded file
    std::stringstream ss;
    ss << "cmp -s " << filename << " " << filename << ".downloaded";
    int cmp_return_val = system(ss.str().c_str());

    REQUIRE(0 == cmp_return_val);
}

void check_read_write_results(const std::string& bucket_name, const std::string& filename, const std::string& object_prefix)
{
    std::stringstream filename_ss;
    filename_ss << filename << ".downloaded";
    std::string downloaded_filename = filename_ss.str();
    filename_ss.str("");;
    filename_ss << filename << ".comparison";
    std::string comparison_filename = filename_ss.str();

    // download the file and compare (using s3 client with system calls for now)
    std::stringstream ss;
    ss << "aws --endpoint-url http://" << hostname << " s3 cp s3://" << bucket_name << "/" << object_prefix
       << filename << " " << downloaded_filename;
    int download_return_val = system(ss.str().c_str());

    REQUIRE(0 == download_return_val);

    ss.str("");
    ss << "cmp -s " << downloaded_filename << " " << comparison_filename;
    int cmp_return_val = system(ss.str().c_str());

    REQUIRE(0 == cmp_return_val);
}


void upload_part(const char* const hostname,
                 const char* const bucket_name,
                 const char* const access_key,
                 const char* const secret_access_key,
                 const char* const filename,
                 const char* const object_prefix,
                 const int thread_count,
                 int thread_number,
                 bool multipart_flag,
                 const std::string& s3_signature_version_str = "4",
                 const std::string& s3_protocol_str = "http",
                 const std::string& s3_sts_date_str = "date",
                 bool server_encrypt_flag = false)
{

    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate);
    if (!ifs.good()) {
        throw std::runtime_error("failed to open input file");
    }

    uint64_t file_size = ifs.tellg();
    uint64_t start = thread_number * (file_size / thread_count);

    // figure out my part
    uint64_t end = 0;
    if (thread_number == thread_count - 1) {
        end = file_size;
    } else {
        end = start + file_size / thread_count;
    }

    ifs.seekg(start, std::ios::beg);

    uint64_t current_buffer_size = end - start;

    printf("%s:%d (%s) [[%d]] [file_size=%lu][start=%lu][end=%lu][current_buffer_size=%lu]\n",
            __FILE__, __LINE__, __FUNCTION__,
            thread_number, file_size, start, end, current_buffer_size);

    // read your part
    char *current_buffer;
    try {
        current_buffer = new char[current_buffer_size];
    } catch(std::bad_alloc&) {
        throw std::runtime_error("failed to allocate memory for buffer");
    }

    ifs.read((char*)(current_buffer), current_buffer_size);

    s3_transport_config s3_config;
    s3_config.hostname = hostname;
    s3_config.object_size = file_size;
    s3_config.number_of_transfer_threads = thread_count;
    s3_config.part_size = current_buffer_size;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.debug_flag = true;
    s3_config.multipart_upload_flag = multipart_flag;
    s3_config.shared_memory_timeout_in_seconds = 10;
    s3_config.s3_signature_version_str = s3_signature_version_str;
    s3_config.s3_protocol_str = s3_protocol_str;
    s3_config.s3_sts_date_str = s3_sts_date_str;
    s3_config.server_encrypt_flag = server_encrypt_flag;

    s3_transport tp1{s3_config};
    odstream ds1{tp1, std::string(object_prefix)+filename};

    REQUIRE(ds1.is_open());

    ds1.seekp(start);

    // doing multiple writes of 10MiB here just to test that that works
    const uint64_t max_write_size = 10*1024*1024;
    uint64_t write_offset = 0;
    while (write_offset < current_buffer_size) {
        uint64_t write_size = std::min(max_write_size, current_buffer_size - write_offset);
        ds1.write(current_buffer + write_offset, write_size);
        write_offset += write_size;
    }

    // will be automatic
    ds1.close();

    delete[] current_buffer;

    ifs.close();
}

void download_part(const char* const hostname,
                   const char* const bucket_name,
                   const char* const access_key,
                   const char* const secret_access_key,
                   const char* const filename,  // original filename
                   const char* const object_prefix,
                   const int thread_count,
                   int thread_number)
{

    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate);
    if (!ifs.good()) {
        throw std::runtime_error("failed to open input file");
    }

    uint64_t file_size = ifs.tellg();

    // thread in irods only deal with sequential bytes.  figure out what bytes this
    // thread deals with
    size_t start = thread_number * (file_size / thread_count);
    size_t end = 0;
    if (thread_number == thread_count - 1) {
        end = file_size;
    } else {
        end = start + file_size / thread_count;
    }

    // open output stream for downloaded file
    std::ofstream ofs;
    ofs.open((std::string(filename) + std::string(".downloaded")).c_str(),
            std::ios::out | std::ios::binary);

    if (!ofs.good()) {
        fprintf(stderr, "failed to open file %s\n", filename);
        return;
    }

    size_t current_buffer_size = end - start;
    char *current_buffer = static_cast<char*>(malloc(current_buffer_size * sizeof(char)));

    s3_transport_config s3_config;
    s3_config.hostname = hostname;
    s3_config.object_size = file_size;
    s3_config.number_of_transfer_threads = 20;
    s3_config.part_size = 0;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.debug_flag = false;
    s3_config.multipart_upload_flag = false;
    s3_config.shared_memory_timeout_in_seconds = 10;

    s3_transport tp1{s3_config};

    idstream ds1{tp1, std::string(object_prefix)+filename};

    REQUIRE(ds1.is_open());

    ds1.seekg(start);
    ofs.seekp(start, std::ios::beg);

    size_t offset = 0;
    size_t max_read_length = 1024*1024;

    // break read up into parts like iRODS
    while (offset < current_buffer_size) {
        size_t read_size = offset + max_read_length < current_buffer_size
            ? max_read_length
            : current_buffer_size - offset;
        ds1.read(current_buffer, read_size);
        ofs.write(current_buffer, read_size);
        offset += read_size;
    }
    ofs.close();

    printf("READ DONE FOR %d\n", thread_number);

    // will be automatic
    ds1.close();
    printf("CLOSE DONE FOR %d\n", thread_number);

    free(current_buffer);

}

// to test downloading file to cache
void read_write_on_file(const char *hostname,
                        const char *bucket_name,
                        const char *access_key,
                        const char *secret_access_key,
                        const char *filename,
                        const char* const object_prefix,
                        const int thread_count,
                        int thread_number,
                        const char *comparison_filename,
                        std::ios_base::openmode open_modes)
{

    printf("%s:%d (%s) [[%d]] [open file for read/write]\n",
            __FILE__, __LINE__, __FUNCTION__, thread_number);

    std::fstream fs;
    fs.open(comparison_filename, open_modes);
    if (!fs.good()) {
        throw std::runtime_error("failed to open/create comparison file");
    }

    s3_transport_config s3_config;
    s3_config.hostname = hostname;
    s3_config.number_of_transfer_threads = thread_count;
    s3_config.part_size = 0;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.debug_flag = true;
    s3_config.multipart_upload_flag = false;
    s3_config.shared_memory_timeout_in_seconds = 10;

    s3_transport tp1{s3_config};
    dstream ds1{tp1, std::string(object_prefix)+filename, open_modes};

    REQUIRE(ds1.is_open());

    if (thread_number == 0) {

        // test offset write from end
        std::string write_string = "all of this text will be added to the end of the file. "
          "adding some more text so we have enough for the various seeks below in case "
          "the file was truncated.";

        ds1.seekp(0, std::ios_base::end);
        ds1.write(write_string.c_str(), write_string.length());
        fs.seekp(0, std::ios_base::end);
        fs.write(write_string.c_str(), write_string.length());

        // test offset write from beginning
        write_string = "xxx";
        ds1.seekg(10, std::ios_base::beg);
        ds1.write(write_string.c_str(), write_string.length());
        fs.seekg(10, std::ios_base::beg);
        fs.write(write_string.c_str(), write_string.length());

        // if appending to file just return
        if (open_modes & std::ios_base::app) {
            fs.close();
            ds1.close();
            printf("CLOSE DONE FOR %d\n", thread_number);
            return;
        }

        // test offset read
        char read_str[21];
        read_str[20] = 0;
        char read_str_comparison[21];
        read_str_comparison[20] = 0;

        // seek and read
        ds1.seekg(10, std::ios_base::beg);
        ds1.read(read_str, 20);
        fs.seekg(10, std::ios_base::beg);
        fs.read(read_str_comparison, 20);
        REQUIRE(std::string(read_str) == std::string(read_str_comparison));

        // read again
        ds1.read(read_str, 20);
        fs.read(read_str_comparison, 20);
        //REQUIRE(std::string(read_str) == std::string(read_str_comparison));

        // seek current and read
        ds1.seekg(10, std::ios_base::cur);
        ds1.read(read_str, 5);
        fs.seekg(10, std::ios_base::cur);
        fs.read(read_str_comparison, 5);
        //REQUIRE(std::string(read_str) == std::string(read_str_comparison));

        // seek negative from end and read
        ds1.seekg(-20, std::ios_base::end);
        ds1.read(read_str, 20);
        fs.seekg(-20, std::ios_base::end);
        fs.read(read_str_comparison, 20);
        //REQUIRE(std::string(read_str) == std::string(read_str_comparison));
    }

    fs.close();

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(2s);
    // will be automatic
    ds1.close();
    printf("CLOSE DONE FOR %d\n", thread_number);
}

void do_upload_process(const std::string& bucket_name,
                       const std::string& filename,
                       const std::string& object_prefix,
                       const std::string& keyfile,
                       int process_count)
{
    std::string access_key, secret_access_key;
    read_keys(keyfile, access_key, secret_access_key);

    upload_stage_and_cleanup(bucket_name, filename, object_prefix);

    for (int process_number = 0; process_number < process_count; ++process_number) {

        int pid = fork();

        if (0 == pid) {
            upload_part(hostname.c_str(), bucket_name.c_str(), access_key.c_str(),
                    secret_access_key.c_str(), filename.c_str(), object_prefix.c_str(),
                    process_count, process_number, true);
            return;
        }

        printf("%s:%d (%s) [%d] started process %d\n", __FILE__, __LINE__, __FUNCTION__,
                getpid(), pid);
    }

    int pid;
    while ((pid = wait(nullptr)) > 0) {
        printf("%s:%d (%s) process %d finished\n", __FILE__, __LINE__, __FUNCTION__, pid);
    }

    check_upload_results(bucket_name, filename, object_prefix);
}

void do_download_process(const std::string& bucket_name,
                         const std::string& filename,
                         const std::string& object_prefix,
                         const std::string& keyfile,
                         int process_count)
{
    std::string access_key, secret_access_key;
    read_keys(keyfile, access_key, secret_access_key);

    download_stage_and_cleanup(bucket_name, filename, object_prefix);

    for (int process_number = 0; process_number < process_count; ++process_number) {

        int pid = fork();

        if (0 == pid) {
            download_part(hostname.c_str(), bucket_name.c_str(), access_key.c_str(),
                    secret_access_key.c_str(), filename.c_str(), object_prefix.c_str(), process_count, process_number);
            return;
        }

        printf("%s:%d (%s) [%d] started process %d\n", __FILE__, __LINE__, __FUNCTION__,
                getpid(), pid);
    }

    int pid;
    while ((pid = wait(nullptr)) > 0) {
        printf("%s:%d (%s) process %d finished\n", __FILE__, __LINE__, __FUNCTION__, pid);
    }

    check_download_results(bucket_name, filename, object_prefix);
}

void do_upload_thread(const std::string& bucket_name,
                      const std::string& filename,
                      const std::string& object_prefix,
                      const std::string& keyfile,
                      int thread_count,
                      const std::string& s3_signature_version_str = "4",
                      const std::string& s3_protocol_str = "http",
                      const std::string& s3_sts_date_str = "both")
{
    std::string access_key, secret_access_key;
    read_keys(keyfile, access_key, secret_access_key);

    upload_stage_and_cleanup(bucket_name, filename, object_prefix);

    irods::thread_pool writer_threads{thread_count};

    for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

        irods::thread_pool::post(writer_threads, [bucket_name, access_key,
                secret_access_key, filename, object_prefix, thread_count, thread_number,
                s3_signature_version_str, s3_protocol_str, s3_sts_date_str] () {


            upload_part(hostname.c_str(), bucket_name.c_str(), access_key.c_str(), secret_access_key.c_str(),
                    filename.c_str(), object_prefix.c_str(), thread_count, thread_number, true, s3_signature_version_str,
                    s3_protocol_str, s3_sts_date_str, false);
        });
    }

    writer_threads.join();

    check_upload_results(bucket_name, filename, object_prefix);
}

void do_upload_single_part(const std::string& bucket_name,
                           const std::string& filename,
                           const std::string& object_prefix,
                           const std::string& keyfile,
                           const std::string& s3_signature_version_str = "4",
                           const std::string& s3_protocol_str = "http",
                           const std::string& s3_sts_date_str = "both",
                           bool server_encrypt_flag = false)
{
    std::string access_key, secret_access_key;
    read_keys(keyfile, access_key, secret_access_key);

    upload_stage_and_cleanup(bucket_name, filename, object_prefix);

    upload_part(hostname.c_str(), bucket_name.c_str(), access_key.c_str(),
            secret_access_key.c_str(), filename.c_str(), object_prefix.c_str(), 1, 0,
            false, s3_signature_version_str,
            s3_protocol_str, s3_sts_date_str, server_encrypt_flag);

    check_upload_results(bucket_name, filename, object_prefix);
}

void do_download_thread(const std::string& bucket_name,
                        const std::string& filename,
                        const std::string& object_prefix,
                        const std::string& keyfile,
                        int thread_count,
                        const std::string& s3_signature_version_str = "2",
                        const std::string& s3_protocol_str = "http")
{
    std::string access_key, secret_access_key;
    read_keys(keyfile, access_key, secret_access_key);

    download_stage_and_cleanup(bucket_name, filename, object_prefix);

    irods::thread_pool reader_threads{thread_count};

    for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

        irods::thread_pool::post(reader_threads, [bucket_name, access_key,
                secret_access_key, filename, object_prefix, thread_count, thread_number] () {


            download_part(hostname.c_str(), bucket_name.c_str(), access_key.c_str(),
                    secret_access_key.c_str(), filename.c_str(), object_prefix.c_str(),
                    thread_count, thread_number);
        });
    }

    reader_threads.join();

    check_download_results(bucket_name, filename, object_prefix);
}

void do_read_write_thread(const std::string& bucket_name,
                          const std::string& filename,
                          const std::string& object_prefix,
                          const std::string& keyfile,
                          int thread_count,
                          std::ios_base::openmode open_modes = std::ios_base::in | std::ios_base::out)
{
    std::string access_key, secret_access_key;
    read_keys(keyfile, access_key, secret_access_key);

    read_write_stage_and_cleanup(bucket_name, filename, object_prefix);

    std::string comparison_filename = filename + ".comparison";

    irods::thread_pool writer_threads{thread_count};

    for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

        irods::thread_pool::post(writer_threads, [bucket_name, access_key,
                secret_access_key, filename, object_prefix, thread_count, thread_number,
                comparison_filename, open_modes] () {


            read_write_on_file(hostname.c_str(), bucket_name.c_str(), access_key.c_str(), secret_access_key.c_str(),
                    filename.c_str(), object_prefix.c_str(), thread_count, thread_number, comparison_filename.c_str(),
                    open_modes);
        });
    }

    writer_threads.join();

    check_read_write_results(bucket_name, filename, object_prefix);
}


TEST_CASE("quick test", "[quick_test]")
{

    SECTION("upload large file with multiple threads")
    {
        int thread_count = 7;
        std::string filename = "large_file";
        std::string object_prefix = "dir1/dir2/";
        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }
}

/*TEST_CASE("shmem tests", "[shmem]")
{

    SECTION("test shmem with named mutex already locked")
    {
        using constants = irods::experimental::io::s3_transport::constants;
        namespace bi = boost::interprocess;

        // lock the named mutex and leave locked, will time out and shmem will be
        // created succcessfully
        std::string object_key = "dir1/dir2/large_file";
        std::string shmem_key = constants::SHARED_MEMORY_KEY_PREFIX +
                std::to_string(std::hash<std::string>{}(object_key));

        bi::named_mutex mutex(bi::open_or_create, shmem_key.c_str());
        mutex.lock();

        int thread_count = 7;
        std::string filename = "large_file";
        std::string object_prefix = "dir1/dir2/";
        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }
}*/

TEST_CASE("shmem tests 2", "[shmem2]")
{

    SECTION("test shmem with internal lock locked")
    {
        namespace bi = boost::interprocess;
        namespace bc = boost::container;

        using segment_manager       = bi::managed_shared_memory::segment_manager;
        using void_allocator        = bc::scoped_allocator_adaptor<bi::allocator<void, segment_manager>>;

        using constants = irods::experimental::io::s3_transport::constants;

        // recreate the structure that is used in the managed shared_memory_object

        const std::string SHARED_DATA_NAME{"SharedData"};
        struct ipc_object
        {
            ipc_object(void_allocator&& alloc_inst, time_t access_time)
                : thing(alloc_inst)
                , last_access_time_in_seconds(access_time)
            {}

            io::s3_transport::shared_data::multipart_shared_data thing;

            time_t last_access_time_in_seconds;
            bi::interprocess_recursive_mutex access_mutex;

        };

        std::string object_key = "dir1/dir2/large_file";
        std::string shmem_key = constants::SHARED_MEMORY_KEY_PREFIX +
                std::to_string(std::hash<std::string>{}(object_key));

        const time_t now = time(0);
        bi::managed_shared_memory shm{bi::open_or_create, shmem_key.c_str(), constants::MAX_S3_SHMEM_SIZE};
        ipc_object *object = shm.find_or_construct<ipc_object>(SHARED_DATA_NAME.c_str())
                        (  static_cast<void_allocator>(shm.get_segment_manager()), now);


        // set some inconsistent state in the object in shared memory
        // including leaving the interprocess recursive mutex locked
        // set access time to a value that is considered expired
        (object->thing.ref_count)++;
        (object->thing.file_open_counter)++;
        object->access_mutex.lock();
        object->last_access_time_in_seconds = now - 20;

        int thread_count = 7;
        std::string filename = "large_file";
        std::string object_prefix = "dir1/dir2/";
        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }
}

TEST_CASE("s3_transport_upload_multiple_threads", "[upload][thread]")
{
    int thread_count = 2;
    std::string filename = "medium_file";
    std::string object_prefix = "dir1/dir2/";

    SECTION("upload large file with multiple threads")
    {
        thread_count = 10;
        filename = "large_file";
        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }

    SECTION("upload medium file with multiple threads default settings")
    {
        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }

    SECTION("upload medium file with multiple threads under bucket root")
    {
        object_prefix = "";
        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }

    SECTION("upload medium file with multiple threads signature_version=v2")
    {
        const std::string s3_signature_version_str = "v2";
        const std::string s3_protocol_str = "https";
        const std::string s3_sts_date_str = "date";

        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str, s3_sts_date_str);
    }

    SECTION("upload medium file with multiple threads protocol=http")
    {
        const std::string s3_signature_version_str = "v4";
        const std::string s3_protocol_str = "http";
        const std::string s3_sts_date_str = "both";

        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str, s3_sts_date_str);
    }

    SECTION("upload medium file with multiple threads sts_date=amz")
    {
        const std::string s3_signature_version_str = "v4";
        const std::string s3_protocol_str = "https";
        const std::string s3_sts_date_str = "amz";

        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str, s3_sts_date_str);
    }

    SECTION("upload medium file with multiple threads sts_date=date")
    {
        const std::string s3_signature_version_str = "v4";
        const std::string s3_protocol_str = "https";
        const std::string s3_sts_date_str = "date";

        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str, s3_sts_date_str);
    }

    SECTION("upload medium file with multiple threads sts_date=both")
    {
        const std::string s3_signature_version_str = "v4";
        const std::string s3_protocol_str = "https";
        const std::string s3_sts_date_str = "both";

        do_upload_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str, s3_sts_date_str);
    }

    SECTION("upload medium file as single part")
    {
        thread_count = 1;
        do_upload_single_part(bucket_name, filename, object_prefix, keyfile);
    }

    SECTION("upload medium file as single part with server encrypt")
    {
        thread_count = 1;
        const std::string s3_signature_version_str = "4";
        const std::string s3_protocol_str = "http";
        const std::string s3_sts_date_str = "both";
        const bool server_encrypt_flag = true;

        do_upload_single_part(bucket_name, filename, object_prefix, keyfile, s3_signature_version_str,
                s3_protocol_str, s3_sts_date_str, server_encrypt_flag);
    }
}

TEST_CASE("s3_transport_download_large_multiple_threads", "[download][thread]")
{
    int thread_count = 2;
    std::string filename = "medium_file";
    std::string object_prefix = "dir1/dir2/";

    SECTION("download large file with multiple threads")
    {
        thread_count = 8;
        std::string filename = "large_file";
        do_download_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }

    SECTION("download medium file with multiple threads")
    {
        do_download_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }

    SECTION("download medium file under bucket root")
    {
        object_prefix = "";
        do_download_thread(bucket_name, filename, object_prefix, keyfile, thread_count);
    }

    SECTION("download medium file with multiple threads s3_signature_version=2")
    {
        std::string s3_signature_version_str = "2";
        std::string s3_protocol_str = "https";

        do_download_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str);
    }


    SECTION("download medium file with multiple threads protocol=http")
    {
        std::string s3_signature_version_str = "v4";
        std::string s3_protocol_str = "http";

        do_download_thread(bucket_name, filename, object_prefix, keyfile, thread_count,
                s3_signature_version_str, s3_protocol_str);
    }

}

TEST_CASE("s3_transport_upload_large_multiple_processes", "[upload_process][process]")
{
    int process_count = 8;
    std::string filename = "large_file";
    std::string object_prefix = "dir1/dir2/";

    SECTION("upload large file with multiple processes")
    {

        do_upload_process(bucket_name, filename, object_prefix, keyfile, process_count);
    }
}

TEST_CASE("s3_transport_download_large_multiple_processes", "[download_process][process]")
{
    int process_count = 8;
    std::string filename = "medium_file";
    std::string object_prefix = "dir1/dir2/";

    SECTION("upload large file with multiple processes")
    {
        do_download_process(bucket_name, filename, object_prefix, keyfile, process_count);
    }
}

TEST_CASE("s3_transport_readwrite_thread", "[rw][thread]")
{
    int thread_count = 1;
    std::string filename = "medium_file";
    std::string object_prefix = "dir1/dir2/";

    SECTION("read write small file")
    {
        thread_count = 8;
        filename = "small_file";
        do_read_write_thread(bucket_name, filename, object_prefix, keyfile, thread_count);

    }

    SECTION("read write medium file")
    {
        thread_count = 8;
        do_read_write_thread(bucket_name, filename, object_prefix, keyfile, thread_count);

    }

    SECTION("read write medium file open with truncate")
    {

        std::ios_base::openmode open_modes = std::ios_base::in | std::ios_base::out | std::ios_base::trunc;
        do_read_write_thread(bucket_name, filename, object_prefix, keyfile, thread_count, open_modes);

    }

    SECTION("read write medium file open with append")
    {

        std::ios_base::openmode open_modes = std::ios_base::in | std::ios_base::out | std::ios_base::app;
        do_read_write_thread(bucket_name, filename, object_prefix, keyfile, thread_count, open_modes);

    }
}

