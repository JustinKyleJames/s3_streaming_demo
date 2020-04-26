#include "catch.hpp"

#include "s3_transport.hpp"
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

void upload_part(const char* const hostname,
                 const char* const bucket_name,
                 const char* const access_key,
                 const char* const secret_access_key,
                 const char* const filename,
                 const int thread_count,
                 int thread_number)
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
    s3_config.object_size = file_size;
    s3_config.number_of_transfer_threads = thread_count;
    s3_config.part_size = current_buffer_size;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.thread_identifier = thread_number;
    s3_config.debug_flag = false;
    s3_config.multipart_flag = true;
    s3_config.shared_memory_timeout_in_seconds = 60;

    s3_transport tp1{s3_config};
    odstream ds1{tp1, filename};

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


// to test downloading file to cache
void read_write_on_file(const char *hostname, const char *bucket_name,
                        const char *access_key,
                        const char *secret_access_key,
                        const char *filename,
                        const int thread_count,
                        int thread_number,
                        const char *comparison_filename)
{

    printf("%s:%d (%s) [[%d]] [open file for read/write]\n",
            __FILE__, __LINE__, __FUNCTION__, thread_number);

    std::fstream fs;
    fs.open(comparison_filename, std::ios::out | std::ios::in);
    if (!fs.good()) {
        throw std::runtime_error("failed to open/create comparison file");
    }

    s3_transport_config s3_config;
    s3_config.number_of_transfer_threads = thread_count;
    s3_config.part_size = 0;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.thread_identifier = thread_number;
    s3_config.debug_flag = false;
    s3_config.multipart_flag = false;
    s3_config.shared_memory_timeout_in_seconds = 60;

    s3_transport tp1{s3_config};
    dstream ds1{tp1, filename};

    REQUIRE(ds1.is_open());

    if (thread_number == 0) {

        // test offset write from end
        std::string write_string = "adding this to end\n";
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
        REQUIRE(std::string(read_str) == std::string(read_str_comparison));

        // seek current and read
        ds1.seekg(10, std::ios_base::cur);
        ds1.read(read_str, 5);
        fs.seekg(10, std::ios_base::cur);
        fs.read(read_str_comparison, 5);
        REQUIRE(std::string(read_str) == std::string(read_str_comparison));

        // seek negative from end and read
        ds1.seekg(-20, std::ios_base::end);
        ds1.read(read_str, 20);
        fs.seekg(-20, std::ios_base::end);
        fs.read(read_str_comparison, 20);
        REQUIRE(std::string(read_str) == std::string(read_str_comparison));
    }

    fs.close();

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(2s);
    // will be automatic
    ds1.close();
    printf("CLOSE DONE FOR %d\n", thread_number);
}

namespace fs = irods::experimental::filesystem;
namespace io = irods::experimental::io;

std::string keyfile = "/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair";
std::string hostname = "s3.amazonaws.com";
std::string bucket_name = "justinkylejames1";   // TODO random bucket with auto generation

TEST_CASE("s3_transport_upload_large_multiple_threads", "[upload][thread][large]")
{

    SECTION("upload large file with multiple threads")
    {
        // multiple threads

        const int thread_count = 8;
        const std::string filename = "large_file";

        // clean up from a previous test, ignore errors
        std::stringstream ss;
        ss << "aws s3 rm s3://" << bucket_name << "/"
           << filename;
        system(ss.str().c_str());

        ss.str("");
        ss << filename << ".downloaded";
        remove(ss.str().c_str());

        // done cleanup

        std::string access_key, secret_access_key;

        read_keys(keyfile, access_key, secret_access_key);

        std::thread *writer_threads = new std::thread[thread_count];

        for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

            writer_threads[thread_number] = std::thread(upload_part, hostname.c_str(),
                        bucket_name.c_str(), access_key.c_str(), secret_access_key.c_str(),
                        filename.c_str(), thread_count, thread_number);
        }

        for (int thread_number = 0; thread_number < thread_count; ++thread_number) {
            writer_threads[thread_number].join();
        }

        delete[] writer_threads;

        // download the file and compare (using s3 client with system calls for now)
        ss.str("");
        ss << "aws s3 cp s3://" << bucket_name << "/"
           << filename << " " << filename << ".downloaded";
        int download_return_val = system(ss.str().c_str());

        REQUIRE(0 == download_return_val);

        ss.str("");
        ss << "cmp -s " << filename << " " << filename << ".downloaded";
        int cmp_return_val = system(ss.str().c_str());

        REQUIRE(0 == cmp_return_val);

    }
}

TEST_CASE("s3_transport_upload_large_multiple_processes", "[upload][process][large]")
{
    SECTION("upload large file with multiple processes")
    {
        // multiple threads

        const int process_count = 8;
        const std::string filename = "large_file";

        // clean up from a previous test, ignore errors
        std::stringstream ss;
        ss << "aws s3 rm s3://" << bucket_name << "/"
           << filename;
        system(ss.str().c_str());

        ss.str("");
        ss << filename << ".downloaded";
        remove(ss.str().c_str());
        // done cleanup

        std::string access_key, secret_access_key;

        read_keys(keyfile, access_key, secret_access_key);

        for (int process_number = 0; process_number < process_count; ++process_number) {

            int pid = fork();

            if (0 == pid) {
                upload_part(hostname.c_str(), bucket_name.c_str(), access_key.c_str(),
                        secret_access_key.c_str(), filename.c_str(), process_count, process_number);
                return;
            }

            printf("%s:%d (%s) [%d] started process %d\n", __FILE__, __LINE__, __FUNCTION__,
                    getpid(), pid);
        }

        int pid;
        while ((pid = wait(nullptr)) > 0) {
            printf("%s:%d (%s) process %d finished\n", __FILE__, __LINE__, __FUNCTION__, pid);
        }

        // download the file and compare (using s3 client with system calls for now)
        ss.str("");
        ss.clear();
        ss << "aws s3 cp s3://" << bucket_name << "/"
           << filename << " " << filename << ".downloaded";
        int download_return_val = system(ss.str().c_str());

        REQUIRE(0 == download_return_val);

        ss.str("");
        ss << "cmp -s " << filename << " " << filename << ".downloaded";
        int cmp_return_val = system(ss.str().c_str());

        REQUIRE(0 == cmp_return_val);

    }
}

TEST_CASE("s3_transport_readwrite_thread", "[rw][thread][small]")
{
    SECTION("read write small file")
    {
        // multiple threads

        const int thread_count = 8;
        const std::string filename = "small_file";

        // stage the file to s3 and cleanup
        std::stringstream ss;
        ss << "aws s3 cp " << filename << " s3://" << bucket_name << "/"
           << filename;
        system(ss.str().c_str());

        std::stringstream filename_ss;
        filename_ss << filename << ".downloaded";
        std::string downloaded_filename = filename_ss.str();
        filename_ss.str("");;
        filename_ss << filename << ".comparison";
        std::string comparison_filename = filename_ss.str();

        remove(downloaded_filename.c_str());
        // done staging and cleanup

        ss.str("");
        ss << "cp " << filename << " " << comparison_filename;
        system(ss.str().c_str());

        std::string access_key, secret_access_key;

        read_keys(keyfile, access_key, secret_access_key);

        std::thread *writer_threads = new std::thread[thread_count];

        for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

            writer_threads[thread_number] = std::thread(read_write_on_file, hostname.c_str(),
                        bucket_name.c_str(), access_key.c_str(), secret_access_key.c_str(),
                        filename.c_str(), thread_count, thread_number, comparison_filename.c_str());
        }

        for (int thread_number = 0; thread_number < thread_count; ++thread_number) {
            writer_threads[thread_number].join();
        }

        delete[] writer_threads;

        // download the file and compare (using s3 client with system calls for now)
        ss.str("");
        ss << "aws s3 cp s3://" << bucket_name << "/"
           << filename << " " << downloaded_filename;
        int download_return_val = system(ss.str().c_str());

        REQUIRE(0 == download_return_val);

        ss.str("");
        ss << "cmp -s " << downloaded_filename << " " << comparison_filename;
        int cmp_return_val = system(ss.str().c_str());

        REQUIRE(0 == cmp_return_val);

    }
}

