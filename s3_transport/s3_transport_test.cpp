#include "s3_transport.hpp"
#include <filesystem/filesystem.hpp>
#include <dstream.hpp>
#include <mutex>
#include <condition_variable>
#include <jansson.h>
#include <fstream>
#include <thread>
#include <chrono>
#include <sys/wait.h>

const long transfer_buffer_size_for_parallel_transfer_in_megabytes = 4;

using odstream            = irods::experimental::io::odstream;
using idstream            = irods::experimental::io::idstream;
using dstream             = irods::experimental::io::dstream;
using s3_transport        = irods::experimental::io::s3_transport::s3_transport<char>;
using s3_transport_config = irods::experimental::io::s3_transport::config;

void upload_part(int thread_number, const int thread_count, const uint32_t file_size,
                 const bool debug_flag, const char *hostname, const char *bucket_name,
                 const char *access_key, const char *secret_access_key, const char *filename);

void download_part(int thread_number, const int thread_count, const uint32_t file_size,
                   const bool debug_flag, const char *hostname, const char *bucket_name,
                   const char *access_key, const char *secret_access_key, const char *filename);

void open_file_with_read_write(int thread_number, const int thread_count, const uint32_t file_size,
                               const bool debug_flag, const char *hostname,
                               const char *bucket_name, const char * access_key,
                               const char *secret_access_key, const char *filename);

void usage()
{
    std::cerr << "Usage:  s3_transport_test <config_file> <upload_file> ['upload'|'download'|'both'|'rw'] [s3_prefix]"
              << std::endl;
}

bool use_multipart_flag = true;
std::string s3_prefix = "";

int main(int argc, char **argv)
{

    setbuf(stdout, nullptr);

    namespace bi = boost::interprocess;

    if (argc < 3 || argc > 5) {
        usage();
        return 1;
    }

    std::string mode = "both";
    if (argc >= 4) {
        mode = argv[3];
        if (mode != "upload" && mode != "download" && mode != "both" && mode != "rw") {
            std::cerr << "mode must be upload|download|both|rw" << std::endl;
            usage();
        }
    }

    if (argc >= 5) {
        s3_prefix = argv[4];
    }

    std::string config_file = argv[1];
    std::string filename = argv[2];
    size_t multipart_size = transfer_buffer_size_for_parallel_transfer_in_megabytes*1024*1024;

    // read configuration file
    std::ifstream t(config_file);
    std::string config_str((std::istreambuf_iterator<char>(t)),
                             std::istreambuf_iterator<char>());
    json_t *root;
    json_error_t error;
    root = json_loads(config_str.c_str(), 0, &error);

    if(!root)
    {
        fprintf(stderr, "error: on line %d in %s: %s\n",
                error.line, config_file.c_str(), error.text);
        return 1;
    }

    json_t *keyfile_json_object = json_object_get(root, "keyfile");
    if(!json_is_string(keyfile_json_object))
    {
        fprintf(stderr, "error: keyfile missing or is not a string in %s\n",
                config_file.c_str());
        json_decref(root);
        return 1;
    }
    std::string keyfile = json_string_value(keyfile_json_object);

    json_t *hostname_json_object = json_object_get(root, "hostname");
    if(!json_is_string(hostname_json_object))
    {
        fprintf(stderr, "error: (%s): hostname missing or is not a string\n",
                config_file.c_str());
        json_decref(root);
        return 1;
    }
    std::string hostname = json_string_value(hostname_json_object);

    json_t *bucket_name_json_object = json_object_get(root, "bucket_name");
    if(!json_is_string(bucket_name_json_object))
    {
        fprintf(stderr, "error: (%s) bucket_name missing or is not a string\n",
                config_file.c_str());
        json_decref(root);
        return 1;
    }
    std::string bucket_name = json_string_value(bucket_name_json_object);

    json_t *thread_count_json_object = json_object_get(root, "thread_count");
    if(!json_is_integer(thread_count_json_object))
    {
        fprintf(stderr, "error: (%s) thread_count missing or is not an integer\n",
                config_file.c_str());
        json_decref(root);
        return 1;
    }
    size_t thread_count = json_integer_value(thread_count_json_object);

    json_t *debug_flag_json_object = json_object_get(root, "debug_flag");
    bool debug_flag = json_is_true(debug_flag_json_object) ? true : false;

    json_t *use_multiprocess_flag_json_object = json_object_get(root, "use_multiprocess_flag");
    bool use_multiprocess_flag = json_is_true(use_multiprocess_flag_json_object) ? true : false;

    json_t *use_multipart_flag_json_object = json_object_get(root, "use_multipart_flag");
    use_multipart_flag = json_is_true(use_multipart_flag_json_object) ? true : false;

    if (!use_multipart_flag) {
        use_multiprocess_flag = false;
        thread_count = 1;
    }

    // AWS
    std::string access_key;
    std::string secret_access_key;

    // open and read keyfile
    std::ifstream key_ifs;

    key_ifs.open(keyfile.c_str());
    if (!key_ifs.good()) {
        fprintf(stderr, "failed to open key file %s\n", keyfile.c_str());
        return 1;
    }

    if (!std::getline(key_ifs, access_key)) {
        std::cerr << "Key file does not have a access_key." << std::endl;
        return 1;
    }
    if (!std::getline(key_ifs, secret_access_key)) {
        std::cerr << "Key file does not have an secret_access_key." << std::endl;
        return 1;
    }

    // determine file size
    uint32_t file_size;
    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate);
    if (!ifs.good()) {
        fprintf(stderr, "failed to open file %s\n", filename.c_str());
        return 1;
    }

    file_size = ifs.tellg();
    ifs.close();

    if (mode == "upload" || mode == "both") {

        if (use_multiprocess_flag) {

            // multiple processes

            for (int process_number = 0; process_number < thread_count; ++process_number) {

                int pid = fork();

                if (0 == pid) {
                    upload_part(process_number, thread_count, file_size, debug_flag, hostname.c_str(), bucket_name.c_str(),
                                access_key.c_str(), secret_access_key.c_str(), filename.c_str());
                    return 0;
                }

                printf("%s:%d (%s) [%d] started process %d\n", __FILE__, __LINE__, __FUNCTION__,
                        getpid(), pid);
            }

            int pid;
            while ((pid = wait(nullptr)) > 0) {
                printf("%s:%d (%s) process %d finished\n", __FILE__, __LINE__, __FUNCTION__, pid);
            }

        } else {

            // multiple threads

            std::thread *writer_threads = new std::thread[thread_count];

printf("%s:%d (%s) =============== thread_count=%zu ==================\n", __FILE__, __LINE__, __FUNCTION__, thread_count);

            for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

                printf("%s:%d (%s) start thread %d\n", __FILE__, __LINE__, __FUNCTION__,
                        thread_number);

                writer_threads[thread_number] = std::move(std::thread(upload_part, thread_number,
                            thread_count, file_size, debug_flag, hostname.c_str(), bucket_name.c_str(),
                            access_key.c_str(), secret_access_key.c_str(), filename.c_str()));
            }


            for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

                printf("%s:%d (%s) calling join for writer thread %d\n", __FILE__, __LINE__,
                        __FUNCTION__, thread_number);

                writer_threads[thread_number].join();

                printf("%s:%d (%s) joined writer thread %d\n", __FILE__, __LINE__,
                        __FUNCTION__, thread_number);
            }

            delete[] writer_threads;
        }

    }

    printf("**************************************************************************\n");

    if (mode == "download" || mode == "both") {

        if (use_multiprocess_flag) {

            // multiple processes

            for (int process_number = 0; process_number < thread_count; ++process_number) {

                int pid = fork();
                if (pid == 0) {
                    download_part(process_number, thread_count, file_size, debug_flag, hostname.c_str(), bucket_name.c_str(),
                                access_key.c_str(), secret_access_key.c_str(), filename.c_str());
                    return 0;
                }
                printf("%s:%d (%s) [%d] started process %d\n", __FILE__, __LINE__, __FUNCTION__,
                        getpid(), pid);
            }

            int pid;
            while ((pid = wait(nullptr)) > 0) {
                printf("%s:%d (%s) process %d finished\n", __FILE__, __LINE__, __FUNCTION__, pid);
            }

        } else {

            // multiple threads

            std::thread *reader_threads = new std::thread[thread_count];

            for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {
                printf("%s:%d (%s) start reader thread %d\n", __FILE__, __LINE__, __FUNCTION__,
                        thread_number);
                reader_threads[thread_number] = std::move(std::thread(download_part, thread_number,
                            thread_count, file_size, debug_flag, hostname.c_str(), bucket_name.c_str(),
                            access_key.c_str(), secret_access_key.c_str(), filename.c_str()));
            }


            for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

                printf("%s:%d (%s) calling join for reader thread %d\n", __FILE__, __LINE__,
                        __FUNCTION__, thread_number);

                reader_threads[thread_number].join();

                printf("%s:%d (%s) joined reader thread %d\n", __FILE__, __LINE__,
                        __FUNCTION__, thread_number);
            }

            delete[] reader_threads;
        }

    }

    if (mode == "rw") {


        std::thread *rw_threads = new std::thread[thread_count];

        for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {
            printf("%s:%d (%s) start reader thread %d\n", __FILE__, __LINE__, __FUNCTION__,
                    thread_number);
            rw_threads[thread_number] = std::move(std::thread(open_file_with_read_write, thread_number,
                        thread_count, file_size, debug_flag, hostname.c_str(), bucket_name.c_str(),
                        access_key.c_str(), secret_access_key.c_str(), filename.c_str()));
        }


        for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {

            printf("%s:%d (%s) calling join for rw thread %d\n", __FILE__, __LINE__,
                    __FUNCTION__, thread_number);

            rw_threads[thread_number].join();

            printf("%s:%d (%s) joined wr thread %d\n", __FILE__, __LINE__,
                    __FUNCTION__, thread_number);
        }
        delete[] rw_threads;

    }

    return 0;

}

// to test downloading file to cache
void open_file_with_read_write(int thread_number,
                               const int thread_count,
                               const uint32_t file_size,
                               const bool debug_flag,
                               const char *hostname,
                               const char *bucket_name,
                               const char * access_key,
                               const char *secret_access_key,
                               const char *filename)
{

    printf("%s:%d (%s) [open file for read/write thread=%u, seq=%u]\n",
            __FILE__, __LINE__, __FUNCTION__, thread_number, thread_number);

    s3_transport_config s3_config;
    s3_config.object_size = file_size;
    s3_config.number_of_transfer_threads = 5;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.thread_identifier = thread_number;
    s3_config.debug_flag = debug_flag;
    s3_config.multipart_flag = use_multipart_flag;

    s3_transport tp1{s3_config};
    dstream ds1{tp1, s3_prefix + filename};

    if (!ds1.is_open()) {
        printf("[%d] Open failed.  Exiting...\n", thread_number);
        return;
    }


    if (thread_number == 0) {

        // test offset write
        ds1.seekp(0, std::ios_base::end);
        printf("Adding - adding this to end\n");
        std::string write_string = "adding this to end\n";
        ds1.write(write_string.c_str(), write_string.length());

        // test offset read
        char read_str[21];
        read_str[20] = 0;
        printf("------ read 20 bytes at offset 10 -----\n");
        ds1.seekg(10, std::ios_base::beg);
        ds1.read(read_str, 20);
        printf("[%s]\n", read_str);
        printf("------ read 20 more bytes         -----\n");
        ds1.read(read_str, 20);
        printf("[%s]\n", read_str);
        printf("------ read 20 more bytes skip 10 -----\n");
        ds1.seekg(10, std::ios_base::cur);
        ds1.read(read_str, 20);
        printf("[%s]\n", read_str);
        printf("------ read last 20 bytes         -----\n");
        ds1.seekg(-20, std::ios_base::end);
        ds1.read(read_str, 20);
        printf("[%s]\n", read_str);
        printf("---------------------------------------\n");
    }

    // will be automatic
    //ds1.close();
    printf("CLOSE DONE FOR %d\n", thread_number);
}

void upload_part(int thread_number,
                 const int thread_count,
                 const uint32_t file_size,
                 const bool debug_flag,
                 const char *hostname,
                 const char *bucket_name,
                 const char *access_key,
                 const char *secret_access_key,
                 const char *filename)
{


    printf("%s:%d (%s) [upload thread=%u, seq=%u] writing from file into s3\n",
            __FILE__, __LINE__, __FUNCTION__, thread_number, thread_number);

    // thread in irods only deal with sequential bytes.  figure out what bytes this thread deals with
    uint32_t start = thread_number * (file_size / thread_count);
    uint32_t end = 0;
    if (thread_number == thread_count - 1) {
        end = file_size;
    } else {
        end = start + file_size / thread_count;
    }

    std::ifstream ifs;

    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate);
    if (!ifs.good()) {
        fprintf(stderr, "failed to open file %s\n", filename);
        return;
    }

    ifs.seekg(start, std::ios::beg);


    uint32_t current_buffer_size = end - start;
    char *current_buffer = new char[current_buffer_size];
    ifs.read((char*)(current_buffer), current_buffer_size);

    printf("%s:%d (%s) [thread=%u, seq=%u] done reading file\n",
            __FILE__, __LINE__, __FUNCTION__, thread_number, thread_number);

    /*****************************************
     * This part actually goes in S3 plugin. *
     *****************************************/

    s3_transport_config s3_config;
    s3_config.object_size = file_size;
    s3_config.number_of_transfer_threads = 20;
    s3_config.part_size = current_buffer_size;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.thread_identifier = thread_number;
    s3_config.debug_flag = debug_flag;
    s3_config.multipart_flag = use_multipart_flag;

    s3_transport tp1{s3_config};
    odstream ds1{tp1, s3_prefix + filename};

    if (!ds1.is_open()) {
        printf("[%d] Open failed.  Exiting...\n", thread_number);
        return;
    }

    ds1.seekp(start);

    // doing two writes here just to test that that works
    ds1.write(current_buffer, current_buffer_size);
    //ds1.write(current_buffer, 100);
    //ds1.write(current_buffer+100, current_buffer_size-100);

    printf("WRITE DONE FOR %d\n", thread_number);

    // will be automatic
    ds1.close();
    printf("CLOSE DONE FOR %d\n", thread_number);

    /*****************************************/


    // s3FileWrite copies its buffer so that iRODS can delete it after
    // s3FileWrite returns
    delete[] current_buffer;

    ifs.close();

}

void download_part(int thread_number,
                   const int thread_count,
                   const uint32_t file_size,
                   const bool debug_flag,
                   const char *hostname,
                   const char *bucket_name,
                   const char *access_key,
                   const char *secret_access_key,
                   const char *filename)
{
    printf("%s(%d, %d, %u, %d, %s, %s, %s, %s, %s)\n",
          __FUNCTION__,
          thread_number,
          thread_count,
          file_size,
          debug_flag,
          hostname,
          bucket_name,
          access_key,
          secret_access_key,
          filename);

    printf("%s:%d (%s) [download thread=%u, seq=%u] reading from s3 into file \n",
            __FILE__, __LINE__, __FUNCTION__, thread_number, thread_number);

    // thread in irods only deal with sequential bytes.  figure out what bytes this
    // thread deals with
    size_t start = thread_number * (file_size / thread_count);
    size_t end = 0;
    if (thread_number == thread_count - 1) {
        end = file_size;
    } else {
        end = start + file_size / thread_count;
    }

    // open output stream for test
    std::ofstream ofs;
    ofs.open((std::string(filename) + std::string(".downloaded")).c_str(),
            std::ios::out | std::ios::binary);

    if (!ofs.good()) {
        fprintf(stderr, "failed to open file %s\n", filename);
        return;
    }

    size_t current_buffer_size = end - start;
    char *current_buffer = static_cast<char*>(malloc(current_buffer_size * sizeof(char)));

    /*****************************************
     * This part actually goes in S3 plugin. *
     *****************************************/

    printf("tp1{%u, %d, %d, %d, %s, %s, %s, %s, %d, %s, %s, %s, %d}\n",
            file_size, 100, 1, 1, hostname, bucket_name, access_key,
            secret_access_key, true, "V4", "http", "amz", true);

    s3_transport_config s3_config;
    s3_config.object_size = file_size;
    s3_config.number_of_transfer_threads = 20;
    s3_config.part_size = 0;
    s3_config.bucket_name = bucket_name;
    s3_config.access_key = access_key;
    s3_config.secret_access_key = secret_access_key;
    s3_config.thread_identifier = thread_number;
    s3_config.debug_flag = debug_flag;
    s3_config.multipart_flag = use_multipart_flag;

    s3_transport tp1{s3_config};

    idstream ds1{tp1, s3_prefix + filename};

    if (!ds1.is_open()) {
        printf("[%d] Open failed.  Exiting...\n", thread_number);
        return;
    }

    ds1.seekg(start);
    ds1.read(current_buffer, current_buffer_size);


    /*****************************************/

    printf("write at %ld of size %ld\n", start, current_buffer_size);

    ofs.seekp(start, std::ios::beg);
    ofs.write(current_buffer, current_buffer_size);
    ofs.close();

    printf("READ DONE FOR %d\n", thread_number);

    // will be automatic
    ds1.close();
    printf("CLOSE DONE FOR %d\n", thread_number);

    //delete[] current_buffer;
    free(current_buffer);
    /*****************************************/

    // s3FileWrite copies its buffer so that iRODS can delete it after
    // s3FileWrite returns

}
