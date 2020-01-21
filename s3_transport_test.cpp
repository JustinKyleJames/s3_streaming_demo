#include "s3_transport.hpp"
#include <filesystem/filesystem.hpp>
#include <dstream.hpp>
#include <mutex>
#include <condition_variable>
#include <jansson.h>
#include <fstream>
#include <thread>

const long transfer_buffer_size_for_parallel_transfer_in_megabytes = 4;

using odstream          = irods::experimental::io::odstream;
using s3_transport      = irods::experimental::io::s3_transport<char>;
using upload_manager_t  = irods::experimental::io::upload_manager_t;


void doit(int thread_number, S3BucketContext *bucket_context, upload_manager_t *manager, 
    const int thread_count, const size_t file_size,
    const bool debug_flag, const std::string& filename); 

int main(int argc, char **argv) { 

    if (3 != argc) { 
        std::cerr << "Usage:  s3_transport_test <config_file> <upload_file>" << std::endl;
        return 1; 
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
        fprintf(stderr, "error: on line %d in %s: %s\n", error.line, config_file.c_str(), error.text);
        return 1;
    }

    json_t *keyfile_json_object = json_object_get(root, "keyfile");
    if(!json_is_string(keyfile_json_object))
    {
        fprintf(stderr, "error: keyfile missing or is not a string in %s\n", config_file.c_str());
        json_decref(root);
        return 1;
    }
    std::string keyfile = json_string_value(keyfile_json_object);

    json_t *hostname_json_object = json_object_get(root, "hostname");
    if(!json_is_string(hostname_json_object))
    {
        fprintf(stderr, "error: (%s): hostname missing or is not a string\n", config_file.c_str());
        json_decref(root);
        return 1;
    }
    std::string hostname = json_string_value(hostname_json_object);

    json_t *bucket_name_json_object = json_object_get(root, "bucket_name");
    if(!json_is_string(bucket_name_json_object))
    {
        fprintf(stderr, "error: (%s) bucket_name missing or is not a string\n", config_file.c_str());
        json_decref(root);
        return 1;
    }
    std::string bucket_name = json_string_value(bucket_name_json_object);

    json_t *thread_count_json_object = json_object_get(root, "thread_count");
    if(!json_is_integer(thread_count_json_object))
    {
        fprintf(stderr, "error: (%s) thread_count missing or is not an integer\n", config_file.c_str());
        json_decref(root);
        return 1;
    }
    size_t thread_count = json_integer_value(thread_count_json_object);

    json_t *debug_flag_json_object = json_object_get(root, "debug_flag");
    bool debug_flag = json_is_true(debug_flag_json_object) ? true : false;

    // AWS
    std::string key_id;
    std::string access_key;

    // open and read keyfile
    std::ifstream key_ifs;

    key_ifs.open(keyfile.c_str());
    if (!key_ifs.good()) {
        fprintf(stderr, "failed to open key file %s\n", keyfile.c_str());
        return 1;
    }

    if (!std::getline(key_ifs, key_id)) {
        std::cerr << "Key file does not have a key_id." << std::endl;
        return 1;
    }
    if (!std::getline(key_ifs, access_key)) {
        std::cerr << "Key file does not have an access_key." << std::endl;
        return 1;
    }

    std::mutex              upload_manager_mtx;
    std::condition_variable upload_manager_cv;
    upload_manager_t        manager{upload_manager_mtx, upload_manager_cv};


    S3BucketContext bucket_context;
    bucket_context.hostName = hostname.c_str(); 
    bucket_context.bucketName = bucket_name.c_str(); 
    bucket_context.protocol = S3ProtocolHTTP;
    bucket_context.stsDate = S3STSAmzOnly;
    bucket_context.uriStyle = S3UriStylePath;
    bucket_context.accessKeyId = key_id.c_str(); 
    bucket_context.secretAccessKey = access_key.c_str();
    bucket_context.securityToken = nullptr;

    // determine file size 
    size_t file_size;
    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate); 
    if (!ifs.good()) {
        fprintf(stderr, "failed to open file %s\n", filename.c_str());
        return 1;
    }

    file_size = ifs.tellg();
    ifs.close();

    std::thread *writer_threads = new std::thread[thread_count];

    for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {
        if (debug_flag) {
            printf("%s:%d (%s) start thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
        }
        writer_threads[thread_number] = std::move(std::thread(doit, thread_number, &bucket_context, &manager, 
                    thread_count, file_size, debug_flag, filename));
    }


    for (int thread_number = 0; thread_number <  thread_count; ++thread_number) {
        if (debug_flag) {
            printf("%s:%d (%s) calling join for thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
        }
        writer_threads[thread_number].join();
        if (debug_flag) {
            printf("%s:%d (%s) joined thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
        }
    }

    delete[] writer_threads;

    return 0;

}

void doit(int thread_number, S3BucketContext *bucket_context, upload_manager_t *manager, 
    const int thread_count, const size_t file_size, 
    const bool debug_flag, const std::string& filename) { 

    int seq = thread_number + 1;

    if (debug_flag) {
        printf("%s:%d (%s) [thread=%u, seq=%u] reading file\n", __FILE__, __LINE__, __FUNCTION__, thread_number, seq);
    }

    // thread in irods only deal with sequential bytes.  figure out what bytes this thread deals with
    size_t start = thread_number * (file_size / thread_count);
    size_t end = 0;
    if (thread_number == thread_count - 1) {
        end = file_size;
    } else {
        end = start + file_size / thread_count;
    }

    std::ifstream ifs;

    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate); 
    if (!ifs.good()) {
        fprintf(stderr, "failed to open file %s\n", filename.c_str());
        return;
    }

    ifs.seekg(start, std::ios::beg);


    size_t current_buffer_size = end - start;//end - ifs.tellg() > multipart_size ? multipart_size : end - ifs.tellg();
    char *current_buffer = new char[current_buffer_size];
    ifs.read((char*)(current_buffer), current_buffer_size);

    if (debug_flag) {
        printf("%s:%d (%s) [thread=%u, seq=%u] done reading file\n", __FILE__, __LINE__, __FUNCTION__, thread_number, seq);
    }

    /*****************************************
     * This part actually goes in S3 plugin. *
     *****************************************/

    s3_transport tp1{seq, current_buffer_size, thread_count, file_size, 1, 1, *bucket_context, 
        *manager, thread_number==0, true};

    odstream ds1{tp1, "test1"};
    ds1.write(current_buffer, current_buffer_size);

    printf("WRITE DONE FOR %d\n", seq);

    // will be automatic
    ds1.close();
    printf("CLOSE DONE FOR %d\n", seq);

    /*****************************************/


    // s3FileWrite copies its buffer so that iRODS can delete it after
    // s3FileWrite returns
    delete[] current_buffer;
    
    ifs.close();

}
