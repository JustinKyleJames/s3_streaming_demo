#include <libs3.h>
#include <iostream>
#include <cerrno>
#include <cstring>
#include <cstdio>

struct callback_data {

    callback_data(uint64_t buffer_size_)
        : buffer_size{buffer_size_}
        , offset{0}
    {

        buffer = new char[buffer_size];
    }

    ~callback_data() {
        delete[] buffer;
    }

    uint64_t buffer_size;
    uint64_t offset;
    char *buffer;
};

void log_status(
    S3Status status,
    const S3ErrorDetails *error)
{

    if( status != S3StatusOK ) {
        printf( "  S3Status: [%s] - %d\n", S3_get_status_name( status ), (int) status );
    }
    if (error && error->message)
        printf( "  Message: %s\n", error->message);
    if (error && error->resource)
        printf( "  Resource: %s\n", error->resource);
    if (error && error->furtherDetails)
        printf( "  Further Details: %s\n", error->furtherDetails);
    if (error && error->extraDetailsCount) {
        printf( "%s", "  Extra Details:\n");

        for (int i = 0; i < error->extraDetailsCount; i++) {
            printf( "    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
        }
    }
}


int main(int argc, char ** argv) {

    if (argc != 4) {
        std::cerr << "Usage: read_bytes_libs3 <access_key> <secret_access_key> <object_key>" << std::endl;
        return EINVAL;
    }

    char *access_key = argv[1];
    char *secret_access_key = argv[2];
    char *object_key = argv[3];
    std::cout << access_key << std::endl << secret_access_key << std::endl << object_key << std::endl;

    // must initialize s3
    S3_initialize( "s3", S3_INIT_ALL | S3_INIT_SIGNATURE_V4, "s3.amazonaws.com" );

    uint64_t bytes_to_read = 1024*1024;

    callback_data data(bytes_to_read);

    // callbacks for S3_get_object()
    S3GetObjectHandler get_object_handler = {
        {   [] (const S3ResponseProperties *properties, void *callbackData) -> S3Status {
                std::cout << "response properties callback" << std::endl;
                return S3StatusOK;
            },
            [] (S3Status status, const S3ErrorDetails *error, void *callbackData) -> void {
                log_status(status, error);
                std::cout << "response complete callback" << std::endl;
            }
        },
        [] (int bufferSize, const char *buffer, void *callbackData) -> S3Status {

            std::cout << "reading " << bufferSize << " bytes" << std::endl;
            callback_data *cb = static_cast<callback_data *>(callbackData);
            memcpy(&(cb->buffer[cb->offset]), buffer, bufferSize);
            cb->offset += bufferSize;
            return S3StatusOK;
        }
    };

    S3BucketContext bucket_context;
    bzero (&bucket_context, sizeof (bucket_context));
    bucket_context.hostName        = "s3.amazonaws.com";
    bucket_context.bucketName      = "justinkylejames1";
    bucket_context.accessKeyId     = access_key;
    bucket_context.secretAccessKey = secret_access_key;
    bucket_context.protocol        = S3ProtocolHTTPS;
    bucket_context.stsDate         = S3STSDateOnly;
    bucket_context.uriStyle        = S3UriStylePath;

    S3_get_object( &bucket_context,
            object_key,
            nullptr,        // conditions to satisfy, none if null
            0,              // offset
            bytes_to_read,  // length
            nullptr,        // request context - null means immediate and synchronous
            &get_object_handler,
            &data );

    // go ahead and write the bytes read to a file called output
    FILE *fptr;
    fptr = fopen("output","w+");
    fwrite(data.buffer, 1, data.buffer_size, fptr);
    fclose(fptr);

    return 0;
}
