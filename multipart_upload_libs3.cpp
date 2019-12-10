// irods includes
//#include <irods_error.hpp>
//#include <rcMisc.h>

// stdlib includes
#include <sstream>
#include <vector>
#include <string>
#include <ctime>
#include <tuple>
#include <fstream>
#include <thread>
#include <atomic>
#include <mutex>


// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>

// system includes
#include <openssl/md5.h>
#include <sys/file.h>
#include <sys/param.h>
#include <errno.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <dirent.h>

// other includes
#include <string.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>
#include <libs3.h>
#include <openssl/ssl.h>


#include "ring_buffer.hpp"

// **** just simulate irods::error ****

namespace irods 
{
    class error {
        public:
            error() {}
            error(bool _status, long long code, std::string _message) {
                status = _status;
            }
            bool ok() const {
                return status;
            }
        private:
            bool status;
            
    };  // end class error
} // end namespace irods


const long transfer_buffer_size_for_parallel_transfer_in_megabytes = 4;
thread_local int thread_nbr;

const long long S3_GET_ERROR = 1;
const long long S3_PUT_ERROR = 2;
const long long SYS_MALLOC_ERR = 3;

#define ERROR( code_, message_ ) ( irods::error( false, code_, message_ ) )
#define SUCCESS( ) ( irods::error( true, 0, "" ) )
#define MAX_NAME_LEN   (1024+64)
typedef __int64_t rodsLong_t;

// ************************************

// individual request
struct upload_page_t {
   char *buffer;
   size_t buffer_size;
   off_t offset_of_buffer;     // offset of buffer within the file         
   bool terminate_flag;
}; 

std::atomic<unsigned int> current_buffer_counter;

// maps the irods thread number to the reader thread and ring buffer instance
std::mutex ring_buffer_maps_mutex;
std::map<int, std::thread*> ring_buffer_reader_thread_map;
std::map<int, irods::ring_buffer<upload_page_t>*> ring_buffer_instance_map; 

const size_t S3_DEFAULT_RETRY_WAIT_SEC = 1;
const size_t S3_DEFAULT_RETRY_COUNT = 1;

typedef struct S3Auth {
    char accessKeyId[MAX_NAME_LEN];
    char secretAccessKey[MAX_NAME_LEN];
} s3Auth_t;

typedef struct s3Stat
{
    char key[MAX_NAME_LEN];
    rodsLong_t size;
    time_t lastModified;
} s3Stat_t;

typedef struct callback_data
{
    char *original_bytes_ptr;
    char *bytes;               // pointer to last entry on ring_buffer
    size_t buffer_size;
    irods::ring_buffer<upload_page_t> *ring_buffer_instance_ptr;
   
    rodsLong_t contentLength, originalContentLength;
    S3Status status;
    int keyCount;
    s3Stat_t s3Stat;    /* should be a pointer if keyCount > 1 */

    S3BucketContext *pCtx; /* To enable more detailed error messages */
} callback_data_t;

typedef struct upload_manager
{
    char *upload_id;    /* Returned from S3 on MP begin */
    char **etags;       /* Each upload part's MD5 */

    /* Below used for the upload completion command, need to send in XML */
    char *xml;
    long remaining;
    long offset;

    S3BucketContext *pCtx; /* To enable more detailed error messages */

    S3Status status;
} upload_manager_t;

typedef struct multipart_data
{
    int seq;                       /* Sequence number, i.e. which part */
    int mode;                      /* PUT or COPY */
    S3BucketContext *pSrcCtx;      /* Source bucket context, ignored in a PUT */
    const char *srcKey;            /* Source key, ignored in a PUT */
    callback_data put_object_data; /* File being uploaded */
    upload_manager_t *manager;     /* To update w/the MD5 returned */

    S3Status status;
    bool enable_md5;
    bool server_encrypt;
} multipart_data_t;

size_t string_to_size_t(const std::string& str) {
    std::stringstream sstream(str);
    size_t result;
    sstream >> result;
    return result;
}  // end string_to_size_t

//////////////////////////////////////////////////////////////////////
// s3 specific functionality
static bool S3Initialized = false; // so we only initialize the s3 library once
static boost::mutex g_hostnameIdxLock;

S3ResponseProperties savedProperties;

// just a dummy function for now 
std::string get_resource_name() {
    return "";
} // end get_resource_name

// Sleep for *at least* the given time, plus some up to 1s additional
// The random addition ensures that threads don't all cluster up and retry
// at the same time (dogpile effect)
void s3_sleep(
    int _s,
    int _ms ) {
    // We're the only user of libc rand(), so if we mutex around calls we can
    // use the thread-unsafe rand() safely and randomly...if this is changed
    // in the future, need to use rand_r and init a static seed in this function
    static boost::mutex randMutex;
    randMutex.lock();
    int random = rand();
    randMutex.unlock();
    int addl = (int)(((double)random / (double)RAND_MAX) * 1000.0); // Add up to 1000 ms (1 sec)
    useconds_t us = ( _s * 1000000 ) + ( (_ms + addl) * 1000 );
    usleep( us );
} // end s3_sleep

// Returns timestamp in usec for delta-t comparisons
static unsigned long long usNow() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long long us = (tv.tv_sec) * 1000000LL + tv.tv_usec;
    return us;
} // end usNow

// Return a malloc()'d C string containing the ASCII MD5 signature of the file
// from start through length bytes, using pread to not affect file pointers.
// The returned string needs to be free()d by the caller
static char *s3CalcMD5( int fd, off_t start, off_t length, const std::string& resource_name )
{
    char *buff; // Temp buff to do MD5 calc on
    unsigned char md5_bin[MD5_DIGEST_LENGTH];
    MD5_CTX md5_ctx;
    long read;

    buff = (char *)malloc( 1024*1024 ); // 1MB chunk reads
    if ( buff == NULL ) {
        printf( "[resource_name=%s] Out of memory in S3 MD5 calculation, MD5 checksum will NOT be used for upload.\n", resource_name.c_str() );
        return NULL;
    }

    MD5_Init( &md5_ctx );
    for ( read=0; (read + 1024*1024) < length; read += 1024*1024 ) {
        long ret = pread( fd, buff, 1024*1024, start );
        if ( ret != 1024*1024 ) {
            printf( "[resource_name=%s] Error during MD5 pread of file, checksum will NOT be used for upload.\n", resource_name.c_str() );
            free( buff );
            return NULL;
        }
        MD5_Update( &md5_ctx, buff, 1024*1024 );
        start += 1024 * 1024;
    }
    // Partial read for the last bit
    long ret = pread( fd, buff, length-read, start );
    if ( ret != length-read ) {
        printf( "[resource_name=%s] Error during MD5 pread of file, checksum will NOT be used for upload.\n", resource_name.c_str() );
        free( buff );
        return NULL;
    }
    MD5_Update( &md5_ctx, buff, length-read );
    MD5_Final( md5_bin, &md5_ctx );
    free( buff );

    // Now we need to do BASE64 encoding of the MD5_BIN
    BIO *bmem, *b64;
    BUF_MEM *bptr;

    b64 = BIO_new(BIO_f_base64());
    bmem = BIO_new(BIO_s_mem());
    if ( (b64 == NULL) || (bmem == NULL) ) {
        printf( "[resource_name=%s] Error during Base64 allocation, checksum will NOT be used for upload.\n", resource_name.c_str() );
        return NULL;
    }

    b64 = BIO_push(b64, bmem);
    BIO_write(b64, md5_bin, MD5_DIGEST_LENGTH);
    if (BIO_flush(b64) != 1) {
        printf( "[resource_name=%s] Error during Base64 computation, checksum will NOT be used for upload.\n", resource_name.c_str() );
        return NULL;
    }
    BIO_get_mem_ptr(b64, &bptr);

    char *md5_b64 = (char*)malloc( bptr->length );
    if ( md5_b64 == NULL ) {
        printf( "[resource_name=%s] Error during MD5 allocation, checksum will NOT be used for upload.\n", resource_name.c_str() );
        return NULL;
    }
    memcpy( md5_b64, bptr->data, bptr->length-1 );
    md5_b64[bptr->length-1] = 0;  // 0-terminate the string, not done by BIO_*
    BIO_free_all(b64);

    return md5_b64;
}  // end s3CalcMD5

static void StoreAndLogStatus (
    S3Status status,
    const S3ErrorDetails *error,
    const char *function,
    const S3BucketContext *pCtx,
    S3Status *pStatus )
{
    int i;

    *pStatus = status;
    if( status != S3StatusOK ) {
        printf( "  S3Status: [%s] - %d\n", S3_get_status_name( status ), (int) status );
        printf( "    S3Host: %s\n", pCtx->hostName );
    }
    if (status != S3StatusOK && function )
        printf( "  Function: %s\n", function );
    if (error && error->message)
        printf( "  Message: %s\n", error->message);
    if (error && error->resource)
        printf( "  Resource: %s\n", error->resource);
    if (error && error->furtherDetails)
        printf( "  Further Details: %s\n", error->furtherDetails);
    if (error && error->extraDetailsCount) {
        printf( "%s", "  Extra Details:\n");

        for (i = 0; i < error->extraDetailsCount; i++) {
            printf( "    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
        }
    }
}  // end StoreAndLogStatus

void responseCompleteCallback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
}  // end responseCompleteCallback

S3Status responsePropertiesCallback(
    const S3ResponseProperties *properties,
    void *callbackData)
{
    // Here we are saving the only 2 things iRODS actually cares about.
    savedProperties.lastModified = properties->lastModified;
    savedProperties.contentLength = properties->contentLength;
    return S3StatusOK;
} // end responsePropertiesCallback

static int putObjectDataCallback(
    int libs3_buffer_size,
    char *libs3_buffer,
    void *callbackData)
{
    // keep reading a bufferSize bytes from buffer.
    // if buffer is empty, get another from the ring_buffer
    callback_data_t *data = (callback_data_t *) callbackData;
    irods::ring_buffer<upload_page_t> *ring_buffer_instance_ptr = data->ring_buffer_instance_ptr;

    // if we've exhausted our current buffer, read the next buffer from the ring_buffer
    if (data->buffer_size == 0) {
        upload_page_t page;
        ring_buffer_instance_ptr->read(page);

        // if we get a terminate there are no more bytes to be read
        if (page.terminate_flag) {
            delete[] data->original_bytes_ptr;
            return 0;
        }
        delete[] data->original_bytes_ptr;
        data->original_bytes_ptr = data->bytes = page.buffer;
        data->buffer_size = page.buffer_size;
    }

    int length = 0;

    if (0 < data->buffer_size) {

        // bufferSize is the size of *buffer provided by libs3
        // data->buffer_size is the size of data->bytes we set up

        length = libs3_buffer_size > data->buffer_size ? data->buffer_size : libs3_buffer_size;


        printf("%s:%d (%s) [thread=%d] libs3_buffer_size=%d data->buffer_size=%zu length=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_nbr, libs3_buffer_size, data->buffer_size, length);
fflush(stdout);

        memcpy(libs3_buffer, data->bytes, length);
    }
    data->buffer_size -= length;
    data->bytes += length;

    return length;
} // end putObjectDataCallback


// Get S3 Signature version from plugin property map
S3SignatureVersion s3GetSignatureVersion ()
{
    return S3SignatureV2; // default
} // end s3GetSignatureVersion

std::string s3GetHostname() {
    return "s3.amazonaws.com";
    //return "127.0.0.1:9000";
} // end s3GetHostname

S3Protocol s3GetProto()
{
    return S3ProtocolHTTP;
} // end s3GetProto

bool s3GetEnableMD5 ()
{
    return false;
} // end s3GetEnableMD5

bool s3GetEnableMultiPartUpload ()
{
    return true;
} // end s3GetEnableMultiPartUpload

static bool s3GetServerEncrypt ()
{
    return false;
} // end s3GetServerEncrypt

static boost::mutex g_mrdLock; // Multirange download has a mutex-protected global work queue
static volatile int g_mrdNext = 0;
static int g_mrdLast = -1;

S3STSDate s3GetSTSDate()
{
    return S3STSAmzOnly;
} // end s3GetSTSDate

static multipart_data_t *g_mpuData = NULL;
static char *g_mpuUploadId = NULL;
static const char *g_mpuKey = NULL;
static irods::error g_mpuResult;  // Last thread error written wins, mutex protected

void print_g_mpuData(int part_cnt) {
    for (int i = 0; i < part_cnt; ++i) {
       printf("------ g_mpuData ------\n");
       printf("g_mpuData[%d].seq: %d\n", i, g_mpuData[i].seq);
       printf("g_mpuData[%d].mode: %d\n", i, g_mpuData[i].mode);
       printf("g_mpuData[%d].enable_md5: %d\n", i, g_mpuData[i].enable_md5);
       printf("g_mpuData[%d].server_encrypt: %d\n", i, g_mpuData[i].server_encrypt);
       if (g_mpuData[i].manager) {
           printf("g_mpuData[%d].manager->offset: %ld\n", i, g_mpuData[i].manager->offset);
           printf("m_gmpuData[%d].anager->remaining: %ld\n", i, g_mpuData[i].manager->remaining);
       }
   }
} // end print_g_mpuData

    int seq;                       /* Sequence number, i.e. which part */
    int mode;                      /* PUT or COPY */
    S3BucketContext *pSrcCtx;      /* Source bucket context, ignored in a PUT */
    const char *srcKey;            /* Source key, ignored in a PUT */
    callback_data put_object_data; /* File being uploaded */
    upload_manager_t *manager;     /* To update w/the MD5 returned */

    S3Status status;
    bool enable_md5;
    bool server_encrypt;

/******************* Multipart Initialization Callbacks *****************************/

/* Captures the upload_id returned and stores it away in our data structure */
static S3Status mpuInitXmlCB (
    const char* upload_id,
    void *callbackData )
{
    upload_manager_t *manager = (upload_manager_t *)callbackData;
    manager->upload_id = strdup(upload_id);
    return S3StatusOK;
} // end mpuInitXmlCB

static S3Status mpuInitRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
} // end mpuInitRespPropCB

static void mpuInitRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    upload_manager_t *data = (upload_manager_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
} // end mpuInitRespCompCB


/******************* Multipart Put Callbacks *****************************/

/* Upload data from the part, use the plain callback_data reader */
static int mpuPartPutDataCB (
    int bufferSize,
    char *buffer,
    void *callbackData)
{
    return putObjectDataCallback( bufferSize, buffer, &((multipart_data_t*)callbackData)->put_object_data );
} // end mpuPartPutDataCB

static S3Status mpuPartRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    multipart_data_t *data = (multipart_data_t *)callbackData;

    int seq = data->seq;
    const char *etag = properties->eTag;
    if (etag) {
        data->manager->etags[seq - 1] = strdup(etag);
    } else {
        data->manager->etags[seq - 1] = strdup("");
    }

    return S3StatusOK;
} // end mpuPartRespPropCB

static void mpuPartRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    multipart_data_t *data = (multipart_data_t *)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
} // end mpuPartRespCompCB

/******************* Multipart Commit Callbacks *****************************/
/* Uploading the multipart completion XML from our buffer */
static int mpuCommitXmlCB (
    int bufferSize,
    char *buffer,
    void *callbackData )
{
    upload_manager_t *manager = (upload_manager_t *)callbackData;
    long ret = 0;
    if (manager->remaining) {
        int toRead = ((manager->remaining > bufferSize) ?
                      bufferSize : manager->remaining);
        memcpy(buffer, manager->xml+manager->offset, toRead);
        ret = toRead;
    }
    manager->remaining -= ret;
    manager->offset += ret;

    return (int)ret;
} // end mpuCommitXmlCB

static S3Status mpuCommitRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
} // end mpuCommitRespPropCB

static void mpuCommitRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    upload_manager_t *data = (upload_manager_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
} // end mpuCommitRespCompCB

static S3Status mpuCancelRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
} // mpuCancelRespPropCB

// S3_abort_multipart_upload() does not allow a callbackData parameter, so pass the
// final operation status using this global.
static S3Status g_mpuCancelRespCompCB_status = S3StatusOK;
static S3BucketContext *g_mpuCancelRespCompCB_pCtx = NULL;
static void mpuCancelRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    S3Status *pStatus = (S3Status*)&g_mpuCancelRespCompCB_status;
    StoreAndLogStatus( status, error, __FUNCTION__, g_mpuCancelRespCompCB_pCtx, pStatus );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
} // end mpuCancelRespCompCB

static void mpuCancel( S3BucketContext *bucketContext, const char *key, const char *upload_id)
{
    S3AbortMultipartUploadHandler abortHandler = { { mpuCancelRespPropCB, mpuCancelRespCompCB } };
    std::stringstream msg;
    S3Status status;

    std::string resource_name = get_resource_name();

    msg << "[resource_name=" << resource_name << "] " << "Cancelling multipart upload: key=\"" << key << "\", upload_id=\"" << upload_id << "\"";
    printf( "%s\n", msg.str().c_str() );
    g_mpuCancelRespCompCB_status = S3StatusOK;
    g_mpuCancelRespCompCB_pCtx = bucketContext;
    S3_abort_multipart_upload(bucketContext, key, upload_id, &abortHandler);
    status = g_mpuCancelRespCompCB_status;
    if (status != S3StatusOK) {
        msg.str( std::string() ); // Clear
        msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ << " - Error cancelling the multipart upload of S3 object: \"" << key << "\"";
        if (status >= 0) {
            msg << " - \"" << S3_get_status_name(status) << "\"";
        }
        printf( "%s:%d (%s) [thread=%d] %s\n", __FILE__, __LINE__, __FUNCTION__, thread_nbr, msg.str().c_str() );
    }
} // end mpuCancel



/* Multipart worker thread, grabs a job from the queue and uploads it */
static void mpuWorkerThread(void *bucketContextParam, int thread_number, int thread_count, size_t file_size)
{
    thread_nbr = thread_number;

    S3BucketContext bucketContext = *((S3BucketContext*)bucketContextParam);

    irods::ring_buffer<upload_page_t> *ring_buffer_instance_ptr = nullptr;
    {
        std::lock_guard<std::mutex> lock(ring_buffer_maps_mutex);
        ring_buffer_instance_ptr = ring_buffer_instance_map[thread_number];
    }

    int seq = thread_number + 1;

    upload_page_t page;

    // read the first page
    printf("%s:%d (%s) [thread=%d] waiting to read [seq=%d]\n", __FILE__, __LINE__, __FUNCTION__, thread_number, seq);
    ring_buffer_instance_ptr->read(page);
    printf("%s:%d (%s) [thread=%d] read page [buffer=%p][buffer_size=%zu][terminate_flag=%d][offset=%ld]\n", __FILE__, __LINE__, __FUNCTION__, thread_number, page.buffer, page.buffer_size, page.terminate_flag, page.offset_of_buffer);

    std::string resource_name = get_resource_name();

    irods::error result;
    std::stringstream msg;
    S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };

    size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
    size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;

    multipart_data_t partData{};
    
    size_t retry_cnt = 0;
    do {
        // Work on a local copy of the structure in case an error occurs in the middle
        // of an upload.  If we updated in-place, on a retry the part would start
        // at the wrong offset and length.
        partData = g_mpuData[thread_number];
        partData.seq = seq;
        partData.put_object_data.pCtx = &bucketContext;
        partData.put_object_data.original_bytes_ptr = partData.put_object_data.bytes = page.buffer;
        partData.put_object_data.buffer_size = page.buffer_size;
        partData.put_object_data.ring_buffer_instance_ptr = ring_buffer_instance_ptr;

        // Each part is floor(file_size/thread_count) long.  The last thread has the extra bytes.
        if (thread_number == thread_count -1) {
            partData.put_object_data.contentLength = file_size - (thread_count-1) * (file_size / thread_count);
        } else {
            partData.put_object_data.contentLength = file_size / thread_count;
        }

        msg.str( std::string() ); // Clear
        msg << "Multipart:  Start part " << (int)seq << ", key \"" << g_mpuKey << "\", uploadid \"" << g_mpuUploadId << 
            ", len " << (int)partData.put_object_data.contentLength;
        printf( "%s:%d (%s) [thread=%u] %s\n", __FILE__, __LINE__, __FUNCTION__, thread_number, msg.str().c_str() );
        msg.str( std::string() ); // Clear

        S3PutProperties *putProps = NULL;
        putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
        putProps->md5 = nullptr;
        if ( putProps && partData.enable_md5 ) {
            // jjames - not sure how to do MD5 piecewise 
            //putProps->md5 = s3CalcMD5( partData.put_object_data.fd, partData.put_object_data.offset, partData.put_object_data.contentLength, resource_name );
        }
        putProps->expires = -1;
        unsigned long long usStart = usNow();
        std::string hostname = s3GetHostname();
        bucketContext.hostName = hostname.c_str(); 

        printf("%s:%d (%s) [thread=%d] S3_upload_part (ctx, key, props, handler, %d, uploadId, %ld, 0, partData)\n", __FILE__, __LINE__, __FUNCTION__, thread_number, seq, partData.put_object_data.contentLength);
        S3_upload_part(&bucketContext, g_mpuKey, putProps, &putObjectHandler, seq, g_mpuUploadId, partData.put_object_data.contentLength, 0, &partData);
        printf("%s:%d (%s) [thread=%d] S3_upload_part returned\n", __FILE__, __LINE__, __FUNCTION__, thread_number);


        unsigned long long usEnd = usNow();
        double bw = (g_mpuData[seq-1].put_object_data.contentLength / (1024.0 * 1024.0)) / ( (usEnd - usStart) / 1000000.0 );
        // Clear up the S3PutProperties, if it exists
        if (putProps) {
            if (putProps->md5) free( (char*)putProps->md5 );
            free( putProps );
        }
        msg << "Multipart:  -- END -- BW=" << bw << " MB/s";
        printf( "%s:%d (%s) [thread=%u] %s\n", __FILE__, __LINE__, __FUNCTION__, thread_number, msg.str().c_str() );
        if (partData.status != S3StatusOK) s3_sleep( retry_wait, 0 );
    } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && (++retry_cnt < retry_count_limit));
    if (partData.status != S3StatusOK) {
        msg.str( std::string() ); // Clear
        msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ << " - Error putting the S3 object: \"" << g_mpuKey << "\"" << " part " << seq;
        if(partData.status >= 0) {
            msg << " - \"" << S3_get_status_name(partData.status) << "\"";
        }
        result = ERROR( S3_PUT_ERROR, msg.str() );
        printf( "%s:%d (%s) [thread=%u] %s\n", __FILE__, __LINE__, __FUNCTION__, thread_number, msg.str().c_str() );
        g_mpuResult = result;
    }


} // end mpuWorkerThread


irods::error initialize_multipart_upload(
    const std::string& _filename,
    const std::string& _object_key,
    size_t _fileSize,
    size_t thread_count,
    const std::string& _key_id,
    const std::string& _access_key,
    const std::string& _bucket,
    upload_manager_t& manager,
    S3BucketContext& bucketContext,
    S3PutProperties*& putProps
    )
{

    irods::error result = SUCCESS();
    irods::error ret;
    int cache_fd = -1;
    int err_status = 0;
    size_t retry_cnt    = 0;
    bool enable_md5 = s3GetEnableMD5 ();
    bool server_encrypt = s3GetServerEncrypt ();
    std::stringstream msg;

    std::string resource_name = get_resource_name();

    size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
    size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;

    callback_data_t data;

    bzero (&bucketContext, sizeof (bucketContext));
    bucketContext.bucketName = _bucket.c_str();
    bucketContext.protocol = s3GetProto();
    bucketContext.stsDate = s3GetSTSDate();
    bucketContext.uriStyle = S3UriStylePath;
    bucketContext.accessKeyId = _key_id.c_str();
    bucketContext.secretAccessKey = _access_key.c_str();

    putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
    putProps->md5 = nullptr;
    if ( putProps && enable_md5 )
        putProps->md5 = s3CalcMD5( cache_fd, 0, _fileSize, get_resource_name() );
    if ( putProps && server_encrypt )
        putProps->useServerSideEncryption = true;
    putProps->expires = -1;

    // Multi-part upload or copy
    memset(&manager, 0, sizeof(manager));

    manager.upload_id = NULL;
    manager.remaining = 0;
    manager.offset  = 0;
    manager.xml = NULL;

    g_mpuResult = SUCCESS();

    msg.str( std::string() ); // Clear

    long seq;
    size_t total_parts = thread_count;
    

    multipart_data_t partData;
    int partContentLength = 0;

    bzero (&data, sizeof (data));
    data.contentLength = data.originalContentLength = _fileSize;

    // Allocate all dynamic storage now, so we don't start a job we can't finish later
    manager.etags = (char**)calloc(sizeof(char*) * total_parts, 1);
    if (!manager.etags) {
        // Clear up the S3PutProperties, if it exists
        if (putProps) {
            if (putProps->md5) free( (char*)putProps->md5 );
            free( putProps );
        }
        std::string msg =  boost::str(boost::format("[resource_name=%s] Out of memory error in S3 multipart ETags allocation.") % resource_name.c_str());
        printf( "%s\n", msg.c_str() );
        result = ERROR( SYS_MALLOC_ERR, msg.c_str() );
        return result;
    }
    g_mpuData = (multipart_data_t*)calloc(total_parts, sizeof(multipart_data_t));
    if (!g_mpuData) {
        // Clear up the S3PutProperties, if it exists
        if (putProps) {
            if (putProps->md5) free( (char*)putProps->md5 );
            free( putProps );
        }
        free(manager.etags);
        std::string msg =  boost::str(boost::format("[resource_name=%s] Out of memory error in S3 multipart g_mupData allocation.") % resource_name.c_str());
        printf( "%s\n", msg.c_str() );
        result = ERROR( SYS_MALLOC_ERR, msg.c_str() );
        return result;
    }
    // Maximum XML completion length with extra space for the <complete...></complete...> tag
    manager.xml = (char *)malloc((total_parts+2) * 256);
    if (manager.xml == NULL) {
        // Clear up the S3PutProperties, if it exists
        if (putProps) {
            if (putProps->md5) free( (char*)putProps->md5 );
            free( putProps );
        }
        free(g_mpuData);
        free(manager.etags);
        std::string msg =  boost::str(boost::format("[resource_name=%s] Out of memory error in S3 multiparts XML allocation.") % resource_name.c_str());
        printf( "%s\n", msg.c_str() );
        result = ERROR( SYS_MALLOC_ERR, msg.c_str() );
        return result;
    }

    retry_cnt = 0;
    // These expect a upload_manager_t* as cbdata
    S3MultipartInitialHandler mpuInitialHandler = { {mpuInitRespPropCB, mpuInitRespCompCB }, mpuInitXmlCB };
    do {
        std::string hostname = s3GetHostname();
        bucketContext.hostName = hostname.c_str(); 
        manager.pCtx = &bucketContext;
        S3_initiate_multipart(&bucketContext, _object_key.c_str(), putProps, &mpuInitialHandler, NULL, &manager);
        if (manager.status != S3StatusOK) s3_sleep( retry_wait, 0 );
    } while ( (manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < retry_count_limit));
    if (manager.upload_id == NULL || manager.status != S3StatusOK) {
        // Clear up the S3PutProperties, if it exists
        if (putProps) {
            if (putProps->md5) free( (char*)putProps->md5 );
            free( putProps );
        }
        msg.str( std::string() ); // Clear
        msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ << " - Error initiating multipart upload of the S3 object: \"" << _object_key << "\"";
        if(manager.status >= 0) {
            msg << " - \"" << S3_get_status_name(manager.status) << "\"";
        }
        printf( "%s\n", msg.str().c_str() );
        result = ERROR( S3_PUT_ERROR, msg.str() );
        return result; // Abort early
    }

    g_mpuUploadId = manager.upload_id;
    g_mpuKey = _object_key.c_str();
    for(seq = 1; seq <= total_parts; seq ++) {
        memset(&partData, 0, sizeof(partData));
        partData.manager = &manager;
        partData.seq = seq;
        partData.put_object_data = data;
        partData.enable_md5 = s3GetEnableMD5();
        partData.server_encrypt = s3GetServerEncrypt();
        g_mpuData[seq-1] = partData;
        data.contentLength -= partContentLength;
    }

    unsigned long long usStart = usNow();

    manager.remaining = 0;
    manager.offset  = 0;

    return result;

} // end initialize_multipart_upload

irods::error complete_multipart_upload(
    const std::string& _object_key,
    const int totalSeq,
    S3BucketContext& bucketContext,
    upload_manager_t& manager,
    S3PutProperties*& putProps)
{

    irods::error result = SUCCESS();

    std::stringstream msg;
    unsigned int retry_cnt = 0;
    size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
    size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;

    if (g_mpuResult.ok()) { // If someone aborted, don't complete...
        msg.str( std::string() ); // Clear
        msg << "Multipart:  Completing key \"" << _object_key.c_str() << "\"";
        printf( "%s\n", msg.str().c_str() );

        int i;
        strcpy(manager.xml, "<CompleteMultipartUpload>\n");
        manager.remaining = strlen(manager.xml);
        char buf[256];
        int n;
        for ( i = 0; i < totalSeq; i++ ) {
            n = snprintf( buf, 256, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n", i + 1, manager.etags[i] );
            strcpy( manager.xml+manager.remaining, buf );
            manager.remaining += n;
        }
        strcat( manager.xml + manager.remaining, "</CompleteMultipartUpload>\n" );
        manager.remaining += strlen( manager.xml+manager.remaining );
        int manager_remaining = manager.remaining;
        manager.offset = 0;
        retry_cnt = 0;
        S3MultipartCommitHandler commit_handler = { {mpuCommitRespPropCB, mpuCommitRespCompCB }, mpuCommitXmlCB, NULL };
        do {
            // On partial error, need to restart XML send from the beginning
            manager.remaining = manager_remaining;
            manager.offset = 0;
            std::string hostname = s3GetHostname();
            bucketContext.hostName = hostname.c_str(); 
            manager.pCtx = &bucketContext;
            S3_complete_multipart_upload(&bucketContext, _object_key.c_str(), &commit_handler, manager.upload_id, manager.remaining, NULL, &manager);
            printf("%s:%d (%s) [manager.status=%s]\n", __FILE__, __LINE__, __FUNCTION__, S3_get_status_name(manager.status));
            if (manager.status != S3StatusOK) s3_sleep( retry_wait, 0 );
        } while ((manager.status != S3StatusOK) && S3_status_is_retryable(manager.status) && ( ++retry_cnt < retry_count_limit));
        if (manager.status != S3StatusOK) {
            msg.str( std::string() ); // Clear
            msg << __FUNCTION__ << " - Error putting the S3 object: \"" << _object_key << "\"";
            if(manager.status >= 0) {
                msg << " - \"" << S3_get_status_name( manager.status ) << "\"";
            }
            g_mpuResult = ERROR( S3_PUT_ERROR, msg.str() );
        }
    }
    if ( !g_mpuResult.ok() && manager.upload_id ) {
        // Someone aborted after we started, delete the partial object on S3
        printf("Cancelling multipart upload\n");
        mpuCancel( &bucketContext, _object_key.c_str(), manager.upload_id);
        // Return the error
        result = g_mpuResult;
    }
    // Clean up memory
    if (manager.xml) free(manager.xml);
    if (manager.upload_id) free(manager.upload_id);
    for (int i=0; manager.etags && i<totalSeq; i++) {
        if (manager.etags[i]) free(manager.etags[i]);
    }
    if (manager.etags) free(manager.etags);
    if (g_mpuData) free(g_mpuData);
    // Clear up the S3PutProperties, if it exists
    if (putProps) {
        if (putProps->md5) {
            //free( (char*)putProps->md5 );
        }
        free( putProps );
    }

    return result;
} // end complete_multipart_upload

// s3FileWrite just adds to ring buffer and starts a new reader thread if one does not exist
void s3FileWrite(char *buffer, size_t buffer_size, off_t offset, S3BucketContext *bucket_context_ptr, int thread_number, int thread_count, size_t file_size) {

    printf("%s:%d (%s) [thread=%u] [buffer_size=%zu][offset=%ld]\n", __FILE__, __LINE__, __FUNCTION__, thread_number, buffer_size, offset);

    // We must copy the buffer because it will persist after s3FileWrite returns.
    // Not sure when iRODS would delete it.
    char *copied_buffer = new char[buffer_size];
    memcpy(copied_buffer, buffer, buffer_size);

    irods::ring_buffer<upload_page_t> *ring_buffer_instance_ptr = nullptr;
    {
        std::lock_guard<std::mutex> lock(ring_buffer_maps_mutex);
        if (ring_buffer_reader_thread_map.count(thread_number) == 0) {
            ring_buffer_instance_ptr = ring_buffer_instance_map[thread_number] = new irods::ring_buffer<upload_page_t>(3);
            ring_buffer_reader_thread_map[thread_number] = new std::thread(mpuWorkerThread, bucket_context_ptr, thread_number, thread_count, file_size);
        } else {
            ring_buffer_instance_ptr = ring_buffer_instance_map[thread_number];
        }
    }

    // blocks until it can write to buffer, returns immediately once written
    printf("%s:%d (%s) [thread=%u] about to write to ring_buffer\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
    ring_buffer_instance_ptr->write({copied_buffer, buffer_size, offset, false});
    printf("%s:%d (%s) [thread=%u] wrote to ring_buffer\n", __FILE__, __LINE__, __FUNCTION__, thread_number);

} // end s3FileWrite

// emulates the irods behavior of taking an existing list of character buffers and sending them to the plugin
void irods_emulator(unsigned int thread_number, const char *filename, size_t file_size, size_t thread_count, S3BucketContext *bucket_context_ptr, size_t multipart_size) {

    printf("%s:%d (%s) [thread=%u] starting irods_emulator\n", __FILE__, __LINE__, __FUNCTION__, thread_number);

    // TODO We are setting the thread count here
    // thread in irods only deal with sequential bytes.  figure out what bytes this thread deals with
    size_t start = thread_number * (file_size / thread_count);
    size_t end = 0;
    if (thread_number == thread_count -1) {
        end = file_size;
    } else {
        end = start + file_size / thread_count;
    }

    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate); 
    ifs.seekg(start, std::ios::beg);

    size_t offset = start;
    while (offset < end) {
        size_t current_buffer_size = end - ifs.tellg() > multipart_size ? multipart_size : end - ifs.tellg();
        char *current_buffer = new char[current_buffer_size];
        ifs.read((char*)(current_buffer), current_buffer_size);
        s3FileWrite(current_buffer, current_buffer_size, offset, bucket_context_ptr, thread_number, thread_count, file_size);
        offset += current_buffer_size;

        // s3FileWrite copies its buffer so that iRODS can delete it after
        // s3FileWrite returns
        delete[] current_buffer;
    }
    ifs.close();

} // end irods_emulator


int main(int argc, char **argv) { 

    if (argc != 3) { 
        std::cerr << "Usage:  multipart_upload_libs3 <file> <thread count>" << std::endl;
        return 1; 
    }

    std::string filename = argv[1];
    size_t multipart_size = transfer_buffer_size_for_parallel_transfer_in_megabytes*1024*1024;

    size_t thread_count = string_to_size_t(argv[2]);

    std::string hostname = s3GetHostname();

    // AWS
    std::string key_id;
    std::string access_key;

    // open and read keyfile
    std::ifstream key_ifs;
    //key_ifs.open("/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair");
    key_ifs.open("minio.keypair");
    if (!std::getline(key_ifs, key_id)) {
        std::cerr << "Key file does not have a key_id." << std::endl;
        return 1;
    }
    if (!std::getline(key_ifs, access_key)) {
        std::cerr << "Key file does not have an access_key." << std::endl;
        return 1;
    }

    // determine file size 
    size_t file_size;
    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate); 
    file_size = ifs.tellg();
    ifs.close();

    // this is done on file open if we are writing
    std::string bucket_name = "justinkylejames1";
    std::string object_key = filename;

    std::atomic_init(&current_buffer_counter, static_cast<unsigned int>(0));

    // initialize s3
    int flags = S3_INIT_ALL;
    S3PutProperties *putProps = nullptr;
    S3SignatureVersion signature_version = s3GetSignatureVersion();
    if (signature_version == S3SignatureV4) {
        flags |= S3_INIT_SIGNATURE_V4;
    }
    int status = S3_initialize( "s3", flags, hostname.c_str() );
    if (status != S3StatusOK) {
        fprintf(stderr, "S3_initialize returned error\n");
        return 1;
    }


    upload_manager_t manager;
    S3BucketContext bucket_context;
    irods::error ret = initialize_multipart_upload(filename, object_key, file_size, thread_count, key_id, access_key, bucket_name, manager, bucket_context, putProps); 
    if (!ret.ok()) {
        fprintf(stderr, "initialize_multipart_upload returned error\n");
        return 1;
    }


    // THIS PART IS DONE FOR EACH THREAD 
  
    // simulates irods writing to multiple threads 
    std::thread *writer_threads = new std::thread[thread_count];

    for (unsigned int thread_number = 0; thread_number <  thread_count; ++thread_number) {
        printf("%s:%d (%s) start thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
        writer_threads[thread_number] = std::move(std::thread(irods_emulator, thread_number, filename.c_str(), file_size, thread_count, &bucket_context, multipart_size));  
    }

    // this is just irods waiting for s3FileWrite to finish on all writes and then deleting the threads
    for (unsigned int thread_number = 0; thread_number <  thread_count; ++thread_number) {
        printf("%s:%d (%s) calling join for thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
        writer_threads[thread_number].join();
        printf("%s:%d (%s) joined thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
    }

    delete[] writer_threads;

    printf("%s:%d (%s) done parallel part\n", __FILE__, __LINE__, __FUNCTION__);


    // THIS PART IS DONE ONCE AT END (s3FileClose) 

    for (unsigned int thread_number = 0; thread_number <  thread_count; ++thread_number) {

        irods::ring_buffer<upload_page_t> *ring_buffer_instance_ptr = nullptr; 
        std::thread *ring_buffer_reader_thread_ptr = nullptr;
        {
            std::lock_guard<std::mutex> lock(ring_buffer_maps_mutex);
            ring_buffer_instance_ptr = ring_buffer_instance_map[thread_number]; 
            ring_buffer_reader_thread_ptr = ring_buffer_reader_thread_map[thread_number];
        }

        // write terminate message to ring_buffer
        printf("%s:%d (%s) write terminate message ring_buffer_reader thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
        ring_buffer_instance_ptr->write({nullptr, 0, 0, true});
        printf("%s:%d (%s) done writing terminate message ring_buffer_reader thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);

        printf("%s:%d (%s) calling join for ring_buffer_reader thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
   
        ring_buffer_reader_thread_ptr->join();

        printf("%s:%d (%s) joined ring_buffer_reader thread %d\n", __FILE__, __LINE__, __FUNCTION__, thread_number);
    }

    size_t part_count = thread_count;

    ret = complete_multipart_upload(object_key, part_count, bucket_context, manager, putProps);
    printf("%s:%d (%s) done complete_multipart_upload\n", __FILE__, __LINE__, __FUNCTION__);

    return 0;
} // end main

