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

// **** just simulate irods::error ****

std::mutex total_bytes_sent_mutex;
int total_bytes_sent = 0;

namespace irods {
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
            
    };
}

const long long S3_GET_ERROR = 1;
const long long S3_PUT_ERROR = 2;
const long long SYS_MALLOC_ERR = 3;


#define ERROR( code_, message_ ) ( irods::error( false, code_, message_ ) )
#define SUCCESS( ) ( irods::error( true, 0, "" ) )
#define MAX_NAME_LEN   (1024+64)
typedef __int64_t rodsLong_t;

// ************************************


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
    // jjames - changed this to just have the data pointer in callback_data_t since we don't have a cache file
    char *bytes;

    //int fd;
    long offset;       /* For multiple upload */
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

typedef struct multirange_data
{
    int seq;
    callback_data get_object_data;
    S3Status status;

    S3BucketContext *pCtx; /* To enable more detailed error messages */
} multirange_data_t;

/*#ifdef ERROR_INJECT
// Callback error injection
// If defined(ERROR_INJECT), then the specified pread/write below will fail.
// Only 1 failure happens, but this is OK since for every irods command we
// actually restart from 0 since the .SO is reloaded
// Pairing this with LIBS3 error injection will exercise the error recovery
// and retry code paths.
static boost::mutex g_error_mutex;
static long g_werr{0}; // counter
static long g_rerr{0}; // counter
static long g_merr{0}; // counter
static const long g_werr_idx{4}; // Which # pwrite to fail
static const long g_rerr_idx{4}; // Which # pread to fail
static const long g_merr_idx{4}; // Which part of Multipart Finish XML to fail
#endif*/

size_t string_to_size_t(const std::string& str) {
    std::stringstream sstream(str);
    size_t result;
    sstream >> result;
    return result;
}

//////////////////////////////////////////////////////////////////////
// s3 specific functionality
static bool S3Initialized = false; // so we only initialize the s3 library once
static boost::mutex g_hostnameIdxLock;

S3ResponseProperties savedProperties;

// just a dummy function for now 
std::string get_resource_name() {
    return "";
}

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
}

// Returns timestamp in usec for delta-t comparisons
static unsigned long long usNow() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long long us = (tv.tv_sec) * 1000000LL + tv.tv_usec;
    return us;
}

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
}

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
}

void responseCompleteCallback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
}

S3Status responsePropertiesCallback(
    const S3ResponseProperties *properties,
    void *callbackData)
{
    // Here we are saving the only 2 things iRODS actually cares about.
    savedProperties.lastModified = properties->lastModified;
    savedProperties.contentLength = properties->contentLength;
    return S3StatusOK;
}

static S3Status getObjectDataCallback(
    int bufferSize,
    const char *buffer,
    void *callbackData)
{
    callback_data_t *cb = (callback_data_t *)callbackData;
    std::string resource_name = "";

    if (bufferSize != 0 && buffer != NULL && callbackData != NULL) {
        printf("Invalid input parameter\n");
    }

    // TODO
    ssize_t wrote = 0; //pwrite(cb->fd, buffer, bufferSize, cb->offset);
    if (wrote>0) cb->offset += wrote;

/*#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_werr++;
    if (g_werr == g_werr_idx) {
        printf("[resource_name=%s] Injecting a PWRITE error during S3 callback\n", resource_name.c_str() );
        g_error_mutex.unlock();
        return S3StatusAbortedByCallback;
    }
    g_error_mutex.unlock();
#endif*/

    printf("DEBUG [wrote=%zd][bufferSize=%d]\n", wrote, bufferSize);
    return ((wrote < (ssize_t) bufferSize) ?
            S3StatusAbortedByCallback : S3StatusOK);
}

static int putObjectDataCallback(
    int bufferSize,
    char *buffer,
    void *callbackData)
{
    callback_data_t *data = (callback_data_t *) callbackData;
    long ret = 0;

    int length = bufferSize;

    if (data->contentLength) {
printf("%s:%d (%s) [data->contentLength=%ld][bufferSize=%d]\n", __FILE__, __LINE__, __FUNCTION__, data->contentLength, (unsigned)bufferSize);
        length = ((data->contentLength > (unsigned) bufferSize) ?
                      (unsigned) bufferSize : data->contentLength);

printf("%s:%d (%s) [data->offset=%ld][length=%d]\n", __FILE__, __LINE__, __FUNCTION__, data->offset, length);
        memcpy(buffer, &(data->bytes[data->offset]), length);

        char tmp[100] = {0};
        strncpy(tmp, data->bytes, 100);
printf("%s:%d (%s) sending %s...\n", __FILE__, __LINE__, __FUNCTION__, tmp);

        //ret = pread(data->fd, buffer, length, data->offset);
    }
printf("%s:%d (%s) [data->contentLength=%ld]\n", __FILE__, __LINE__, __FUNCTION__, data->contentLength);
    data->contentLength -= length;
    data->offset += length;

total_bytes_sent_mutex.lock();
total_bytes_sent += length;
total_bytes_sent_mutex.unlock();

printf("%s:%d (%s) NEW [data->contentLength=%ld][data->offset=%ld]\n", __FILE__, __LINE__, __FUNCTION__, data->contentLength, data->offset);
    //data->contentLength -= ret;
    //data->offset += ret;

/*#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_rerr++;
    if (g_rerr == g_rerr_idx) {
        printf("[resource_name=%s] Injecting pread error in S3 callback\n", get_resource_name().c_str());
        ret = -1;
    }
    g_error_mutex.unlock();
#endif*/

    return (long)length;
}


// Get S3 Signature version from plugin property map
S3SignatureVersion s3GetSignatureVersion ()
{
    return S3SignatureV2; // default
}

std::string s3GetHostname() {
    return "s3.amazonaws.com";
    //return "127.0.0.1:9000";
}

S3Protocol s3GetProto()
{
    return S3ProtocolHTTP;
}

bool s3GetEnableMD5 ()
{
    return false;
}


bool s3GetEnableMultiPartUpload ()
{
    return true;
}

static bool s3GetServerEncrypt ()
{
    return false;
}

static boost::mutex g_mrdLock; // Multirange download has a mutex-protected global work queue
static volatile int g_mrdNext = 0;
static int g_mrdLast = -1;
static multirange_data_t *g_mrdData = NULL;
static const char *g_mrdKey = NULL;

static S3Status mrdRangeGetDataCB (
    int bufferSize,
    const char *buffer,
    void *callbackData)
{
    multirange_data_t *data = (multirange_data_t*)callbackData;
    return getObjectDataCallback( bufferSize, buffer, &(data->get_object_data) );
}

static S3Status mrdRangeRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    // Don't need to do anything here
    return S3StatusOK;
}

static void mrdRangeRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    multirange_data_t *data = (multirange_data_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}


/*static void mrdWorkerThread (void *bucketContextParam, void *pluginPropertyMapParam)
{
    S3BucketContext bucketContext = *((S3BucketContext*)bucketContextParam);

    std::string resource_name = get_resource_name();

    irods::error result;
    std::stringstream msg;
    S3GetObjectHandler getObjectHandler = { {mrdRangeRespPropCB, mrdRangeRespCompCB }, mrdRangeGetDataCB };

    size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
    size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;

    // Will break out when no work detected 
    while (1) {
        // Pointer is probably safe w/o mutex, but JIK...
        g_mrdLock.lock();
        bool ok = (g_mrdResult.ok());
        g_mrdLock.unlock();
        if (!ok) break;

        int seq;
        g_mrdLock.lock();
        if (g_mrdNext >= g_mrdLast) {
            g_mrdLock.unlock();
            break;
        }
        seq = g_mrdNext + 1;
        g_mrdNext++;
        g_mrdLock.unlock();

        size_t retry_cnt = 0;
        multirange_data_t rangeData;
        do {
            // Work on a local copy of the structure in case an error occurs in the middle
            // of an upload.  If we updated in-place, on a retry the part would start
            // at the wrong offset and length.
            rangeData = g_mrdData[seq-1];
            rangeData.pCtx = &bucketContext;

            msg.str( std::string() ); // Clear
            msg << "Multirange:  Start range " << (int)seq << ", key \"" << g_mrdKey << "\", offset "
                << (long)rangeData.get_object_data.offset << ", len " << (int)rangeData.get_object_data.contentLength;
            printf( msg.str().c_str() );

            unsigned long long usStart = usNow();
            std::string hostname = s3GetHostname();
            bucketContext.hostName = hostname.c_str(); 
            S3_get_object( &bucketContext, g_mrdKey, NULL, rangeData.get_object_data.offset,
                           rangeData.get_object_data.contentLength, 0, &getObjectHandler, &rangeData );
            unsigned long long usEnd = usNow();
            double bw = (g_mrdData[seq-1].get_object_data.contentLength / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
            msg << " -- END -- BW=" << bw << " MB/s";
            printf( msg.str().c_str() );
            if (rangeData.status != S3StatusOK) s3_sleep( retry_wait, 0 );
        } while ((rangeData.status != S3StatusOK) && S3_status_is_retryable(rangeData.status) && (++retry_cnt < retry_count_limit));
        if (rangeData.status != S3StatusOK) {
            msg.str( std::string() ); // Clear
            msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ 
                << " - Error getting the S3 object: \"" << g_mrdKey << "\" range " << seq;
            if (rangeData.status >= 0) {
                msg << " - \"" << S3_get_status_name( rangeData.status ) << "\"";
            }
            result = ERROR( S3_GET_ERROR, msg.str() );
            printf( msg.str().c_str() );
            g_mrdLock.lock();
            g_mrdResult = result;
            g_mrdLock.unlock();
        }
    }
}*/


S3STSDate s3GetSTSDate()
{
    return S3STSAmzOnly;
}

static boost::mutex g_mpuLock; // Multipart upload has a mutex-protected global work queue
static volatile int g_mpuNext = 0;
static int g_mpuLast = -1;
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
       printf("g_mpuData[%d].manager->offset: %ld\n", i, g_mpuData[i].manager->offset);
       printf("m_gmpuData[%d].anager->remaining: %ld\n", i, g_mpuData[i].manager->remaining);
   }
}

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
}

static S3Status mpuInitRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

static void mpuInitRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    upload_manager_t *data = (upload_manager_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}


/******************* Multipart Put Callbacks *****************************/

/* Upload data from the part, use the plain callback_data reader */
static int mpuPartPutDataCB (
    int bufferSize,
    char *buffer,
    void *callbackData)
{
    return putObjectDataCallback( bufferSize, buffer, &((multipart_data_t*)callbackData)->put_object_data );
}

static S3Status mpuPartRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    multipart_data_t *data = (multipart_data_t *)callbackData;

    int seq = data->seq;
    const char *etag = properties->eTag;
fprintf(stderr, "%s:%d (%s) [seq-1=%d][properties->eTag=%p]\n", __FILE__, __LINE__, __FUNCTION__, seq-1, properties->eTag);
    if (etag) {
        data->manager->etags[seq - 1] = strdup(etag);
    } else {
        data->manager->etags[seq - 1] = strdup("");
    }

    return S3StatusOK;
}

static void mpuPartRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    multipart_data_t *data = (multipart_data_t *)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->put_object_data.pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}

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

/*#ifdef ERROR_INJECT
    g_error_mutex.lock();
    g_merr++;
    if (g_merr == g_merr_idx) {
        printf("[resource_name=%s] Injecting a XML upload error during S3 callback\n", get_resource_name().c_str());
        g_error_mutex.unlock();
        ret = -1;
    }
    g_error_mutex.unlock();
#endif*/

    return (int)ret;
}

static S3Status mpuCommitRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

static void mpuCommitRespCompCB (
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    upload_manager_t *data = (upload_manager_t*)callbackData;
    StoreAndLogStatus( status, error, __FUNCTION__, data->pCtx, &(data->status) );
    // Don't change the global error, we may want to retry at a higher level.
    // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
}

static S3Status mpuCancelRespPropCB (
    const S3ResponseProperties *properties,
    void *callbackData)
{
    return S3StatusOK;
}

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
}

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
printf("%s:%d (%s) [hostname=%s]\n", __FILE__, __LINE__, __FUNCTION__, bucketContext->hostName);
    S3_abort_multipart_upload(bucketContext, key, upload_id, &abortHandler);
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
    status = g_mpuCancelRespCompCB_status;
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
    if (status != S3StatusOK) {
        msg.str( std::string() ); // Clear
        msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ << " - Error cancelling the multipart upload of S3 object: \"" << key << "\"";
        if (status >= 0) {
            msg << " - \"" << S3_get_status_name(status) << "\"";
        }
        printf( "%s\n", msg.str().c_str() );
    }
}


/* Multipart worker thread, grabs a job from the queue and uploads it */
static void mpuWorkerThread(void *bucketContextParam, void *bytesParam)
{
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
    S3BucketContext bucketContext = *((S3BucketContext*)bucketContextParam);
    char *bytes = (char*)bytesParam;


    std::string resource_name = get_resource_name();

    irods::error result;
    std::stringstream msg;
    S3PutObjectHandler putObjectHandler = { {mpuPartRespPropCB, mpuPartRespCompCB }, &mpuPartPutDataCB };

    size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
    size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;


    /* Will break out when no work detected */
    while (1) {
        // Pointer is probably safe w/o mutex, but JIK...
        g_mpuLock.lock();
        bool ok = (g_mpuResult.ok());
        g_mpuLock.unlock();
        if (!ok) break;

        int seq;
        g_mpuLock.lock();
        if (g_mpuNext >= g_mpuLast) {
            g_mpuLock.unlock();
            break;
        }
        seq = g_mpuNext + 1;
        g_mpuNext++;
        g_mpuLock.unlock();

        multipart_data_t partData{};
        
        size_t retry_cnt = 0;
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
        do {
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
            // Work on a local copy of the structure in case an error occurs in the middle
            // of an upload.  If we updated in-place, on a retry the part would start
            // at the wrong offset and length.
            partData = g_mpuData[seq-1];
            partData.put_object_data.pCtx = &bucketContext;
            msg.str( std::string() ); // Clear
            msg << "Multipart:  Start part " << (int)seq << ", key \"" << g_mpuKey << "\", uploadid \"" << g_mpuUploadId << "\", offset "
                << (long)partData.put_object_data.offset << ", len " << (int)partData.put_object_data.contentLength;
            printf( "%s\n", msg.str().c_str() );
            msg.str( std::string() ); // Clear

            S3PutProperties *putProps = NULL;
            putProps = (S3PutProperties*)calloc( sizeof(S3PutProperties), 1 );
            if ( putProps && partData.enable_md5 ) {
                // jjames - not sure how to do MD5 piecewise 
                //putProps->md5 = s3CalcMD5( partData.put_object_data.fd, partData.put_object_data.offset, partData.put_object_data.contentLength, resource_name );
            }
            putProps->expires = -1;
            unsigned long long usStart = usNow();
            std::string hostname = s3GetHostname();
            bucketContext.hostName = hostname.c_str(); 
printf("%s:%d (%s) [hostname=%s][bucketName=%s][accessKeyId=%s][secretAccessKey=%s][g_mpuKey=%s]\n", __FILE__, __LINE__, __FUNCTION__, bucketContext.hostName, bucketContext.bucketName, bucketContext.accessKeyId, bucketContext.secretAccessKey, g_mpuKey);

            partData.put_object_data.bytes = bytes;
            partData.put_object_data.offset = 0;

            S3_upload_part(&bucketContext, g_mpuKey, putProps, &putObjectHandler, seq, g_mpuUploadId, partData.put_object_data.contentLength, 0, &partData);
            unsigned long long usEnd = usNow();
            double bw = (g_mpuData[seq-1].put_object_data.contentLength / (1024.0 * 1024.0)) / ( (usEnd - usStart) / 1000000.0 );
            // Clear up the S3PutProperties, if it exists
            if (putProps) {
                if (putProps->md5) free( (char*)putProps->md5 );
                free( putProps );
            }
            msg << " -- END -- BW=" << bw << " MB/s";
            printf( "%s\n", msg.str().c_str() );
printf("%s:%d (%s) [partData.status=%s]\n", __FILE__, __LINE__, __FUNCTION__, S3_get_status_name(partData.status));
            if (partData.status != S3StatusOK) s3_sleep( retry_wait, 0 );
        } while ((partData.status != S3StatusOK) && S3_status_is_retryable(partData.status) && (++retry_cnt < retry_count_limit));
        if (partData.status != S3StatusOK) {
            msg.str( std::string() ); // Clear
            msg << "[resource_name=" << resource_name << "] " << __FUNCTION__ << " - Error putting the S3 object: \"" << g_mpuKey << "\"" << " part " << seq;
            if(partData.status >= 0) {
                msg << " - \"" << S3_get_status_name(partData.status) << "\"";
            }
            result = ERROR( S3_PUT_ERROR, msg.str() );
            printf( "%s\n", msg.str().c_str() );
            g_mpuResult = result;
        }
    }
}


irods::error initialize_multipart_upload(
    const std::string& _filename,
    const std::string& _object_key,
    size_t _fileSize,
    size_t chunksize,
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
    long totalSeq = (_fileSize + chunksize - 1) / chunksize;
printf("%s:%d (%s) [totalSeq=%ld]\n", __FILE__, __LINE__, __FUNCTION__, totalSeq);

    multipart_data_t partData;
    int partContentLength = 0;

    bzero (&data, sizeof (data));
    //datajjames - .fd = cache_fd;
    data.contentLength = data.originalContentLength = _fileSize;

    // Allocate all dynamic storage now, so we don't start a job we can't finish later
    manager.etags = (char**)calloc(sizeof(char*) * totalSeq, 1);
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
    g_mpuData = (multipart_data_t*)calloc(totalSeq, sizeof(multipart_data_t));
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
    manager.xml = (char *)malloc((totalSeq+2) * 256);
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
printf("%s:%d (%s) Calling S3_initiate_multipart [bucketContext.hostName=%s]\n", __FILE__, __LINE__, __FUNCTION__, bucketContext.hostName);
        S3_initiate_multipart(&bucketContext, _object_key.c_str(), putProps, &mpuInitialHandler, NULL, &manager);
printf("%s:%d (%s) S3_initiate_multipart [manager.status=%s]\n", __FILE__, __LINE__, __FUNCTION__, S3_get_status_name(manager.status));
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

    g_mpuNext = 0;
    g_mpuLast = totalSeq;
    g_mpuUploadId = manager.upload_id;
    g_mpuKey = _object_key.c_str();
    for(seq = 1; seq <= totalSeq ; seq ++) {
        memset(&partData, 0, sizeof(partData));
        partData.manager = &manager;
        partData.seq = seq;
        partData.put_object_data = data;
        partContentLength = (data.contentLength > chunksize)?chunksize:data.contentLength;
printf("%s:%d (%s) [partContentLength=%d][chunkSize=%zu]\n", __FILE__, __LINE__, __FUNCTION__, partContentLength, chunksize);
        partData.put_object_data.contentLength = partContentLength;
        partData.put_object_data.offset = (seq-1) * chunksize;
        partData.enable_md5 = s3GetEnableMD5();
        partData.server_encrypt = s3GetServerEncrypt();
        g_mpuData[seq-1] = partData;
        data.contentLength -= partContentLength;
    }

    unsigned long long usStart = usNow();

    unsigned long long usEnd = usNow();
    double bw = (_fileSize / (1024.0*1024.0)) / ( (usEnd - usStart) / 1000000.0 );
    printf( "MultipartBW=%lf\n", bw);

    manager.remaining = 0;
    manager.offset  = 0;

    return result;

}

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
        if (putProps->md5) free( (char*)putProps->md5 );
        free( putProps );
    }

    return result;
}


int main(int argc, char **argv) { 

   if (argc < 3){ 
       std::cerr << "Usage:  multipart_upload <file> <multipart_size in MB>" << std::endl;
       return 1; 
   }

   std::string filename = argv[1];
   size_t multipart_size = string_to_size_t(argv[2]) * 1024 * 1024;
   if (multipart_size < 5 * 1024 * 1024) {
       std::cerr << "Minimum multipart size is 5 MB" << std::endl;
       return 1;
    }

    std::string hostname = s3GetHostname();

    // AWS
    std::string key_id = "xxx";
    std::string access_key = "xxx";

    // read input file into buffers
    std::vector<unsigned char*> char_buffers;
    std::vector<size_t> char_buffer_sizes;
    std::ifstream ifs;
    ifs.open(filename, std::ios::in | std::ios::binary | std::ios::ate); 
    size_t file_size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    while (ifs.tellg() < file_size) {
        size_t current_buffer_size = file_size - ifs.tellg() > multipart_size ? multipart_size : file_size - ifs.tellg();
        unsigned char *current_buffer = new unsigned char[current_buffer_size];
        ifs.read((char*)(current_buffer), current_buffer_size);
        char_buffers.push_back(current_buffer);
        char_buffer_sizes.push_back(current_buffer_size);

        std::cout << "read " << current_buffer_size << std::endl;
    }
    ifs.close();

    std::string bucket_name = "justinkylejames1";
    std::string object_key = filename;

    // initialize s3
    int flags = S3_INIT_ALL;
    S3PutProperties *putProps = NULL;
    S3SignatureVersion signature_version = s3GetSignatureVersion();
    if (signature_version == S3SignatureV4) {
        flags |= S3_INIT_SIGNATURE_V4;
    }
    int status = S3_initialize( "s3", flags, hostname.c_str() );

    upload_manager_t manager;
    S3BucketContext bucket_context;
    irods::error ret = initialize_multipart_upload(filename, object_key, file_size, multipart_size, key_id, access_key, bucket_name, manager, bucket_context, putProps); 

    print_g_mpuData(char_buffers.size());

printf("%s:%d (%s) [char_buffers.size()=%lu]\n", __FILE__, __LINE__, __FUNCTION__, char_buffers.size());

printf("%s:%d (%s) done initial part.\n", __FILE__, __LINE__, __FUNCTION__);


    // THIS PART IS DONE FOR EACH THREAD (s3FileWrite)
    
    std::thread *writer_threads = new std::thread[char_buffers.size()];

    for (unsigned int part_number = 0; part_number <  char_buffers.size(); ++part_number) {
printf("%s:%d (%s) start thread %d\n", __FILE__, __LINE__, __FUNCTION__, part_number);
        writer_threads[part_number] = std::thread(mpuWorkerThread, &bucket_context, char_buffers[part_number]);
    }

    for (unsigned int part_number = 0; part_number <  char_buffers.size(); ++part_number) {
        writer_threads[part_number].join();
printf("%s:%d (%s) joined thread %d\n", __FILE__, __LINE__, __FUNCTION__, part_number);
    }


printf("%s:%d (%s) done parallel part\n", __FILE__, __LINE__, __FUNCTION__);
    // THIS PART IS DONE ONCE AT END (s3FileClose) 
    
    ret = complete_multipart_upload(object_key, char_buffers.size(), bucket_context, manager, putProps);
printf("%s:%d (%s) done complete_multipart_upload\n", __FILE__, __LINE__, __FUNCTION__);

    // delete allocated memory (TODO change to smart ptr)
    for (int i = 0; i < char_buffers.size(); ++i) {
        delete[] char_buffers[i];
    }

    delete[] writer_threads;

printf("%s:%d (%s) [total_bytes_sent=%d]\n", __FILE__, __LINE__, __FUNCTION__, total_bytes_sent);



    return 0;
}

