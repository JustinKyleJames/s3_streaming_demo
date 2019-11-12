#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/Array.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>

#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <thread>

/*#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/platform/FileSystem.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/transfer/TransferHandle.h>*/


using namespace std;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Client;
using namespace Aws::Http;
using namespace Aws::Utils;

static const char* const ALLOCATION_TAG = "S3_MULTIPART_TEST";
typedef Aws::Utils::Array<unsigned char> ByteBuffer;

std::shared_ptr<S3Client> createClient() {
    ClientConfiguration config;
    config.region = Aws::Region::US_EAST_1;
    config.scheme = Scheme::HTTPS;
    config.connectTimeoutMs = 30000;
    config.requestTimeoutMs = 30000;
    //config.readRateLimiter = Limiter;
    //config.writeRateLimiter = Limiter;
    config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);

    return Aws::MakeShared<S3Client>(ALLOCATION_TAG, 
            Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG), config, 
            AWSAuthV4Signer::PayloadSigningPolicy::Never /*signPayloads*/, true /*useVirtualAddressing*/);
}

size_t string_to_size_t(const std::string& str) {
    std::stringstream sstream(str);
    size_t result;
    sstream >> result;
    return result;
}

std::shared_ptr<Aws::StringStream> Create5MbStreamForUploadPart(const char* partTag);
UploadPartOutcomeCallable makeUploadPartOutcomeAndGetCallable(unsigned partNumber, const ByteBuffer& md5OfStream, const std::shared_ptr<Aws::IOStream>& partStream,
                                                                     const Aws::String& bucketName, const std::string& objectName, const Aws::String& uploadId);

int s3FileWrite(void *_buf, int _len, unsigned int part_number, CompletedPart *completedPart, const Aws::String *bucket_name, std::string *filename, const Aws::String *upload_id); 

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
  
    // read input file into buffers
    std::vector<unsigned char*> char_buffers;
    std::vector<size_t> char_buffer_sizes;
    ifstream ifs;
    ifs.open(filename, ios::in | ios::binary | ios::ate); 
    size_t file_size = ifs.tellg();
    ifs.seekg(0, ios::beg);

    while (ifs.tellg() < file_size) {
        size_t current_buffer_size = file_size - ifs.tellg() > multipart_size ? multipart_size : file_size - ifs.tellg();
        unsigned char *current_buffer = new unsigned char[current_buffer_size];
        ifs.read((char*)(current_buffer), current_buffer_size);
        char_buffers.push_back(current_buffer);
        char_buffer_sizes.push_back(current_buffer_size);

        std::cout << "read " << current_buffer_size << std::endl;
    }
    ifs.close();

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    std::shared_ptr<S3Client> client = createClient();

    Aws::String bucketName = "justinkylejames1";
    const char* multipartKeyName = filename.c_str();

    // THIS PART IS DONE ONCE AT BEGINNING (s3FileOpen)

    CreateMultipartUploadRequest createMultipartUploadRequest;
    createMultipartUploadRequest.SetBucket(bucketName);
    createMultipartUploadRequest.SetKey(multipartKeyName);
    createMultipartUploadRequest.SetContentType("text/plain");

    CreateMultipartUploadOutcome createMultipartUploadOutcome = client->CreateMultipartUpload(
            createMultipartUploadRequest);

    if (!createMultipartUploadOutcome.IsSuccess()) {
        std::cerr << "createMultipartUploadOutcome failed" << std::endl;
        return 1;
    }

    Aws::String upload_id = createMultipartUploadOutcome.GetResult().GetUploadId();

printf("%s:%d (%s) done initial part.  upload_id=%s\n", __FILE__, __LINE__, __FUNCTION__, upload_id.c_str());


    // THIS PART IS DONE FOR EACH THREAD (s3FileWrite)
    
    std::thread *writer_threads = new std::thread[char_buffers.size()];
    CompletedPart *completedPart = new CompletedPart[char_buffers.size()];

    for (unsigned int part_number = 0; part_number <  char_buffers.size(); ++part_number) {
        //s3FileWrite(char_buffers[part_number], char_buffer_sizes[part_number], part_number + 1, &completedPart[part_number], &bucketName, &filename, &upload_id);
printf("%s:%d (%s) start thread %d\n", __FILE__, __LINE__, __FUNCTION__, part_number);
        writer_threads[part_number] = std::thread(s3FileWrite, char_buffers[part_number], char_buffer_sizes[part_number], part_number + 1, &completedPart[part_number], &bucketName, &filename, &upload_id);
        //std::thread t(s3FileWrite, char_buffers[part_number], char_buffer_sizes[part_number], part_number + 1, &completedPart[part_number], &bucketName, &filename, &upload_id);
        //t.join();
    }

    for (unsigned int part_number = 0; part_number <  char_buffers.size(); ++part_number) {
        writer_threads[part_number].join();
printf("%s:%d (%s) joined thread %d\n", __FILE__, __LINE__, __FUNCTION__, part_number);
    }


printf("%s:%d (%s) done parallel part\n", __FILE__, __LINE__, __FUNCTION__);
    // THIS PART IS DONE ONCE AT END (s3FileClose) 
    
    CompleteMultipartUploadRequest completeMultipartUploadRequest;
    completeMultipartUploadRequest.SetBucket(bucketName);
    completeMultipartUploadRequest.SetKey(multipartKeyName);
    completeMultipartUploadRequest.SetUploadId(createMultipartUploadOutcome.GetResult().GetUploadId());

    CompletedMultipartUpload completedMultipartUpload;
    for (int i = 0; i < char_buffers.size(); ++i) { 
        completedMultipartUpload.AddParts(completedPart[i]);
    }

    completeMultipartUploadRequest.WithMultipartUpload(completedMultipartUpload);
    
    CompleteMultipartUploadOutcome completeMultipartUploadOutcome = client->CompleteMultipartUpload(
            completeMultipartUploadRequest);
    
    if (!completeMultipartUploadOutcome.IsSuccess()) {
        std::cerr << "completeMultipartUploadOutcome failed" << std::endl;
        return 1;
    }

    // delete allocated memory (TODO change to smart ptr)
    for (int i = 0; i < char_buffers.size(); ++i) {
        delete[] char_buffers[i];
    }

    Aws::ShutdownAPI(options);
    delete[] completedPart;
    delete[] writer_threads;

    return 0;
}

int s3FileWrite(void *_buf, int _len, unsigned int part_number, CompletedPart *completedPart, const Aws::String *bucket_name, std::string *filename, const Aws::String *upload_id) {

    // create null terminated string from _buf (see if there is another option to avoid copy)
    unsigned char *null_terminated_buffer = new unsigned char[_len+1];
    memcpy(null_terminated_buffer, _buf, _len);
    null_terminated_buffer[_len] = 0;

    Aws::String scratchString;
    scratchString.reserve(_len+1);

    // 5MB is a hard minimum for multi part uploads; make sure the final string is at least that long

    scratchString.append( (const char*)null_terminated_buffer );

    delete[] null_terminated_buffer;

    std::shared_ptr<Aws::StringStream> streamPtr = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG, scratchString);

    streamPtr->seekg(0);
    streamPtr->seekp(0, std::ios_base::end);

    ByteBuffer partMd5(HashingUtils::CalculateMD5(*streamPtr));
    UploadPartOutcomeCallable uploadPartOutcomeCallable = makeUploadPartOutcomeAndGetCallable(part_number, partMd5, streamPtr, *bucket_name,
                                                *filename, *upload_id);

printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, part_number);
    UploadPartOutcome uploadPartOutcome = uploadPartOutcomeCallable.get();                  // delay here
printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, part_number);

    if (!uploadPartOutcome.IsSuccess()) {
        std::cerr << "UploadPartOutcome failed" << std::endl;
        return 0;
    }


    completedPart->SetETag(uploadPartOutcome.GetResult().GetETag());
    completedPart->SetPartNumber(part_number);

    return 1;

}


/*std::shared_ptr<Aws::StringStream> Create5MbStreamForUploadPart(const char* partTag)
{
    uint32_t fiveMbSize = 5 * 1024 * 1024;

    Aws::StringStream patternStream;
    patternStream << "Multi-Part upload Test Part " << partTag << ":" << std::endl;
    Aws::String pattern = patternStream.str();

    Aws::String scratchString;
    scratchString.reserve(fiveMbSize);

    // 5MB is a hard minimum for multi part uploads; make sure the final string is at least that long
    uint32_t patternCopyCount = static_cast< uint32_t >( fiveMbSize / pattern.size() + 1 );
    for(uint32_t i = 0; i < patternCopyCount; ++i)
    {
        scratchString.append( pattern );
    }

    std::shared_ptr<Aws::StringStream> streamPtr = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG, scratchString);

    streamPtr->seekg(0);
    streamPtr->seekp(0, std::ios_base::end);

    return streamPtr;
}*/


UploadPartOutcomeCallable makeUploadPartOutcomeAndGetCallable(unsigned partNumber, const ByteBuffer& md5OfStream,
                                                                     const std::shared_ptr<Aws::IOStream>& partStream,
                                                                     const Aws::String& bucketName, const std::string& objectName, const Aws::String& uploadId)
{
printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, partNumber);
    // Create a client
    ClientConfiguration config;
    config.region = Aws::Region::US_EAST_1;
    config.scheme = Scheme::HTTPS;
    config.connectTimeoutMs = 30000;
    config.requestTimeoutMs = 30000;
    //config.readRateLimiter = Limiter;
    //config.writeRateLimiter = Limiter;
    config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);
printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, partNumber);

    UploadPartRequest uploadPartRequest;
    uploadPartRequest.SetBucket(bucketName);
    uploadPartRequest.SetKey(objectName.c_str());
    uploadPartRequest.SetPartNumber(partNumber);
    uploadPartRequest.SetUploadId(uploadId);
    uploadPartRequest.SetBody(partStream);
    uploadPartRequest.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(md5OfStream));
printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, partNumber);

    auto startingPoint = partStream->tellg();
    partStream->seekg(0LL, partStream->end);
    uploadPartRequest.SetContentLength(static_cast<long>(partStream->tellg()));
    partStream->seekg(startingPoint);
printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, partNumber);

    static std::shared_ptr<S3Client> client;

    client = Aws::MakeShared<S3Client>(ALLOCATION_TAG,                                                    // delay here
            Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG), config, 
            AWSAuthV4Signer::PayloadSigningPolicy::Never /*signPayloads*/, true /*useVirtualAddressing*/);
printf("%s:%d (%s) partNumber=%d\n", __FILE__, __LINE__, __FUNCTION__, partNumber);

    return client->UploadPartCallable(uploadPartRequest);   // submits and returns a future
}
