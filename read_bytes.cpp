#include <streambuf>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/Object.h>
#include <iostream>

long scanObject(Aws::String region,
                Aws::String bucket_name,
                Aws::String object_name);


int main(int argc, char** argv)
{
   long scanned=0;

   if (argc < 4){ 
       std::cerr << "Usage:  read_bytes <region> <bucket> <object>" << std::endl;
       return(1); 
   }
   Aws::String region = argv[1];
   Aws::String bucket_name = argv[2];
   Aws::String object_name = argv[3];

   Aws::SDKOptions options;
   Aws::InitAPI(options);

   scanned = scanObject(region,bucket_name,object_name);
   printf("Scanned %ld bytes of object\n",scanned);

   Aws::ShutdownAPI(options);

   return(0);
}

long scanObject(Aws::String region,
                Aws::String bucket_name,
                Aws::String object_name)
{
   int   rbytes, nbytes;
   char range[48], *buffer;
   long filesize, bytesleft, start;

   Aws::Client::ClientConfiguration cconfig;
   cconfig.region = region;
   Aws::S3::S3Client s3_client(cconfig);

   Aws::S3::Model::HeadObjectRequest head_object_request;
   head_object_request.WithBucket(bucket_name).WithKey(object_name);
   Aws::S3::Model::HeadObjectOutcome head_object;
   head_object = s3_client.HeadObject(head_object_request);
   if (!head_object.IsSuccess())
   {
      printf("AWS Object, /%s/%s, failed to locate\n",
             region.c_str(),object_name.c_str());
      return(-1);
   }

   filesize = head_object.GetResult().GetContentLength();
   bytesleft = filesize;

   start = 0;
   nbytes = 1048576;
   buffer = (char*)malloc(nbytes*2);

   Aws::S3::Model::GetObjectRequest read_request;
   while(bytesleft > 0)
   {
      if (bytesleft > 1048576){ nbytes = 1048576; }else{ nbytes = bytesleft; }

      sprintf(range,"bytes=%ld-%ld",start,start+nbytes-1);

      read_request.SetBucket(bucket_name);
      read_request.SetKey(object_name);
      read_request.SetRange(range);

      auto results = s3_client.GetObject(read_request);

      if (!results.IsSuccess())
      {
         printf("\nUnable to fetch object range %s from s3 bucket.\n",range);
         return(-1);
      }
      rbytes = results.GetResult().GetContentLength();

      std::streambuf* body = results.GetResult().GetBody().rdbuf();
      body->sgetn(buffer,rbytes);

      start += rbytes;
      bytesleft -= rbytes;
   }

   return(filesize);
}
