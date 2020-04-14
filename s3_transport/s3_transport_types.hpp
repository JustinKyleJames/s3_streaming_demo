#ifndef S3_TRANSPORT_TYPES_HPP
#define S3_TRANSPORT_TYPES_HPP

#include "circular_buffer.hpp"
#include <libs3.h>

namespace irods::experimental::io::s3_transport
{

    struct libs3_types
    {
        using status = S3Status;
        const static status status_ok = status::S3StatusOK;
        using bucket_context = S3BucketContext;
        using char_type   = char;
        using buffer_type = char_type*;
        using error_details = S3ErrorDetails;
        using response_properties = S3ResponseProperties;
    };

} // irods::experimental::io::s3_transport

#endif // S3_TRANSPORT_TYPES_HPP

