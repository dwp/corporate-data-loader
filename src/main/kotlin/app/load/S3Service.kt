package app.load

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary

class S3Service {
    fun objectSummaries(s3Connection: AmazonS3, s3BucketName: String, s3Prefix: String): MutableList<S3ObjectSummary> {
        val objectSummaries = mutableListOf<S3ObjectSummary>()
        val request = listObjectsV2Request(s3BucketName, s3Prefix)
        var objectListing: ListObjectsV2Result?

        do {
            objectListing = s3Connection.listObjectsV2(request)
            objectSummaries.addAll(objectListing.objectSummaries)
            request.continuationToken = objectListing.nextContinuationToken
        } while (objectListing != null && objectListing.isTruncated)

        return objectSummaries
    }

    private fun listObjectsV2Request(s3BucketName: String, s3Prefix: String): ListObjectsV2Request =
        ListObjectsV2Request().apply {
            bucketName = s3BucketName
            prefix = s3Prefix
        }
}
