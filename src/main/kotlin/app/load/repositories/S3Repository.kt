package app.load.repositories

import app.load.configurations.AwsConfiguration
import app.load.configurations.S3Configuration
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.S3ObjectSummary

class S3Repository(private val amazonS3: AmazonS3,
                   private val bucket: String,
                   private val objectPrefix: String,
                   private val topicName: String) {

    fun objectSummaries(objectSummaries: MutableList<S3ObjectSummary> = mutableListOf(), nextContinuationToken: String = ""):
            List<S3ObjectSummary> {
        val request = listObjectsRequest(nextContinuationToken)
        val objectListing = amazonS3.listObjectsV2(request)
        objectSummaries.addAll(objectListing.objectSummaries)

        if (objectListing != null && !objectListing.isTruncated) {
            val filenameRe = Regex("""/\Q${topicName}\E_\d+_\d+-\d+\.jsonl\.gz$""")
            return objectSummaries.filter { topicName.isBlank() || filenameRe.find(it.key) != null }
        }

        return objectSummaries(objectSummaries, objectListing.nextContinuationToken)
    }

    private fun listObjectsRequest(nextContinuationToken: String) =
            ListObjectsV2Request().apply {
                bucketName = bucket
                prefix = objectPrefix
                if (nextContinuationToken.isNotBlank()) {
                    continuationToken = nextContinuationToken
                }
            }

    companion object {
        fun connect() = S3Repository(amazonS3, S3Configuration.bucket, S3Configuration.prefix, S3Configuration.topicName)
        private val amazonS3: AmazonS3 by lazy {
            if (AwsConfiguration.useLocalStack) {
                AmazonS3ClientBuilder.standard().run {
                    withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://aws:4566", "eu-west-2"))
                    withClientConfiguration(ClientConfiguration().apply {
                        withProtocol(Protocol.HTTP)
                    })
                    withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials("accessKey", "secretKey")))
                    withPathStyleAccessEnabled(true)
                    disableChunkedEncoding()
                    build()
                }
            } else {
                AmazonS3ClientBuilder.standard().run {
                    withCredentials(DefaultAWSCredentialsProviderChain())
                    withRegion(AwsConfiguration.region)
                    withClientConfiguration(ClientConfiguration().apply {
                        maxConnections = S3Configuration.maxConnections
                    })
                    build()
                }
            }
        }
    }
}
