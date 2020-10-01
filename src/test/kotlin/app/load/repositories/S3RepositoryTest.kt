package app.load.repositories

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class S3RepositoryTest: StringSpec() {
    init {

        val bucket = "bucket"
        val objectPrefix = "prefix"

        "Pages when there are truncated results" {

            val objectSummaries1 = objectSummaries(1)
            val truncatedResult1 = objectSummaryResult(1, true, objectSummaries1)

            val objectSummaries2 = objectSummaries(2)
            val truncatedResult2 = objectSummaryResult(2, true, objectSummaries2)

            val objectSummaries3 = objectSummaries(3)
            val finalResult = objectSummaryResult(3, false, objectSummaries3)

            val requestCaptor = argumentCaptor<ListObjectsV2Request>()
            val amazonS3 = mock<AmazonS3> {
                on {
                    listObjectsV2(requestCaptor.capture())
                } doReturnConsecutively listOf(truncatedResult1, truncatedResult2, finalResult)
            }

            val s3Repository = S3Repository(amazonS3, bucket, objectPrefix, "db.database.collection")
            val actual = s3Repository.objectSummaries()

            actual shouldBe listOf(objectSummaries1, objectSummaries2, objectSummaries3).flatten()

            requestCaptor.allValues.size shouldBe 3

            requestCaptor.allValues.forEachIndexed { index, request ->
                request.bucketName shouldBe bucket
                request.prefix shouldBe objectPrefix
                request.continuationToken shouldBe if (index == 0) {
                    null
                }
                else {
                    "$index"
                }
            }
        }

        "Filters out non-targeted topics" {
            val (targetedSummaries, filteredSummaries) = mixedSummaries(1)
            val amazonS3 = amazonS3(targetedSummaries, filteredSummaries)
            val s3Repository = S3Repository(amazonS3, bucket, objectPrefix, "db.database.collection")
            val actual = s3Repository.objectSummaries()
            actual shouldBe targetedSummaries
        }

        "Doesn't filter if topic is blank" {
            val (targetedSummaries, filteredSummaries) = mixedSummaries(1)
            val amazonS3 = amazonS3(targetedSummaries, filteredSummaries)
            val s3Repository = S3Repository(amazonS3, bucket, objectPrefix, "")
            val actual = s3Repository.objectSummaries()
            actual shouldBe targetedSummaries + filteredSummaries
        }
    }

    private fun amazonS3(targetedSummaries: List<S3ObjectSummary>, filteredSummaries: List<S3ObjectSummary>): AmazonS3 =
            amazonS3(objectSummaryResult(1, false, targetedSummaries + filteredSummaries))


    private fun amazonS3(result: ListObjectsV2Result): AmazonS3 =
        mock<AmazonS3> {
            on {
                listObjectsV2(any<ListObjectsV2Request>())
            } doReturn result
        }

    private fun mixedSummaries(requestNumber: Int) =
            Pair(objectSummaries(requestNumber), filteredSummaries(requestNumber))

    private fun objectSummaries(requestNumber: Int): List<S3ObjectSummary> =
            listOf(objectSummary(requestNumber, 1), objectSummary(requestNumber, 2))

    private fun filteredSummaries(requestNumber: Int): List<S3ObjectSummary> =
            listOf(filteredSummary(requestNumber, 1), filteredSummary(requestNumber, 2))

    private fun objectSummary(requestNumber: Int, objectNumber: Int) =
            mock<S3ObjectSummary> {
                on { key } doReturn key("db.database.collection", requestNumber, objectNumber)
            }

    private fun filteredSummary(requestNumber: Int, objectNumber: Int) =
            mock<S3ObjectSummary> {
                on { key } doReturn key("db.filtered_database.filtered_collection", requestNumber, objectNumber)
            }

    private fun key(topic: String, requestNumber: Int, objectNumber: Int) =
            "corporate_storage/2020/01/01/database/collection/${topic}_${requestNumber}_${objectNumber}-${objectNumber + 10}.jsonl.gz"

    private fun objectSummaryResult(requestNumber: Int, truncated: Boolean, summaries: List<S3ObjectSummary>): ListObjectsV2Result =
            mock {
                on { isTruncated } doReturn truncated
                on { nextContinuationToken } doReturn "$requestNumber"
                on { objectSummaries } doReturn summaries
            }
}
