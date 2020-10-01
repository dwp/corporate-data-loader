
import app.load.domain.HBasePayload
import app.load.utility.TextUtils
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import java.sql.Connection
import java.sql.PreparedStatement


class MetadataStoreServiceTest : StringSpec({

    "Batch insert" {

        val statement = mock<PreparedStatement>()
        val sql = insertSql()

        val connection = mock<Connection> {
            on { prepareStatement(sql) } doReturn statement
        }

        val client = MetadataStoreService(connection)
        val payloads = (1..100).map { payloadNumber ->
            HBasePayload(key = "key-$payloadNumber".toByteArray(),
                        version = payloadNumber.toLong(),
                        topic = "topic-$payloadNumber",
                        partition = 2 * payloadNumber,
                        offset = (payloadNumber + 100).toLong())
        }

        client.recordBatch(payloads)
        verify(connection, times(1)).prepareStatement(sql)
        verifyNoMoreInteractions(connection)

        val textUtils = TextUtils()
        for (i in 1..100) {
            verify(statement, times(1)).setString(1, textUtils.printableKey("key-$i".toByteArray()))
            verify(statement, times(1)).setLong(2, i.toLong())
            verify(statement, times(1)).setString(3, "topic-$i")
            verify(statement, times(1)).setInt(4, 2 * i)
            verify(statement, times(1)).setLong(5, (i + 100).toLong())
        }
        verify(statement, times(100)).addBatch()
        verify(statement, times(1)).executeBatch()
        verifyNoMoreInteractions(statement)
    }
})

private fun insertSql(): String {
    return """
            INSERT INTO ucfs (hbase_id, hbase_timestamp, topic_name, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()
}
