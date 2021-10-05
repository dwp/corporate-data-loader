package app.load.mapreduce

import app.load.utility.Converter
import app.load.utility.MessageParser
import com.beust.klaxon.JsonObject
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory

class UcMapper: Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>() {

    public override fun map(key: LongWritable, value: Text, context: Context) {
        try {
            val validBytes = bytes(value)
            val json = convertor.convertToJson(validBytes)
            val (ordered, hbaseKey) = messageParser.generateKeyFromRecordBody(json)
            ordered?.let {
                hKey(hbaseKey).let { hkey ->
                    context.write(hkey, keyValue(hkey, json, validBytes))
                    context.getCounter(Counters.DATAWORKS_SUCCEEDED_RECORD_COUNTER).increment(1)
                }
            } ?: run {
                logger.error("Failed to parse id from '$json', target table '${context.configuration[targetTableKey]}")
                context.getCounter(Counters.DATAWORKS_FAILED_RECORD_COUNTER).increment(1)
            }
        } catch (e: Exception) {
            logger.error(
                "Failed to map record '$value', " +
                        "target table '${context.configuration[targetTableKey]}': '${e.message}' ", e
            )
            context.getCounter(Counters.DATAWORKS_FAILED_RECORD_COUNTER).increment(1)
        }
    }

    private fun hKey(key: ByteArray): ImmutableBytesWritable = ImmutableBytesWritable().apply { set(key) }

    private fun keyValue(key: ImmutableBytesWritable, json: JsonObject, bytes: ByteArray): KeyValue =
        KeyValue(key.get(), columnFamily, columnQualifier, version(json), bytes)

    private fun version(json: JsonObject): Long =
        with(convertor) { getTimestampAsLong(getLastModifiedTimestamp(json).first) }

    private fun bytes(value: Text): ByteArray = value.bytes.sliceArray(0 until value.length)

    private val messageParser = MessageParser()
    private val convertor = Converter()
    private val columnFamily by lazy { Bytes.toBytes("cf") }
    private val columnQualifier by lazy { Bytes.toBytes("record") }

    companion object {
        private val logger = LoggerFactory.getLogger(UcMapper::class.java)
        private const val targetTableKey = "hbase.table"
    }
}
