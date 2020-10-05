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

class UcMapper: Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>() {

    public override fun map(key: LongWritable, value: Text, context: Context) {
        val validBytes = bytes(value)
        val json = convertor.convertToJson(validBytes)
        hKey(json)?.let { hkey ->
            context.write(hkey, keyValue(hkey, json, validBytes))
        }
    }

    private fun hKey(json: JsonObject): ImmutableBytesWritable? =
            messageParser.generateKeyFromRecordBody(json).let {(_, key) ->
                ImmutableBytesWritable().apply { set(key) }
            }

    private fun keyValue(key: ImmutableBytesWritable, json: JsonObject, bytes: ByteArray): KeyValue =
        KeyValue(key.get(), columnFamily, columnQualifier, version(json), bytes)

    private fun version(json: JsonObject): Long =
        with (convertor) { getTimestampAsLong(getLastModifiedTimestamp(json).first) }

    private fun bytes(value: Text): ByteArray = value.bytes.sliceArray(0 until value.length)

    private val messageParser = MessageParser()
    private val convertor = Converter()
    private val columnFamily by lazy { Bytes.toBytes("cf") }
    private val columnQualifier by lazy { Bytes.toBytes("record") }
}
