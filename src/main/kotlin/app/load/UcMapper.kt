package app.load

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class UcMapper: Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>() {

    override fun map(key: LongWritable, value: Text, context: Context) {
        hKey(value)?.let { context.write(it, keyValue(it, value)) }
    }

    private fun hKey(value: Text): ImmutableBytesWritable? =
            Regex("""^\d+""").find(value.toString())?.let {
                ImmutableBytesWritable().apply {
                    set(Bytes.toBytes(it.value))
                }
            }

    private fun keyValue(key: ImmutableBytesWritable, value: Text) =
            KeyValue(key.get(), Bytes.toBytes("cf"), Bytes.toBytes("record"), Bytes.toBytes(value.toString()))
}
