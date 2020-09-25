package app.load

import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream

class UcRecordReader: RecordReader<LongWritable, Text>() {

    override fun initialize(split: InputSplit, context: TaskAttemptContext) =
        (split as FileSplit).path.let { path ->
            path.getFileSystem(context.configuration).let { fs ->
                input = BufferedReader(InputStreamReader(GZIPInputStream(fs.open(path))))
            }
        }

    override fun nextKeyValue() = hasNext(input?.readLine())
    override fun close() = IOUtils.closeStream(input)
    override fun getCurrentKey(): LongWritable = LongWritable()
    override fun getCurrentValue(): Text? = value
    override fun getProgress(): Float = .5f

    private fun hasNext(line: String?) =
            if (line != null) {
                value = Text(line)
                true
            } else {
                false
            }

    private var input: BufferedReader? = null
    private var value: Text? = null
}
