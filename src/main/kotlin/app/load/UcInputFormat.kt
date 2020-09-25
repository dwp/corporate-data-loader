package app.load

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

class UcInputFormat: FileInputFormat<LongWritable, Text>() {
    override fun createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader<LongWritable, Text> =
        UcRecordReader().apply {
            initialize(split, context)
        }
}
