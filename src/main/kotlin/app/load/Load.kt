package app.load

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser


fun main(args: Array<String>) {

    // table name should be args[0]
    // input prefix args[1]
    // output directory args[2]

    HBaseConfiguration.create().also { configuration ->
        jobInstance(configuration).also { job ->
            ConnectionFactory.createConnection(configuration).use { connection ->
                connection.getTable(tableName()).use { table ->
                    HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName()))
                }
            }

            // args[2] should be a s3 prefix/path - get list of objects and add to input path.
            arguments(args).map(::Path).also { paths ->
                paths.drop(1).forEach { inputFile -> FileInputFormat.addInputPath(job, inputFile) }
                val outputDirectory = paths[0]
                FileOutputFormat.setOutputPath(job, outputDirectory)
                job.waitForCompletion(true)
                with (LoadIncrementalHFiles(configuration)) {
                    run(arrayOf(outputDirectory.toString(), "epl"))
                }
            }
        }
    }
}

private fun arguments(args: Array<String>) =
    Configuration().let { GenericOptionsParser(it, args).remainingArgs }

private fun jobInstance(configuration: Configuration) =
    Job.getInstance(configuration, "hbase bulk import").apply {
        setJarByClass(UcMapper::class.java)
        mapperClass = UcMapper::class.java
        mapOutputKeyClass = ImmutableBytesWritable::class.java
        mapOutputValueClass = KeyValue::class.java
        inputFormatClass = UcInputFormat::class.java
    }

private fun tableName() = TableName.valueOf("epl")
