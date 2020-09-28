package app.load

import app.load.configurations.LoadConfiguration
import app.load.mapreduce.UcInputFormat
import app.load.mapreduce.UcMapper
import app.load.repositories.S3Repository
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

fun main() {

    HBaseConfiguration.create().also { configuration ->
        jobInstance(configuration).also { job ->
            ConnectionFactory.createConnection(configuration).use { connection ->
                val targetTable = tableName(LoadConfiguration.HBase.table)
                connection.getTable(targetTable).use { table ->
                    HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(targetTable))
                }
            }

            S3Repository.connect().let { s3Repository ->
                s3Repository.objectSummaries().asSequence().map {"s3://${it.bucketName}/${it.key}"}
                        .map(::Path).toList()
                        .forEach { path -> FileInputFormat.addInputPath(job, path) }
            }

            FileOutputFormat.setOutputPath(job, Path(LoadConfiguration.MapReduce.outputDirectory))
            job.waitForCompletion(true)
            with (LoadIncrementalHFiles(configuration)) {
                run(arrayOf(LoadConfiguration.MapReduce.outputDirectory, LoadConfiguration.HBase.table))
            }
        }
    }
}

private fun jobInstance(configuration: Configuration) =
    Job.getInstance(configuration, "HBase corparate data bulk loader").apply {
        setJarByClass(UcMapper::class.java)
        mapperClass = UcMapper::class.java
        mapOutputKeyClass = ImmutableBytesWritable::class.java
        mapOutputValueClass = KeyValue::class.java
        inputFormatClass = UcInputFormat::class.java
    }

private fun tableName(name: String) = TableName.valueOf(name)
