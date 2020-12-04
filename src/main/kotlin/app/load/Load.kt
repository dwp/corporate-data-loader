package app.load

import app.load.configurations.CorporateMemoryConfiguration
import app.load.configurations.MapReduceConfiguration
import app.load.mapreduce.UcInputFormat
import app.load.mapreduce.UcMapper
import app.load.repositories.S3Repository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
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
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.slf4j.LoggerFactory
import java.util.*


class Load : Configured(), Tool {

    override fun run(args: Array<out String>?): Int {
        conf.also { configuration ->
            jobInstance(configuration).also { job ->
                ConnectionFactory.createConnection(configuration).use { connection ->
                    val targetTable = tableName(CorporateMemoryConfiguration.table)
                    connection.getTable(targetTable).use { table ->
                        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(targetTable))
                    }
                }

                val summaries = S3Repository.connect().allObjectSummaries()

                if (summaries.isNotEmpty()) {
                    FileInputFormat.setInputPaths(job, *summaries.asSequence().map { "s3://${it.bucketName}/${it.key}" }
                            .map(::Path).toList().toTypedArray())
                    FileOutputFormat.setOutputPath(job, Path(MapReduceConfiguration.outputDirectory))

                    if (job.waitForCompletion(true)) {
                        logCounters(job)
                        with(LoadIncrementalHFiles(configuration)) {
                            val returnCode = run(arrayOf(MapReduceConfiguration.outputDirectory, CorporateMemoryConfiguration.table))
                            logger.info("Load HFiles returned code $returnCode")
                        }
                    }
                    else {
                        logger.info("Job failed")
                        logCounters(job)
                    }
                }
            }
        }
        return 0
    }

    private fun logCounters(job: Job) {
        job.counters.forEach { counterGroup ->
            counterGroup.forEach { counter ->
                logger.info("COUNTER ${counter.displayName}: ${counter.value}")
            }
        }
    }

    private fun jobInstance(configuration: Configuration) =
            Job.getInstance(configuration,
                "HBase corporate data loader '${CorporateMemoryConfiguration.table}': '${Date()}'").apply {
                setJarByClass(UcMapper::class.java)
                mapperClass = UcMapper::class.java
                mapOutputKeyClass = ImmutableBytesWritable::class.java
                mapOutputValueClass = KeyValue::class.java
                inputFormatClass = UcInputFormat::class.java
            }

    private fun tableName(name: String) = TableName.valueOf(name)

    companion object {
        private val logger = LoggerFactory.getLogger(Load::class.java)
    }
}

fun main(args: Array<String>) {
    ToolRunner.run(HBaseConfiguration.create(), Load(), args)
}

