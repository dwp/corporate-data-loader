package app.load

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
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

    val targetTable = tableName(args[0])

    HBaseConfiguration.create().also { configuration ->
        jobInstance(configuration).also { job ->
            ConnectionFactory.createConnection(configuration).use { connection ->
                connection.getTable(targetTable).use { table ->
                    HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(targetTable))
                }
            }


            // args[1] is the input bucket, args[2] is the prefix
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

//val amazonS3 by lazy {
//    if (Config.AwsS3.useLocalStack) {
//        AmazonS3ClientBuilder.standard()
//                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(localstackServiceEndPoint, localstackSigningRegion))
//                .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
//                .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(localstackAccessKey, localstackSecretKey)))
//                .withPathStyleAccessEnabled(true)
//                .disableChunkedEncoding()
//                .build()
//    }
//    else {
//        AmazonS3ClientBuilder.standard()
//                .withCredentials(DefaultAWSCredentialsProviderChain())
//                .withRegion(Config.AwsS3.region)
//                .withClientConfiguration(ClientConfiguration().apply {
//                    maxConnections = Config.AwsS3.maxConnections
//                })
//                .build()
//}


private fun arguments(args: Array<String>) =
    Configuration().let { GenericOptionsParser(it, args).remainingArgs }

private fun jobInstance(configuration: Configuration) =
    Job.getInstance(configuration, "HBase corparate data bulk loader").apply {
        setJarByClass(UcMapper::class.java)
        mapperClass = UcMapper::class.java
        mapOutputKeyClass = ImmutableBytesWritable::class.java
        mapOutputValueClass = KeyValue::class.java
        inputFormatClass = UcInputFormat::class.java
    }

private fun tableName(name: String) = TableName.valueOf(name)
