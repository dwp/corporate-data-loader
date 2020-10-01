package app.load.mapreduce

import app.load.configurations.CorporateMemoryConfiguration
import app.load.configurations.MetadataStoreConfiguration
import app.load.domain.HBasePayload
import app.load.utility.Converter
import app.load.utility.MessageParser
import com.beust.klaxon.JsonObject
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.LineNumberReader
import java.util.zip.GZIPInputStream
import kotlin.streams.toList

class UcRecordReader: RecordReader<LongWritable, Text>() {

    override fun initialize(split: InputSplit, context: TaskAttemptContext) =
        (split as FileSplit).path.let { path ->
            logger.info("Starting split", "path" to path.toString())
            path.getFileSystem(context.configuration).let { fs ->
                input = BufferedReader(InputStreamReader(GZIPInputStream(fs.open(path))))
                currentFileSystem = fs
                currentPath = path
            }
        }

    override fun nextKeyValue() = hasNext(input?.readLine())

    override fun close() {
        IOUtils.closeStream(input)
        logger.info("Completed split", "path" to "${currentPath?.toString()}")
        if (MetadataStoreConfiguration.writeToMetadataStore) {
            Regex(CorporateMemoryConfiguration.topicPattern).let { regex ->
                regex.find(currentPath.toString())?.let { result ->
                    val (topic, partition, firstOffset) = result.destructured
                    LineNumberReader(InputStreamReader(GZIPInputStream(currentFileSystem?.open(currentPath)))).use { reader ->
                        val payloads = metadataStorePayloads(reader, topic, partition, firstOffset)
                        MetadataStoreService.connect().use { it.recordBatch(payloads) }
                        logger.info("Written split to metadatastore", "path" to "${currentPath?.toString()}",
                                "topic" to topic, "partition" to "$partition", "first_offset" to "$firstOffset")
                    }
                }
            }
        }
    }

    private fun metadataStorePayloads(reader: LineNumberReader, topic: String, partition: String, firstOffset: String): List<HBasePayload> =
        reader.lines().map { line ->
            with(convertor) {
                hBasePayload(convertToJson(line), topic, partition, firstOffset.toInt() + reader.lineNumber)
            }
        }.toList()


    private fun Converter.hBasePayload(json: JsonObject, topic: String, partition: String, offset: Int): HBasePayload {
        val (_, key) = messageParser.generateKeyFromRecordBody(json)
        val (timestamp) = getLastModifiedTimestamp(json)
        val version = getTimestampAsLong(timestamp)
        return HBasePayload(key, version, topic, partition.toInt(), offset.toLong())
    }

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
    private var currentPath: Path? = null
    private var currentFileSystem: FileSystem? = null
    private val messageParser = MessageParser()
    private val convertor = Converter()

    companion object {
        private val logger = DataworksLogger.getLogger(UcRecordReader::class.java.toString())
    }
}
