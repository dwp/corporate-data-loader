package app.load.mapreduce

import app.load.configurations.CorporateMemoryConfiguration
import app.load.configurations.MetadataStoreConfiguration
import app.load.domain.HBasePayload
import app.load.services.MetadataStoreService
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
import org.slf4j.LoggerFactory
import java.io.*
import java.util.zip.GZIPInputStream
import kotlin.streams.toList
import kotlin.time.ExperimentalTime

class UcRecordReader: RecordReader<LongWritable, Text>() {

    override fun initialize(split: InputSplit, context: TaskAttemptContext) =
        (split as FileSplit).path.let { path ->
            try {
                logger.info("Starting split '${path}', target table '${context.configuration["hbase.table"]}'")
                path.getFileSystem(context.configuration).let { fs: FileSystem ->
                    input = with (ByteArrayOutputStream()) {
                        use { target -> fs.open(path).use { source -> source.copyTo(target) } }
                        BufferedReader(InputStreamReader(GZIPInputStream(ByteArrayInputStream(this.toByteArray()))))
                    }
                    currentFileSystem = fs
                    currentPath = path
                }
            } catch (e: Exception) {
                logger.error("Failed to initialize split '$path', target table '${context.configuration["hbase.table"]}': '${e.message}'.", e)
                context.getCounter(Counters.DATAWORKS_FAILED_SPLIT_COUNTER).increment(1)
            }
        }

    override fun nextKeyValue() =
        try {
            hasNext(input?.readLine())
        } catch (e: Exception) {
            logger.error("Failed to read from split '$currentPath': '${e.message}'.", e)
            throw e
        }

    @ExperimentalTime
    override fun close() {
        IOUtils.closeStream(input)
        logger.info("Completed split ${currentPath?.toString()}")
        if (MetadataStoreConfiguration.writeToMetadataStore) {
            Regex(CorporateMemoryConfiguration.archivedFilePattern).let { regex ->
                regex.find(currentPath.toString())?.let { result ->
                    val (topic, partition, firstOffset) = result.destructured
                    LineNumberReader(InputStreamReader(GZIPInputStream(currentFileSystem?.open(currentPath)))).use { reader ->
                        val payloads = metadataStorePayloads(reader, topic, partition, firstOffset)
                        MetadataStoreService.connect().use { it.recordBatch(payloads) }
                        logger.info("Written split to metadatastore ${currentPath?.toString()} " +
                                "topic $topic, partition $partition, first_offset $firstOffset")
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
        private val logger = LoggerFactory.getLogger(UcRecordReader::class.java)
    }
}
