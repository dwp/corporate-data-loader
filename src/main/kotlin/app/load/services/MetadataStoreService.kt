
import app.load.configurations.MetadataStoreConfiguration
import app.load.domain.HBasePayload
import app.load.utility.TextUtils
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import kotlin.system.measureTimeMillis

class MetadataStoreService(private val connection: Connection): AutoCloseable {

    fun recordBatch(payloads: List<HBasePayload>) {
        if (MetadataStoreConfiguration.writeToMetadataStore) {
            logger.info("Putting batch into metadata store", "size" to "${payloads.size}")
            val timeTaken = measureTimeMillis {
                with(recordProcessingAttemptStatement) {
                    payloads.forEach {
                        setString(1, textUtils.printableKey(it.key))
                        setLong(2, it.version)
                        setString(3, it.topic)
                        setInt(4, it.partition)
                        setLong(5, it.offset)
                        addBatch()
                    }
                    executeBatch()
                }
            }
            logger.info("Put batch into metadata store", "time_taken" to "$timeTaken", "size" to "${payloads.size}")
        }
        else {
            logger.info("Not putting batch into metadata store", "write_to_metadata_store" to "${MetadataStoreConfiguration.writeToMetadataStore}")
        }
    }


    private val recordProcessingAttemptStatement by lazy {
        connection.prepareStatement("""
            INSERT INTO ${MetadataStoreConfiguration.metadataStoreTable} (hbase_id, hbase_timestamp, topic_name, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent())
    }

    companion object {
        private val isUsingAWS = MetadataStoreConfiguration.useAwsSecrets
        private val secretHelper: SecretHelperInterface =  if (isUsingAWS) AWSSecretHelper() else DummySecretHelper()

        fun connect(): MetadataStoreService {
            val (url, properties) = connectionProperties()
            return MetadataStoreService(DriverManager.getConnection(url, properties))
        }

        private fun connectionProperties(): Pair<String, Properties> {
            val hostname = MetadataStoreConfiguration.properties["rds.endpoint"]
            val port = MetadataStoreConfiguration.properties["rds.port"]
            val jdbcUrl = "jdbc:mysql://$hostname:$port/${MetadataStoreConfiguration.properties.getProperty("database")}"
            val propertiesWithPassword: Properties = MetadataStoreConfiguration.properties.clone() as Properties
            propertiesWithPassword["password"] = password
            return Pair(jdbcUrl, propertiesWithPassword)
        }

        private val password by lazy {
            val secretName = MetadataStoreConfiguration.properties.getProperty("rds.password.secret.name")
            secretHelper.getSecret(secretName)
        }

        val logger = DataworksLogger.getLogger(MetadataStoreService::class.toString())
        val textUtils = TextUtils()
    }

    override fun close() = connection.close()
}
