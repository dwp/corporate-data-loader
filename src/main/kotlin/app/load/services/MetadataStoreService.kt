package app.load.services
import app.load.helpers.SecretHelperInterface
import app.load.configurations.MetadataStoreConfiguration
import app.load.domain.HBasePayload
import app.load.helpers.impl.AWSSecretHelper
import app.load.helpers.impl.DummySecretHelper
import app.load.utility.TextUtils
import org.apache.commons.dbcp2.BasicDataSource
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.util.*
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.hours

@ExperimentalTime
class MetadataStoreService(private val connection: Connection): AutoCloseable {

    fun recordBatch(payloads: List<HBasePayload>) {
        logger.info("Putting batch into metadata store, size ${payloads.size}")
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
        println("Put batch into metadata store, time_taken $timeTaken, size: ${payloads.size}")
        logger.info("Put batch into metadata store time_taken $timeTaken, size ${payloads.size}")
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

        fun connect(): MetadataStoreService = MetadataStoreService(dataSource.connection)

        private val dataSource by lazy {
            val (databaseUrl, databaseProperties) = connectionProperties()
            BasicDataSource().apply {
                url = databaseUrl
                validationQuery = "SELECT count(*) from ${MetadataStoreConfiguration.metadataStoreTable}"
                maxConnLifetimeMillis = 12.hours.toLongMilliseconds()
                databaseProperties.forEach { (name, value) ->
                    addConnectionProperty(name.toString(), value.toString())
                }
            }
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
