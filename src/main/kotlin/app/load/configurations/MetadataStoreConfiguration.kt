package app.load.configurations

import java.io.File
import java.util.*

object MetadataStoreConfiguration {
    val writeToMetadataStore = (System.getenv("METADATA_STORE_UPDATE") ?: "false").toBoolean()
    val metadataStoreTable = System.getenv("METADATA_STORE_TABLE") ?: "ucfs"

    val useAwsSecrets = (System.getenv("METADATA_STORE_USE_AWS_SECRETS") ?: "true").toBoolean()

    val properties = Properties().apply {
        put("user", System.getenv("METADATA_STORE_USERNAME") ?: "k2hbwriter")
        put("rds.password.secret.name", System.getenv("METADATA_STORE_PASSWORD_SECRET_NAME") ?: "password")
        put("database", System.getenv("METADATA_STORE_DATABASE_NAME") ?: "metadatastore")
        put("rds.endpoint", System.getenv("METADATA_STORE_ENDPOINT") ?: "metadatastore")
        put("rds.port", System.getenv("METADATA_STORE_PORT") ?: "3306")
        put("use.aws.secrets", useAwsSecrets)

        if (useAwsSecrets) {
            (System.getenv("K2HB_RDS_CA_CERT_PATH") ?: "/certs/AmazonRootCA1.pem").also {
                put("ssl_ca_path", it)
                put("ssl_ca", File(it).readText(Charsets.UTF_8))
                put("ssl_verify_cert", true)
            }
        }
    }
}
