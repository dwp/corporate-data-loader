package app.load.configurations

object S3Configuration {
    val bucket = System.getenv("S3_BUCKET") ?: "corporatestorage"
    val prefix = System.getenv("S3_PREFIX") ?: "data"
    val topicName = System.getenv("TOPIC_NAME") ?: ""
    val maxConnections = (System.getenv("S3_MAX_CONNECTIONS") ?: "1000").toInt()
}
