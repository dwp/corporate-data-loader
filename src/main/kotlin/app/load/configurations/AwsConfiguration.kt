package app.load.configurations

object AwsConfiguration {
    val useLocalStack = (System.getenv("AWS_USE_LOCALSTACK") ?: "false").toBoolean()
    val region = System.getenv("AWS_REGION") ?: "eu-west-2"
}
