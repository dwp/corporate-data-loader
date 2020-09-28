package app.load.configurations

class LoadConfiguration {

    object Aws {
        val useLocalStack = (System.getenv("AWS_USE_LOCALSTACK") ?: "false").toBoolean()
        val region = System.getenv("AWS_REGION") ?: "eu-west-2"
    }

    object S3 {
        val bucket = System.getenv("S3_BUCKET") ?: "corporatestorage"
        val prefix = System.getenv("S3_PREFIX") ?: "data"
        val maxConnections = (System.getenv("S3_MAX_CONNECTIONS") ?: "1000").toInt()
    }

    object HBase {
        val table = System.getenv("HBASE_TABLE") ?: "data"
    }

    object MapReduce {
        val outputDirectory = System.getenv("MAP_REDUCE_OUTPUT_DIRECTORY") ?: "/user/hadoop/bulk"
    }
}
