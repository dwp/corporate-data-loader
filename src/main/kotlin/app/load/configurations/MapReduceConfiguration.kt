package app.load.configurations

object MapReduceConfiguration {
    val outputDirectory = System.getenv("MAP_REDUCE_OUTPUT_DIRECTORY") ?: "/user/hadoop/bulk"
}
