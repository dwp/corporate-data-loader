package app.load.configurations

object CorporateMemoryConfiguration {
    val table = System.getenv("HBASE_TABLE") ?: "data"
}
