package app.load.configurations

object CorporateMemoryConfiguration {
    val qualifiedTablePattern = System.getenv("HBASE_QUALIFIED_TABLE_PATTERN") ?: """^\w+\.([-\w]+)\.([-.\w]+)$"""
    val table = System.getenv("HBASE_TABLE") ?: "data"
}
