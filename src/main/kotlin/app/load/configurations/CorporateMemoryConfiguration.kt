package app.load.configurations

object CorporateMemoryConfiguration {
    val table = System.getenv("HBASE_TABLE") ?: "data"
    val archivedFilePattern = System.getenv("HBASE_ARCHIVED_FILE_PATTERN") ?: """(db\.[-\w]+\.[-\w]+)_(\d+)_(\d+)-(\d+)\.jsonl\.gz$"""
}
