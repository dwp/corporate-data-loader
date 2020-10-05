package app.load.domain


data class HBasePayload(val key: ByteArray, val version: Long, val topic: String, val partition: Int, val offset: Long) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as HBasePayload

        if (!key.contentEquals(other.key)) return false
        if (version != other.version) return false
        if (topic != other.topic) return false
        if (partition != other.partition) return false
        if (offset != other.offset) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + version.hashCode()
        result = 31 * result + topic.hashCode()
        result = 31 * result + partition
        result = 31 * result + offset.hashCode()
        return result
    }
}
