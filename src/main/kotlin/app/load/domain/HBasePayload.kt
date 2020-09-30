package app.load.domain


data class HBasePayload(val key: ByteArray, val body: ByteArray, val id: String, val version: Long,
                        val topic: String, val partition: Int, val offset: Long)
