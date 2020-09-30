package app.load.configurations

import java.util.*

object SecretManagerConfiguration {
    val properties = Properties().apply {
        put("region", System.getenv("SECRET_MANAGER_REGION") ?: "eu-west-2")
    }
}
