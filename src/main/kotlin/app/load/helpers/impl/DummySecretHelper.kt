import org.slf4j.LoggerFactory

class DummySecretHelper: SecretHelperInterface {

    companion object {
        val logger = LoggerFactory.getLogger(DummySecretHelper::class.java.toString())
    }

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from dummy secret manager", "secret_name", secretName)

        try {
            return if (secretName == "password") "password" else System.getenv("DUMMY_SECRET_${secretName.toUpperCase()}") ?: "NOT_SET"
        } catch (e: Exception) {
            logger.error("Failed to get dummy secret manager result", e, "secret_name", secretName)
            throw e
        }
    }
}
