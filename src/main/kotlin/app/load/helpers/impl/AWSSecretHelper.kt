import app.load.configurations.SecretManagerConfiguration
import com.amazonaws.services.secretsmanager.*
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory

class AWSSecretHelper: SecretHelperInterface {

    companion object {
        val logger = LoggerFactory.getLogger(AWSSecretHelper::class.java.toString())
    }

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from aws secret manager", "secret_name", secretName)

        val region = SecretManagerConfiguration.properties["region"].toString()

        val client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build()
        val getSecretValueRequest = GetSecretValueRequest().withSecretId(secretName)

        val getSecretValueResult = client.getSecretValue(getSecretValueRequest)

        logger.debug("Successfully got value from aws secret manager", "secret_name", secretName)

        val secretString = getSecretValueResult.secretString

        @Suppress("UNCHECKED_CAST")
        val map = ObjectMapper().readValue(secretString, Map::class.java) as Map<String, String>

        return map["password"]
    }
}
