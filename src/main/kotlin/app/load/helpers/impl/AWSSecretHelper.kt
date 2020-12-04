import app.load.configurations.AwsConfiguration
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory

class AWSSecretHelper: SecretHelperInterface {

    companion object {
        private val logger = LoggerFactory.getLogger(AWSSecretHelper::class.java)
    }

    override fun getSecret(secretName: String): String? {
        logger.info("Getting value from aws secret manager secret_name to $secretName")

        val client = AWSSecretsManagerClientBuilder.standard().run {
            withRegion(AwsConfiguration.region)
            build()
        }

        val getSecretValueRequest = GetSecretValueRequest().apply { withSecretId(secretName) }
        val getSecretValueResult = client.getSecretValue(getSecretValueRequest)
        val secretString = getSecretValueResult.secretString
        val map = ObjectMapper().readValue(secretString, Map::class.java) as Map<String, String>
        return map["password"]
    }
}
