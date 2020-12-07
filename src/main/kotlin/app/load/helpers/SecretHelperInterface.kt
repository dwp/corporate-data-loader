package app.load.helpers

interface SecretHelperInterface {
    fun getSecret(secretName: String): String?
}
