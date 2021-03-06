package app.load.helpers.impl

import app.load.helpers.SecretHelperInterface

class DummySecretHelper: SecretHelperInterface {
    override fun getSecret(secretName: String): String =
            if (secretName == "password") "password" else System.getenv("DUMMY_SECRET_${secretName.toUpperCase()}") ?: "NOT_SET"
}
