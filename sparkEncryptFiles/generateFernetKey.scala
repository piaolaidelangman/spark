package sparkCryptoFiles

import java.security.SecureRandom
import java.io.ByteArrayOutputStream
import java.util.Base64.getEncoder

/**
 * @author diankun.an
*/

class Key extends Serializable{

    def generateKey(): String = {
        val random = new SecureRandom()
        val signingKey = new Array[Byte](16)
        random.nextBytes(signingKey)
        val encryptionKey = new Array[Byte](16)
        random.nextBytes(encryptionKey)

        val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream(32)

        byteStream.write(signingKey)
        byteStream.write(encryptionKey)
        getEncoder().encodeToString(byteStream.toByteArray())
    }

}
