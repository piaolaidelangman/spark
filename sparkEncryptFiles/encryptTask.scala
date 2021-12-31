package sparkCryptoFiles

import java.util.Base64
import java.util.Arrays.copyOfRange
import java.time.Instant
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory, Mac}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

/**
 * @author diankun.an
 */
class encryptTask extends Serializable{

  def encryptBytesWithJavaAESGCM(content: Array[Byte], key:Array[Byte], keyLen: Int = 128): Array[Byte] = {

    val iv = new Array[Byte](16)
    val secureRandom: SecureRandom = new SecureRandom()
    secureRandom.nextBytes(iv)
    val gcmParameterSpec = new GCMParameterSpec(128, iv)

    val secretKeySpec = new SecretKeySpec(key, "AES")

    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, gcmParameterSpec)
    val cipherTextWithoutIV = cipher.doFinal(content)
    cipher.getIV ++ cipherTextWithoutIV
  }

  def encryptBytesWithJavaAESCBC(content: Array[Byte], secret: Array[Byte]): Array[Byte] = {
    val encoder = Base64.getUrlEncoder()
    // val bytes = (new String(content)).getBytes(UTF_8)

    //  get IV
    val random = new SecureRandom()
    val initializationVector: Array[Byte] = new Array[Byte](16)
    random.nextBytes(initializationVector)
    val ivParameterSpec: IvParameterSpec = new IvParameterSpec(initializationVector)

    // key encrypt
    val signingKey: Array[Byte] = copyOfRange(secret, 0, 16)
    val encryptKey: Array[Byte] = copyOfRange(secret, 16, 32)
    val encryptionKeySpec: SecretKeySpec = new SecretKeySpec(encryptKey, "AES")

    val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, encryptionKeySpec, ivParameterSpec)

    val cipherText: Array[Byte] = cipher.doFinal(content)
    val timestamp: Instant = Instant.now()

    // sign
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream(25 + cipherText.length)
    val dataStream: DataOutputStream = new DataOutputStream(byteStream)

    val version: Byte = (0x80).toByte
    dataStream.writeByte(version)
    dataStream.writeLong(timestamp.getEpochSecond())
    dataStream.write(ivParameterSpec.getIV())
    dataStream.write(cipherText)

    val mac: Mac = Mac.getInstance("HmacSHA256")
    val signingKeySpec = new SecretKeySpec(signingKey, "HmacSHA256")
    mac.init(signingKeySpec)
    val hmac: Array[Byte] = mac.doFinal(byteStream.toByteArray())

    // to bytes
    val outByteStream: ByteArrayOutputStream = new ByteArrayOutputStream(57 + cipherText.length)
    val dataOutStream: DataOutputStream = new DataOutputStream(outByteStream)
    dataOutStream.writeByte(version)
    dataOutStream.writeLong(timestamp.getEpochSecond())
    dataOutStream.write(ivParameterSpec.getIV())
    dataOutStream.write(cipherText)
    dataOutStream.write(hmac)

    if (timestamp == null) {
        throw new cryptoException("timestamp cannot be null")
    }
    if (ivParameterSpec == null || ivParameterSpec.getIV().length != 16) {
        throw new cryptoException("Initialization Vector must be 128 bits")
    }
    if (cipherText == null || cipherText.length % 16 != 0) {
        throw new cryptoException("Ciphertext must be a multiple of 128 bits")
    }
    if (hmac == null || hmac.length != 32) {
        throw new cryptoException("hmac must be 256 bits")
    }

    val resultString = new String(encoder.encodeToString(outByteStream.toByteArray()))
    resultString.getBytes
  }

}