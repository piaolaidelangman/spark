package sparkCryptoFiles

import java.util.Base64
import java.util.Arrays.copyOfRange
import java.io.{ByteArrayInputStream, DataInputStream}
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

import java.time.Instant

/**
 * @author diankun.an
 */

class decryptTask extends Serializable{

  def decryptBytesWithJavaAESGCM(content: Array[Byte], key: Array[Byte], keyLen: Int = 128): Array[Byte] = {
    val cipherTextWithIV = content
    val initializationVector = cipherTextWithIV.slice(0, 16)
    val gcmParameterSpec = new GCMParameterSpec(128, initializationVector)

    val secretKeySpec = new SecretKeySpec(key, "AES")
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, gcmParameterSpec)
    val cipherTextWithoutIV = cipherTextWithIV.slice(16, cipherTextWithIV.length)
    cipher.doFinal(cipherTextWithoutIV)
  }

  def decryptBytesWithJavaAESCBC(content: Array[Byte], secret: Array[Byte]): String = {
    val decoder = Base64.getUrlDecoder()
    val bytes = decoder.decode(new String(content))

    val inputStream: ByteArrayInputStream = new ByteArrayInputStream(bytes)
    val dataStream: DataInputStream = new DataInputStream(inputStream)

    val version: Byte = dataStream.readByte()
    if(version.compare((0x80).toByte) != 0){
      throw new cryptoException("Version error!")
    }
    val encryptKey: Array[Byte] = copyOfRange(secret, 16, 32)

    val timestampSeconds: Long = dataStream.readLong()

    val initializationVector: Array[Byte] = read(dataStream, 16)
    val ivParameterSpec = new IvParameterSpec(initializationVector)

    val cipherText: Array[Byte] = read(dataStream, bytes.length - 57)

    val hmac: Array[Byte] = read(dataStream, 32)
    if(initializationVector.length != 16){
      throw new cryptoException("Initialization Vector must be 128 bits")
    }
    if (cipherText == null || cipherText.length % 16 != 0) {
        throw new cryptoException("Ciphertext must be a multiple of 128 bits")
    }
    if (hmac == null || hmac.length != 32) {
        throw new cryptoException("hmac must be 256 bits")
    }

    val secretKeySpec = new SecretKeySpec(encryptKey, "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec)

    new String(cipher.doFinal(cipherText))
  }

  def read(stream: DataInputStream, numBytes: Int): Array[Byte]={
    val retval = new Array[Byte](numBytes)
    val bytesRead: Int = stream.read(retval)
    if (bytesRead < numBytes) {
      throw new cryptoException("Not enough bits to read!")
    }
    retval
  }
}