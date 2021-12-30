package sparkCryptoFiles

import org.apache.spark.SparkContext

import java.util.Base64
import java.util.Arrays.copyOfRange
import java.time.Instant
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory, Mac}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

/**
 * @author diankun.an
 */
class encryptTask extends Serializable{

 def encryptWithJavaAESGCM(content: String, secret: String, salt: String, keyLen: Int = 128): String = {
    Base64.getEncoder.encodeToString(encryptBytesWithJavaAESGCM(content.getBytes(), secret, salt, keyLen))
  }

  def encryptBytesWithJavaAESGCM(content: Array[Byte], secret: String, salt: String, keyLen: Int = 128): Array[Byte] = {
    // Default IV len in GCM is 12
    val iv = new Array[Byte](12)
    val secureRandom: SecureRandom = SecureRandom.getInstance("SHA1PRNG")
    secureRandom.nextBytes(iv)
    val gcmParameterSpec = new GCMParameterSpec(128, iv)
    val secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
    val spec = new PBEKeySpec(secret.toCharArray, salt.getBytes(), 65536, keyLen)
    val tmp = secretKeyFactory.generateSecret(spec)
    val secretKeySpec = new SecretKeySpec(tmp.getEncoded, "AES")

    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, gcmParameterSpec)
    val cipherTextWithoutIV = cipher.doFinal(content)
    cipher.getIV ++ cipherTextWithoutIV
  }

  def encryptBytesWithJavaAESCBC(content: Array[Byte], secret: Array[Byte]): Array[Byte] = {
    val encoder = Base64.getUrlEncoder()
    val bytes = (new String(content)).getBytes(UTF_8)

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

    val cipherText: Array[Byte] = cipher.doFinal(bytes)
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

    val resultString = new String(encoder.encodeToString(outByteStream.toByteArray()).getBytes, UTF_8)
    resultString.getBytes
  }
}
object encryptFiles {
    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path to a txt which contains files' pwd to be encrypted
        val outputPath = args(1)
        val encryptMethod = args(2)
        val secret = args(3)

        val sc = new SparkContext()
        val task = new encryptTask()

        if(Files.exists(Paths.get(outputPath)) == false){
          Files.createDirectory(Paths.get(outputPath))
        }
        if (encryptMethod == "AESGCM"){
          val salt = args(4)
          val output = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            val tmpOutputPath = Paths.get(outputPath, name.split("/").last)
            Files.write(tmpOutputPath, task.encryptBytesWithJavaAESGCM(bytesData.toArray, secret, salt))
            tmpOutputPath.toString + " AES/GCM encrypt successfully saved!"
          }}
          output.foreach(println)

        }else if (encryptMethod == "AESCBC"){
          val decoder = Base64.getDecoder()
          val encoder = Base64.getEncoder()
          val key = decoder.decode(decoder.decode(encoder.encodeToString(secret.getBytes)))
          val output = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            val tmpOutputPath = Paths.get(outputPath, name.split("/").last)
            Files.write(tmpOutputPath, task.encryptBytesWithJavaAESCBC(bytesData.toArray, key))
            tmpOutputPath.toString + " AES/CBC encrypt successfully saved!"
          }}
          output.foreach(println)
        }else{
          println("Error! no such encrypt method!")
        }

        sc.stop()
    }
}



