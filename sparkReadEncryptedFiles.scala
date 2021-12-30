package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import java.util.Base64
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

import java.time.{Duration, Instant}
import java.time.temporal.TemporalAmount

import com.macasaet.fernet.{Key, Validator, StringValidator, Token}
/**
 * @author diankun.an
 */
class decryptTask extends Serializable{
  
  def decryptWithJavaAESGCM(content: String, secret: String, salt: String, keyLen: Int = 128): String = {
      new String(decryptBytesWithJavaAESGCM(Base64.getDecoder.decode(content), secret, salt, keyLen))
  }

  def decryptBytesWithJavaAESGCM(content: Array[Byte], secret: String, salt: String, keyLen: Int = 128): Array[Byte] = {
    val cipherTextWithIV = content
    val iv = cipherTextWithIV.slice(0, 12)
    val gcmParameterSpec = new GCMParameterSpec(128, iv)
    val secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
    val spec = new PBEKeySpec(secret.toCharArray, salt.getBytes(), 65536, keyLen)
    val tmp = secretKeyFactory.generateSecret(spec)
    val secretKeySpec = new SecretKeySpec(tmp.getEncoded, "AES")
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, gcmParameterSpec)
    val cipherTextWithoutIV = cipherTextWithIV.slice(12, cipherTextWithIV.length)
    cipher.doFinal(cipherTextWithoutIV)
  }

  def decryptBytesWithJavaAESCBC(content: Array[Byte], secret: Array[Byte]): String = {
    val decoder = Base64.getUrlDecoder()
    // val decoder = Base64.getMimeDecoder()
    // val encoder = Base64.getUrlEncoder()
    val bytes = decoder.decode(new String(content))
    // val bytes = content
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

    val secretKeySpec = new SecretKeySpec(encryptKey, "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec)

    new String(cipher.doFinal(cipherText))
  }
  def read(stream: DataInputStream, numBytes: Int): Array[Byte]={
    val retval = new Array[Byte](numBytes)
    val bytesRead: Int = stream.read(retval)
    if (bytesRead < numBytes) {
      throw new cryptoException("Not enough bits to generate a Token")
    }
    retval
  }
}
object decryptFiles {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path to a txt which contains encrypted files' pwd
        val decryptMethod = args(1) // Java or Fernet
        val secret = args(2)

        val sc = new SparkContext()
        val task: decryptTask = new decryptTask()
        var decryption = sc.emptyRDD[String]
        
        if (decryptMethod == "AESGCM"){
          val salt = args(3)
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            new String(task.decryptBytesWithJavaAESGCM(bytesData.toArray, secret, salt))
          }}.cache()
        }else if (decryptMethod == "AESCBC"){
          val decoder = Base64.getDecoder()
          val encoder = Base64.getEncoder()
          val key = decoder.decode(decoder.decode(encoder.encodeToString(secret.getBytes)))
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            task.decryptBytesWithJavaAESCBC(bytesData.toArray, key)
          }}.cache()
        }else{
          println("Error! no such decrypt method!")
        }

        // RDD to DataFrame
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        // iris.csv's schema, change this according to your data.
        val schema = new StructType(Array(
        StructField("sepal length", DoubleType, false),
        StructField("sepal width", DoubleType, false),
        StructField("petal length", DoubleType, false),
        StructField("petal width", DoubleType, false),
        StructField("class", StringType, false)))

        // split a file(string) into rows
        val rowRDD = decryption
        .flatMap(_.split("\n"))
        .map(_.split(","))
        .map(attributes => Row(attributes(0).toDouble, attributes(1).toDouble, attributes(2).toDouble, attributes(3).toDouble, attributes(4).toString))

        val df = spark.createDataFrame(rowRDD, schema) // RDD[String] To DataFrame

        df.printSchema()
        df.show()

        sc.stop()
        spark.stop()
    }
}
