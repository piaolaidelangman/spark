package sparkDecryptFiles

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

  def decryptBytesWithFernet(content: Array[Byte], secret: String): String = {
    val validator = new StringValidator() {
      override def  getTimeToLive() : TemporalAmount = {
        return Duration.ofHours(24);
      }
    };
    val key = new Key(secret)
    val token: Token = Token.fromString(new String(content));
    token.validateAndDecrypt(key, validator)
  }
}
object decryptFiles {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path to a txt which contains encrypted files' pwd
        val decryptMethod = args(1)
        val secret = args(2)

        val sc = new SparkContext()
        val task: decryptTask = new decryptTask()
        var decryption = sc.emptyRDD[String]
        
        if (decryptMethod == "Java"){
          val salt = args(3)
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            new String(task.decryptBytesWithJavaAESGCM(bytesData.toArray, secret, salt))
          }}.cache()
        }else if (decryptMethod == "Fernet"){
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            task.decryptBytesWithFernet(bytesData.toArray, secret)
          }}.cache()
        }else{
          println("Error! no such decrypt method!")
        }

        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val schema = new StructType(Array(
        StructField("sepal length", DoubleType, false),
        StructField("sepal width", DoubleType, false),
        StructField("petal length", DoubleType, false),
        StructField("petal width", DoubleType, false),
        StructField("class", StringType, false)))

        //split a file into rows
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
