package piaolaidelangman.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import java.nio.file.{Files, Paths}
import java.util.Base64
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

import com.macasaet.fernet.{Key, Validator, StringValidator, Token}

class decryptTask extends Serializable{
  val secret = "1111111111"
  val salt = "2222222222"
  val validator = new StringValidator() {};
  def decryptWithAESGCM(content: String, keyLen: Int = 128): String = {
      new String(decryptBytesWithAESGCM(Base64.getDecoder.decode(content), keyLen))
  }

  def decryptBytesWithAESGCM(content: Array[Byte], keyLen: Int = 128): Array[Byte] = {
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
  def decryptFile(content: Array[Byte], secret: String): String = {
      val key = new Key(secret)
      val token = Token.fromBytes(content)
      token.validateAndDecrypt(key, validator)
  }
}
object decrypt {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path to a txt which contains encrypted files' pwd
        val secret = args(1)
        
        val sc = new SparkContext()
        // val validator = new StringValidator() {};

        val task: decryptTask = new decryptTask()
        
        // val input = sc.textFile(inputPath)
        // val decryption = input.map{
        //   row =>{
        //     val byteArray = Files.readAllBytes(Paths.get(row))
        //     new String(task.decryptBytesWithAESGCM(byteArray))
        //   }
        // }// RDD[String] which contains decrypted files

        val decryption = sc.binaryFiles(inputPath)
        .map{ case (name, bytesData) => { 
          println("## success " + name)
          task.decryptFile(bytesData.toArray, secret)

          // val key = new Key(secret)
          // val token = Token.fromBytes(bytesData.toArray)
          // token.validateAndDecrypt(key, validator)

          // Files.write(Paths.get(outputPath + name.split("/").last), task.encryptFile(bytesData.toArray, secret))
          // name + " encrypt success"
        }}

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
    }
}


