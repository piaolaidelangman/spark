package sparkEncryptFiles

import org.apache.spark.SparkContext

import java.nio.file.{Files, Paths}
import java.util.Base64
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

import com.macasaet.fernet.{Key, Validator, StringValidator, Token}

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

  def encryptBytesWithFernet(content: Array[Byte], secret: String): Array[Byte] = {
      val key = new Key(secret)
      val token: Token = Token.generate(key, content);
      token.serialise().getBytes
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

        if(Files.exists(Paths.get(tmpOutputPath)) == false){
          Files.createDirectory(Paths.get(tmpOutputPath))
        }
        if (encryptMethod == "Java"){
          val salt = args(4)
          val output = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            val tmpOutputPath = outputPath + name.split("/").last
            Files.write(Paths.get(tmpOutputPath), task.encryptBytesWithJavaAESGCM(bytesData.toArray, secret, salt))
            tmpOutputPath + " Java encrypt successfully saved!"
          }}
          output.foreach(println)

        }else if (encryptMethod == "Fernet"){
          val output = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            val tmpOutputPath = outputPath + name.split("/").last
            Files.write(Paths.get(tmpOutputPath), task.encryptBytesWithFernet(bytesData.toArray, secret))
            tmpOutputPath + " Fernet encrypt successfully saved!"
          }}
          output.foreach(println)
        }else{
          println("Error! no such encrypt method!")
        }

        sc.stop()
    }
}



