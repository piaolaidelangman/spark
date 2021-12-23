package piaolaidelangman.spark

import org.apache.spark.SparkContext

import java.nio.file.{Files, Paths}
import java.util.Base64
import java.security.SecureRandom
import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, PBEKeySpec, SecretKeySpec}

import com.macasaet.fernet.{Key, Validator, StringValidator, Token}

class encryptTask extends Serializable{
  val secret = "1111111111"
  val salt = "2222222222"

 def encryptWithAESGCM(content: String, keyLen: Int = 128): String = {
    Base64.getEncoder.encodeToString(encryptBytesWithAESGCM(content.getBytes(), keyLen))
  }

  def encryptBytesWithAESGCM(content: Array[Byte], keyLen: Int = 128): Array[Byte] = {
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

  def encryptFile(content: Array[Byte], secret: String): Array[Byte] = {
      val key = new Key(secret)
      println("## success key: " + key.serialise());
      val token: Token = Token.generate(key, content);
      token.serialise().getBytes
  }
}
object encrypt {
    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path to a txt which contains files' pwd to be encrypted
        val outputPath = args(1)
        val secret = args(2)
        val sc = new SparkContext()
        val task = new encryptTask()

        val output = sc.binaryFiles(inputPath)
        .map{ case (name, bytesData) => { 
          println("## success " + name)
          Files.write(Paths.get(outputPath + name.split("/").last), task.encryptFile(bytesData.toArray, secret))
          name + " encrypt success"
        }}
        output.map(println)
        output.count()
        // val bytesFile = Files.readAllBytes(Paths.get(inputPath))

        // final Key key = new Key("***key i got from python**");
        // final Token token = Token.fromString("***cipher text i got from python***");
        // val key: Key = Key.generateKey();
        // val key = Key(secret)
        // println("## success key: " + key.serialise());
        // val token: Token = Token.generate(key, bytesFile);
        // val encryptText = token.serialise()
        // println("## success encrypt: " + encryptText)
        // val decryptedText: String = token.validateAndDecrypt(key, validator)
        // println("## success decrypt: " + decryptedText)
        // Files.write(Paths.get(outputPath), encryptText.getBytes)

    }
}



