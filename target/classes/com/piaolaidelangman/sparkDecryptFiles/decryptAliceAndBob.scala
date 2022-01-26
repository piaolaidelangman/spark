package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import java.util.Base64

/**
 * @author diankun.an
 */

object decryptCsvFromDb {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path where contains encrypted files
        val decryptMethod = args(1) // AESGCM or AESCBC
        val secret = args(2)
        val decoder = Base64.getDecoder()
        val encoder = Base64.getEncoder()
        val key = decoder.decode(decoder.decode(encoder.encodeToString(secret.getBytes)))

        val sc = new SparkContext()
        val task = new DecryptTask()
        var decryption = sc.emptyRDD[String]
        
        if (decryptMethod == "AESGCM"){
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            new String(task.decryptBytesWithJavaAESGCM(bytesData.toArray, key))
          }}.cache()
        }else if (decryptMethod == "AESCBC"){
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            val result = task.decryptBytesWithJavaAESCBC(bytesData.toArray, key)
            // println(result)
            result
          }}.cache()
        }else{
          println("Error! no such decrypt method!")
        }

        // RDD to DataFrame
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val aliceSchema = new StructType(Array(
        StructField("id", StringType, false),
        StructField("a1169", StringType, false),
        StructField("b1209", StringType, false),
        StructField("c1028", StringType, false),
        StructField("c1034", StringType, false)))

        val bobSchema = new StructType(Array(
        StructField("id", StringType, false),
        StructField("c_id", StringType, false)))

        var aliceRDD = decryption.filter(_.startsWith("id,a1169"))
        var bobRDD = decryption.filter(_.startsWith("id,c_id"))

        val aliceRowRdd = aliceRDD
        .flatMap(_.split("\n"))
        .flatMap(_.split("\r"))
        .map(_.split(","))
        .map(attributes => {
            Row(attributes(0), attributes(1), attributes(10), attributes(23), attributes(26))
        })

        val bobRowRdd = bobRDD
        .flatMap(_.split("\n"))
        .flatMap(_.split("\r"))
        .map(_.split(","))
        .map(attributes => {
          Row(attributes(0), attributes(1))
        })

        val alice = spark.createDataFrame(aliceRowRdd, aliceSchema) // RDD[String] To DataFrame
        val bob = spark.createDataFrame(bobRowRdd, bobSchema) // RDD[String] To DataFrame


        alice.printSchema()
        alice.show()

        bob.printSchema()
        bob.show()


        val result1 = bob.filter(bob("c_id") === "ab").join(alice, alice("id") === bob("id")).filter("c1028 is not null").groupBy("c1028").count()
        result1.show()
        println("Result1 have %d rows!".format(result1.count()))

        val result2 = alice.filter("a1169 in (151, 152)").union(alice.filter("b1209 in (220, 330)")).union(alice.filter("c1034 in (422, 63)")).distinct().count()
        println("Result2 have %d rows!".format(result2))

        sc.stop()
        spark.stop()
    }
}
