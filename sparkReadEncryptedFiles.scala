package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import java.util.Base64
import java.nio.file.{Files, Paths}

/**
 * @author diankun.an
 */

object decryptFiles {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path to a txt which contains encrypted files' pwd
        val decryptMethod = args(1) // AESGCM or AESCBC
        val secret = args(2)
        val decoder = Base64.getDecoder()
        val encoder = Base64.getEncoder()
        val key = decoder.decode(decoder.decode(encoder.encodeToString(secret.getBytes)))

        val sc = new SparkContext()
        val task: decryptTask = new decryptTask()
        var decryption = sc.emptyRDD[String]
        
        if (decryptMethod == "AESGCM"){
          decryption = sc.binaryFiles(inputPath)
          .map{ case (name, bytesData) => {
            new String(task.decryptBytesWithJavaAESGCM(bytesData.toArray, key))
          }}.cache()
        }else if (decryptMethod == "AESCBC"){
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
