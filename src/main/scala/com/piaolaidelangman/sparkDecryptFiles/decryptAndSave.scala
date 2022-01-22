package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import java.util.Base64

/**
 * @author diankun.an
 */

object selectAll {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path where contains encrypted files
        val decryptMethod = args(1) // AESGCM or AESCBC
        val secret = args(2)
        val outputPath = args(3)
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

        // // iris.csv's schema, change this according to your data.
        // val schema = new StructType(Array(
        // StructField("ds_rk", StringType, false),
        // StructField("ds", StringType, false),
        // StructField("brand", StringType, false),
        // StructField("userid", StringType, false),
        // StructField("searchclick", StringType, false),
        // StructField("pv", StringType, false),
        // StructField("collect", StringType, false),
        // StructField("cart", StringType, false),
        // StructField("member", StringType, false),
        // StructField("pay", StringType, false)))

        // new way to generae a schema with first row is head
        val decryptionRDD = decryption.flatMap(_.split("\n")).flatMap(_.split("\r"))
        val schemaString = decryptionRDD.first()
        val fields = schemaString.split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        val rowRDD = decryptionRDD.map(_.split(",")).map(stringArray => Row.fromSeq(stringArray))

        val sqlString = schemaString.split(",")(0) + " != '" + schemaString.split(",")(0) +"'"
        // val df = spark.createDataFrame(rowRDD,schema).filter("ds_rk != 'ds_rk'")
        val df = spark.createDataFrame(rowRDD,schema).filter(sqlString)

        df.printSchema()
        df.show()

        df.write.option("header",true).csv(outputPath)

        sc.stop()
        spark.stop()

    }
}
