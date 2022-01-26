package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{udf, col}
import java.util.Base64

/**
 * @author diankun.an
 */

object EncryptColumnFromSameEncryptedTables {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path where contains encrypted files
        val decryptMethod = args(1) // AESGCM or AESCBC
        val secret = args(2)
        val outputPath = args(3)  // path to csv-saved
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
            task.decryptBytesWithJavaAESCBC(bytesData.toArray, key)
          }}.cache()
        }else{
          println("Error! no such decrypt method!")
        }

        // RDD to DataFrame
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val decryptionRDD = decryption.flatMap(_.split("\n")).flatMap(_.split("\r"))
        val schemaString = decryptionRDD.first()
        val schemaArray = schemaString.split(",")
        val fields = schemaArray.map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        val rowRDD = decryptionRDD.map(_.split(",")).map(stringArray => Row.fromSeq(stringArray))

        val sqlString = schemaArray(0) + " != '" + schemaArray(0) +"'"
        var df = spark.createDataFrame(rowRDD,schema).filter(sqlString)

        df.printSchema()
        df.show()

        val encryptTask = new EncryptTask()
        val convertCase =  (x:String) => {
            new String(encryptTask.encryptBytesWithJavaAESCBC(x.getBytes, key))
        }
        val convertUDF = udf(convertCase)

        for(columnName <- schemaArray) {
          if(columnName != "userid"){
            df = df.withColumn(columnName,convertUDF(df(columnName)))
          }
        }
        spark.udf.register("convertUDF", convertCase)
        df.createOrReplaceTempView("testTableName")
        var convertSql = "select "
        for(columnName <- schemaArray) {
          if(columnName != "userid"){
            convertSql += "convertUDF("+columnName+") as "+columnName+", "
          }else{
            convertSql += "userid, "
          }
        }
        convertSql = convertSql.dropRight(2)
        convertSql += " from testTableName"
        df = spark.sql(convertSql)

        df.show()
        df.write.option("header",true).csv(outputPath)

        sc.stop()
        spark.stop()

    }
}
