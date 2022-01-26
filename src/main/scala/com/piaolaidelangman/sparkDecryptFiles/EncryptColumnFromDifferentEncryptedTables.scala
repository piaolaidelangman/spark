package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf

import java.util.Base64
import java.nio.file.{Files, Paths}

import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author diankun.an
 */

object EncryptColumnFromDifferentTables {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0) // path where contains encrypted files
        val secret = args(1)
        val outputPath = args(2)  // path to csv-saved
        val decoder = Base64.getDecoder()
        val encoder = Base64.getEncoder()

        val key = decoder.decode(decoder.decode(encoder.encodeToString(secret.getBytes)))
        
        val sc = new SparkContext()
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        val task = new DecryptTask()
        var decryption = sc.emptyRDD[String]

        val fs = FileSystem.get(sc.hadoopConfiguration)
        fs.listStatus(new Path(inputPath)).filter(_.isFile).map(_.getPath).foreach(x=>{
    
            var data = sc.textFile(x.toString.split(":")(1))
            data = data.map(stringContent => {
                task.decryptBytesWithJavaAESCBC(stringContent.getBytes, key)
            }).flatMap(_.split("\n"))

            val schemaArray = data.first.split(",")

            val fields = schemaArray.map(fieldName => StructField(fieldName, StringType, true))
            val schema = StructType(fields)
            
            val rowRdd = data.map(s=>Row.fromSeq(s.split(",")))

            var df = spark.createDataFrame(rowRdd,schema)

            val encryptTask = new EncryptTask()
            val convertCase =  (x:String) => {
                new String(encryptTask.encryptBytesWithJavaAESCBC(x.getBytes, key))
            }
            val convertUDF = udf(convertCase)

            spark.udf.register("convertUDF", convertCase)
            df.createOrReplaceTempView("testTableName")
            var convertSql = "select "
            for(columnName <- schemaArray) {
                convertSql += "convertUDF("+columnName+") as "+columnName+", "
            }
            convertSql = convertSql.dropRight(2)
            convertSql += " from testTableName"
            df = spark.sql(convertSql)

            df.write.mode("overwrite").option("header",true).csv(Paths.get(outputPath, x.toString.split("/").last).toString)

        }) 

        sc.stop()
        spark.stop()

    }
}

