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

object encryptColumnFromDifferentTables {

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
        val task: decryptTask = new decryptTask()
        var decryption = sc.emptyRDD[String]

        val fs = FileSystem.get(sc.hadoopConfiguration)
        fs.listStatus(new Path(inputPath)).filter(_.isFile).map(_.getPath).foreach(x=>{
    
            val byteArray = Files.readAllBytes(Paths.get(x.toString.split(":")(1)))
            val decryptString = (new String(task.decryptBytesWithJavaAESCBC(byteArray, key))).split("\n").map(s=>s.dropRight(1))

            val schemaArray = decryptString(0).split(",")

            val fields = schemaArray.map(fieldName => StructField(fieldName, StringType, true))
            val schema = StructType(fields)
            
            val stringRow = decryptString.drop(1).map(s=>Row.fromSeq(s.split(",")))

            val rowRdd = sc.parallelize(stringRow)

            var df = spark.createDataFrame(rowRdd,schema)

            val encryptTask: encryptTask = new encryptTask()
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

            df.write.option("header",true).csv(Paths.get(outputPath, x.toString.split("/").last).toString)
        }) 

        sc.stop()
        spark.stop()

    }
}

