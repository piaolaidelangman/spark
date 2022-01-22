package sparkCryptoFiles

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{udf, col}
import java.util.Base64

import scala.collection.immutable.Seq
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.file.{Files, Paths}



/**
 * @author diankun.an
 */

object decryptColumn {

    def main(args: Array[String]): Unit = {

        val inputPath = args(0)
        val secret = args(1)
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
            var encryptDf = spark.read.option("delimiter", ",").option("header", "true").csv(inputPath)
    

            val convertCase =  (x:String) => {
                new String(task.decryptBytesWithJavaAESCBC(x.getBytes, key))
            }
            val convertUDF = udf(convertCase)

            spark.udf.register("convertUDF", convertCase)
            encryptDf.createOrReplaceTempView("testTableName")
            var convertSql = "select convertUDF(ds_rk) as ds_rk, convertUDF(userid) as userid from testTableName"

            encryptDf = spark.sql(convertSql)
            encryptDf.show()

        }) 


        sc.stop()
        spark.stop()

    }
}

