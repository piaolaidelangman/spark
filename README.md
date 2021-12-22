# Spark Decrypt files
## Envirronment
* Spark 3.1.2
* Sbt

## Data Prepare
* Encrypted files. Put the pwd of binary encrypted files into a `input.txt` file 

* Package

run:
```
sbt package
```
You will get `./target/scala-2.12/spark_2.12-0.1.jar`

## Run command
```
spark-submit \
  --master local[2] \
  --class piaolaidelangman.spark.decrypt \
  /path/to/target/scala-2.12/spark_2.12-0.1.jar \
  /path/to/input.txt
```
I use iris.cvs and the output is:
```
+------------+-----------+------------+-----------+------------+
|sepal length|sepal width|petal length|petal width|       class|
+------------+-----------+------------+-----------+------------+
|         5.1|        3.5|         1.4|        0.2|Iris-setosa
|         4.9|        3.0|         1.4|        0.2|Iris-setosa
|         4.7|        3.2|         1.3|        0.2|Iris-setosa
|         4.6|        3.1|         1.5|        0.2|Iris-setosa
|         5.0|        3.6|         1.4|        0.2|Iris-setosa
|         5.4|        3.9|         1.7|        0.4|Iris-setosa
|         4.6|        3.4|         1.4|        0.3|Iris-setosa
|         5.0|        3.4|         1.5|        0.2|Iris-setosa
|         4.4|        2.9|         1.4|        0.2|Iris-setosa
|         4.9|        3.1|         1.5|        0.1|Iris-setosa
|         5.4|        3.7|         1.5|        0.2|Iris-setosa
|         4.8|        3.4|         1.6|        0.2|Iris-setosa
|         4.8|        3.0|         1.4|        0.1|Iris-setosa
|         4.3|        3.0|         1.1|        0.1|Iris-setosa
|         5.8|        4.0|         1.2|        0.2|Iris-setosa
|         5.7|        4.4|         1.5|        0.4|Iris-setosa
|         5.4|        3.9|         1.3|        0.4|Iris-setosa
|         5.1|        3.5|         1.4|        0.3|Iris-setosa
|         5.7|        3.8|         1.7|        0.3|Iris-setosa
|         5.1|        3.8|         1.5|        0.3|Iris-setosa
+------------+-----------+------------+-----------+------------+
only showing top 20 rows
```

## Parameter
**Decrypt method and Key:**

In `spark_read_encrypted_files.scala` class `decryptTask`, change these code according to your need.

**Schema:**
In `spark_read_encrypted_files.scala` line 61, change schema according to your need.

