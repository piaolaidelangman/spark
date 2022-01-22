# Spark Decrypt files
### This is a project to encrypt and decrypt files in spark with AES/GCM or AES/CBC.
## Environment
* [Spark 3.x](https://spark.apache.org/downloads.html)
* [Maven](https://maven.apache.org/)

## Prepare
* Files need to be decrypted. Put these files into a folder. These files should be encrypted either by [AES/GCM]() or [AES/CBC]() use a key. I provide a encrypt-files example with both method in [here](https://github.com/piaolaidelangman/spark-read-ecrypted-files/blob/main/sparkEncryptFiles.scala).

* Build

  run:
  ```bash
  mvn clean package
  ```

  You will get `./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar`

## Run command
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.decryptFiles \
  ./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /tmp/AESCBC \
  AESCBC LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```
or
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.decryptFiles \
  ./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar \
  /tmp/AESGCM \
  AESGCM LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```
I use [iris.csv](https://github.com/piaolaidelangman/spark-read-ecrypted-files/tree/main/sparkEncryptFiles/originData) and the output is:
```js
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
You can fild the .csv files(Not encrypted) in `./originData` folder.

Please modify the path in the command according to your needs.

## Usage
* inputPath: String. A folder contains encrypt files.
* decryptMethod: String. "AESCBC" or "AESGCM". A method used to decrypt files according your encrypt method.
* secret: String. "AESCBC" or "AESGCM" decrypt method needs this parameter.
