# Spark Decrypt files
### This is a project to encrypt and decrypt files in spark with AES/GCM or AES/CBC only use java and spark dependencies.
## Environment
* [Spark 3.x](https://spark.apache.org/downloads.html)
* [Maven](https://maven.apache.org/)

## Prepare
* Files need to be decrypted. Put these files into a folder. These files should be encrypted either by `AES-GCM` or `AES-CBC` use a secret key.

* Build

  run:
  ```bash
  mvn clean package
  ```

  You will get `./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar`

## Encrypt Files

### Generate secret key:

```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.testKey \
  ./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar
```

The output is:
```bash
Successfully generate key:
LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```

### **Encrypt**
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.encryptFiles \
  ./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar \
  ./src/main/scala/com/piaolaidelangman/sparkEncryptFiles/originData/ \
  /tmp/AESCBC \
  AESCBC LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```
or
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.encryptFiles \
  ./target/sparkcryptofiles-1.0-SNAPSHOT-jar-with-dependencies.jar \
  ./src/main/scala/com/piaolaidelangman/sparkEncryptFiles/originData/ \
  /tmp/AESGCM \
  AESGCM LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```
I use [iris.csv]() and the output is:

```bash
/tmp/AESGCM/iris_2.csv AES-GCM encrypt successfully saved!
/tmp/AESGCM/iris_1.csv AES-GCM encrypt successfully saved!
```
Then I get the `iris.csv`(Encrypted) in folder `/tmp/AESGCM`.

Please modify the path in the command according to your needs.

### **Usage**
* inputPath: String. A folder contains encrypt files.
* outputPath: String. The path where encrypted files to be saved.
* encryptMethod: String. "AESCBC" or "AESGCM". A method used to decrypt files.
* secret: String. "AESCBC" or "AESGCM" encrypt method needs this parameter.

## Decrypt files

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
The output is:
```bash
+------------+-----------+------------+-----------+---------------+
|sepal length|sepal width|petal length|petal width|          class|
+------------+-----------+------------+-----------+---------------+
|         6.6|        3.0|         4.4|        1.4|Iris-versicolor|
|         6.8|        2.8|         4.8|        1.4|Iris-versicolor|
|         6.7|        3.0|         5.0|        1.7|Iris-versicolor|
|         6.0|        2.9|         4.5|        1.5|Iris-versicolor|
|         5.7|        2.6|         3.5|        1.0|Iris-versicolor|
|         5.5|        2.4|         3.8|        1.1|Iris-versicolor|
|         5.5|        2.4|         3.7|        1.0|Iris-versicolor|
|         5.8|        2.7|         3.9|        1.2|Iris-versicolor|
|         6.0|        2.7|         5.1|        1.6|Iris-versicolor|
|         5.4|        3.0|         4.5|        1.5|Iris-versicolor|
|         6.0|        3.4|         4.5|        1.6|Iris-versicolor|
|         6.7|        3.1|         4.7|        1.5|Iris-versicolor|
|         6.3|        2.3|         4.4|        1.3|Iris-versicolor|
|         5.6|        3.0|         4.1|        1.3|Iris-versicolor|
|         5.5|        2.5|         4.0|        1.3|Iris-versicolor|
|         5.5|        2.6|         4.4|        1.2|Iris-versicolor|
|         6.1|        3.0|         4.6|        1.4|Iris-versicolor|
|         5.8|        2.6|         4.0|        1.2|Iris-versicolor|
|         5.0|        2.3|         3.3|        1.0|Iris-versicolor|
|         5.6|        2.7|         4.2|        1.3|Iris-versicolor|
+------------+-----------+------------+-----------+---------------+
only showing top 20 rows
```

Please modify the path in the command according to your needs.

### **Usage**
* inputPath: String. A folder contains encrypt files.
* decryptMethod: String. "AESCBC" or "AESGCM". A method used to decrypt files according your encrypt method.
* secret: String. "AESCBC" or "AESGCM" decrypt method needs this parameter.

There are more decrypt files demo in [src/main/scala/com/piaolaidelangman/sparkDecryptFiles/](). For [example](), encrypt some column from a encrypted table then save to file.
