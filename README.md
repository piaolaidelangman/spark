# Spark Decrypt files
### This is a project to encrypt and decrypt files in spark with AES/GCM.
## Envirronment
* Spark 3.x
* Sbt

## Prepare
* Files need to be decrypted. Put these files into a folder. These files should be encrypted either by AES/GCM use a secret and a salt or [Fernet](https://github.com/l0s/fernet-java8). I provide a encrypt-files example with both method in [here](https://github.com/piaolaidelangman/spark-read-ecrypted-files/blob/main/sparkEncryptFiles.scala).

* Build

  run:
  ```
  sbt package
  ```
  You will get `./target/scala-2.12/sparkdecryptfiles_2.12-0.1.0.ja`

## Run command
```
spark-submit \
  --master local[2] \
  --class sparkDecryptFiles.decryptFiles \
  --jars /path/to/jars/fernet-java8-1.4.2.jar \
  /path/to/target/scala-2.12/sparkdecryptfiles_2.12-0.1.0.jar \
  /path/to/filesNeedsToBeDecrypt \
  Java 1111111111 22222222222
```
or
```
spark-submit \
  --master local[2] \
  --class sparkDecryptFiles.decryptFiles \
  --jars /path/to/jars/fernet-java8-1.4.2.jar \
  /path/to/target/scala-2.12/sparkdecryptfiles_2.12-0.1.0.jar \
  /path/to/filesNeedsToBeDecrypt \
  Fernet YLcuLTk2BXFCr2QLwvmERFlYCkmKyGLCnpUv9jevV8k=
```
I use iris.csv and the output is:
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
You can fild the .csv files(Not encrypted) in `originData` folder.

## Usage
* inputPath: String. A folder contains encrypt files.
* encryptMethod: String. "Java" or "Fernet". A method used to decrypt files according your encrypt method.
* secret: String. Java decrypt method AES/GCM needs parameters 'secret' and 'salt'. In Fernet, 'secret' is a key.
* salt: String. As mentioned above. Decrypt method 'Fernet' doesn't need this parameter.

**Note:**

I use fernet-java8's jar because even if I add library depencency in [build.sbt](https://github.com/piaolaidelangman/spark-read-ecrypted-files/blob/main/build.sbt#L11), still got a error:
```
java.lang.NoClassDefFoundError: com/macasaet/fernet/Key
```
And I haven't find the reason.
