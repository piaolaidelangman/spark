# Spark Encrypt files
## Data Prepare
* Put files to be encrypted into a folder.

## Run
After [build](https://github.com/piaolaidelangman/spark-read-ecrypted-files#prepare), run:
```
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkEncryptFiles.encryptFiles \
  --jars /path/to/jars/fernet-java8-1.4.2.jar \
  /path/to/target/scala-2.12/sparkdecryptfiles_2.12-0.1.0.jar \
  /path/to/filesNeedsToBeEncrypt \
  /path/to/filesEncrypted \
  Java 1111111111 2222222222
```
or
```
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkEncryptFiles.encryptFiles \
  --jars /path/to/jars/fernet-java8-1.4.2.jar \
  /path/to/target/scala-2.12/sparkdecryptfiles_2.12-0.1.0.jar \
  /path/to/filesNeedsToBeEncrypt \
  /path/to/filesEncrypted \
  Fernet YLcuLTk2BXFCr2QLwvmERFlYCkmKyGLCnpUv9jevV8k=
```
I use iris.csv and the output is:
```
/tmp/encryptedFiles/iris_2.csv Java encrypt successfully saved!
/tmp/encryptedFiles/iris_1.csv Java encrypt successfully saved!

```
You can fild the .csv files(Encrypted) in `../originData` folder.

If you doesn't have a Fernet key, you can use this [function]().

Please modify the path in the command according to your needs.

## Usage
* inputPath: String. A folder contains encrypt files.
* outputPath: String. The path where encrypted files to be saved.
* encryptMethod: String. "Java" or "Fernet". A method used to decrypt files according your encrypt method.
* secret: String. Java decrypt method AES/GCM needs parameters 'secret' and 'salt'. In Fernet, 'secret' is a key.
* salt: String. As mentioned above. Encrypt method 'Fernet' doesn't need this parameter.