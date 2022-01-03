# Spark Encrypt files
## Data Prepare
* Put files to be encrypted into a folder.
* Prepare a secret-key or [generate]().

## Run
After [build](https://github.com/piaolaidelangman/spark-read-ecrypted-files#prepare), run:
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.encryptFiles \
  ./target/scala-2.12/sparkcryptofiles_2.12-0.1.0.jar \
  ./sparkEncryptFiles/originData \
  /tmp/AESCBC \
  AESCBC LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```
or
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.encryptFiles \
  ./target/scala-2.12/sparkcryptofiles_2.12-0.1.0.jar \
  ./sparkEncryptFiles/originData \
  /tmp/AESGCM \
  AESGCM LDlxjm0y3HdGFniIGviJnMJbmFI+lt3dfIVyPJm1YSY=
```
I use [iris.csv](https://github.com/piaolaidelangman/spark-read-ecrypted-files/tree/main/sparkEncryptFiles/originData) and the output is:

```bash
/tmp/AESGCM/iris_2.csv AES/GCM encrypt successfully saved!
/tmp/AESGCM/iris_1.csv AES/GCM encrypt successfully saved!
```
Then I get the `iris.csv`(Encrypted) in folder `/tmp/AESGCM`.

Please modify the path in the command according to your needs.

If you donot have a secret-key, you can generate one use:
```bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class sparkCryptoFiles.testKey \
  ./target/scala-2.12/sparkcryptofiles_2.12-0.1.0.jar
```
The output is:
```js
Successfully generate key:
ft5eLMNQpJul8MEpNFHa+UwLzVOo+KlsWrCEGZvhpKM=
```

## Usage
* inputPath: String. A folder contains encrypt files.
* outputPath: String. The path where encrypted files to be saved.
* encryptMethod: String. "AESCBC" or "AESGCM". A method used to decrypt files.
* secret: String. "AESCBC" or "AESGCM" encrypt method needs this parameter.