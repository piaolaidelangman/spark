# Spark Encrypt files
## Data Prepare
* Put files to be encrypted into a folder.

## Run
After [build](), run:
```
spark-submit \
  --master local[2] \
  --class sparkDecryptFiles.decryptFiles \
  -- jars /path/to/jars/fernet-java8-1.4.2.jar \
  /path/to/target/scala-2.12/sparkDecrypt_2.12-0.1.jar \
  /path/to/filesNeedsToBeDecrypt
```

I use iris.csv and the output is:
```

```
You can fild the .csv files(Encrypted) in `originData` folder.

## Usage
* inputPath: String. A folder contains encrypt files.
* outputPath: String. The path where encrypted files to be saved.
* encryptMethod: String. "Java" or "Fernet". A method used to decrypt files according your encrypt method.
* secret: String. Java decrypt method AES/GCM needs parameters 'secret' and 'salt'. In Fernet, 'secret' is a key.
* salt: String. As mentioned above. Encrypt method 'Fernet' doesn't need this parameter.