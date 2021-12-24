package piaolaidelangman.spark

import com.macasaet.fernet.Key

object generateFernetKey {

    def main(args: Array[String]): Unit = {
        val key = Key.generateKey();
        println("successfully generate key:\n" + key.serialise)
    }
}
