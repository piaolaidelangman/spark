package piaolaidelangman.spark

import com.macasaet.fernet.Key

object fernetKey {

    def generateFernetKey(): String = {
        val key = Key.generateKey();
        key.serialise
    }
}
