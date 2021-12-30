package sparkCryptoFiles

import java.lang.RuntimeException

class cryptoException (message: String, cause: Throwable) 
  extends RuntimeException(message) {
    if (cause != null)
      initCause(cause)

    def this(message: String) = this(message, null)  
}