#!/usr/bin/env groovy

// Call the script using  
// groovy -cp /opt/kv-1.2.123/lib/kvclient-1.2.123.jar Groovy_oraclenosql.groovy

import oracle.kv.KVStore
import oracle.kv.KVStoreConfig
import oracle.kv.KVStoreFactory
import oracle.kv.Key
import oracle.kv.Value
import oracle.kv.ValueVersion
import oracle.kv.KeyValueVersion
import oracle.kv.Direction
// Groovy imports many important java classes automatically.

class Groovy_oraclenosql {
 
    KVStore store
    Key myKey
    Value myValue
    String keysString = ""
    String valueString = ""
    String errorMessage = ""
    String positiveMessage = ""
    String storeName = "mystore"
    String hostName = "localhost"
    String port = "5000"
    String operation = ""
    String encoding = "ISO-8859-1"
    Integer nFunctionsPassedTest = 0
    Integer nFunctionsTested = 0

    public static void main (args) {
        try {
            Groovy_oraclenosql java_oraclenosql = new Groovy_oraclenosql ()
        } catch (Exception ex) {
            println (ex.toString ())
        }
        println("Hello world")
    }
    
    public Groovy_oraclenosql () {
        // Run all functions.
        test ()
        System.out.println("Default Encoding = " + System.getProperty("file.encoding"));
    }
    
    def test () {
        connect ("mystore", "localhost:5000")
        _evalPositiveMessage ("connect")
        put ("MyTest/MComp2/-/mComp1/mComp2", "Johannes Läufer", false)
        _evalPositiveMessage ("put")
        get ("MyTest/MComp2/-/mComp1/mComp2", false)
        _evalPositiveMessage ("get") 
        countAll (false)
        _evalPositiveMessage ("countAll")
        putIfAbsent ("MyTest/MComp2/-/mComp1/mComp3", "Juanito el Caminante", 
                     false)
        _evalPositiveMessage ("putIfAbsent")
        putIfPresent ("MyTest/MComp2/-/mComp1/mComp2",
                      "Johannes Läufer 2", false)
        _evalPositiveMessage ("putIfPresent")
        storeIterator ("MyTest", true)
        _evalPositiveMessage ("storeIterator")
        delete ("MyTest/MComp2/-/mComp1/mComp2", false)
        _evalPositiveMessage ("delete")
        delete ("MyTest/MComp2/-/mComp1/mComp3", false)
        println (nFunctionsPassedTest + " functions passed out of " +
                            nFunctionsTested)
        nFunctionsPassedTest = 0
        nFunctionsTested = 0
        countAll (true)
    }
    
    // Methods in Groovy can be written as in java
    // or prepending the keyword "def".
    def connect (storeName, connectionString) {
        // connectionString is hostName + ":" + port
        // e.g. localhost:5000
        KVStoreConfig kVStoreConfig = 
            new KVStoreConfig (storeName, connectionString)
        try {
            store = KVStoreFactory.getStore(kVStoreConfig)
            positiveMessage = "connect: passed"
        } catch (Exception ex) {
            errorMessage = "ERROR: No connection to the store. " + 
                           ex.toString ()
            _printErrorMessage ("False") 
        }
    }
     
    def _prepareKey (String keysString) {
        // e.g. keysString = "Test/HelloWorld/Java/-/message_text"
        // myKey contains either an error message or a Key.

        List<String> majorComponents = new ArrayList<String> ()
        List<String> minorComponents = new ArrayList<String> ()

        String [] keysArray = keysString.split ("/")
        boolean isMajor = true
        for (i in 0..keysArray.length -1) {
            if (keysArray [i] == "-") {
                isMajor = false
            }
            if (isMajor) {
                majorComponents.add (keysArray [i])
            } else {
                if (keysArray [i] != "-") {
                    minorComponents.add (keysArray [i])
                }
            }
        }
        if ((majorComponents.size () > 0) && (minorComponents.size () > 0)) {
            myKey = Key.createKey (majorComponents, minorComponents)
        } else if ((majorComponents.size () > 0) & 
                   (minorComponents.size () <= 0)) {
            myKey = Key.createKey (majorComponents)
        } else {
            errorMessage = "ERROR: The String could not be transformed " +
                           "to a Key."
            _printErrorMessage ("False")
            return null
        }
        return myKey
    }

    def _storeFunctions (String what, String keysString, 
                         String valueString, boolean isPrintOutput) {
        myKey = _prepareKey (keysString)
        if (myKey == null) return
        
        // put & Co. return a Version, get returns a ValueVersion, 
        // delete returns a boolean.
        try {
            switch (what) {
                case "delete" : 
                    boolean isSuccess = store.delete (myKey)
                    if (isPrintOutput) println (isSuccess)
                    break
                case "get" :
                    // store.get returns Null or the valueVersion.
                    ValueVersion valueVersion = store.get (myKey)
                    if (valueVersion != null) {
                        String myValueString = new String (
                                               valueVersion.getValue ().
                                               getValue (), encoding)
                        if (isPrintOutput) println (myValueString)
                    } else {
                        // If this is a test, assuming a proper key was put,
                        // it did not pass it.
                        return
                    }
                    break
                case "put" :
                    myValue = Value.createValue (valueString.getBytes ())
                    store.put (myKey, myValue)
                    break
                case "putIfAbsent" :
                    myValue = Value.createValue (valueString.getBytes ())
                    store.putIfAbsent (myKey, myValue)
                    break
                case "putIfPresent" :
                    myValue = Value.createValue (valueString.getBytes ())
                    store.putIfPresent (myKey, myValue)
                    break
            }
            positiveMessage = what + ": passed"
        } catch (Exception ex) {
            errorMessage = "ERROR in " + what + ": " + ex.toString ()
            _printErrorMessage ("False")
            return
        }
        return
    }

    def storeIterator (String keysString, boolean isPrintOutput) {
        // This only works for iterating over PARTIAL major components.
        // Usage: storeIterator("Test/HelloWorld")

        myKey = _prepareKey (keysString)

        //+++TODO.
        //_checkStore()

        try {
            Iterator iterator = store.storeIterator (Direction.UNORDERED, 
                                                     0, myKey, null, null)    
            while (iterator.hasNext ()) {
                Object object = iterator.next ()
                KeyValueVersion keyValueVersion = (KeyValueVersion) object
                String key =  keyValueVersion.getKey ().toString ()
                String value = new String (keyValueVersion.getValue ().
                                           getValue (), encoding)
                if (isPrintOutput) println (key + ", "  + value)
            }
            positiveMessage = "storeIterator: passed"
        } catch (Exception ex) {
            errorMessage = "ERROR in storeIterator: " + ex.toString ()
            _printErrorMessage ("False")
        }         
    }

    def countAll (boolean isPrintOutput) {
        try {
            Iterator iterator = store.storeKeysIterator (Direction.UNORDERED, 0)
            // int can be written, actually there is no int in Groovy.
            // Everything is an object.
            Integer i = 0
            while (iterator.hasNext ()){        
                i = i + 1
                iterator.next ()            
            }
            if (isPrintOutput) println ("Total number of Records: " + i)
            positiveMessage = "countAll: passed"
        } catch (Exception ex) {
            errorMessage = "ERROR in countAll: " + ex.toString ()
            _printErrorMessage ("False")
        }
    }

    def get (String keysString, boolean isPrintOutput) {
        _storeFunctions ("get", keysString, "", isPrintOutput)
        return
    }
    
    def delete (String keysString, boolean isPrintOutput) {
        _storeFunctions ("delete", keysString, "", isPrintOutput)
        return
    }
    
    def put (String keysString, String valueString, 
                      boolean isPrintOutput) {
        _storeFunctions ("put", keysString, valueString, isPrintOutput)
        return
    }
    
    def putIfPresent (String keysString, String valueString, 
                      boolean isPrintOutput) {  
        _storeFunctions ("putIfPresent", keysString, valueString, isPrintOutput)
        return
    }
    
    def putIfAbsent (String keysString, String valueString, 
                               boolean isPrintOutput) { 
        _storeFunctions ("putIfAbsent", keysString, valueString, isPrintOutput)
        return
    }
    
    def _evalPositiveMessage (String what) {
        if (positiveMessage.equals ("")) {
            println (what + ": NOT PASSED")
        } else {
            println (positiveMessage)
            nFunctionsPassedTest = nFunctionsPassedTest + 1
        }
        positiveMessage = ""
        nFunctionsTested = nFunctionsTested + 1
    }
    
    def _printErrorMessage (String isAbort) {
        println ("Error: " + errorMessage)
        errorMessage = ""
        if (isAbort.equals ("True")) System.exit (1)
    }
}