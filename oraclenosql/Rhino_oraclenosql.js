/* java -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar:/opt/rhino1_7R3/js.jar 
              org.mozilla.javascript.tools.shell.Main Rhino_oraclenosql.js
*/

// From http://www.mozilla.org/rhino/scriptjava.html:
// Prefix external package names with "Packages".
importClass (Packages.oracle.kv.KVStore);
importClass (Packages.oracle.kv.KVStoreConfig);
importClass (Packages.oracle.kv.KVStoreFactory);
importClass (Packages.oracle.kv.Key);
importClass (Packages.oracle.kv.Value);
importClass (Packages.oracle.kv.ValueVersion);
importClass (Packages.oracle.kv.KeyValueVersion);
importClass (Packages.oracle.kv.Direction);
importClass (java.util.ArrayList);

function connect (storeName, hostName, port) {
    try {
        store = KVStoreFactory.getStore
            (new KVStoreConfig (storeName, hostName + ":" + port));
    } catch (err) {
        // From http://www.mozilla.org/rhino/ScriptingJava.html:
        // Rhino wraps Java exceptions into error objects with properties:
        // javaException and rhinoException.
        print (err.javaException);
    }
    return store;
}

function _prepareKey (keysString) {
    // e.g. keysString = "Test/HelloWorld/Java/-/message_text"
    // myKey contains either an error message or a Key.

    majorComponents = new java.util.ArrayList();
    minorComponents = new ArrayList();

    keysArray = keysString.split ("/");
    isMajor = true;
    for (i = 0; i < keysArray.length; i++) {
        if (keysArray [i] == "-") {
            isMajor = false;
        }
        if (isMajor) {
            majorComponents.add (keysArray [i]);
        } else {
            if (keysArray [i] != "-") {
                minorComponents.add (keysArray [i]);
            }
        }
    }
    if ((majorComponents.size () > 0) && (minorComponents.size () > 0)) {
        myKey = Key.createKey (majorComponents, minorComponents);
    } else if ((majorComponents.size () > 0) & (minorComponents.size () <= 0)) {
        myKey = Key.createKey (majorComponents);
    } else {
        errorMessage = "ERROR: The String could not be transformed to a Key.";
        print ("False");
        return null;
    }
    return myKey;
}

function _storeFunctions (what, keysString, valueString, isPrintOutput) {

    myKey = _prepareKey (keysString);
    if (myKey == null) return;
         
    // +++TODO
    // errorMessage = _checkStore()
        
    // put & Co. return a Version, get returns a ValueVersion, 
    // delete returns a boolean.
    try {
        if (what.equals ("delete")) {
            isSuccess = store.delete (myKey);
            if (isPrintOutput) print (isSuccess);
        } else if (what.equals ("get")) {
            // store.get returns Null or the valueVersion.
            valueVersion = store.get (myKey);
            if (valueVersion != null) {
                // java.lang.String must be.
                myValueString = new java.lang.String (valueVersion.getValue ().
                                                   getValue ());
                if (isPrintOutput) print (myValueString);
            } else {
                // If this is a test, assuming a proper key was put,
                // it did not pass it.
                return;
            }
        } else if (what.equals ("put")) {
            // put, putIfAbsent, putIfPresent.
            myValue = Value.createValue (valueString.getBytes ());

            store.put (myKey, myValue);
        } else if (what.equals ("putIfAbsent")) {
            myValue = Value.createValue (valueString.getBytes ());
            store.putIfAbsent (myKey, myValue);
        } else if (what.equals ("putIfPresent")) {
            myValue = Value.createValue (valueString.getBytes ());
            store.putIfPresent (myKey, myValue);
        }
        positiveMessage = what + ": passed";
    } catch (err) {
        // The error could come from java; then it is err.javaException.
        // But rhinoException provides a wrapper for both exceptions
        // coming from java or coming from rhino itself.
        errorMessage = "ERROR in " + what + ": " + err.rhinoException;
        print (errorMessage);
        return;
    }
    return;
}

function get (keysString, isPrintOutput) {
    _storeFunctions ("get", keysString, "", isPrintOutput);
    return;
}

function test (myStore, host, port) {
    // if connect() fails, store will be null.
    store = connect(myStore, host, port); 
    get ("Test/HelloWorld/Java/-/message_text", true);    
}

test ("mystore", "localhost", 5000);
