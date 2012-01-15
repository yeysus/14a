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

function _evalPositiveMessage (what) {
    if (positiveMessage != "") {
        print (positiveMessage);
        nFunctionsPassedTest = nFunctionsPassedTest + 1;
    } else {
        print (what + ": NOT PASSED TEST.");
    }
    positiveMessage = "";
    nFunctionsTested = nFunctionsTested + 1;
    return;
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
                positiveMessage = "get: passed test.";
            } else {
                // If this is a test, assuming a proper key was put,
                // it did not pass it.
                return;
            }
        } else if (what.equals ("put")) {
            javaValueString = new java.lang.String (valueString);
            myValue = Value.createValue (javaValueString.getBytes ());
            store.put (myKey, myValue);
            positiveMessage = "put: passed test.";
        } else if (what.equals ("putIfAbsent")) {
            myValue = Value.createValue (valueString.getBytes ());
            store.putIfAbsent (myKey, myValue);
            positiveMessage = "putIfAbsent: passed test.";
        } else if (what.equals ("putIfPresent")) {
            myValue = Value.createValue (valueString.getBytes ());
            store.putIfPresent (myKey, myValue);
            positiveMessage = "putIfPresent: passed test.";
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

function connect (storeName, hostName, port) {

    try {
        store = KVStoreFactory.getStore
            (new KVStoreConfig (storeName, hostName + ":" + port));
        positiveMessage = "connect: passed test.";
    } catch (err) {
        // From http://www.mozilla.org/rhino/ScriptingJava.html:
        // Rhino wraps Java exceptions into error objects with properties:
        // javaException and rhinoException.
        print (err.javaException);
    }
    return store;
}

function countAll () {
    try {
        iterator = store.storeKeysIterator (Direction.UNORDERED, 0);
        i = 0;
        while (iterator.hasNext()) {
            i = i + 1;
            iterator.next ();
        }
        print ("Total number of Records: " + i);
        positiveMessage = "countAll: passed test.";
    } catch (err) {
        errorMessage = "Error in countAll(): " + err.rhinoException;
        print (errorMessage);
        errorMessage = "";
        return
    }
    return
}

function get (keysString, isPrintOutput) {
    _storeFunctions ("get", keysString, "", isPrintOutput);
    return;
}

function put (keysString, valueString, isPrintOutput) {
    _storeFunctions ("put", keysString, valueString, isPrintOutput);
    return;
}

// delete is a keyword in javascript.
function del (keysString, isPrintOutput) {
    _storeFunctions ("delete", keysString, "", isPrintOutput);
    return;
}

function test (storeName, hostName, port) {
    // if connect() fails, store will be null.
    nFunctionsPassedTest = 0;
    nFunctionsTested = 0;
    positiveMessage = "";
    print ("Starting Test.");
    store = connect(storeName, hostName, port);
    _evalPositiveMessage ("connect");
    countAll ();
    _evalPositiveMessage ("countAll");    
    put ("MyTest/MComp2/-/mComp1/mComp2", "Juanito el Caminante", false);
    _evalPositiveMessage ("put");     
    get ("MyTest/MComp2/-/mComp1/mComp2", true);
    _evalPositiveMessage ("get");    
    del ("MyTest/MComp2/-/mComp1/mComp2", true); 
    _evalPositiveMessage ("delete");
    print (nFunctionsPassedTest + " functions passed the test out of " + 
        nFunctionsTested);
    countAll (); 
    nFunctionsPassedTest = 0;
    nFunctionsTested = 0;
    positiveMessage = "";    
}

// Modified from http://www.manamplified.org/archives/2005/11/
//                      javascript-command-line-parsing.html
// In bit.ly: http://bit.ly/A88tVW
function readArguments () {

    var params = [];

    for each (var arg in args) {
        // Argument is in a good format, -a=parameter
        if (arg.indexOf ("-") == 0) {
            arg = arg.substring (1).split ("=");
            switch (arg[0]) {
                case "s":
                    // s: store name.
                    storeName = arg[1];
                    break;
                case "h":
                    // h: host name.
                    hostName = arg[1];
                    break;
                case "p":
                    // p: port.
                    port = arg[1];
                    break;
                default:
                    print ("+++++ ERROR: Option not recognized: " + arg[0]);
                    break;
            }
        } else {
            print ("+++++ ERROR: Option not recognized: " + arg + " +++++");
            print ("+++++ Maybe you need to prefix it with a '-' +++++");
        }
    }
}

function fillDefaults () {
    var isUsingDefaults = false;
    if (storeName == "") {
        storeName = defaultStoreName;
        isUsingDefaults = true;
    }
    if (hostName == "") {
        hostName = defaultHostName;
        isUsingDefaults = true;
    }        
    if (port == "") {
        port = defaultPort;
        isUsingDefaults = true;
    }
    if (isUsingDefaults) print ("+++ Using default(s). +++");
}

// JavaScript stores passed command-line arguments in the variable "arguments".
args = arguments;
storeName = "";
hostName = "";
port = "";
defaultStoreName = "mystore",
defaultHostName = "localhost";
defaultPort = "5000";
readArguments ();
fillDefaults ();
test (storeName, hostName, port);
