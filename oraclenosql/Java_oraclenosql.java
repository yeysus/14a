import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.KeyValueVersion;
import oracle.kv.Direction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.Scanner;

// Go to the directory where Java_oraclenosql.java is and run with

// javac -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar Java_oraclenosql.java
// java -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar Java_oraclenosql
//       [arguments]
// Arguments are: -s store_name
//                -h host_name
//                -p port
//                -t
//                -i
// -t runs some tests and quits.
// -i runs in interactive mode.

// Heavily modified from HelloBigDataWorld.java and the Getting Started
// documentation from Oracle's distribution of 
// Oracle NoSQL Community Edition, version 1.2.123.
// Original message below.
/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2011 Oracle and/or its affiliates.  All rights reserved.
 *
 */

public class Java_oraclenosql {
 
    final KVStore store;
    Key myKey = null;
    Value myValue = null;
    String keysString = "";
    String valueString = "";
    String errorMessage = "";
    String positiveMessage = "";
    String storeName = "mystore";
    String hostName = "localhost";
    String port = "5000";
    String cliPrefix = ">>> ";
    int nFunctionsPassedTest = 0;
    int nFunctionsTested = 0;
    boolean isTest = false;
    boolean isInteractive = false;

    public static void main (String args[]) {
        try {
            Java_oraclenosql java_oraclenosql = new Java_oraclenosql (args);
        } catch (Exception ex) {
            System.out.println (ex.toString ());
        }
    }
    
    public Java_oraclenosql (String[] argv) {

        int nArgs = argv.length;
        int argc = 0;

        while (argc < nArgs) {
            final String arg = argv[argc++];

            if (arg.equals ("-s")) {
                if (argc < nArgs) {
                    storeName = argv[argc++];
                } else {
                    errorMessage = "storeName requires an argument";
                    // True means the program will abort after printing.
                    _printErrorMessage ("True");
                }
            } else if (arg.equals ("-h")) {
                if (argc < nArgs) {
                    hostName = argv[argc++];
                } else {
                    errorMessage = "hostName requires an argument";
                    _printErrorMessage ("True");
                }
            } else if (arg.equals ("-p")) {
                if (argc < nArgs) {
                    port = argv[argc++];
                } else {
                    errorMessage = "port requires an argument";
                    _printErrorMessage ("True");
                }
            } else if (arg.equals ("-t")) {
                isTest = true;
            } else if (arg.equals ("-i")) {
                isInteractive = true;
            } else {
                errorMessage = "Argument " + arg + " unknown.";
                _printErrorMessage ("False");
            }
        }
        
        store = KVStoreFactory.getStore
            (new KVStoreConfig (storeName, hostName + ":" + port));
            
        if (nArgs == 0) {
            System.out.println ("No arguments were given; using defaults");
        }
        
        if (isTest) {
            // Run the tests.
            test ();
        }
        
		// Infinite loop for reading Strings from the console.
        // Abandon with quit()
		String operation = "";
        Scanner scanner = new Scanner (System.in);
		while (isInteractive) {
		    System.out.print (cliPrefix);
			operation = scanner.nextLine ();

            if (operation.equals ("")) {
                continue;
            }
            
            // operation must be in the form: function(arguments)
            if ((operation.indexOf ('(') > 0) && 
                (operation.indexOf ('(') < operation.indexOf (')'))) {
                
                // Get function name.
                String functionName = 
                    operation.substring (0, operation.indexOf ('(')).trim ();
                // Function name must be known.
                // Determine if an element is in a java array:
                // In stackoverflow.com/questions/1128723/
                // bit.ly: http://bit.ly/yOOPLg
                if (Arrays.asList ("get", "delete", "put", "multiDelete", 
                    "countAll", "getAllKeys", "storeIterator").contains 
                                                   (functionName)) {
                    
                    // Get arguments of functionName.                    
                    String functionArgument =
                        operation.substring (operation.indexOf ('(') + 1, 
                                             operation.indexOf (')'));
                    if (functionArgument.indexOf (',') < 0) {
                        keysString = functionArgument;
                    } else {
                        keysString = functionArgument.substring (0, functionArgument.indexOf (','));
                        valueString = functionArgument.substring (functionArgument.indexOf (',') + 1);
                    }
                    if (Arrays.asList ("get", "delete", "put").contains 
                                                       (functionName)) {
                        _storeFunctions (functionName, true);
                    } else if (functionName.equals ("countAll")) {
                        countAll (true);
                    } else if (functionName.equals ("getAllKeys")) {
                        getAllKeys (true);
                    } else if (functionName.equals ("storeIterator")) {
                        storeIterator (functionArgument, true);
                    }
                    if (errorMessage != "") _printErrorMessage ("False");
                } else if (functionName.equals ("quit")) {
                    scanner.close ();
                    System.exit (0);
                } else {
                    errorMessage = "Operation " + functionName + 
                        " could not be identified.";
                    _printErrorMessage ("False");
                }                
            } else {
                errorMessage = "Operation: " + operation + 
                    " must be in the form: function(arg1, arg2, ...).";
                _printErrorMessage ("False");
            }
		}
    }
    
    private void test () {
        System.out.println ("Starting Tests.");
        countAll (true);
        countAll (false);
        _evalPositiveMessage ("countAll");
        put ("MyTest/MComp2/-/mComp1/mComp2", "Johannes Läufer", false);
        _evalPositiveMessage ("put");
        get ("MyTest/MComp2/-/mComp1/mComp2", false);
        _evalPositiveMessage ("get"); 
        putIfAbsent ("MyTest/MComp2/-/mComp1/mComp3", "Juanito el Caminante", 
                     false);
        _evalPositiveMessage ("putIfAbsent");
        putIfPresent ("MyTest/MComp2/-/mComp1/mComp2","Johannes Läufer 2", false);
        _evalPositiveMessage ("putIfPresent");
        storeIterator ("MyTest", false);
        _evalPositiveMessage ("storeIterator");
        getAllKeys (false);
        _evalPositiveMessage ("getAllKeys");
        delete ("MyTest/MComp2/-/mComp1/mComp2", false);
        _evalPositiveMessage ("delete");
        multiDelete ("MyTest/MComp2", false);
        _evalPositiveMessage ("multiDelete");
        System.out.println (nFunctionsPassedTest + " functions passed out of " +
                            nFunctionsTested);
        nFunctionsPassedTest = 0;
        nFunctionsTested = 0;
        countAll (true);
    }
     
    private void _prepareKey () {
        // e.g. keysString = "Test/HelloWorld/Java/-/message_text"

        List<String> majorComponents = new ArrayList<String> ();
        List<String> minorComponents = new ArrayList<String> ();

        String [] keysArray = keysString.split ("/");
        boolean isMajor = true;
        for (int i = 0; i < keysArray.length; i++) {
            if (keysArray [i].equals ("-")) {
                isMajor = false;
                continue;
            }
            if (isMajor) {
                majorComponents.add (keysArray [i]);
            } else {
                minorComponents.add (keysArray [i]);
            }
        }

        if ((majorComponents.size () > 0) && (minorComponents.size () > 0)) {
            myKey = Key.createKey (majorComponents, minorComponents);
        } else if ((majorComponents.size () > 0) & (minorComponents.size () <= 0)) {
            myKey = Key.createKey (majorComponents);
        } else {
            errorMessage = "ERROR: The String could not be transformed to a Key.";
            _printErrorMessage ("False");
            return;
        }
        return;
    }

    private void _storeFunctions (String what, boolean isPrintOutput) {

        _prepareKey ();
        if (myKey == null) return;
         
        // +++TODO
        // errorMessage = _checkStore()
        
        // put & Co. return a Version, get returns a ValueVersion, 
        // delete returns a boolean.
        try {
            if (what.equals ("delete")) {
                boolean isSuccess = store.delete (myKey);
                if (isPrintOutput) System.out.println (isSuccess);
            } else if (what.equals ("multiDelete")) {
                int isSuccess = store.multiDelete (myKey, null, null);
                if (isPrintOutput) System.out.println (isSuccess);
            } else if (what.equals ("get")) {
                // store.get returns Null or the valueVersion.
                ValueVersion valueVersion = store.get (myKey);
                if (valueVersion != null) {
                    // toString () from getValue ().getValue () does not work.
                    String myValueString = new String (valueVersion.getValue ().
                                                       getValue ());
                    if (isPrintOutput) System.out.println (myValueString);
                } else {
                    // If this is a test, assuming a proper key was put,
                    // it did not pass it.
                    if (isPrintOutput) {
                        System.out.println ("Key " + keysString + 
                                            " could not be found.");
                    }
                    return;
                }
            } else if (what.equals ("put")) {
                // put, putIfAbsent, putIfPresent.
                myValue = Value.createValue (valueString.getBytes ());
                
                // Reflection is giving me too many troubles. So forget it.
                store.put (myKey, myValue);
            } else if (what.equals ("putIfAbsent")) {
                myValue = Value.createValue (valueString.getBytes ());
                store.putIfAbsent (myKey, myValue);
            } else if (what.equals ("putIfPresent")) {
                myValue = Value.createValue (valueString.getBytes ());
                store.putIfPresent (myKey, myValue);
            }
            positiveMessage = what + ": passed";
        } catch (Exception ex) {
            errorMessage = "ERROR in " + what + ": " + ex.toString ();
            _printErrorMessage ("False");
            return;
        }
        return;
    }

    private void storeIterator (String thisKeysString, boolean isPrintOutput) {
        // This only works for iterating over PARTIAL major components.
        // Usage: storeIterator ("Test/HelloWorld")
        keysString = thisKeysString;
        _prepareKey ();

        //+++TODO.
        //_checkStore();

        try {
            Iterator iterator = store.storeIterator (Direction.UNORDERED, 
                                                     0, myKey, null, null);    
            while (iterator.hasNext ()) {
                Object object = iterator.next ();
                KeyValueVersion keyValueVersion = (KeyValueVersion) object;
                String key =  new String (keyValueVersion.getKey ().toString ());
                String value = new String (keyValueVersion.getValue ().
                                           getValue ());
                if (isPrintOutput) System.out.println (key + ", "  + value);
            }
            positiveMessage = "storeIterator: passed";
        } catch (Exception ex) {
            errorMessage = "ERROR in storeIterator: " + ex.toString ();
            _printErrorMessage ("False");
        }         
    }

    private void countAll (boolean isPrintOutput) {
        try {
            Iterator iterator = store.storeKeysIterator (Direction.UNORDERED, 0);
            int i = 0;
            while (iterator.hasNext ()){        
                i = i + 1;
                iterator.next ();            
            }
            if (isPrintOutput) System.out.println ("Total number of Records: " + i);
            positiveMessage = "countAll: passed";
        } catch (Exception ex) {
            errorMessage = "ERROR in countAll: " + ex.toString ();
            _printErrorMessage ("False");
        }
    }
    
    private void getAllKeys (boolean isPrintOutput) {
        try {
            Iterator iterator = store.storeKeysIterator (Direction.UNORDERED, 0);
            while (iterator.hasNext ()){        
                Object object = iterator.next ();
                Key thisKey = (Key) object;
                String key =  new String (thisKey.toString ());
                if (isPrintOutput) System.out.println (key);           
            }
            positiveMessage = "getAllKeys: passed";
        } catch (Exception ex) {
            errorMessage = "ERROR in getAllKeys: " + ex.toString ();
            _printErrorMessage ("False");
        }
    }

    private void get (String thisKeysString, boolean isPrintOutput) {
        keysString = thisKeysString;
        _storeFunctions ("get", isPrintOutput);
        return;
    }
    
    private void delete (String thisKeysString, boolean isPrintOutput) {
        keysString = thisKeysString;
        _storeFunctions ("delete", isPrintOutput);
        return;
    }
    
    private void multiDelete (String thisKeysString, boolean isPrintOutput) {
        keysString = thisKeysString;
        _storeFunctions ("multiDelete", isPrintOutput);
        return;
    }
    
    private void put (String thisKeysString, String thisValueString, 
                      boolean isPrintOutput) {
        keysString = thisKeysString;
        valueString = thisValueString;
        _storeFunctions ("put", isPrintOutput);
        return;
    }
    
    private void putIfPresent (String thisKeysString, String valueString, 
                      boolean isPrintOutput) {  
        keysString = thisKeysString;
        _storeFunctions ("putIfPresent", isPrintOutput);
        return;
    }
    
    private void putIfAbsent (String thisKeysString, String valueString, 
                               boolean isPrintOutput) { 
        keysString = thisKeysString;
        _storeFunctions ("putIfAbsent", isPrintOutput);
        return;
    }
    
    private void _evalPositiveMessage (String what) {
        if (positiveMessage.equals ("")) {
            System.out.println (what + ": NOT PASSED");
        } else {
            System.out.println (positiveMessage);
            nFunctionsPassedTest = nFunctionsPassedTest + 1;
        }
        positiveMessage = "";
        nFunctionsTested = nFunctionsTested + 1;
    }
    
    private void _printErrorMessage (String isAbort) {
        System.out.println ("Error: " + errorMessage);
        errorMessage = "";
        if (isAbort.equals ("True")) System.exit (1);
    }
}
