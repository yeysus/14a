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
import java.lang.Class;
import java.lang.reflect.*;
import java.io.*;

// Go to the directory where Java_oraclenosql.java is and run with

// javac -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar Java_oraclenosql.java
// java -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar Java_oraclenosql

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
    Key myKey;
    Value myValue;
    String keysString = "";
    String valueString = "";
    String errorMessage = "";
    String positiveMessage = "";
    String storeName = "mystore";
    String hostName = "localhost";
    String port = "5000";
    String operation = "";
    String encoding = "ISO-8859-1";
    int nFunctionsPassedTest = 0;
    int nFunctionsTested = 0;

    public static void main (String args[]) {
        try {
            Java_oraclenosql java_oraclenosql = new Java_oraclenosql (args);
        } catch (Exception ex) {
            System.out.println (ex.toString ());
        }
    }
    
    public Java_oraclenosql (String[] argv) {

        //+++TODO; NOT TRIED.
        int nArgs = argv.length;
        int argc = 0;

        while (argc < nArgs) {
            final String arg = argv[argc++];

            if (arg.equals ("-storeName")) {
                if (argc < nArgs) {
                    storeName = argv[argc++];
                } else {
                    errorMessage = "storeName requires an argument";
                    // True means the program will abort after printing.
                    printErrorMessage ("True");
                }
            } else if (arg.equals ("-hostName")) {
                if (argc < nArgs) {
                    hostName = argv[argc++];
                } else {
                    errorMessage = "hostName requires an argument";
                    printErrorMessage ("True");
                }
            } else if (arg.equals ("-port")) {
                if (argc < nArgs) {
                    port = argv[argc++];
                } else {
                    errorMessage = "port requires an argument";
                    printErrorMessage ("True");
                }
            } else if (arg.equals ("-encoding")) {
                if (argc < nArgs) {
                    encoding = argv[argc++];
                } else {
                    errorMessage = "encoding requires an argument";
                    printErrorMessage ("True");
                }
            } else if (arg.equals ("-operation")) {
                if (argc < nArgs) {
                    operation = argv[argc++];
                } else {
                    errorMessage = "operation requires an argument";
                    printErrorMessage ("True");
                }
            } else if (arg.equals ("-operationArgs")) {
                if (argc < nArgs) {
                    operation = argv[argc++];
                } else {
                    errorMessage = "operationArgs requires an argument";
                    errorMessage += "enclosed in \"";
                    printErrorMessage ("True");
                }
            } else {
                errorMessage = "Argument unknown.";
                printErrorMessage ("False");
            }
        }
        
        store = KVStoreFactory.getStore
            (new KVStoreConfig (storeName, hostName + ":" + port));

        // For decent output of non-English characters in the console.
        try {
            System.setOut (new PrintStream (new FileOutputStream (
                FileDescriptor.out), true, encoding));
        } catch (Exception ex) {
            System.out.println ("The character encoding, " +
                encoding + ", could not be set\n");              
        }
            
        if (nArgs == 0) {
            // Run the tests.
            test ();
        }
        
        // Operate. +++TODO. User reflection to check if method exists.
        // But I don't want to use all of them. Also not reflection.
    }
    
    private void test () {
        put ("MyTest/MComp2/-/mComp1/mComp2", "Corralejo", false);
        _evalPositiveMessage ("put");
        get ("MyTest/MComp2/-/mComp1/mComp2", false);
        _evalPositiveMessage ("get"); 
        countAll ();
        _evalPositiveMessage ("countAll");
        putIfAbsent ("MyTest/MComp2/-/mComp1/mComp3", "Juanito el Caminante", 
                     false);
        _evalPositiveMessage ("putIfAbsent");
        putIfPresent ("MyTest/MComp2/-/mComp1/mComp2","Johannes Läufer", false);
        _evalPositiveMessage ("putIfPresent");
        storeIterator ("MyTest/MComp2", true);
        _evalPositiveMessage ("storeIterator");
        
        System.out.println (nFunctionsPassedTest + " functions passed out of " +
                            nFunctionsTested);
        nFunctionsPassedTest = 0;
        nFunctionsTested = 0;
    }
     
    private Key _prepareKey (String keysString) {
        // e.g. keysString = "Test/HelloWorld/Java/-/message_text"
        // myKey contains either an error message or a Key.

        List<String> majorComponents = new ArrayList<String> ();
        List<String> minorComponents = new ArrayList<String> ();

        String [] keysArray = keysString.split ("/");
        boolean isMajor = true;
        for (int i = 0; i < keysArray.length; i++) {
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
            printErrorMessage ("False");
            return null;
        }
        return myKey;
    }

    private void _storeFunctions (String what, String keysString, 
                                  String valueString, boolean isPrintOutput) {
        Class myStoreClass;

        myKey = _prepareKey (keysString);
        if (myKey == null) return;
         
        // +++TODO
        // errorMessage = _checkStore()
        
        //store_function = getattr (store, "%s" % what);
        
        // put & Co. return a Version, get returns a ValueVersion, 
        // delete returns a boolean.
        try {
            if (what.equals ("delete")) {
                boolean isSuccess = store.delete (myKey);
                if (isPrintOutput) System.out.println (isSuccess);
            } else if (what.equals ("get")) {
                // store.get returns Null or the valueVersion.
                ValueVersion valueVersion = store.get (myKey);
                if (valueVersion != null) {
                    // toString () from getValue ().getValue () does not work.
                    String myValueString = new String (valueVersion.getValue ().
                                                       getValue (), encoding);
                    if (isPrintOutput) System.out.println (myValueString);
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
            printErrorMessage ("False");
            return;
        }
        return;
    }

    private void storeIterator (String keysString, boolean isPrintOutput) {
        // This only works for iterating over major components.
        // Usage: storeIterator("Test/HelloWorld")

        myKey = _prepareKey (keysString);

        //+++TODO.
        //_checkStore();

        try {
            Iterator iterator = store.storeIterator (Direction.UNORDERED, 
                                                     0, myKey, null, null);    
            while (iterator.hasNext ()) {
                Object object = iterator.next ();
                KeyValueVersion keyValueVersion = (KeyValueVersion) object;
                String key =  keyValueVersion.getKey ().toString ();
                String value = new String (keyValueVersion.getValue ().
                                           getValue (), encoding);
                if (isPrintOutput) System.out.println (key + ", "  + value);
            }
            positiveMessage = "storeIterator: passed";
        } catch (Exception ex) {
            errorMessage = "ERROR in storeIterator: " + ex.toString ();
            printErrorMessage ("False");
        }         
    }
    
    private void printErrorMessage (String isAbort) {
        System.out.println ("Error: " + errorMessage);
        errorMessage = "";
        if (isAbort.equals ("True")) System.exit (1);
    }

    private void countAll () {
        try {
            Iterator iterator = store.storeKeysIterator (Direction.UNORDERED, 0);
            int i = 0;
            while (iterator.hasNext ()){        
                i = i + 1;
                iterator.next ();            
            }
            System.out.println ("Total number of Records: " + i);
            positiveMessage = "countAll: passed";
        } catch (Exception ex) {
            errorMessage = "ERROR in countAll: " + ex.toString ();
            printErrorMessage ("False");
        }
    }
   
    private void getTODO (String keysString) {
        // e.g. get ("Test/HelloWorld/Java/-/message_text")
        // String valueString = "Hello World from Java, Tomcat, and Oracle NoSQL";
        
        List<String> majorComponents = new ArrayList<String>();
        List<String> minorComponents = new ArrayList<String>();
        
        String[] keysArray = keysString.split ("/");
        // Define the major and minor components of the key.
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

        Key myKey = Key.createKey (majorComponents, minorComponents);
        
        try {
            final ValueVersion valueVersion = store.get (myKey);
            if (valueVersion != null) {
                String theValue = new String (valueVersion.getValue ().
                                              getValue (), 
                                              encoding);
                System.out.println (theValue);}
        } catch (Exception ex) {
            errorMessage = "ERROR in get: " + ex.toString ();
            printErrorMessage ("False");
        }
    }

    private void get (String keysString, boolean isPrintOutput) {
        // get ("Test/HelloWorld/Jython/-/message_text")  
        _storeFunctions ("get", keysString, "", isPrintOutput);
        return;
    }
    
    private void put (String keysString, String valueString, 
                      boolean isPrintOutput) {
        // put ("Test/HelloWorld/Jython/-/message_text", "Hello World")  
        _storeFunctions ("put", keysString, valueString, isPrintOutput);
        return;
    }
    
    private void putIfPresent (String keysString, String valueString, 
                      boolean isPrintOutput) {
        // putIfPresent ("Test/HelloWorld/Jython/-/message_text", "Hello World")  
        _storeFunctions ("putIfPresent", keysString, valueString, isPrintOutput);
        return;
    }
    
    private void putIfAbsent (String keysString, String valueString, 
                               boolean isPrintOutput) {
        // putIfAbsent ("Test/HelloWorld/Jython/-/message_text", "Hello World")  
        _storeFunctions ("putIfAbsent", keysString, valueString, isPrintOutput);
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
}
