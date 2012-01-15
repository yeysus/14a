Scripts to manipulate String data of an Oracle NoSQL database (community edition). Languages: Jython, Java, JRuby, Clojure, Groovy, Rhino.

### Requisites

1.  Oracle NoSQL community edition, version 1.2.123, up & running.
2.  For the Jython script: Jython 2.5.2.
3.  For Java: Java 1.6.0_30.
4.  For the JRuby script: JRuby 1.6.5.1.
5.  For Clojure: Clojure 1.3.0.
6.  Groovy: 1.8.5.
7.  Rhino: 1.7R3.

    Oracle NoSQL Database, Jython, Java, JRuby, Clojure, Groovy, are installed on the SAME machine.

    Tested on a CentOS 5.7 virtual machine.

### Usage (Jython)

1.  Modify the first lines of the file jython_oraclenosql.py to reflect
   the location of Oracle's jar files, kvstore-xxx.jar and je.jar.
2.  On a command prompt, type

    /absolute/path/jython -i /absolute/path/Jython_oraclenosql.py
 
    Or, to properly display non-ascii characters on the console:

    /absolute/path/jython -C iso-8859-1 -i /absolute/path/Jython_oraclenosql.py

3.  Assuming above created Jython's console and 
   preloaded jython_oraclenosql.py, the functions that can be called are:

    * connect("oracle_store_name", "host:port")
    * countAll()
    * version()
    * test("oracle_store_name", "host:port")
    * putIfPresent("MajorComponent1/MajorComponent2/-/MinorComponent1","Value")
    * putIfAbsent("MajorComponent1/MajorComponent2/-/MinorComponent1","Value")   
    * put("MajorComponent1/MajorComponent2/-/MinorComponent1","Value")
    * get("MajorComponent1/MajorComponent2/-/MinorComponent1")
    * delete("MajorComponent1/MajorComponent2/-/MinorComponent1")
    * multiDelete("MajorComponent1")
    * multiGet("MajorComponent1/MajorComponent2")
    * storeIterator ("MajorComponent1")

### Usage (Java)
1.  Navigate to the directory where the file Java_oraclenosql.java resides.
2.  javac -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar Java_oraclenosql.java.
3.  java -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar Java_oraclenosql.


    The arguments are not tested yet, the program runs the test function which calls put, get, storeIterator, countAll.

### Usage (JRuby)
1.  Modify the last 2 lines of the file Jruby_oraclenosql.rb to suit the environment (store name, host, port, key).
2.  Run: "/opt/jruby/bin/jruby /path/to/Jruby_oraclenosql.rb"

    "connect", "put", "get", "countAll", "test" (simply tests all other functions), are working.

### Usage (Clojure)
Uwe, das ist fuer Dich.

1.  Modify the last lines of the file Clojure_oraclenosql.clj to reflect the environment.
2.  Modify the script clj to reflect the classpath.
3.  Navigate to the folder containing both scripts, "clj" and "Clojure_oraclenosql.clj".
4.  Run with "./clj Clojure_oraclenosql.clj". It connects to the store, writes and reads one value.

### Usage (Groovy)
1.  Modify the top lines of the script with the store name, host, port, encoding.
2.  Navigate to the directory where the file Groovy_oraclenosql.groovy is.
3.  Run with "groovy -cp /opt/kv-1.2.123/lib/kvclient-1.2.123.jar Groovy_oraclenosql.groovy"

    It runs some automatic tests (connect, put, get, delete, storeIterator).
    
### Usage (Rhino).
1.  Modify the last lines of the script Rhino_oraclenosql.js with the store name, host, port.
2.  Navigate to the directory where the file Rhino_oraclenosql.js is.
3.  Run with java -cp .:/opt/kv-1.2.123/lib/kvclient-1.2.123.jar:/opt/rhino1_7R3/js.jar org.mozilla.javascript.tools.shell.Main Rhino_oraclenosql.js

    It runs an automatic test (connect, get).