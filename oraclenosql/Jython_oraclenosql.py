# Jython script to manipulate data in Oracle NoSQL databases, community edition.
# Those were the days.
# -*- coding: iso-8859-1 -*-
import sys
import jarray
import array
import inspect
from java.util import ArrayList
from java.util import List
from java.util import Iterator
from java.util import SortedMap

def main():
    # Read arguments passed in the command line.
    # sys.argv[0] contains the name of the script file, e.g.
    # Jython_oraclenosql.py.
    # optparse is deprecated since Python 2.7, so better don't use Jython's 
    # equivalent.
    # As part of the import block, kvclient.jar has to be passed to the script
    # like:
    # sys.path.append('/opt/kv-1.2.123/lib/kvclient-1.2.123.jar')
    # I have absolutely no idea of how to append a jar file to the sy.path like
    # sys.path.append('/opt/kv-1.2.123/lib/kvclient-1.2.123.jar') in the 
    # command line, so I do it here with brute force:
    global storeName
    global connectionString
    global kvclientpath
    arglen = len(sys.argv)
    isTest = False
    if (arglen > 1):
        for i in range (1, arglen):
            myarg = (sys.argv[i]).lower()
            if (myarg == "-test"):
                isTest = True
                continue
            if (myarg.startswith("-storename")):
                myArray = myarg.split("=")
                storeName = myArray[1]   
                continue
            if (myarg.startswith("-connectionstring")):
                myArray = myarg.split("=")
                connectionString = myArray[1]
                continue
            if (myarg.startswith("-kvclientpath")):
                myArray = myarg.split("=")
                kvclientpath = myArray[1]
                sys.path.append(kvclientpath)
                continue
            if (sys.argv[i] == "help"):
                _printUsage() 
                break
    global KVStore
    global KVStoreConfig
    global KVStoreFactory
    global Key
    global Value
    global ValueVersion
    global Direction
    from oracle.kv import KVStore
    from oracle.kv import KVStoreConfig
    from oracle.kv import KVStoreFactory
    from oracle.kv import Key
    from oracle.kv import Value
    from oracle.kv import ValueVersion
    from oracle.kv import Direction
    if isTest:
        test(storeName, connectionString)

# Prints usage message and exit.
def _printUsage():
    print "Usage: "
    print "Interactive mode:"
    print "jython -i /absolute/path/Jython_oraclenosql.py arg1 arg2 arg3 arg4"
    print "Non-Interactive mode: "
    print "jython /absolute/path/Jython_oraclenosql.py arg1 arg2 arg3 arg4"
    print "Valid arguments and examples:"
    print "-kvclientpath=/opt/kv-1.2.123/lib/kvclient-1.2.123.jar"
    print "-storename=Name_of_the_store"
    print "-connectionstring=host_name:port"
    print "-test"
    print "help"
                
# Prints errorMessage, sets it to "" and returns.
def _printErrorMessage(myErrorMessage):
    global errorMessage
    errorMessage = myErrorMessage
    if (errorMessage != ""):
        print errorMessage
        errorMessage = ""
    return  

def _validateConnectionString(connectionString):
    # connectionString should be string:integer.
    global errorMessage
    errorMessage = ""
    myArray = connectionString.split(":")
    if (len(myArray) != 2):
        errorMessage = "ERROR: The connection string must include the host name \n"
        errorMessage += "and the port in the form host:port.\n"
        errorMessage += "e.g. connect(\"myhost\",\"localhost:5000\")"
        return errorMessage
    try:
        int (myArray[1])
    except ValueError:
        errorMessage = "ERROR: The port must be an Integer."
    return errorMessage

def connect(storeName, connectionString):
    # Catch a java exception.
    # http://stackoverflow.com/questions/2045786/how-to-catch-an-exception
    #     -in-python-and-get-a-reference-to-the-exception-withou
    global errorMessage
    global positiveMessage
    global store
    assert isinstance(storeName, str), "ERROR: Please enter a String as the name of the store."
    if not isinstance (connectionString, str):
        print ("ERROR: Please enter a String as the connections string.")
        print ("e.g. connect (\"mystore\",\"localhost:5000\")")
        return
    errorMessage = _validateConnectionString(connectionString)
    if (errorMessage != ""):
        print errorMessage
        errorMessage = ""
        return
    hosts = [connectionString]
    try:
        kVStoreConfig = KVStoreConfig(storeName, hosts)
        store = KVStoreFactory.getStore(kVStoreConfig)
        message = "Connected to the Oracle NoSQL store: \"" + storeName + "\"."
        print message
        positiveMessage = "connect: passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "ERROR: Connection to the store: " + str(instance)
        print errorMessage
        errorMessage = ""
    return

def _checkStore():
    global errorMessage
    errorMessage = ""  
    try:
        global store
        store
    except:
        errorMessage = "ERROR: Define the store connection first. \n"
        errorMessage += "Type: \n"
        errorMessage += "connect(\"Store_Name\", \"Connection_String\")\n."
        errorMessage += "e.g. connect(\"mystore\",\"localhost:5000\")"
    return errorMessage

def _prepareKey(keysString):
    # e.g. keysString = "Test/HelloWorld/Java/-/message_text"
    # myKey contains either an error message or a Key.
    global myKey
    majorComponents = ArrayList()
    minorComponents = ArrayList()
    global errorMessage
    if not isinstance (keysString, str):
        errorMessage = "ERROR: Please enter a String as Key."
        return
    keysArray = keysString.split("/")
    isMajor = True
    for i in range (0, len(keysArray)):
        if (keysArray [i] == "-"):
            isMajor = False
        if (isMajor):
            majorComponents.add(keysArray [i])
        else:
            if (keysArray [i] != "-"):
                minorComponents.add (keysArray [i])
    if ((len (majorComponents) > 0) & (len (minorComponents) > 0)):
        myKey = Key.createKey(majorComponents, minorComponents)
    elif ((len (majorComponents) > 0) & (len (minorComponents) <= 0)):
        myKey = Key.createKey(majorComponents)
    else:
        errorMessage = "ERROR: The String could not be transformed to a Key."
        return        
    return myKey

def get(keysString):
    # e.g. get("Test/HelloWorld/Java/-/message_text")
    what = inspect.stack()[0][3]
    valueString = ""
    _storeFunctions(what, keysString, valueString)
    return

def _storeFunctions(what, keysString, valueString):
    # Use jarray to convert a String to a Java Bytes Array(String.getBytes()).
    # store.delete(key) returns a bool.
    # store.get returns None or the value.
    global errorMessage
    global positiveMessage
    global myKey
    global myValue
    myKey = _prepareKey(keysString)
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    if not isinstance (valueString, str):
        message = "ERROR: Please enter a String as Value."
        print (message)
        return
    store_function = getattr (store, "%s" % what)
    try:
        if ((what == "delete") | (what == "get")):
            valueVersion = store_function(myKey)
            if isinstance(valueVersion, bool):
                print (valueVersion)
            elif (valueVersion is not None):
                myValue = valueVersion.getValue().getValue().tostring()
                print (myValue)
            else:
                print (valueVersion)
        else:
            myValue = Value.createValue(jarray.array(valueString, 'b'))
            store_function(myKey, myValue)
        positiveMessage = what + ": passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error in store operation: " + str(instance)
        print errorMessage
        errorMessage = ""
        return
    return

def put(keysString, valueString):
    # Usage: on a single line,
    # put("Test/HelloWorld/Jython/-/message_text", "Hello World") 
    what = inspect.stack()[0][3]  
    _storeFunctions(what, keysString, valueString)
    return

def putIfPresent(keysString, valueString):
    # Usage: on a single line,
    # putIfPresent("Test/HelloWorld/Jython/-/message_text", "Hello World")           
    what = inspect.stack()[0][3]  
    _storeFunctions(what, keysString, valueString)
    return

def putIfAbsent(keysString, valueString):
    # Usage: on a single line,
    # putIfAbsent("Test/HelloWorld/Jython/-/message_text", "Hello World")           
    what = inspect.stack()[0][3]  
    _storeFunctions(what, keysString, valueString)
    return

def delete(keysString):
    # e.g. delete("Test/HelloWorld/Java/-/message_text")
    what = inspect.stack()[0][3]
    valueString = ""
    _storeFunctions(what, keysString, valueString)
    return

def multiDelete(keysString):
    # To delete multiple records sharing the same major path components.
    # e.g. multiDelete("Test/HelloWorld/Java/")
    global errorMessage
    global positiveMessage
    global myKey
    myKey = _prepareKey(keysString)
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    try:
        store.multiDelete(myKey, None, None)
        positiveMessage = "multiDelete: passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error in multiDelete: " + str(instance)
        print errorMessage
        errorMessage = ""
        return        
    return

def multiGet(keysString):
    # To get multiple records sharing the same major path components.
    # e.g. multiGet("Test/HelloWorld/Java/")
    global errorMessage
    global positiveMessage
    global myKey
    myKey = _prepareKey(keysString)
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    try:
        result = store.multiGet(myKey, None, None)
        for myRecord in result.entrySet():
            myValue = myRecord.getValue().getValue().getValue().tostring()
            print(myValue)           
        positiveMessage = "multiGet: passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error in multiGet: " + str(instance)
        print errorMessage
        errorMessage = ""
        return        
    return

def storeIterator(keysString):
    # This only works for iterating over PARTIAL major components.
    # Usage: storeIterator("Test/HelloWorld")
    global errorMessage
    global positiveMessage
    global myKey
    myKey = _prepareKey(keysString)
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    try:
        iterator = store.storeIterator(Direction.UNORDERED, 0, myKey, None, None)
        while (iterator.hasNext()):
            keyValueVersion = iterator.next()
            key = keyValueVersion.getKey().toString()
            valueArray = keyValueVersion.getValue().getValue()
            valueArray.tostring()
            # no attr valueArray.toString()
            print (key + ", " + valueArray.tostring().decode("iso-8859-1"))
        positiveMessage = "storeIterator: passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error in storeIterator: " + str(instance)
        print errorMessage
        errorMessage = ""
        return            
    return

def countAll():
    global errorMessage
    global positiveMessage
    errorMessage = _checkStore()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return 
    try:
        iterator = store.storeKeysIterator(Direction.UNORDERED, 0)
        i = 0
        while (iterator.hasNext()):
            i = i + 1
            iterator.next()
        print ("Total number of Records: " + str(i))
        positiveMessage = "countAll: passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error in countAll(): " + str(instance)
        print errorMessage
        errorMessage = ""
        return
    return

def getAll():
    global errorMessage
    global positiveMessage
    errorMessage = _checkStore()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return 
    try:
        iterator = store.storeKeysIterator(Direction.UNORDERED, 0)
        while (iterator.hasNext()):
            element = iterator.next()
            valueString = store.get(element).getValue().getValue().tostring()
            print (element.toString())
        positiveMessage = "getAll: passed"
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error in getAll(): " + str(instance)
        print errorMessage
        errorMessage = ""
        return
    return

def version():
    print ("0.1.5")

def _evalPositiveMessage():
    global positiveMessage
    global nFunctionsPassedTest
    global nFunctionsTested
    if (positiveMessage is not ""):
        print (positiveMessage)
        nFunctionsPassedTest = nFunctionsPassedTest + 1
    else:
        print ("NOT PASSED")
    positiveMessage = ""
    nFunctionsTested = nFunctionsTested + 1
    return    

def test(storeName, connectionString):
    # Test all functions.
    global positiveMessage
    global nFunctionsPassedTest
    global nFunctionsTested
    nFunctionsPassedTest = 0
    nFunctionsTested = 0
    connect(storeName, connectionString)
    _evalPositiveMessage()
    countAll()
    _evalPositiveMessage()
    put("MyTest/MComp2/-/mComp1/mComp2","Johannes Läufer")
    _evalPositiveMessage()
    get("MyTest/MComp2/-/mComp1/mComp2")
    _evalPositiveMessage()
    putIfAbsent("MyTest/MComp2/-/mComp1/mComp3","Juanito el Caminante")
    _evalPositiveMessage()
    putIfPresent("MyTest/MComp2/-/mComp1/mComp2","Johannes Lufer 2")
    _evalPositiveMessage()
    getAll()
    _evalPositiveMessage()
    storeIterator("MyTest")
    _evalPositiveMessage()
    multiGet("MyTest/MComp2")
    _evalPositiveMessage()
    delete("MyTest/MComp2/-/mComp1/mComp2")
    _evalPositiveMessage()
    multiDelete("MyTest/MComp2")
    _evalPositiveMessage()  
    print (str(nFunctionsPassedTest) + " functions passed out of " + \
        str(nFunctionsTested))
    countAll()
    nFunctionsPassedTest = 0
    nFunctionsTested = 0    
    return

global storeName
global connectionString
global kvclientpath
global store
global myKey
global myValue
global errorMessage
global positiveMessage
global nFunctionsPassedTest
global nFunctionsTested
errorMessage = ""
positiveMessage = ""
nFunctionsPassedTest = 0
nFunctionsTested = 0
# Defaults.
storeName = "mystore"
connectionString = "localhost:5000"
kvclientpath = "/opt/kv-1.2.123/lib/kvclient-1.2.123.jar"
# Start.
main()
