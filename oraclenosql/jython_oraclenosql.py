# Jython script to manipulate data in Oracle NoSQL databases, community edition.
import sys
sys.path.append('/opt/kv-1.2.123/lib/kvstore-1.2.123.jar')
sys.path.append('/opt/kv-1.2.123/lib/je.jar')
import jarray
import inspect
from oracle.kv import KVStore
from oracle.kv import KVStoreConfig
from oracle.kv import KVStoreFactory
from oracle.kv import Key
from oracle.kv import Value
from oracle.kv import ValueVersion
from oracle.kv import Direction
from com.sleepycat.persist import EntityStore
from java.util import ArrayList
from java.util import List
from java.util import Iterator

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
    if not isinstance(storeName, str):
        print ("ERROR: Please enter a String as the name of the store.")
        return
    if not isinstance (connectionString, str):
        print ("ERROR: Please enter a String as the connections string.")
        print ("e.g. connect (\"mystore\",\"localhost:5000\")")
        return        
    if (storeName == ""): 
        storeName = "mystore"
    if (connectionString == ""): 
        connectionString = "localhost:5000"
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
        errorMessage = "ERROR: Define your store connection first. \n"
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

def storeIterator(keysString):
    # This only works for iterating over major components.
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
            element = iterator.next()
            key = element.getKey().toString()
            value = element.getValue().getValue().tostring()
            print (key + ", "  + value)
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

def version():
    print ("0.1.2")

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
    put("MyTest/MComp2/-/mComp1/mComp2","Johannes Laeufer")
    _evalPositiveMessage()
    get("MyTest/MComp2/-/mComp1/mComp2")
    _evalPositiveMessage()
    delete("MyTest/MComp2/-/mComp1/mComp2")
    _evalPositiveMessage()
    countAll()
    _evalPositiveMessage()
    putIfAbsent("MyTest/MComp2/-/mComp1/mComp2","Juanito el Caminante")
    _evalPositiveMessage()
    putIfPresent("MyTest/MComp2/-/mComp1/mComp2","Corralejo")
    _evalPositiveMessage()
    storeIterator("MyTest/MComp2")
    _evalPositiveMessage()
    multiDelete("MyTest/MComp2")
    _evalPositiveMessage()  
    print (str(nFunctionsPassedTest) + " functions passed out of " + \
        str(nFunctionsTested))
    nFunctionsPassedTest = 0
    nFunctionsTested = 0    
    return
