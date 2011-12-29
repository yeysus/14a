# Jython file.
import sys
sys.path.append('/opt/kv-1.2.123/lib/kvstore-1.2.123.jar')
sys.path.append('/opt/kv-1.2.123/lib/je.jar')
import jarray
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

def _validateConnectionString (connectionString):
    # connectionString should be string:integer.
    global errorMessage
    errorMessage = ""
    myArray = connectionString.split (":")
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

def connect (storeName, connectionString):
    # Catch a java exception.
    # http://stackoverflow.com/questions/2045786/how-to-catch-an-exception
    #     -in-python-and-get-a-reference-to-the-exception-withou
    if not isinstance (storeName, str):
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
    global errorMessage
    errorMessage = _validateConnectionString (connectionString)
    if (errorMessage != ""):
        print errorMessage
        errorMessage = ""
        return
    hosts = [connectionString]
    global store
    try:
        kVStoreConfig = KVStoreConfig (storeName, hosts)
        store = KVStoreFactory.getStore (kVStoreConfig)
        message = "Connected to the Oracle NoSQL store: \"" + storeName + "\"."
        print message
    except:
        instance = sys.exc_info()[1]
        errorMessage = "Error connecting to the store: " + str(instance)
        print errorMessage
        errorMessage = ""
    return

def _checkStore ():
    global errorMessage
    errorMessage = ""  
    try:
        global store
        store
    except:
        errorMessage = "ERROR: Define your store connection first. \n"
        errorMessage += "Type: \n"
        errorMessage += "connect (\"Store_Name\", \"Connection_String\")\n."
        errorMessage += "e.g. connect (\"mystore\",\"localhost:5000\")"
    return errorMessage

def _prepareKey (keysString):
    # e.g. keysString = "Test/HelloWorld/Java/-/message_text"
    # myKey contains either an error message or a Key.
    global myKey
    majorComponents = ArrayList()
    minorComponents = ArrayList()
    global errorMessage
    if not isinstance (keysString, str):
        errorMessage = "ERROR: Please enter a String as Key."
        return
    keysArray = keysString.split ("/")
    isMajor = True
    for i in range (0, len(keysArray)):
        if (keysArray [i] == "-"):
            isMajor = False
        if (isMajor):
            majorComponents.add (keysArray [i])
        else:
            if (keysArray [i] != "-"):
                minorComponents.add (keysArray [i])
    if ((len (majorComponents) > 0) & (len (minorComponents) > 0)):
        myKey = Key.createKey (majorComponents, minorComponents)
    elif ((len (majorComponents) > 0) & (len (minorComponents) <= 0)):
        myKey = Key.createKey (majorComponents)
    else:
        errorMessage = "ERROR: The String could not be transformed to a Key."
        return        
    return myKey   

def get (keysString):
    # e.g. get("Test/HelloWorld/Java/-/message_text")
    global myKey
    myKey = _prepareKey (keysString)
    global errorMessage
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore ()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    valueVersion = store.get (myKey)
    if (valueVersion is None):
        print ("ERROR: The Key \"" + keysString + "\" was not found.")
    else:
        myValue = valueVersion.getValue ().getValue ().tostring ()
        print (myValue)
    return

def _putFamily (keysString, valueString):
    # Use jarray to convert a String to a Java Bytes Array (String.getBytes()).
    global errorMessage
    global myKey
    global myValue
    myKey = _prepareKey (keysString)
    if (errorMessage != ""):
        return
    if not isinstance (valueString, str):
        message = "ERROR: Please enter a String as Value."
        print (message)
        return
    errorMessage = _checkStore ()
    if (errorMessage != ""):
        return
    myValue = Value.createValue (jarray.array (valueString, 'b'))
    return

def put (keysString, valueString):
    # Usage: on a single line,
    # put ("Test/HelloWorld/Jython/-/message_text", "Hello World")           
    # Use jarray to convert a String to a Java Bytes Array (String.getBytes()).
    _putFamily (keysString, valueString)
    global errorMessage
    global myKey
    global myValue
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    store.put (myKey, myValue)
    return

def delete (keysString):
    # e.g. delete ("Test/HelloWorld/Java/-/message_text")
    global myKey
    myKey = _prepareKey (keysString)
    global errorMessage
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore ()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    store.delete (myKey)
    return

def storeIterator (keysString):
    # This only works for iterating over major components.
    # Usage: storeIterator ("Test/HelloWorld")
    global errorMessage
    global myKey
    global myValue
    myKey = _prepareKey (keysString)
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    errorMessage = _checkStore ()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return
    iterator = store.storeIterator (Direction.UNORDERED, 0, myKey, None, None)    
    while (iterator.hasNext ()):
        value = iterator.next ().getValue ().getValue ().tostring ()
        print (value)
    return

def countAll ():
    global errorMessage
    errorMessage = _checkStore ()
    if (errorMessage != ""):
        print (errorMessage)
        errorMessage = ""
        return 
    iterator = store.storeKeysIterator (Direction.UNORDERED, 0)
    i = 0
    while (iterator.hasNext ()):
        i = i + 1
        iterator.next ()            
    print ("Total number of Records: " + str(i))
