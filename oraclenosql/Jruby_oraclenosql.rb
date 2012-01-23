require '/opt/kv-1.2.123/lib/kvclient-1.2.123.jar'
require 'java'

java_import 'oracle.kv.KVStore'
java_import 'oracle.kv.KVStoreConfig'
java_import 'oracle.kv.KVStoreFactory'
java_import 'oracle.kv.Key'
java_import 'oracle.kv.Value'
java_import 'oracle.kv.ValueVersion'
java_import 'oracle.kv.Direction'
java_import 'java.util.ArrayList'

def connect(storeName, connectionString)
    # .to_java converts a Ruby array to a Java array.
    # (:string) specifies the type, otherwise it would be an
    #           array of objects.
    hosts = [connectionString].to_java(:string)
    begin
        # Use the keyword 'new' for constructors.
        kVStoreConfig = KVStoreConfig.new(storeName, hosts)
        $store = KVStoreFactory.getStore(kVStoreConfig)
        message = "Connected to the Oracle NoSQL store: \"" + storeName + "\"."
        puts message
        $positiveMessage = "connect: passed"
    rescue Exception => ex
        $errorMessage = "ERROR: Connection to the store: #{ex}"
        puts $errorMessage
        $errorMessage = ""
    end
    return
end

def _prepareKey(keysString)
    # e.g. keysString = "Test/HelloWorld/Java/-/message_text"
    majorComponents = ArrayList.new()
    minorComponents = ArrayList.new()
    keysArray = keysString.split("/")
    isMajor = true
    for i in 0..keysArray.length - 1
        if (keysArray [i] == "-")
            isMajor = false
        end
        if (isMajor)
            majorComponents.add(keysArray [i])
        else
            if (keysArray [i] != "-")
                minorComponents.add(keysArray [i])
            end
        end
    end
    if ((majorComponents.length > 0) & (minorComponents.length > 0))
        myKey = Key.createKey(majorComponents, minorComponents)
    elsif ((majorComponents.length > 0) & (minorComponents.length <= 0))
        myKey = Key.createKey(majorComponents)
    else
        $errorMessage = "ERROR: The String could not be transformed into a Key."
        return
    end
    return myKey
end

def _storeFunctions(what, keysString, valueString)
    myKey = _prepareKey(keysString)
    return if ($errorMessage != "")
    begin
        case what
        when "put"
            myValue = Value.createValue(valueString.to_java_bytes)
            $store.put(myKey, myValue)
        when "get"
            valueVersion = $store.get(myKey)
            if (!valueVersion.nil?)
                valueVersionValueBytes = valueVersion.getValue().getValue()
            else
                puts valueVersion
            end
            myValue = String.from_java_bytes valueVersionValueBytes
            puts myValue
        when "delete"
            myValue = $store.delete(myKey)
            puts myValue
        end
        $positiveMessage = what + ": passed"
    rescue Exception => ex
        $errorMessage = "ERROR: store operation: #{ex}"
        print $errorMessage
        $errorMessage = ""
        return
    end
    return
end

def countAll()
    begin
        # To reference a constant, use ::
        iterator = $store.storeKeysIterator(Direction::UNORDERED, 0)
        i = 0
        while (iterator.hasNext())
            i = i + 1
            iterator.next()
        end
        puts ("Total number of Records: " + i.to_s())
        $positiveMessage = "countAll: passed"
    rescue Exception => ex
        $errorMessage = "ERROR: countAll(): #{ex}"
        puts $errorMessage
        $errorMessage = ""
        return
    end
    return
end

def put(keysString, valueString)
    _storeFunctions("put", keysString, valueString)
    return
end

def get(keysString)
    _storeFunctions("get", keysString, "")
    return
end

def delete(keysString)
    _storeFunctions("delete", keysString, "")
    return
end

def _evalPositiveMessage(what)
    if ($positiveMessage != "")
        puts ($positiveMessage)
        $nFunctionsPassedTest = $nFunctionsPassedTest + 1
    else
        puts(what + ": NOT PASSED")
    end
    $positiveMessage = ""
    $nFunctionsTested = $nFunctionsTested + 1
    return
end

def test(store_name, connection_string)
    connect(store_name, connection_string)
    _evalPositiveMessage("connect")
    countAll()
    _evalPositiveMessage("countAll")
    put("MyTest/MComp2/-/mComp1/mComp2","Johannes Läufer")
    _evalPositiveMessage("put")
    get("MyTest/MComp2/-/mComp1/mComp2")
    _evalPositiveMessage("get")
    delete("MyTest/MComp2/-/mComp1/mComp2")
    _evalPositiveMessage("delete")
    countAll()
    
    puts ($nFunctionsPassedTest.to_s() + " functions passed out of " + 
          $nFunctionsTested.to_s())
end

# Ruby's global variables start with $.
$errorMessage = ''
$positiveMessage = ''
$nFunctionsPassedTest = 0
$nFunctionsTested = 0
$myKey
$myValue
$store
storeName = 'mystore'
connectionString = 'localhost:5000'
test(storeName, connectionString)


