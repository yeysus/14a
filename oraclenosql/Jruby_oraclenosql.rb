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
        positiveMessage = "connect: passed"
    rescue Exception => ex
        errorMessage = "ERROR: Connection to the store: #{ex}"
        puts errorMessage
        errorMessage = ""
    end
    return
end

def _prepareKey(keysString)
    # e.g. keysString = "Test/HelloWorld/Java/-/message_text"
    # myKey contains either an error message or a Key.
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
        errorMessage = "ERROR: The String could not be transformed to a Key."
        return
    end
    return myKey
end

def get(keysString)
    myKey = _prepareKey(keysString)
    valueVersion = $store.get(myKey)
    valueVersionValueBytes = valueVersion.getValue().getValue()
    myValue = String.from_java_bytes valueVersionValueBytes
    puts myValue
end

# Ruby's global variables start with $.
$errorMessage = ''
$store
puts 'Hallo'
connect('mystore', 'localhost:5000')
get('Test/HelloWorld/Java/-/message_text')

