(import '(oracle.kv KVStore KVStoreConfig KVStoreFactory))
(import '(oracle.kv Key Value ValueVersion Direction))
(import '(java.util ArrayList))

; Define functions using defn. The first word after defn is 
; the name of the function.
; It seems to be that functions have to be defined sequentially first
; in the file and only then they can be called. 
(defn connect [storeName connectionString]
    (println "Connecting.")
    ; To create an array: into-array.
    (def hosts (into-array [connectionString]))
    ; Error handling.
    ; prn is "print".
    (try 
        (def kVStoreConfig (new KVStoreConfig storeName hosts))
        ; store = KVStoreFactory.getStore(kVStoreConfig)
        (def store (. KVStoreFactory getStore kVStoreConfig))
    (catch Exception ex 
        (prn (.toString ex)))
    (finally (prn "Connected to KV store."))
    ) ; End of try-catch-finally block.
) ; End of defn connect.

(defn _prepareKey [keysString]
    ; e.g. keysString = "Test/HelloWorld/Java/-/message_text"
    (def majorComponents (new ArrayList))
    (def minorComponents (new ArrayList))
    (def keysArray (.split keysString "/"))
    (def isMajor (boolean true))
    ; alength: array length.
    (def keysArrayLength (alength keysArray))
    (loop [i 0]
        (when (< i keysArrayLength)
            ; (aget array idx): Value of array at index idx. 
            (if (= (aget keysArray i) "-")
                (def isMajor false))
            (if (boolean isMajor)
                (.add majorComponents (aget keysArray i))
                ; not= means !=.
                (if (not= (aget keysArray i) "-")
                    (.add minorComponents (aget keysArray i))))
            ; test: (println "i:" i)
            (recur (inc i))))
    (def majorComponentsLength (.size majorComponents))
    (def minorComponentsLength (.size minorComponents))
    (if (and (> majorComponentsLength 0) (> minorComponentsLength 0))
        (def myKey (. Key createKey majorComponents minorComponents))
        (if (and (> majorComponentsLength 0) (<= minorComponentsLength 0))
            (def myKey (. Key createKey majorComponents))
            (def errorMessage "ERROR: The String could not be transformed to a Key.")))
) ; End of defn _prepareKey.

(defn _convert_value_to_string [value_version]
    (do
        (def value (.getValue value_version))
        (def valueByteArray (.getValue value))
        (def myValueString (new String valueByteArray))
    )
)

; if syntax: if (expr) (if_expr_true_do_this) (if_expr_false_do_that)
(defn _storeFunctions [what, keysString, valueString]
    (_prepareKey keysString)
    (if (= myKey nil)
        (def errorMessage "ERROR: The Key is nil.")
        (case what
            "get" (do
                  (def value_version (.get store myKey))
                  (if (not= value_version nil) (_convert_value_to_string value_version)))
            "put" (do
                  (def myValue (. Value createValue (.getBytes valueString)))
                  (def value_version (.put store myKey myValue)))
            "delete" (do
                     (def delete_result (.delete store myKey))
                     (prn delete_result))
        )
    )
) ; End of defn _storeFunctions.

(defn count_all []
    (try 
        (def direction_unordered Direction/UNORDERED)
        (def iterator (.storeKeysIterator store direction_unordered 0))
        (loop [i 0]
            (when (.hasNext iterator)
                (def nRecords (+ i 1))
                (.next iterator)           
            (recur (inc i))))
        (prn (str "Total number of Records: " nRecords))
    (catch Exception ex 
        (def errorMessage (.toString ex))
        (prn (str "ERROR in count_all: " errorMessage)))
    (finally ())
    ) ; End of try-catch-finally block.
) ; End of defn test.

; test.
; clojure already has a test function: #'clojure.core/test
(defn testStore []
    (_storeFunctions "put" "MyTest/HelloWorld/-/message_text" "Juanito el Caminante")
    (_storeFunctions "get" "MyTest/HelloWorld/-/message_text" "")
    (println myValueString)
    (_storeFunctions "delete" "MyTest/HelloWorld/-/message_text" "")
    (count_all)
) ; End of defn test.

; Define global variables using def.
(def errorMessage "")
(def storeName "mystore")
(def connectionString "localhost:5000")
(def store)
(println "Starting.")
(connect storeName connectionString)
(testStore)



