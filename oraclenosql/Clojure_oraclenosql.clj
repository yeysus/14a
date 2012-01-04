(import '(oracle.kv KVStore KVStoreConfig KVStoreFactory))
(import '(oracle.kv Key Value ValueVersion Direction))
(import '(java.util ArrayList))

; Define functions using defn. The first word after defn is the name of the function.
; It seems to be that functions have to be defined above in the file and then they can be called. 
(defn connect [storeName connectionString]
    (println "Connecting.")
    ; To create an array: into-array.
    (def hosts (into-array [connectionString]))
    ; Error handling.
    ; prn is "print".
    (try 
        ((def kvStoreConfig (new KVStoreConfig storeName hosts))
        ; store = KVStoreFactory.getStore(kVStoreConfig)
        ;(def kVStoreFactory (new KVStoreFactory))
        ;(. KVStoreFactory getStore kVStoreConfig)
        )
    (catch Exception ex 
        (prn (.toString ex)))
    (finally (prn "in fin"))
    ) ; End of try-catch-finally block.
) ; End of defn connect.

; Define global variables using def.
(def errorMessage "")
(def storeName "mystore")
(def connectionString "localhost:5000")
(def store)
(println "Starting.")
(connect storeName connectionString)


