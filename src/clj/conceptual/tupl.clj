(ns conceptual.tupl
  (:refer-clojure :exclude [load])
  (:require ;;[conceptual.int-sets :as int-sets]
            [conceptual.arrays :as arrays]
            [conceptual.core :as core])
  (:import [conceptual.core DB TuplDB]
           [org.cojen.tupl Cursor Database DatabaseConfig DurabilityMode Index Transaction]
           [clojure.lang Keyword]))

;; TODO pull out
(def ^{:dynamic true :tag String} *default-db-dir* "./tupl-db/")
(def ^{:dynamic true :tag clojure.lang.Keyword} *default-identity* :default)

(def BOGUS Transaction/BOGUS)

(def ^{:dynamic true :tag Index} *index* nil)
(def ^{:dynamic true :tag Cursor} *cursor* nil)
(def ^{:dynamic true :tag Transaction} *transaction* nil)

(def indexes (atom {}))

(def dbs (atom {}))

(def ^{:dynamic true :tag Database} *db* nil)

(def ^:dynamic *initialized?* (atom false))

(defn ^:private nsname
  "Given a keyword return the namespace/name string representation of the keyword.
  Not sure why this isn't in clojure core or at least a method of Keyword."
  [^clojure.lang.Keyword k] (str (.sym k)))

;; TODO do more here
(defn db-config [path]
  (doto (DatabaseConfig.)
    (.baseFilePath path)
    (.maxCacheSize 100000000)
    (.durabilityMode DurabilityMode/NO_REDO)))

(defn db-base-file-path [^Keyword k]
  (str *default-db-dir* (nsname k)))

(defn open-db
  ([] (open-db *default-identity*))
  ([^Keyword k] (Database/open (db-config (db-base-file-path k)))))

(defn db
  "Given a keyword database name opens that database if it exists,
  otherwise creates that database. Returns the default database given no arguments.
  If it does not exist the default database is created. If given a database
  instance returns that database instance."
  ([] (db *default-identity*))
  ([k]
   (cond
    (instance? Database k) k
    (keyword? k)
    (if-let [-db (@dbs k)]
      -db
      (let [new-db (open-db k)]
        (swap! dbs assoc k new-db)
        new-db)))))

(defn- init-0! []
  (alter-var-root
     #'*db*
     (constantly
      (if (.isBound #'*db*)
        (do (when-not (nil? *db*)
              (.close *db*))
            (db))
        (db)))))

(defn init!
  ([] (init! {}))
  ([{:keys [default-db-dir default-identity]
     :or {default-db-dir *default-db-dir*
          default-identity *default-identity*}}]
   (when-not (= *default-db-dir* default-db-dir)
     (alter-var-root #'*default-db-dir* (constantly default-db-dir)))
   (when-not (= *default-identity* default-identity)
     (alter-var-root #'*default-identity* (constantly default-identity)))
   (when-not @*initialized?*
     (init-0!)
     (reset! *initialized?* true))))

(defn re-init! []
  (init-0!)
  (reset! *initialized?* true))

(defn empty-db
  ([] (TuplDB/clone (core/empty-db)))
  ([^Keyword k] (TuplDB/clone (core/empty-db k))))

(defn existing-db
  ([] (existing-db *default-identity*))
  ([^Keyword k] (TuplDB. k)))

(defn clone-db! [^DB db]
  (reset! (core/db-atom db) (TuplDB/clone db)))

(defn clone-current-db! []
  (clone-db! @core/*db*))

(defn shutdown
  "Calls shutdown on the database."
  [] (Thread.
      (fn []
        (println "Shutting Down...")
        (doseq [db (vals @dbs)]
          (.shutdown ^Database db)))))

;;(.addShutdownHook (Runtime/getRuntime) (shutdown))

(defn- open-index
  ([k] (open-index *db* k))
  ([^Database db ^Keyword k]
   (when-let [idx (.openIndex db ^String (nsname k))]
     (swap! indexes assoc k idx)
     idx)))

(defn index
  "Given a string or keyword name opens that index, otherwise
  creates that index. Uses the default database if not specified.
  If given the integer id of an index will return the index instance.
  If given an index instance returns that index instance."
  ([k] (index *db* k))
  ([-db k]
   (let [^Database db0 (db -db)]
     (cond
      (instance? Index k) k
      (instance? Long k) (.indexById db0 ^long k)
      (or (keyword? k)
          (string? k))
      (if-let [^Index idx (@indexes (keyword k))]
        (if (.isClosed idx)
          (open-index k)
          idx)
        (when-let [idx (open-index db0 k)]
          (swap! indexes assoc k idx)
          idx))))))

(defn cursor
  ([idx] (cursor *db* idx))
  ([^Database db idx] (.newCursor ^Index (index db idx) nil)))

(defn transaction
  "Returns a new Transaction with the given durability mode,
  uses the default durability mode if none is specified"
  ([^Database db] (.newTransaction db))
  ([^Database db mode] (.newTransaction db mode)))

(defn with-db* [^Keyword db-key f]
  (let [^Database d (db db-key)]
    (with-bindings* {#'*db* d} f)))

(defmacro with-db
  "Evaluates body in the context of a specified database. The binding
  provides the the database for the evaluation of the body."
  [^Database db & forms]
  `(with-db* ~db (fn [] ~@forms)))

(defn with-index* [idx f]
  (let [^Index idx (index idx)]
    (with-bindings* {#'*index* idx} f)))

(defmacro with-index
  "Evaluates body in the context of a specified index. The binding
  provides the the index for the evaluation of the body."
  [idx & forms]
  `(with-index* ~idx (fn [] ~@forms)))

(defn with-cursor*
  ([idx f]
   (let [^Cursor c (cursor idx)]
     (try
       (with-bindings* {#'*cursor* c} f)
       (finally (.reset c))))))

(defmacro with-cursor
  "Evaluates body in the context of a specified cursor. The binding
  provides the the cursor for the evaluation of the body."
  ([idx & forms]
   `(with-cursor* ~idx (fn [] ~@forms))))

;; TODO needs more work - dont use
(defn with-transaction*
  ([^Database db f]
   (let [^Transaction t (transaction db)]
     (try
       (with-bindings* {#'*transaction* t} f)
       (finally (.commit t))))))

(defmacro with-transaction
  ([idx & forms]
   `(with-transaction* ~idx (fn [] ~@forms))))

(defn delete-0!
  "Unconditionally removes the entry associated with the given key."
  ([^bytes k] (.delete *index* nil k))
  ([^Index idx ^bytes k] (.delete idx nil k))
  ([^Index idx ^Transaction t ^bytes k] (.delete idx t k)))

(defn delete!
  "Unconditionally removes the entry associated with the given key."
  ([k] (delete! *index* k))
  ([idx k] (delete-0! (index idx)
                      (arrays/ensure-bytes k))))

(defn exchange-0!
  "Unconditionally associates a value with the given key, returning the previous value."
  ([^bytes k ^bytes v] (.exchange *index* nil k v))
  ([^Index idx ^bytes k ^bytes v] (.exchange idx nil k v))
  ([^Index idx ^Transaction t ^bytes k ^bytes v] (.exchange idx t k v)))

(defn exchange!
  "Unconditionally associates a value with the given key, returning the previous value."
  ([k v] (exchange! *index* k v))
  ([idx k v] (exchange-0! (index idx)
                          (arrays/ensure-bytes k)
                          (arrays/ensure-bytes v))))

(defn insert-0!
  "Associates a value with the given key, unless a corresponding value already exists."
  ([^bytes k ^bytes v] (.insert *index* nil k v))
  ([^Index idx ^bytes k ^bytes v] (.insert idx nil k v))
  ([^Index idx ^Transaction t ^bytes k ^bytes v] (.insert idx t k v)))

(defn insert!
  "Associates a value with the given key, unless a corresponding value already exists."
  ([k v] (insert! *index* k v))
  ([idx k v] (insert-0! (index idx)
                        (arrays/ensure-bytes k)
                        (arrays/ensure-bytes v))))

(defn replace-0!
  "Associates a value with the given key, but only if a corresponding value already exists."
  ([^bytes k ^bytes v] (.replace *index* nil k v))
  ([^Index idx ^bytes k ^bytes v] (.replace idx nil k v))
  ([^Index idx ^Transaction t ^bytes k ^bytes v] (.replace idx t k v)))

(defn replace!
  "Associates a value with the given key, but only if a corresponding value already exists."
  ([k v] (replace! *index* k v))
  ([idx k v] (replace-0! (index idx)
                         (arrays/ensure-bytes k)
                         (arrays/ensure-bytes v))))

(defn store-0!
  "Unconditionally associates a value with the given key."
  ([^bytes k ^bytes v] (.store *index* nil k v))
  ([^Index idx ^bytes k ^bytes v] (.store idx nil k v))
  ([^Index idx ^Transaction t ^bytes k ^bytes v] (.store idx t k v)))

(defn store!
  "Unconditionally associates a value with the given key."
  ([k v] (store! *index* k v))
  ([idx k v] (store-0! (index idx)
                       BOGUS
                       (arrays/ensure-bytes k)
                       (arrays/ensure-bytes v))))

(defn load-0
  "Returns a copy of the value for the given key, or null if no matching entry exists."
  ([^bytes k] (load-0 *index* k))
  ([^Index idx ^bytes k] (load-0 idx nil k))
  ([^Index idx ^Transaction t ^bytes k] (.load idx t k)))

(defn load
  "Returns a copy of the value for the given key, or null if no matching entry exists."
  ([k] (load *index* k))
  ([idx k]
   (when-let [r (load-0 (index idx)
                      (arrays/ensure-bytes k))]
     (arrays/bytes-> r))))

(defn seek
  "Experimental function aimed at arriving at a uniform interface
  for the in-memory and durable indices. There is the additional
  level of indirection here with the index."
  ([idx k] (load idx k))
  ([db idx k]
   (with-db db
     (load idx k))))

(defn index-range-keys*
  "Eager seq over range of entries for a given index starting at start,
  or from beginning if start is nil, and ending before end,
  or through the end of the index if end is nil.
  Use inside with-cursor."
  ([idx start stop] (index-range-keys* *db* idx start stop))
  ([db idx start stop]
   (let [-idx (index db idx)
         ^Cursor cursor (cursor -idx)
         -start (when start (arrays/ensure-bytes start))
         -stop (when stop (arrays/ensure-bytes stop))]
     (try
       (if -start
         (.findGe cursor -start)
         (.first cursor))
       (loop [k (.key cursor)
              result []]
         (if k
           (do
             ;; this v was not used - bug?
             ;;let [v (.value cursor)]
             (if -stop
               (.nextLt cursor -stop)
               (.next cursor))
             (recur (.key cursor)
                    (conj result (arrays/bytes-> k))))
           result))
       (finally (.reset cursor))))))

(defn index-range-keys
  "Lazy seq over range of keys for a given index starting at start,
  and ending before end. Use inside with-cursor."
  ([start stop] (index-range-keys *cursor* start stop))
  ([^Cursor cursor start stop]
   (let [-start (when start (arrays/ensure-bytes start))
         -stop (when stop (arrays/ensure-bytes stop))]
      (if -start
        (.findGe cursor -start)
        (.first cursor))
      (letfn [(results []
                (when (nil? cursor)
                  (throw (IllegalStateException.
                          "you let the lazy-seq-out")))
                (when-let [k (.key cursor)]
                  (cons (arrays/bytes-> k)
                        (do (if -stop
                              (.nextLt cursor -stop)
                              (.next cursor))
                            (lazy-seq (results))))))]
        (results)))))

(defn index-range*
  "Eager seq over range of entries for a given index starting at start,
  or from beginning if start is nil, and ending before end,
  or through the end of the index if end is nil.
  Use inside with-cursor."
  ([idx start stop] (index-range* *db* idx start stop))
  ([db idx start stop]
   (let [-idx (index db idx)
         ^Cursor cursor (cursor -idx)
         -start (when start (arrays/ensure-bytes start))
         -stop (when stop (arrays/ensure-bytes stop))]
     (try
       (if -start
         (.findGe cursor -start)
         (.first cursor))
       (loop [k (.key cursor)
              result []]
         (if k
           (let [v (.value cursor)]
             (if -stop
               (.nextLt cursor -stop)
               (.next cursor))
             (recur (.key cursor)
                    (conj result [(arrays/bytes-> k)
                                  (arrays/bytes-> v)])))
           result))
       (finally (.reset cursor))))))

(defn index-range
  "Lazy seq over range of entries for a given index starting at start,
  or from beginning if start is nil, and ending before end,
  or through the end of the index if end is nil.
  Use inside with-cursor."
  ([start stop] (index-range *cursor* start stop))
  ([^Cursor cursor start stop]
   (let [-start (when start (arrays/ensure-bytes start))
         -stop (when stop (arrays/ensure-bytes stop))]
      (if -start
        (.findGe cursor -start)
        (.first cursor))
      (letfn [(results []
                (when (nil? cursor)
                  (throw (IllegalStateException.
                          "you let the lazy-seq-out")))
                (when-let [k (.key cursor)]
                  (cons [(arrays/bytes-> k)
                         (arrays/bytes-> (.value cursor))]
                        (do (if -stop
                              (.nextLt cursor -stop)
                              (.next cursor))
                            (lazy-seq (results))))))]
        (results)))))

(defn index-first
  "Grabs the first entry for a given index."
  ([] (index-first *cursor*))
  ([^Cursor cursor]
    (.first cursor)
    (vector (arrays/bytes-> (.key cursor))
           (arrays/bytes-> (.value cursor)))))

(defn index-last
  "Grabs the last entry for a given index."
  ([] (index-last *cursor*))
  ([^Cursor cursor]
    (.last cursor)
    (vector (arrays/bytes-> (.key cursor))
           (arrays/bytes-> (.value cursor)))))

(defn clear!
  "Clears all entries for a given index."
  ([idx] (clear! *db* idx))
  ([db idx]
   (let [^Index -idx (index db idx)
         keys-to-remove (with-db db
                          (with-cursor idx
                            (index-range-keys nil nil)))]
     (doseq [k keys-to-remove]
       (.delete -idx nil k)))))

(defn drop!
  "Drops a given index. If the index is not empty, first clears it."
  ([idx] (drop! *db* idx))
  ([db idx]
   (let [^Index -idx (index db idx)
         cnt (clear! db idx)]
     (.drop -idx)
     cnt)))

(defn index-size
  "Returns the number of entries in a given index."
  ([idx] (index-size *db* idx))
  ([db idx]
   (with-db db
     (with-cursor idx
       (->> (index-range-keys nil nil)
            (count))))))

(defn index-keys
  "Returns the number of entries in a given index."
  ([idx] (index-keys *db* idx))
  ([db idx]
   (with-db db
     (with-cursor idx
       (->> (index-range-keys nil nil)
            (into []))))))

(defn index-names
  ([] (index-names *db*))
  ([^Database db]
   (let [^Cursor c (.newCursor (.indexRegistryByName db) nil)]
     (.first c)
     (loop [c c
            result []]
       (if-let [b (.key c)]
         (recur (do (.next c) c) (conj result (String. b)))
         result)))))
