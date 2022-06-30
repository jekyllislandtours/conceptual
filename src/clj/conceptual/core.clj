(ns conceptual.core
  (:require [conceptual.arrays :refer [int-array-class]]
            [conceptual.int-sets :as int-sets]
            [clojure.data.int-map :as int-map]
            [clojure.test]
            [clojure.pprint])
  (:import [conceptual.core DB DBMap IndexAggregator PersistentDB WritableDB]
           [clojure.lang IFn Keyword]))

;; name of default implicit db
(def ^:dynamic *default-identity* :default)

;; internal keys
(def ^{:private true :const true} -id (int DB/ID_ID))
(def ^{:private true :const true} -key (int DB/KEY_ID))
(def ^{:private true :const true} -type (int DB/TYPE_ID))
(def ^{:private true :const true} -property? (int DB/PROPERTY_TAG_ID))

(def ^:private default-max-id (int 3))

(def ^:private default-keys
  (let [ids (range (inc default-max-id))
        ids-array (int-array ids)]
    (into (int-map/int-map) (map #(list % ids-array) ids))))

;; these are they core values in the db
(def ^:private default-values
  (let [ids (range (inc default-max-id))
        obj-arrays [(object-array [-id :db/id Integer true])
                    (object-array [-key :db/key clojure.lang.Keyword true])
                    (object-array [-type :db/type Class true])
                    (object-array [-property? :db/property? Boolean true])]]
    (into (int-map/int-map) (map #(list %1 %2) ids obj-arrays))))

(def ^:private default-keyword->id
  {:db/id -id :db/key -key :db/type -type :db/property? -property?})

(defn ^:private empty-persistent-db
  ([] (empty-persistent-db *default-identity*))
  ([ident]
    (PersistentDB. ident
                   default-keyword->id
                   default-keys
                   default-values
                   default-max-id)))

(defn empty-db
  "Creates and empty persistent db."
  ([] (empty-persistent-db))
  ([^Keyword ident] (empty-persistent-db ident)))

;; store all of the dbs in this atom
(def ^:private dbs (atom {}))

(defn db-atom
  "Given a db instance returns the atom containing it."
  ([^DB -db] (@dbs (.getIdentity -db))))

(defn create-db!
  "Creates a default db."
  ([] (create-db! *default-identity*))
  ([^Keyword k] (create-db! k empty-db))
  ([^Keyword k ^IFn make-db]
   (let [new-db (make-db k)]
     (if-let [-atom (k @dbs)]
       (do
         (reset! -atom new-db)
         (swap! dbs assoc k -atom))
       (swap! dbs assoc k (atom new-db)))
     new-db)))

(defn db
  "Returns a database instance if it exists."
  ([] (db *default-identity*))
  ([^Keyword k]
   (when-let [a (k @dbs)] @a)))

(def ^{:dynamic true :tag DB} *db* (db-atom (create-db!)))

(def ^{:dynamic true :tag IndexAggregator} *aggr*)

;; TODO push this into a utils
(defn nsname
  "Given a keyword return the namespace/name string representation of the keyword.
  Not sure why this isn't in clojure core or at least a method of Keyword."
  [^Keyword k] (str (.sym k)))

(defn key->id
  "Given a keyword identity returns the id."
  ([^Keyword k] (key->id ^DB @*db* ^Keyword k))
  ([^DB db ^Keyword k] (.keywordToId ^DB db ^Keyword k)))

(defn keys->ids
  "Returns a sorted int array representing a collection of keywords."
  ([ks] (keys->ids @*db* ks))
  ([^DB db ks]
   (let [f (partial key->id ^DB db)]
     (->> (map f ks)
          (into #{})
          (sort)
          (int-array)))))

(defn key-ids
  "Returns the keys for an concept key."
  ([key] (key-ids @*db* key))
  ([^DB db key]
   (if (instance? Number key)
     (.getKeys ^DB db ^int key)
     (when (keyword? key)
       (.getKeys ^DB db ^int (key->id db key))))))

;; TODO: reconcile this with normalize-ids
(defn ordered-ids
  "Like keys->ids but does not eliminate duplicates or sort. This is useful
  for projections where the order of the projection matters."
  ([ks] (ordered-ids @*db* ks))
  ([db ks] (let [f (partial key->id ^DB db)] (int-array (map f ks)))))

(defn normalize-ids
  "Like keys->ids but does not eliminate duplicates or sort. This is useful
  for projections where the order of the projection matters."
  ([ks] (normalize-ids @*db* ks))
  ([db ks] (cond
            (instance? int-array-class ks) ks
            (or (vector? ks)
                (list? ks)
                (set? ks)) (let [f (partial key->id ^DB db)]
                             (int-array (map f ks))))))

(defn id->key
  "Given and id returns the key."
  ([id] (id->key @*db* ^int id))
  ([^DB db id] (.getValue ^DB db ^int id ^int -key)))

(defn ids->keys
  "Given a collection of ids returns a collection of keys."
  ([^ints ids] (ids->keys @*db* ids))
  ([^DB db ^ints ids]
   (let [f (partial id->key ^DB db)] (map f ids))))

(declare seek)

(defn insert-1!
  "Inserts an array of values given an array of keys. Must be a WritableDB"
  ([^ints ks #^Object vs]
   (insert-1! @*db* nil ks vs))
  ([^IndexAggregator aggr ^ints ks #^Object vs]
   (insert-1! @*db* aggr ks vs))
  ([^WritableDB db ^IndexAggregator aggr ^ints ks #^Object vs]
   {:pre [(clojure.test/is (not-any? nil? [ks vs]))]}
   ;;(println "insert-1!: " (seq ks) (seq vs))
   (reset! (db-atom db)
           (.insert ^WritableDB db
                    ^IndexAggregator aggr
                    ^ints ks
                    #^Object vs))
   ;;(println "insert-1!! " (id->key @*db* (get ks 0)) (->persistent-map (seek  @*db* (get ks 0))))
   ))

(defn insert!
  "Inserts into db. Must be a WritableDB."
  ([arg] (insert! @*db* nil arg))
  ([^IndexAggregator aggr arg] (insert! @*db* aggr arg))
  ([^WritableDB db ^IndexAggregator aggr arg]
   {:pre [(when-let [ks (keys arg)]
            (clojure.test/is (not-any? nil? (map (partial key->id ^DB db) ks))
                             (map vector ks (map (partial key->id ^DB db) ks))))]}
   (try
     (when-not (seek ^DB db (:db/key arg))
       (let [items (->> (seq arg)
                        (map #(vector (key->id ^DB db (first %)) (second %)))
                        (sort-by first <))
             ks (int-array (map first items))
             vs (object-array (map second items))]
         (insert-1! ^WritableDB db ^IndexAggregator aggr ^ints ks #^Object vs)))
     (catch Exception e
       (println (.getMessage e))
       (println (->> (seq arg)
                     (map #(vector (try (key->id ^DB db (first %))
                                        (catch Exception e
                                          (identity e)
                                          nil)) (second %)))))))))

(def ^:private db-entries
  [{:db/key :db/fn :db/type clojure.lang.IFn :db/property? true}
   {:db/key :db/dont-index :db/type Boolean :db/property? true}
   {:db/key :db/tag? :db/type Boolean :db/property? true}
   {:db/key :db/fn? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/relation? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/to-many-relation? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/to-one-relation? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/inverse-relation :db/type Integer :db/property? true :db/relation? true :db/to-one-relation? true}
   {:db/key :db/ids :db/type int-array-class :db/relation? true :db/to-many-relation? true :db/property? true}])

(defn update-0!
  "Lowest level update fn. Updates a single key/value in the concept.
   Expects an integer id, and an integer key."
  ([id k ^Object v]
   (update-0! @*db* nil ^int id ^int k v))
  ([^IndexAggregator aggr id k ^Object v]
   (update-0! @*db* aggr ^int id ^int k v))
  ([^WritableDB db ^IndexAggregator aggr id k ^Object v]
   {:pre [(clojure.test/is (not-any? nil? [id k]))]}
   (reset! (db-atom db) (.update ^DB db ^IndexAggregator aggr ^int id ^int k v))))

(defn update-1!
  "Updates an array of values given an array of keys."
  ([id ^ints ks #^Object vs]
   (update-1! @*db* nil id ks vs))
  ([^IndexAggregator aggr id ^ints ks #^Object vs]
   (update-1! @*db* aggr id ks vs))
  ([^WritableDB db ^IndexAggregator aggr id ^ints ks #^Object vs]
   {:pre [(clojure.test/is (not-any? nil? [id ks vs]))]}
   (let [new-db (.update ^WritableDB db
                         ^IndexAggregator aggr
                         ^int id
                         ^ints ks
                         #^Object vs)]
     (reset! (db-atom db) new-db))))

(defn update-2!
  ([^IndexAggregator aggr id ^ints ks #^Object vs]
   (update-2! @*db* aggr id ks vs))
  ([^WritableDB db ^IndexAggregator aggr id ^ints ks #^Object vs]
   (reset! (db-atom db) (.updateInline ^WritableDB db aggr id ks vs))))

;; clean up preconditions
(defn update!
  "Given a map which contains either a :db/id or a :db/key and value as well as the
  keys and values that need to added or updated, updates the concept."
  ([arg] (update! @*db* nil arg))
  ([^IndexAggregator aggr arg] (update! @*db* aggr arg))
  ([^WritableDB db ^IndexAggregator aggr arg]
   (if (:db/id arg)
     (update! db aggr (:db/id arg) (dissoc arg :db/id)) ;; dissoc :db/id
     (if (:db/key arg)
       (update! db aggr (key->id (:db/key arg)) arg)
       (RuntimeException. "update! requires an :db/id or :db/key in the argument."))))
  ([^WritableDB db ^IndexAggregator aggr id arg]
   {:pre [(and (clojure.test/is (not (nil? id)))
               (when-let [ks (keys arg)]
                 (when-let [ks-ids (map key->id ks)]
                   (clojure.test/is (not-any? nil? ks-ids)
                                    (map vector ks ks-ids)))))]}
   (let [key->id-fn (partial key->id db)
         items (sort-by first <
                        (map #(list (key->id-fn (first %1)) (second %1))
                             (seq (dissoc arg :db/id))))
         ^ints ks (int-array (map first items))
         #^Object vs (object-array (map second items))]
     (update-1! db aggr ^int id ks vs))))

;; THIS FN is garbage.... gotta fix!!!!
;; TODO break this up to allow updates on individual properties
;; This is probably going away
(defn index-ids
  ([] (index-ids @*db*))
  ([^WritableDB db]
   (->> (range (.count db))
        (map #(vector % (.getKeys db %)))
        (mapcat (fn [[id ks]] (map #(vector % id) ks)))
        (group-by first)
        (map #(vector (first %) (int-array (sort (into #{} (map second (second %)))))))
        (map (fn [[k v]] (update-0! k (key->id :db/ids) v)))
        (count))))

(defn prime-db!
  ([] (prime-db! *default-identity*))
  ([^Keyword k]
   (when-let [-db (db-atom (db k))]
     (doseq [c db-entries]
       (insert! @-db nil c))
     ;; TODO refactor
     (update! {:db/key :db/property? :db/tag? true})
     (index-ids)
     )))

(defn reset-db!
  ([]
   (reset-db! *default-identity*))
  ([^Keyword k]
   (create-db! k)
   (prime-db! k)))

(defn max-id
  "Returns the max id for the database."
  ([] (max-id ^DB @*db*))
  ([^DB db] (.getMaxId db)))

(defn value-0
  "Lowest level interface to getValue. (swaps the order of id and key to better
  leverage the threading function ->>)."
  ([key id] (.getValue ^DB @*db* ^int id ^int key))
  ([^DB db key id] (.getValue db ^int id ^int key)))

(defn ^:private value-1
  ([key id]
   (value-1 ^DB @*db* key id))
  ([^DB db key id]
   (if (instance? Number key)
     (value-0 db ^int key ^int id)
     (when (keyword? key)
       (value-0 db ^int (key->id db key) ^int id)))))

(defn value
  "Given a key and id returns the value for the key on the given id."
  ([key id] (value ^DB @*db* key id))
  ([^DB db key id]
   (if (instance? Number id)
     (value-1 db key id) ;; do something else here
     (when (keyword? id)
       (value-1 db key (key->id db id))))))

(defn valuei
  "Same as value, but the arguments are swapped."
  ([id key] (value key id))
  ([^DB db id key] (value db key id)))

(defn invoke
  ([key id] (invoke @*db* key id))
  ([^DB db key id]
   (when-let [k-fn (value db :db/fn key)]
     (k-fn id ;;(value db key id)
      ))))

(defn invokei
  "Reversed arguments from invoke."
  ([id key] (invoke @*db* key id))
  ([^DB db id key] (invoke db key id)))

(defn seek
  "Given the id for a concept returns a lazy Map for that concept."
  ([id] (seek @*db* id))
  ([^DB db id]
    (when-let [^int int-id (if (keyword? id) (key->id db id) id)]
     (.get db int-id))))

(defn ids
  "Shorthand method for {:db/ids (seek id)}. Given a the for a property/attribute
   returns the set of concepts having that property/attribute."
  ([id]
   (:db/ids (seek id)))
  ([db id]
    (:db/ids (seek db id))))

(defn proj-0
  ([^ints ks id] (proj-0 db ks id))
  ([^DB db ^ints ks id] (apply vector (map #(value db % id) ks))))

(defn proj
  ([ks ^ints ids]
   (proj ^DB @*db* ks ids))
  ([^DB db ks ^ints ids]
   (let [^ints key-ids (ordered-ids db ks)]
     (.project db key-ids ids))))

(defn project
  ([ks ids]
   (project ^DB @*db* ks ids))
  ([^DB db ks ids]
   (let [^ints key-ids (normalize-ids db ks)]
     (map #(proj-0 db key-ids %) ids)
     ;;(for [^int id ids] (proj-0 key-ids id))
     )))

(defn ->persistent-map [^DBMap m]
  (when m
    (.asPersistentMap (.projectMap m))))

(defn project-map
  ([ks ^ints ids]
   (project-map @*db* ks ids))
  ([^DB db ks ^ints ids]
   (let [^ints key-ids (keys->ids db ks)]
     (for [^int id ids] (into {} (map #(vector %1 (value-0 ^DB db %2 id)) ^ints ks key-ids))))))

(defn ident
  ([arg] (ident ^DB @*db* arg))
  ([^DB db arg] (if (instance? DBMap arg)
                  (:db/key arg)
                  (value ^DB db :db/key arg))))

(defn idents
  "Given a set i.e. has a :db/ids, returns the :db/key's for them"
  ([aset] (idents @*db* aset))
  ([^DB db aset]
   (map (comp :db/key (partial seek ^DB db))
        (if (keyword? aset)
          (ids ^DB db aset) aset))))

(defn scan
  ([args] (scan @*db* args))
  ([^DB db args]
   (map (partial seek ^DB db)
        (if (keyword? args)
          (ids ^DB db args) args))))

(defn into-seq [^DBMap c]
  (map (fn [k v] [k v]) (keys c) (vals c)))

(defn aggregator []
  (if (bound? #'*aggr*) *aggr* (IndexAggregator.)))

(defn apply-aggregator!
  ([]
   (when (bound? #'*aggr*)
     (apply-aggregator! ^DB @*db* ^IndexAggregator *aggr*)))
  ([^IndexAggregator aggr]
   (apply-aggregator! ^DB @*db* aggr))
  ([^DB db ^IndexAggregator aggr]
   (doseq [k (.keys aggr)]
     (reset! (db-atom db)
             (.update db aggr ^int k ^int (key->id ^DB db :db/ids)
                      (int-sets/union (ids ^DB db k) (.ids aggr k))))
     #_(update-0! ^DB db k (key->id ^DB db :db/ids)
                (int-sets/union (ids ^DB db k) (.ids aggr k))))))

(defmacro with-aggr-0
  ([^DB db binding & bodies]
   `(let [~(first binding) (IndexAggregator.)]
      ~@bodies
      (apply-aggregator! ^DB ~db ~(first binding)))))

(defmacro with-aggr
  ([binding & bodies]
   `(let [~(first binding) (IndexAggregator.)]
      ~@bodies
      (apply-aggregator! ~(first binding)))))

;; TODO: support more compaction target types
(defn compact!
  ([] (compact! :r))
  ([type] (compact! @*db* type))
  ([db type]
   (reset!
    (db-atom db)
    ;; type is not currently used here, but will be in the future
    (condp instance? db
      conceptual.core.PersistentDB (case type
                                     (.compactToRDB ^conceptual.core.PersistentDB db))
      conceptual.core.TuplDB (case type
                               (.compactToRDB ^conceptual.core.TuplDB db))))))

;; TODO: make this more versatile... only update selective indices
(defn pickle!
  ([^String filename] (pickle! @*db* filename))
  ([^DB db ^String filename]
   (condp instance? db
     conceptual.core.RDB (conceptual.core.RDB/store db filename))))

(def pickle pickle!)

(defn unpickle [type filename]
  (case type
    :r (conceptual.core.RDB/load filename)
    :tupl (conceptual.core.TuplDB/load filename)
    (conceptual.core.RDB/load filename)))

(defn load-pickle!
  ([& {filename :filename
       type :type
       db :db
       :or {filename "pickle.sz"
            type :r
            db @*db*}}]
   (reset! (db-atom db) (unpickle type filename))))

;; TODO: fix arity
(defn reset-pickle! [& args]
  (reset! (db-atom (get args :db @*db*)) (create-db!))
  (apply load-pickle! args))

(defn load-pickle-0! [filename type]
  (load-pickle! :filename filename :type (keyword type)))

(defn dump
  ([] (dump ^DB @*db*))
  ([^DB db]
   (doseq [i (range (inc (max-id db)))]
     (clojure.pprint/pprint (seek db i)))))

;;(dump)

(defn init! [])
