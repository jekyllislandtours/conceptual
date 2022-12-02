(ns conceptual.core
  (:require [conceptual.arrays :refer [int-array-class]]
            [conceptual.int-sets :as int-sets]
            [clojure.data.int-map :as int-map]
            [clojure.pprint])
  (:import [conceptual.core DB DBMap IndexAggregator PersistentDB WritableDB]
           [clojure.lang IFn Keyword]))

;; name of default implicit db

(def ^{:dynamic true :tag DB} *db* (atom nil))

(def ^{:dynamic true :tag IndexAggregator} *aggr*)

;; internal keys
(def ^{:private true :const true} -id (int DB/ID_ID))
(def ^{:private true :const true} -key (int DB/KEY_ID))
(def ^{:private true :const true} -type (int DB/TYPE_ID))
(def ^{:private true :const true} -property? (int DB/PROPERTY_TAG_ID))
(def ^{:private true :const true} -tag? (int DB/TAG_TAG_ID))
(def ^{:private true :const true} -unique? (int DB/UNIQUE_TAG_ID))

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
  {:db/id -id
   :db/key -key
   :db/type -type
   :db/property? -property?
   :db/tag? -tag?
   :db/unique? -unique?})

(def ^:private default-unique-indices
  {-key default-keyword->id})

(def ^:private tag-default-concept {:db/key :db/tag? :db/type Boolean :db/property? true})
(def ^:private unique-default-concept {:db/key :db/unique? :db/type Boolean :db/property? true :db/tag? true})

(def ^:private extra-db-concepts
  [{:db/key :db/dont-index? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/relation? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/to-many-relation? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/to-one-relation? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/inverse-relation :db/type Integer :db/property? true :db/relation? true :db/to-one-relation? true}
   {:db/key :db/ids :db/type int-array-class :db/relation? true :db/to-many-relation? true :db/property? true}
   {:db/key :db/fn? :db/type Boolean :db/property? true :db/tag? true}
   {:db/key :db/fn :db/type clojure.lang.IFn :db/property? true}])

(defn- empty-persistent-db []
  (PersistentDB. :default
                 default-unique-indices
                 default-keys
                 default-values
                 default-max-id))

(defn db
  "Returns a database instance if it exists."
  [] @*db*)

(defn key->id
  "Given a keyword identity returns the id."
  ([^Keyword k] (key->id ^DB (db) ^Keyword k))
  ([^DB db ^Keyword k] (.keywordToId ^DB db ^Keyword k)))

(defn lookup-id-0
  ([unique-key-id ^Object key] (.lookupId ^DB (db) ^int unique-key-id ^Object key))
  ([^DB db unique-key-id ^Object key] (.lookupId ^DB db ^int unique-key-id ^Object key)))

(defn lookup-id
  ([unique-key ^Object key] (lookup-id ^DB (db) unique-key ^Object key))
  ([^DB db unique-key ^Object key]
   (if (instance? Number unique-key)
     (lookup-id-0 db unique-key key)
     (when (keyword? unique-key)
       (lookup-id-0 db (key->id ^DB db unique-key) key)))))

(defn- map->kvs [^DB db arg]
  (let [items (->> arg
                   (map (fn [[k v]] [(key->id ^DB db k) v]))
                   (sort-by first <))
        ks (int-array (map first items))
        vs (object-array (map second items))]
    [ks vs]))

(defn- map->undefined-keys [^DB db arg]
  (some->> arg keys (remove (partial key->id ^DB db))))

(defn create-db!
  "Creates/resets *db* to a brand new database all bootstrapped and stuff."
  []
  ;; bootstrap
  (reset! *db* (empty-persistent-db))
  ;; insert :db/tag?
  (let [[^ints ks #^Object vs] (map->kvs (db) tag-default-concept)]
    (swap! *db* (fn [^DB db] (.insert ^DB db nil ks vs))))
  ;; add :db/tag? to :db/tag? itself
  (swap! *db* (fn [^DB db] (let [id (key->id ^DB db :db/tag?)]
                             (.update ^DB db nil id id true))))
  ;; add :db/tag? to :db/property?
  (swap! *db* (fn [^DB db] (let [id (key->id ^DB db :db/property?)
                                 kid (key->id ^DB db :db/tag?)]
                             (.update ^DB db nil id kid true))))
  ;; insert :db/unique?
  (let [[^ints ks #^Object vs] (map->kvs (db) unique-default-concept)]
    (swap! *db* (fn [^DB db] (.insert ^DB db nil ks vs))))
  ;; tag :db/key as being unique so it gets indexed
  (swap! *db* (fn [^DB db] (let [id (key->id ^DB db :db/key)
                                 kid (key->id ^DB db :db/unique?)]
                             (.update ^DB db nil id kid true))))
  ;; add remaining items... should now index any :db/unique? keys
  (swap! *db*
         (fn [db]
           (reduce (fn [^DB db c]
                     (let [[^ints ks #^Object vs] (map->kvs db c)]
                       (.insert ^DB db nil ks vs)))
                   db extra-db-concepts)))
  ;; now index all :db/ids, after this should be automatic
  (swap! *db*
         (fn [^DB db]
           (->> (range (.count db))
                (map #(vector % (.getKeys db %)))
                (mapcat (fn [[id ks]] (map #(vector % id) ks)))
                (group-by first)
                (map #(vector (first %)
                              (int-array (sort (into #{} (map second (second %)))))))
                (reduce (fn [db [id v]]
                          (.update ^DB db nil id (key->id db :db/ids) v)) db)))))

;; TODO push this into a utils
(defn nsname
  "Given a keyword return the namespace/name string representation of the keyword.
  Not sure why this isn't in clojure core or at least a method of Keyword."
  [^Keyword k] (str (.sym k)))


(defn keys->ids
  "Returns a sorted int array representing a collection of keywords."
  ([ks] (keys->ids (db) ks))
  ([^DB db ks]
   (let [f (partial key->id ^DB db)]
     (->> (map f ks)
          (into #{})
          (sort)
          (int-array)))))

(defn key-ids
  "Returns the keys for an concept key."
  ([key] (key-ids (db) key))
  ([^DB db key]
   (if (instance? Number key)
     (.getKeys ^DB db ^int key)
     (when (keyword? key)
       (.getKeys ^DB db ^int (key->id db key))))))

;; TODO: reconcile this with normalize-ids
(defn ordered-ids
  "Like keys->ids but does not eliminate duplicates or sort. This is useful
  for projections where the order of the projection matters."
  ([ks] (ordered-ids (db) ks))
  ([db ks] (let [f (partial key->id ^DB db)] (int-array (map f ks)))))

(defn normalize-ids
  "Like keys->ids but does not eliminate duplicates or sort. This is useful
  for projections where the order of the projection matters."
  ([ks] (normalize-ids (db) ks))
  ([db ks] (cond
            (instance? int-array-class ks) ks
            (or (vector? ks)
                (list? ks)
                (set? ks)) (let [f (partial key->id ^DB db)]
                             (int-array (map f ks))))))

(defn id->key
  "Given and id returns the key."
  ([id] (id->key (db) ^int id))
  ([^DB db id] (.getValue ^DB db ^int id ^int -key)))

(defn ids->keys
  "Given a collection of ids returns a collection of keys."
  ([^ints ids] (ids->keys (db) ids))
  ([^DB db ^ints ids]
   (let [f (partial id->key ^DB db)] (map f ids))))

(declare seek)

(defn insert-0!
  "Inserts an array of values given an array of keys. Must be a WritableDB"
  ([^ints ks #^Object vs]
   (insert-0! nil ks vs))
  ([^IndexAggregator aggr ^ints ks #^Object vs]
   (swap! *db* insert-0! aggr ks vs))
  ([^WritableDB db ^IndexAggregator aggr ^ints ks #^Object vs]
   (.insert ^WritableDB db
            ^IndexAggregator aggr
            ^ints ks
            #^Object vs)))

(defn insert!
  "Inserts into db. Must be a WritableDB. The key `:db/id`, if specified in `arg`, is ignored
  and a new `:db/id` will be added."
  ([arg] (insert! nil arg))
  ([^IndexAggregator aggr arg]
   (swap! *db* insert! aggr arg))
  ([^WritableDB db ^IndexAggregator aggr arg]
   (try
     (if-not (key->id ^DB db (:db/key arg))
       (let [[^ints ks #^Object vs] (map->kvs ^DB db (dissoc arg :db/id))]
         (insert-0! ^WritableDB db ^IndexAggregator aggr ^ints ks #^Object vs))
       db)
     (catch Throwable t
       (let [undefined-keys (map->undefined-keys db arg)]
         (throw
          (ex-info (str "Error inserting concept:"
                        (.getMessage t)
                        (when (seq undefined-keys)
                          " undefined-key - update! requires all keys to be defined."))
                   {:arg arg
                    :undefined-keys undefined-keys} t)))))))

(defn update-0!
  "Lowest level update fn. Updates a single key/value in the concept.
   Expects an integer id, and an integer key."
  ([id k ^Object v]
   (update-0! nil ^int id ^int k v))
  ([^IndexAggregator aggr id k ^Object v]
   (swap! *db* (fn [db] (update-0! db aggr ^int id ^int k v))))
  ([^WritableDB db ^IndexAggregator aggr id k ^Object v]
   (.update ^DB db ^IndexAggregator aggr ^int id ^int k v)))

(defn update-1!
  "Updates an array of values given an array of keys."
  ([id ^ints ks #^Object vs]
   (update-1! nil id ks vs))
  ([^IndexAggregator aggr id ^ints ks #^Object vs]
   (swap! *db* (fn [db] (update-1! db aggr id ks vs))))
  ([^WritableDB db ^IndexAggregator aggr id ^ints ks #^Object vs]
   (.update ^WritableDB db
            ^IndexAggregator aggr
            ^int id
            ^ints ks
            #^Object vs)))

(defn update-2!
  [^WritableDB db ^IndexAggregator aggr id arg]
  (try
    (let [[^ints ks #^Object vs] (map->kvs db arg)]
      (update-1! db aggr ^int id ^ints ks #^Object vs))
    (catch Throwable t
      (let [undefined-keys (map->undefined-keys db arg)]
        (throw
         (ex-info (str "Error updating concept: "
                       (.getMessage t)
                       (when (seq undefined-keys)
                         "undefined-key - update! requires all keys to be defined."))
                  {:arg arg
                   :undefined-keys undefined-keys} t))))))

(defn update!
  "Given a map which contains either a :db/id or a :db/key and value as well as the
  keys and values that need to added or updated, updates the concept.
  NOTE: use replace! to be able to remove keys."
  ([arg] (update! nil arg))
  ([^IndexAggregator aggr arg]
   (swap! *db* (fn [db] (update! db aggr arg))))
  ([^WritableDB db ^IndexAggregator aggr arg]
   (if (:db/id arg)
     (update-2! db aggr (:db/id arg) arg) ;; dissoc :db/id
     (if (:db/key arg)
       (update-2! db aggr (key->id db (:db/key arg)) arg)
       (throw
        (ex-info (str "Error updating concept: "
                      (when-not (or (:db/id arg)
                                    (:db/key arg))
                        (format "\n\tmissing id - update! requires a :db/id (%d) or :db/key (%s) in the argument."
                                (:db/id arg)
                                (str (:db/key arg)))))
                 {:arg arg}))))))

(defn replace-0!
  "Updates an array of values given an array of keys."
  ([id ^ints ks #^Object vs]
   (replace-0! nil id ks vs))
  ([^IndexAggregator aggr id ^ints ks #^Object vs]
   (swap! *db* (fn [db] (replace-0! db aggr id ks vs))))
  ([^WritableDB db ^IndexAggregator aggr id ^ints ks #^Object vs]
   (.replace ^WritableDB db
             ^IndexAggregator aggr
             ^int id
             ^ints ks
             #^Object vs)))

(defn replace-1!
  [^WritableDB db ^IndexAggregator aggr id arg]
  (try
    (let [[^ints ks #^Object vs] (map->kvs ^DB db arg)]
      (replace-0! db aggr ^int id ^ints ks #^Object vs))
    (catch Throwable t
      (let [undefined-keys (map->undefined-keys db arg)]
        (throw
         (ex-info (str "Error replacing concept: "
                       (.getMessage t)
                       (when (seq undefined-keys)
                         "undefined-key - replace! requires all keys to be defined."))
                  {:arg arg
                   :undefined-keys undefined-keys} t))))))

(defn replace!
  "Given a map which contains either a :db/id or a :db/key will replace the
   concept with the new keys and values.
  NOTE: use replace! to be able to remove properties."
  ([arg] (replace! nil arg))
  ([^IndexAggregator aggr arg]
   (swap! *db* (fn [db] (replace! db aggr arg))))
  ([^WritableDB db ^IndexAggregator aggr arg]
   (if (:db/id arg)
     (replace-1! db aggr (:db/id arg) arg)
     (if (:db/key arg)
       (replace-1! db aggr (key->id (:db/key arg)) arg)
       (throw
        (ex-info (str "Error replacing concept: "
                      (when-not (or (:db/id arg)
                                    (:db/key arg))
                        (format "\n\tmissing id - replace! requires a :db/id (%d) or :db/key (%s) in the argument."
                                (:db/id arg)
                                (str (:db/key arg)))))
                 {:arg arg}))))))

;; TODO: make this safe or remove
(defn update-inline!
  ([^IndexAggregator aggr id ^ints ks #^Object vs]
   (swap! *db* (fn [db] (update-inline! db aggr id ks vs))))
  ([^WritableDB db ^IndexAggregator aggr id ^ints ks #^Object vs]
   (.updateInline ^WritableDB db aggr id ks vs)))

(defn max-id
  "Returns the max id for the database."
  ([] (max-id ^DB (db)))
  ([^DB db] (.getMaxId db)))

(defn value-0
  "Lowest level interface to getValue. (swaps the order of id and key to better
  leverage the threading function ->>)."
  ([key id] (.getValue ^DB (db) ^int id ^int key))
  ([^DB db key id] (.getValue db ^int id ^int key)))

(defn ^:private value-1
  ([key id]
   (value-1 ^DB (db) key id))
  ([^DB db key id]
   (if (instance? Number key)
     (value-0 db ^int key ^int id)
     (when (keyword? key)
       (value-0 db ^int (key->id db key) ^int id)))))

(defn value
  "Given a key and id returns the value for the key on the given id."
  ([key id] (value ^DB (db) key id))
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
  ([key id] (invoke (db) key id))
  ([^DB db key id]
   (when-let [k-fn (value db :db/fn key)]
     (k-fn id ;;(value db key id)
      ))))

(defn invokei
  "Reversed arguments from invoke."
  ([id key] (invoke (db) key id))
  ([^DB db id key] (invoke db key id)))

(defn lookup
  "Given the id for a concept returns a lazy Map for that concept."
  ([unique-key ^Object key] (lookup (db) unique-key key))
  ([^DB db unique-key ^Object key]
   (when-let [^int unique-key-id (if (keyword? unique-key)
                                   (key->id ^DB db unique-key)
                                   unique-key)]
     (.lookup ^DB db ^int unique-key-id ^Object key))))

(defn seek
  "Given the id for a concept returns a lazy Map for that concept."
  ([id] (seek (db) id))
  ([^DB db id]
    (when-let [^int int-id (if (keyword? id) (key->id db id) id)]
     (.get db int-id))))

(defn ids
  "Shorthand method for {:db/ids (seek id)}. Given a the for a property/attribute
   returns the set of concepts having that property/attribute."
  ([id]
   (ids @*db* id))
  ([db id]
   (or (:db/ids (seek db id)) int-sets/+empty+)))

(defn proj-0
  ([^ints ks id] (proj-0 db ks id))
  ([^DB db ^ints ks id] (apply vector (map #(value db % id) ks))))

(defn proj
  ([ks ^ints ids]
   (proj ^DB (db) ks ids))
  ([^DB db ks ^ints ids]
   (let [^ints key-ids (ordered-ids db ks)]
     (.project db key-ids ids))))

(defn project
  ([ks ids]
   (project ^DB (db) ks ids))
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
   (project-map (db) ks ids))
  ([^DB db ks ^ints ids]
   (let [^ints key-ids (normalize-ids db ks)]
     (for [^int id ids]
       (->> (map #(vector %1 (value-0 ^DB db %2 id)) ^ints ks key-ids)
            (into {}))))))

(defn ident
  ([arg] (ident ^DB (db) arg))
  ([^DB db arg] (if (instance? DBMap arg)
                  (:db/key arg)
                  (value ^DB db :db/key arg))))

(defn idents
  "Given a set i.e. has a :db/ids, returns the :db/key's for them"
  ([aset] (idents (db) aset))
  ([^DB db aset]
   (map (comp :db/key (partial seek ^DB db))
        (if (keyword? aset)
          (ids ^DB db aset) aset))))

(defn scan
  ([args] (scan (db) args))
  ([^DB db args]
   (map (partial seek ^DB db)
        (if (keyword? args)
          (ids ^DB db args) args))))

(defn into-seq [^DBMap c]
  (map (fn [k v] [k v]) (keys c) (vals c)))

(defn aggregator []
  (if (bound? #'*aggr*) *aggr* (IndexAggregator.)))


;; TODO: go through keys and determine if they are of type :db/key?
;; and if they are index them.

(defn apply-aggregator!
  [^IndexAggregator aggr]
  (doseq [k (.keys aggr)]
    (swap! *db*
           (fn [db] (.update ^DB db aggr ^int k ^int (key->id ^DB db :db/ids)
                             (int-sets/difference (int-sets/union (ids ^DB db k) (.ids aggr k))
                                                  (.removeIds aggr k)))))))

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


(defn- db-type
  [db]
  (let [db-type (type db)]
    (if (instance? Class db-type)
      (.getName ^Class db-type)
      db-type)))

(defmulti compact-db! (fn [db _type] [(db-type db) _type]))

(defmethod compact-db! :default
  [db _type]
  (.compactToRDB ^conceptual.core.PersistentDB db))

(defn compact!
  ([] (compact! :r))
  ([type] (swap! *db* (fn [db] (compact! db type))))
  ([db type]
   (compact-db! db type)))


(defmulti pickle-db! (fn [-type _opts] (db-type db)))

(defmethod pickle-db! :default
  [_type {:keys [db filename verbose cipher]}]
  (if cipher
    (conceptual.core.RDB/store ^conceptual.core.RDB db filename cipher)
    (conceptual.core.RDB/store ^conceptual.core.RDB db filename)))

(defn pickle!
  [& {:keys [db filename cipher]
      :or {filename "pickle.sz"
           db (db)}}]
  (pickle-db! :default (cond-> {:db db
                                :filename filename}
                         cipher (assoc :cipher cipher))))


(defmulti unpickle-db! (fn [-type _opts] -type))

(defmethod unpickle-db! :default
  [_type {:keys [filename verbose cipher]}]
  (if cipher
    (conceptual.core.RDB/load filename verbose cipher)
    (conceptual.core.RDB/load filename verbose)))

(defn load-pickle!
  ([& {:keys [filename type verbose cipher db]
       :or {filename "pickle.sz"
            type :default
            verbose false}}]
   (reset! *db* (unpickle-db! type
                              (cond-> {:filename filename
                                       :verbose verbose}
                                cipher (assoc :cipher cipher))))))

;; TODO: fix arity
(defn reset-pickle! [& args]
  (reset! *db* (create-db!))
  (apply load-pickle! args))

(defn dump
  ([] (dump ^DB (db)))
  ([^DB db]
   (doseq [i (range (inc (max-id db)))]
     (clojure.pprint/pprint (seek db i)))))

;;(dump)

(defn init! [])
