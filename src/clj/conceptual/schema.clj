(ns conceptual.schema
  (:require [conceptual.arrays :refer [int-array-class]]
            [conceptual.core :as c
             :refer [*db*
                     *aggr*
                     apply-aggregator! with-aggr
                     insert! update! update-0!
                     seek value value-0 valuei key->id ids project]]
            [conceptual.contracts :refer [defn-checked]])
  (:import [conceptual.core DB IndexAggregator]
           [clojure.lang Keyword]))

(defn declare-property!
  "Given property specs of the form [key type] or [key type opts]
   declares a property."
  ([^Keyword key type]
   (with-aggr [aggr]
     (declare-property! ^DB @*db* ^IndexAggregator aggr key type)))
  ([^DB db ^IndexAggregator aggr ^Keyword key type]
   (declare-property! ^DB @*db* ^IndexAggregator aggr key type nil))
  ([^DB db ^IndexAggregator aggr ^Keyword key type opts]
   ;;{:pre [(clojure.test/is (not-any? nil? [db aggr key type]))]}
   (let [c (merge {:db/key key
                   :db/type type
                   :db/property? true}
                  opts)]
     (insert! ^DB db ^IndexAggregator aggr c))))

(defn declare-properties!
  "Given a list of property specs of the form [key type] or [key type opts],
   declares the set of properties."
  ([args]
   (with-aggr [aggr]
     (declare-properties! c/*default-identity* ^IndexAggregator aggr args)))
  ([^Keyword db-key ^IndexAggregator aggr args]
   ;;{:pre [(clojure.test/is (not-any? nil? [db-key aggr args]))]}
   (doseq [arg args]
     (apply (partial declare-property! ^DB (c/db db-key) ^IndexAggregator aggr) arg))))

(defn declare-tag!
  ([^Keyword key]
   (with-aggr [aggr]
     (declare-tag! ^DB @*db* ^IndexAggregator aggr ^Keyword key)))
  ([^DB db ^IndexAggregator aggr ^Keyword key]
   (declare-tag! ^DB @*db* ^IndexAggregator aggr ^Keyword key nil))
  ([^DB db ^IndexAggregator aggr ^Keyword key opts]
   ;;{:pre [(not-any? nil? [db aggr key])]}
   (declare-property! ^DB db ^IndexAggregator aggr ^Keyword key Boolean
                      (merge {:db/tag? true} opts))))

(defn declare-tags!
  ([args]
   (with-aggr [aggr]
     (declare-tags! c/*default-identity* ^IndexAggregator aggr args)))
  ([^Keyword db-key ^IndexAggregator aggr args]
   ;;{:pre [(clojure.test/is (not-any? nil? [db-key aggr args]))]}
   (doseq [arg args]
     (apply (partial declare-tag! ^DB (c/db db-key) ^IndexAggregator aggr) arg))))

(defn declare-to-one-relation!
  ([^Keyword key]
   (declare-to-one-relation! ^Keyword key nil))
  ([^Keyword key opts]
   (with-aggr [aggr]
     (declare-to-one-relation! ^DB @*db* ^IndexAggregator aggr ^Keyword key opts)))
  ([^DB db ^IndexAggregator aggr ^Keyword key]
   (declare-to-one-relation! ^DB @*db* ^IndexAggregator aggr ^Keyword key nil))
  ([^DB db ^IndexAggregator aggr ^Keyword key opts]
   ;;{:pre [(not-any? nil? [db aggr key])]}
   (declare-property! ^DB db ^IndexAggregator aggr ^Keyword key Integer
                      (merge {:db/relation? true
                              :db/to-one-relation? true}
                             opts))))

(defn declare-to-many-relation!
  ([^Keyword key]
   (declare-to-many-relation! ^Keyword key nil))
  ([^Keyword key opts]
   (with-aggr [aggr]
     (declare-to-many-relation! ^DB @*db* ^IndexAggregator aggr ^Keyword key opts)))
  ([^DB db ^IndexAggregator aggr ^Keyword key]
   (declare-to-many-relation! ^DB @*db* ^IndexAggregator aggr ^Keyword key nil))
  ([^DB db ^IndexAggregator aggr ^Keyword key opts]
   ;;{:pre [(clojure.test/is (not-any? nil? [db aggr key]))]}
   (declare-property! ^DB db ^IndexAggregator aggr ^Keyword key
                      int-array-class
                      (merge {:db/relation? true
                              :db/to-many-relation? true}
                             opts))))

(defn declare-to-one-relations!
  ([args]
   (with-aggr [aggr]
     (declare-to-one-relations! c/*default-identity* ^IndexAggregator aggr args)))
  ([^Keyword db-key ^IndexAggregator aggr args]
   ;;{:pre [(clojure.test/is (not-any? nil? [db-key aggr args]))]}
   (let [tuple-fn (fn [a] (if (> (count a) 1)
                            (vector (first a) {:db/inverse-relation
                                               (value (c/db db-key) :db/id (second a))}) a))]
     (doseq [arg (->> args
                      (map tuple-fn))]
       (apply (partial declare-to-one-relation! ^DB (c/db db-key) ^IndexAggregator aggr) arg)))))

(defn declare-to-many-relations!
  ([args]
   (with-aggr [aggr]
     (declare-to-many-relations! c/*default-identity* ^IndexAggregator aggr args)))
  ([^Keyword db-key ^IndexAggregator aggr args]
   ;;{:pre [(clojure.test/is (not-any? nil? [db-key aggr args]))]}
   (let [tuple-fn (fn [a] (if (> (count a) 1)
                            (vector (first a) {:db/inverse-relation
                                               (value (c/db db-key) :db/id (second a))}) a))]
     (doseq [arg args]
       (apply (partial declare-to-many-relation! ^DB (c/db db-key) ^IndexAggregator aggr) arg)))))

(defn key-map
  "Creates a map of the-key's value to id for the given set key or id.
   Assumes the mapping is one to one."
  ([the-set the-key] (key-map ^DB @*db* the-set the-key))
  ([^DB db the-set the-key]
   ;;{:pre [(clojure.test/is (not-any? nil? [the-set the-key (key->id the-key)]))]}
   (some->> (ids ^DB db the-set)
            (project ^DB db [the-key :db/id])
            (into {}))))

(defn multi-key-map
  "Creates a map of the-key's value to a vector of all of the ids with that value."
  ([the-set the-key] (multi-key-map ^DB @*db* the-set the-key))
  ([^DB db the-set the-key]
   ;;{:pre [(clojure.test/is (not-any? nil? [db the-set the-key (key->id db the-key)]))]}
   (->> the-set
        (partial ids ^DB db)
        (project ^DB db [the-key :db/id])
        (group-by first)
        (map (fn [[k v]] [k (mapv second v)]))
        (into {}))))

(defn add-inverse-relations!
  "Given an seq of [relationKeyA inverseRelationKeyB] adds and inverse relation
  (i.e. :db/inverse-relation) to the concept represented by inverseRelationKeyB."
  ([args]
   (with-aggr [aggr]
     (add-inverse-relations! c/*default-identity* ^IndexAggregator aggr args)))
  ([^Keyword db-key ^IndexAggregator aggr args]
   ;;{:pre [(not-any? nil? [db-key aggr args])]}
   (doseq [arg args]
     (let [db (c/db db-key)]
       (update! ^DB db ^IndexAggregator aggr
                {:db/key (first arg)
                 :db/inverse-relation (some-> arg
                                              second
                                              (partial valuei db :db-id))})))))

(defn add-tag!
  "Add a tag represented by the 'tag-key' to a
   set (i.e. has :db/ids) represented by 'set-key'"
  ([set-key tag-key]
   (with-aggr [aggr]
     (add-tag! c/*default-identity* ^IndexAggregator aggr set-key tag-key)))
  ([^Keyword db-key ^IndexAggregator aggr set-key tag-key]
   ;;{:pre [(clojure.test/is (not-any? nil? [db-key aggr set-key tag-key (key->id tag-key)]))]}
   (let [tag-id (key->id ^DB (c/db db-key) tag-key)]
     (doseq [id (ids ^DB (c/db db-key) set-key)]
       (update-0! ^DB (c/db db-key) ^IndexAggregator aggr id tag-id true)))))

(defn add-tags!
  "Adds a set of tags to the given set key (i.e. has ids)."
  ([set-key tag-keys]
   (with-aggr [aggr]
     (add-tags! c/*default-identity* ^IndexAggregator aggr set-key tag-keys)))
  ([^Keyword db-key ^IndexAggregator aggr set-key tag-keys]
   ;;{:pre [(clojure.test/is (not-any? nil? [db-key aggr set-key tag-keys]))]}
   (doseq [tag-key tag-keys]
     (add-tag! ^DB (c/db db-key) ^IndexAggregator aggr set-key tag-key))))

(defn add-tag-by-fk!
  "Adds a tag to all r.h.s. to a fk to-one-relation."
  ([fk tag-key]
   (with-aggr [aggr]
     (add-tag-by-fk! c/*default-identity* ^IndexAggregator aggr fk tag-key)))
  ([^Keyword db-key ^IndexAggregator aggr fk tag-key]
   ;; {:pre [(clojure.test/is (not-any? nil? [db-key aggr fk tag-key
   ;;                                         (key->id ^DB (c/db db-key) fk)
   ;;                                         (key->id ^DB (c/db db-key) tag-key)]))]}
   (let [fk-id (key->id ^DB (c/db db-key) fk)
         tag-id (key->id ^DB (c/db db-key) tag-key)]
     (doseq [id (->> (ids ^DB (c/db db-key) fk)
                     (map #(value ^DB (c/db db-key) fk-id %)))]
       (update-0! ^DB (c/db db-key) ^IndexAggregator aggr id tag-id true)))))

(defn copy-key!
  "Copies all 'copy-from' values from copy from's 'ids' to a new key 'copy-to'"
  ([copy-from copy-to]
   (with-aggr [aggr]
     (copy-key! ^DB @*db* ^IndexAggregator aggr copy-from copy-to)))
  ([^DB db ^IndexAggregator aggr copy-from copy-to]
   ;;{:pre [(clojure.test/is (not-any? nil? [db aggr copy-from copy-to
                                           ;; (key->id ^DB db copy-from)
                                           ;; (key->id ^DB db copy-to)]))]}
   (doseq [id (ids ^DB db copy-from)]
     (update-0! ^DB db ^IndexAggregator aggr id
                (key->id ^DB db copy-to)
                (value-0 ^DB db (key->id ^DB db copy-from) id)))))

;; TODO: add pre-flight check, rename?
(defn add-new-relation!
  ([relation-keyword relation-type id-val-pairs]
   (with-aggr [aggr]
     (add-new-relation! c/*default-identity* ^IndexAggregator aggr relation-keyword relation-type id-val-pairs)))
  ([^Keyword db-key ^IndexAggregator aggr relation-keyword relation-type id-val-pairs]
   ;; {:pre [(clojure.test/is (not-any? nil? [db-key aggr relation-keyword relation-type id-val-pairs
   ;;                                         (key->id ^DB (c/db db-key) relation-keyword)]))]}
   ((case relation-type
      :to-one declare-to-one-relation!
      :to-many declare-to-many-relation!) ^DB (c/db db-key) ^IndexAggregator aggr [[relation-keyword]])
   (doseq [[eid val] id-val-pairs]
     (let [db (c/db db-key)]
       (update-0! ^DB db ^IndexAggregator aggr eid (key->id ^DB db relation-keyword) val)))))

;; TODO: add pre-flight check, rename?
(defn add-new-set-property!
  ([property-keyword id-val-pairs]
   (with-aggr [aggr]
     (add-new-set-property! c/*default-identity* ^IndexAggregator aggr property-keyword id-val-pairs)))
  ([^Keyword db-key ^IndexAggregator aggr property-keyword id-val-pairs]
   (doseq [p (some->>  id-val-pairs
                       (map (fn [[id vals]] [id (some->> vals
                                                         (filter identity)
                                                         (into #{})
                                                         sort
                                                         int-array)]))
                       (filter (comp not-empty second)))]
     (add-new-relation! db-key ^IndexAggregator aggr property-keyword :to-many))))

(defn rename-column!
  ([set-kw from to]
   (with-aggr [aggr]
     (rename-column! c/*default-identity* ^IndexAggregator aggr set-kw from to)))
  ([^Keyword db-key ^IndexAggregator aggr set-kw from to]
   ;; {:pre [(clojure.test/is (not-any? nil? (concat [db-key aggr]
   ;;                                                (mapcat (fn [x] [x (key->id ^DB (c/db db-key) x)])
   ;;                                                        [set-kw from to]))))]}
   (let [fkw (key->id ^DB (c/db db-key) from)
         tkw (key->id ^DB (c/db db-key) to)]
     (doseq [id (ids ^DB (c/db db-key) set-kw)]
       (let [db (c/db db-key)]
         (update-0! ^DB db ^IndexAggregator aggr id tkw
                    (value ^DB db fkw id)))))))

;; TODO support DB and IndexAggregator overloading, fix clj-kondo issues
#_(defn-checked add-to-one-relation!
  [{:keys [from to join-on fk-join-on relation]
    :or {fk-join-on nil}}]
  (declare-to-one-relation! relation)
  (with-aggr [aggr]
    (let [->ids (multi-key-map to join-on)
          fk (if fk-join-on fk-join-on join-on)]
      (->> (ids from)
           (map seek)
           (filter #(not-empty (->ids (fk %))))
           (mapcat (fn [m] (map #(update-0! aggr (:db/id m) (key->id relation) %) (->ids (fk m)))))
           dorun))))

;; TODO support DB and IndexAggregator overloading, fix clj-kondo issues
#_(defn-checked add-to-many-relation!
  [{:keys [from to pk-join-on join-on relation filter-to-fn]
    :or {filter-to-fn (constantly true)
         filter-from-fm (constantly true)
         pk-join-on nil}}]
  (declare-to-many-relation! relation)
  (with-aggr [aggr]
    (let [pk (if pk-join-on pk-join-on join-on)
          ->ids (multi-key-map from pk)]
      (->> (ids to)
           (map seek)
           (filter filter-to-fn)
           (map (juxt join-on :db/id))
           (group-by first)
           (mapcat (fn [m]
                     (map #(vector % (int-array (sort (into #{} (map second (second m))))))
                          (->ids (first m)))))
           (filter (comp not nil? first))
           (map #(update-0! aggr (first %) (key->id relation) (second %)))
           dorun))))
