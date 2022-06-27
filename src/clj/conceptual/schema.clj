(ns conceptual.schema
  (:require [conceptual.arrays :refer [int-array-class]]
            [conceptual.core :refer [*db*
                                     *aggr*
                                     aggregator apply-aggregator!
                                     insert! update! update-0!
                                     value value-0 valuei key->id ids project]]
            ;;[conceptual.contracts :refer [defn-checked]]
            [clojure.test])
  (:import [conceptual.core DB IndexAggregator]
           [clojure.lang Keyword]))

(defn declare-property!
  "Given property specs of the form [key type] or [key type opts]
   declares a property."
  ([^Keyword key type]
   (declare-property! ^Keyword key type nil))
  ([^Keyword key type opts]
   (let [aggr (aggregator)]
     (declare-property! aggr key type opts)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr ^Keyword key type opts]
   (declare-property! ^DB @*db* ^IndexAggregator aggr ^Keyword key type opts))
  ([^DB db ^IndexAggregator aggr ^Keyword key type opts]
   {:pre [(clojure.test/is (not-any? nil? [db aggr key type]))]}
   (insert! ^DB db ^IndexAggregator aggr (merge {:db/key key
                                                 :db/type type
                                                 :db/property? true} opts))))

(defn declare-properties!
  "Given a list of property specs of the form [key type] or [key type opts],
   declares the set of properties."
  ([args]
   (let [aggr (aggregator)]
     (declare-properties! ^IndexAggregator *aggr* args)
     (apply-aggregator! ^DB @*db* aggr)))
  ([^IndexAggregator aggr args]
   (declare-properties! ^DB @*db* ^IndexAggregator aggr args))
  ([^DB db ^IndexAggregator aggr args]
   (doseq [arg args]
     (apply (partial declare-property! ^DB db ^IndexAggregator aggr) arg))))

(defn declare-tag!
  ([^Keyword key]
   (declare-tag! key nil))
  ([^Keyword key args]
   (let [aggr (aggregator)]
     (declare-property! ^IndexAggregator aggr ^Keyword key Boolean args)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr ^Keyword key args]
   (declare-property! ^DB @*db* ^IndexAggregator aggr ^Keyword key Boolean args))
  ([^DB db ^IndexAggregator aggr ^Keyword key args]
   {:pre [(not-any? nil? [db aggr key key])]}
   (declare-property! ^DB db ^IndexAggregator aggr ^Keyword key Boolean
                      (merge {:db/tag? true} args))))

(defn declare-tags!
  ([args]
   (let [aggr (aggregator)]
     (declare-tags! ^IndexAggregator aggr args)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr args]
   (declare-tags! ^IndexAggregator aggr args))
  ([^DB db ^IndexAggregator aggr args]
   {:pre [(clojure.test/is (not-any? nil? [db aggr args]))]}
   (doseq [arg args]
     (apply (partial declare-tag! ^DB db ^IndexAggregator aggr) arg))))

(defn declare-to-one-relation!
  ([^Keyword key]
   (declare-to-one-relation! key nil))
  ([^Keyword key args]
   (let [aggr (aggregator)]
     (declare-property! ^IndexAggregator aggr ^Keyword key Integer args)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr ^Keyword key args]
   (declare-property! ^DB @*db* ^IndexAggregator aggr ^Keyword key Integer args))
  ([^DB db ^IndexAggregator aggr ^Keyword key args]
   {:pre [(not-any? nil? [db aggr key])]}
   (declare-property! ^DB db ^IndexAggregator aggr ^Keyword key Integer
                      (merge {:db/relation? true
                              :db/to-one-relation? true} args))))

(defn declare-to-many-relation!
  ([^Keyword key]
   (declare-to-many-relation! ^Keyword key nil))
  ([^Keyword key args]
   (let [aggr (aggregator)]
     (declare-property! ^IndexAggregator aggr ^Keyword key int-array-class args)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr ^Keyword key args]
   (declare-property! ^IndexAggregator aggr ^Keyword key int-array-class args))
  ([^DB db ^IndexAggregator aggr ^Keyword key args]
   {:pre [(clojure.test/is (not-any? nil? [db aggr key]))]}
   (declare-property! ^DB db ^IndexAggregator aggr ^Keyword key
                      int-array-class
                      (merge {:db/relation? true
                              :db/to-many-relation? true} args))))

(defn declare-relations!
  ([args opts]
   (let [aggr (aggregator)]
     (declare-relations! ^DB @*db* ^IndexAggregator aggr args opts)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr args opts]
   (declare-relations! ^DB @*db* ^IndexAggregator aggr args opts))
  ([^DB db ^IndexAggregator aggr args & {type :type}]
   {:pre [(clojure.test/is (not-any? nil? [db aggr args type]))]}
   (let [def-fn (case type
                  :to-one declare-to-one-relation!
                  :to-many declare-to-many-relation!)
         tuple-fn (fn [a] (if (> (count a) 1)
                            (vector (first a) {:db/inverse-relation
                                               (value :db/id (second a))}) a))]
     (doseq [arg (->> args
                      (map tuple-fn))]
       (apply (partial def-fn ^DB db ^IndexAggregator aggr) arg)))))

(defn key-map
  "Creates a map of the-key's value to id for the given set key or id.
   Assumes the mapping is one to one."
  ([the-set the-key] (key-map ^DB @*db* the-set the-key))
  ([^DB db the-set the-key]
   {:pre [(clojure.test/is (not-any? nil? [the-set the-key (key->id the-key)]))]}
   (some->> (ids ^DB db the-set)
            (project ^DB db [the-key :db/id])
            (into {}))))

(defn multi-key-map
  "Creates a map of the-key's value to a vector of all of the ids with that value."
  ([the-set the-key] (multi-key-map ^DB @*db* the-set the-key))
  ([^DB db the-set the-key]
   {:pre [(clojure.test/is (not-any? nil? [db the-set the-key (key->id db the-key)]))]}
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
   (let [aggr (aggregator)]
     (add-inverse-relations! ^IndexAggregator aggr args)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr args]
   (add-inverse-relations! ^DB @*db* ^IndexAggregator aggr args))
  ([^DB db ^IndexAggregator aggr args]
   {:pre [(not-any? nil? [db aggr args])]}
   (doseq [arg args]
     (update! ^DB db ^IndexAggregator aggr
              {:db/key (first arg)
               :db/inverse-relation (some-> arg
                                            second
                                            (partial valuei db :db-id))}))))

(defn add-tag!
  "Add a tag represented by the 'tag-key' to a
   set (i.e. has :db/ids) represented by 'set-key'"
  ([set-key tag-key]
   (let [aggr (aggregator)]
     (add-tag! ^IndexAggregator aggr set-key tag-key)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr set-key tag-key]
   (add-tag! ^DB @*db* ^IndexAggregator aggr set-key tag-key))
  ([^DB db ^IndexAggregator aggr set-key tag-key]
   {:pre [(clojure.test/is (not-any? nil? [db aggr set-key tag-key (key->id tag-key)]))]}
   (let [tag-id (key->id tag-key)]
     (doseq [id (ids db set-key)]
       (update-0! ^DB db ^IndexAggregator aggr id tag-id true)))))

(defn add-tags!
  "Adds a set of tags to the given set key (i.e. has ids)."
  ([set-key tag-keys]
   (let [aggr (aggregator)]
     (add-tags! ^IndexAggregator aggr set-key tag-keys)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr set-key tag-keys]
   (add-tags! ^DB @*db* ^IndexAggregator aggr set-key tag-keys))
  ([^DB db ^IndexAggregator aggr set-key tag-keys]
   {:pre [(clojure.test/is (not-any? nil? [db aggr set-key tag-keys]))]}
   (doseq [tag-key tag-keys]
     (add-tag! ^DB db ^IndexAggregator aggr set-key tag-key))))

(defn add-tag-by-fk!
  "Adds a tag to all r.h.s. to a fk to-one-relation."
  ([fk tag-key]
   (let [aggr (aggregator)]
     (add-tag-by-fk! ^IndexAggregator aggr fk tag-key)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr fk tag-key]
   (add-tag-by-fk! ^IndexAggregator aggr fk tag-key))
  ([^DB db ^IndexAggregator aggr fk tag-key]
   {:pre [(clojure.test/is (not-any? nil? [db aggr fk tag-key
                                           (key->id ^DB db fk)
                                           (key->id ^DB db tag-key)]))]}
   (let [fk-id (key->id ^DB db fk)
         tag-id (key->id ^DB db tag-key)]
     (doseq [id (->> (ids ^db db fk)
                     (map #(value ^DB db fk-id %)))]
       (update-0! ^DB db ^IndexAggregator aggr id tag-id true)))))

(defn copy-key!
  "Copies all 'copy-from' values from copy from's 'ids' to a new key 'copy-to'"
  ([copy-from copy-to]
   (let [aggr (aggregator)]
     (copy-key! ^IndexAggregator aggr copy-from copy-to)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr copy-from copy-to]
   (copy-key! ^DB @*db* ^IndexAggregator aggr copy-from copy-to))
  ([^DB db ^IndexAggregator aggr copy-from copy-to]
   {:pre [(clojure.test/is (not-any? nil? [db aggr copy-from copy-to
                                           (key->id ^DB db copy-from)
                                           (key->id ^DB db copy-to)]))]}
   (doseq [id (ids ^DB db copy-from)]
     (update-0! ^DB db ^IndexAggregator aggr id
                (key->id ^DB db copy-to)
                (value-0 ^DB db (key->id ^DB db copy-from) id)))))

;; TODO: add pre-flight check, rename?
(defn add-new-relation!
  ([relation-keyword relation-type id-val-pairs]
   (let [aggr (aggregator)]
     (add-new-relation! ^IndexAggregator aggr relation-keyword relation-type id-val-pairs)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr relation-keyword relation-type id-val-pairs]
   (add-new-relation! ^DB @*db* ^IndexAggregator aggr relation-keyword relation-type id-val-pairs))
  ([^DB db ^IndexAggregator aggr relation-keyword relation-type id-val-pairs]
   {:pre [(clojure.test/is (not-any? nil? [db aggr relation-keyword relation-type id-val-pairs
                                           (key->id ^DB db relation-keyword)]))]}
   (declare-relations! ^DB db ^IndexAggregator aggr [[relation-keyword]] :type relation-type)
   (doseq [[eid val] id-val-pairs]
     (update-0! ^DB db ^IndexAggregator aggr eid (key->id ^DB db relation-keyword) val))))

;; TODO: add pre-flight check, rename?
(defn add-new-set-property!
  ([property-keyword id-val-pairs]
   (let [aggr (aggregator)]
     (add-new-set-property! ^IndexAggregator aggr property-keyword id-val-pairs)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr property-keyword id-val-pairs]
   (add-new-set-property! ^DB @*db* ^IndexAggregator aggr property-keyword id-val-pairs))
  ([^DB db ^IndexAggregator aggr property-keyword id-val-pairs]
   (some->> id-val-pairs
            (map (fn [[id vals]] [id (some->> vals (filter identity) (into #{}) sort int-array)]))
            (filter (comp not-empty second))
            (add-new-relation! ^DB db ^IndexAggregator aggr property-keyword :to-many)
            dorun)))

(defn rename-column!
  ([set-kw from to]
   (let [aggr (aggregator)]
     (rename-column! ^IndexAggregator aggr set-kw from to)
     (apply-aggregator! ^DB @*db* ^IndexAggregator aggr)))
  ([^IndexAggregator aggr set-kw from to]
   (rename-column! ^DB @*db* ^IndexAggregator aggr set-kw from to))
  ([^DB db ^IndexAggregator aggr set-kw from to]
   {:pre [(clojure.test/is (not-any? nil? (concat [db aggr]
                                                  (mapcat (fn [x] [x (key->id ^DB db x)])
                                                          [set-kw from to]))))]}
   (let [fkw (key->id ^DB db from)
         tkw (key->id ^DB db to)]
     (doseq [id (ids ^DB db set-kw)]
       (update-0! ^DB db ^IndexAggregator aggr id tkw (value ^DB fkw id))))))

;; TODO support DB and IndexAggregator overloading, fix clj-kondo issues
;; (defn-checked add-to-one-relation!
;;   [{:keys [from to join-on fk-join-on relation]
;;     :or {fk-join-on nil}}]
;;   (declare-to-one-relation! relation)
;;   (with-aggr [aggr]
;;     (let [->ids (multi-key-map to join-on)
;;           fk (if fk-join-on fk-join-on join-on)]
;;       (->> (ids from)
;;            (map seek)
;;            (filter #(not-empty (->ids (fk %))))
;;            (mapcat (fn [m] (map #(update-0! aggr (:db/id m) (key->id relation) %) (->ids (fk m)))))
;;            dorun))))

;; TODO support DB and IndexAggregator overloading, fix clj-kondo issues
;; (defn-checked add-to-many-relation!
;;   [{:keys [from to pk-join-on join-on relation filter-to-fn]
;;     :or {filter-to-fn (constantly true)
;;          filter-from-fm (constantly true)
;;          pk-join-on nil}}]
;;   (declare-to-many-relation! relation)
;;   (with-aggr [aggr]
;;     (let [pk (if pk-join-on pk-join-on join-on)
;;           ->ids (multi-key-map from pk)]
;;       (->> (ids to)
;;            (map seek)
;;            (filter filter-to-fn)
;;            (map (juxt join-on :db/id))
;;            (group-by first)
;;            (mapcat (fn [m]
;;                      (map #(vector % (int-array (sort (into #{} (map second (second m))))))
;;                           (->ids (first m)))))
;;            (filter (comp not nil? first))
;;            (map #(update-0! aggr (first %) (key->id relation) (second %)))
;;            dorun))))
