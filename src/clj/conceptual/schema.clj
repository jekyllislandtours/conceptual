(ns conceptual.schema
  (:require [conceptual.arrays :refer :all]
            [conceptual.core :refer [*aggr*
                                     with-aggr apply-aggregator!
                                     insert! update! update-0!
                                     seek value value-0 key->id ids project]]
            [conceptual.contracts :refer [defn-checked]])
  (:import [conceptual.core IndexAggregator]
           [clojure.lang Keyword]))

(defn declare-property!
  ([^Keyword key type]
   (declare-property! key type nil))
  ([^Keyword key type args]
   (let [aggr (if (bound? #'*aggr*) *aggr* (IndexAggregator.))]
     (declare-property! aggr key type args)
     (when-not (bound? #'*aggr*)
       (apply-aggregator! aggr))))
  ([^IndexAggregator aggr ^Keyword key type args]
   {:pre [(clojure.test/is (not-any? nil? [key type]))]}
   (insert! aggr (merge {:db/key key
                         :db/type type
                         :db/property? true} args))))

(defn declare-properties! [args]
  (binding [*aggr* (IndexAggregator.)]
    (->> args
         (map #(apply declare-property! %))
         dorun)
    (apply-aggregator! *aggr*)))

(defn declare-tag!
  ([^Keyword key]
   (declare-tag! key nil))
  ([^Keyword key args]
   (declare-property! key Boolean (merge {:db/tag? true} args)))
  ([^IndexAggregator aggr ^Keyword key args]
   (declare-property! aggr key Boolean (merge {:db/tag? true} args))))

(defn declare-tags! [args]
  {:pre [(clojure.test/is (not (nil? args)))]}
  (binding [*aggr* (IndexAggregator.)]
    (->> args
         (map #(apply declare-tag! %))
         dorun)
    (apply-aggregator! *aggr*)))

(defn declare-to-one-relation!
  ([key]
   (declare-to-one-relation! key nil))
  ([key args]
   {:pre [(not (nil? key))]}
   (declare-property! key Integer (merge {:db/relation? true
                                          :db/to-one-relation? true} args))))

(defn declare-to-many-relation!
  ([key]
   (declare-to-many-relation! key nil))
  ([key args]
   {:pre [(clojure.test/is (not (nil? key)))]}
   (declare-property! key int-array-class (merge {:db/relation? true
                                                  :db/to-many-relation? true} args))))

(defn declare-relations! [args & {type :type}]
  {:pre [(clojure.test/is (not-any? nil? [args type]))]}
  (let [def-fn (case type
                 :to-one declare-to-one-relation!
                 :to-many declare-to-many-relation!)
        tuple-fn (fn [a] (if (> (count a) 1)
                           (vector (first a) {:db/inverse-relation
                                             (value :db/id (second a))}) a))]
    (binding [*aggr* (IndexAggregator.)]
      (->> args
           (map tuple-fn)
           (map #(apply def-fn %))
           dorun)
      (apply-aggregator! *aggr*))))

(defn key-map
  "Creates a map of the-key's value to id for the given set key or id.
   Assumes the mapping is one to one."
  [the-set the-key]
  {:pre [(clojure.test/is (not-any? nil? [the-set the-key (key->id the-key)]))]}
  (->> (ids the-set)
       (project [the-key :db/id])
       (into {})))

(defn multi-key-map
  "Creates a map of the-key's value to a vector of all of the ids with that value."
  [the-set the-key]
  {:pre [(clojure.test/is (not-any? nil? [the-set the-key (key->id the-key)]))]}
  (->> the-set
       ids
       (project [the-key :db/id])
       (group-by first)
       (map (fn [[k v]] [k (mapv second v)]))
       (into {})))

(defn add-inverse-relations!
  "Given an seq of [keyA keyB] adds and inverse relation
  (i.e. :db/inverse-relation) to the concept represented by keyB."
  [args]
  {:pre [(not (nil? args))]}
  (with-aggr [aggr]
    (->> args
         (map #(update! aggr {:db/key (first %)
                              :db/inverse-relation (value :db/id (second %))}))
         dorun)))

(defn add-tag!
  "Add a tag represented by the 'tag-key' to a
   set (i.e. has :db/ids) represented by 'the-set-key'"
  [the-set-key tag-key]
  {:pre [(clojure.test/is (not-any? nil? [the-set-key tag-key (key->id tag-key)]))]}
  (let [tag-id (key->id tag-key)]
    (with-aggr [aggr]
      (->> (ids the-set-key)
           (map #(update-0! aggr % tag-id true))
           (dorun)))))

(defn add-tags!
  "Adds a set of tags to the given set key (i.e. has ids)."
  [the-set-key tag-keys]
  {:pre [(clojure.test/is (not-any? nil? [the-set-key tag-keys]))]}
  (doseq [tag-key tag-keys]
    (add-tag! the-set-key tag-key)))

(defn add-tag-by-fk!
  "Adds a tag to all r.h.s. to a fk to-one-relation."
  [fk tag-key]
  {:pre [(clojure.test/is (not-any? nil? [fk tag-key (key->id fk) (key->id tag-key)]))]}
  (let [fk-id (key->id fk)
        tag-id (key->id tag-key)]
    (with-aggr [aggr]
      (->> (ids fk)
           (map #(value fk-id %))
           (map #(update-0! aggr % tag-id true))
           (dorun)))))

(defn-checked add-to-one-relation!
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

(defn-checked add-to-many-relation!
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

(defn copy-key! [copy-from copy-to]
  {:pre [(clojure.test/is (not-any? nil? [copy-from copy-to (key->id copy-from) (key->id copy-to)]))]}
  (with-aggr [aggr]
    (->> (ids copy-from)
         (map #(update-0! aggr % (key->id copy-to) (value-0 (key->id copy-from) %)))
         dorun)))

;; TODO Refactor/Rename...
(defn add-new-relation! [relation-keyword relation-type id-val-pairs]
  (declare-relations! [[relation-keyword]] :type relation-type)
  (with-aggr [aggr]
    (->> id-val-pairs
         (map (fn [[eid val]] (update-0! aggr eid (key->id relation-keyword) val)) )
         (dorun))))

;; TODO Refactor/Rename...
(defn add-new-set-property! [property-keyword id-val-pairs]
  (->> id-val-pairs
       (map (fn [[id vals]] [id (->> vals (filter identity) (into #{}) sort int-array)]))
       (filter (comp not-empty second))
       (add-new-relation! property-keyword :to-many)))

(defn rename-column [set-kw from to]
  {:pre [(clojure.test/is (not-any? nil? (mapcat (fn [x] [x (key->id x)])
                                                [set-kw from to])))]}
  (let [fkw (key->id from)
        tkw (key->id to)]
      (with-aggr [aggr]
     (->> set-kw
          ids
          (map #(update-0! aggr % tkw (value fkw %)))
          dorun))))
