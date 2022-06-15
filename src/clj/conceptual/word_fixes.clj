(ns conceptual.word-fixes
  (:use [conceptual.arrays]
        [conceptual.core])
  (:require [conceptual.int-sets :as int-sets]
            [conceptual.tupl :as tupl]
            [clojure.string :as string])
  (:import [conceptual.core DB]
           [conceptual.util IntegerSets]))

(defn- string->words [s]
  (->> (.split ^String s "[\\s\\./_-]")
       (filter #(and (not= "" %)
                     (not (re-find #"^\d+$" %))))))

(defn- keyword->words [k]
  (string->words (name k)))

(defn word->prefixes [word]
  (let [cnt (.length
             ^String word)]
    (loop [result nil
           idx 1]
      (if (<= idx cnt)
        (recur (cons (subs word 0 idx) result) (inc idx))
        result))))

(defn- non-overlapping-intervals [cnt]
  (let [last-idx (dec cnt)]
    (loop [result nil
           start 0
           end 1]
      (if (= start cnt)
        result
        (recur (cons [start end] result)
               (if (= end cnt) (inc start) start)
               (if (= end cnt)
                 (+ start 2)
                 (inc end)))))))

(defn word->infixes [word]
  (->> (non-overlapping-intervals (.length ^String word))
       (map #(subs word (first %) (second %)))))

(defn group-by-txfm
  "Returns a map of the elements of coll keyed by the result of
  f on each element. The value at each key will be a vector of the
  corresponding elements after applying g, in the order they appeared in coll."
  {:static true}
  [f g coll]
  (persistent!
   (reduce
    (fn [ret x]
      (let [k (f x)]
        (assoc! ret k (conj (get ret k []) (g x)))))
    (transient {}) coll)))

(defn- keyword-fix-index
  ([idx fix-fn the-set]
   (->> the-set
     (map #(vector % (keyword->words (id->key %))))
     (mapcat #(map (fn [w] (vector w (% 0))) (% 1)))
     (group-by-txfm first second)
     (map #(vector (% 0) (IntegerSets/sortAndFilterDuplicates (int-array (% 1)))))
     (mapcat #(map (fn [fix] (vector fix (% 1))) (fix-fn (% 0))))
     (group-by-txfm first second)
     (map #(vector (% 0) (apply int-sets/union (% 1))))
     (map (fn [[k v]] (tupl/store! idx k v)))
     dorun)))

(defn build-prefix-index []
  (keyword-fix-index :prefix
                     word->prefixes
                     (int-sets/difference (ids :db/key)
                                          (ids :db/dont-index))))

(comment (time (build-prefix-index)))

(defn build-infix-index []
  (keyword-fix-index :infix
                     word->infixes
                     (int-sets/difference (ids :db/key)
                                          (ids :db/dont-index))))

(defn build-key->id-index []
  (let [idx (tupl/index :name->id)]
    (->> (ids :db/key)
         (map seek)
         (map #(vector (nsname (:db/key %)) (:db/id %)))
         (sort-by first)
         (map (fn [[k v]] (tupl/store! idx k v)))
         (dorun))))

(comment (tupl/drop-index :name->id))

(comment (tupl/index-size :name->id)
         (tupl/index-size :prefix)
         (tupl/index-size :infix))

(comment (tupl/clear! :prefix)
         (tupl/clear! :infix))

(comment (tupl/drop! :prefix)
         (tupl/drop! :infix))

(comment (tupl/drop-index :name->id))

(comment (.isClosed (tupl/index :name->id)))


(defn ids-by-prefix [s]
  (tupl/load :prefix s))

(defn ids-by-infix [s]
  (tupl/load :infix s))

(defn by-prefix [s]
  (->> (ids-by-prefix s)
       (map seek)))

(defn by-infix [s]
  (->> (ids-by-infix s)
       (map seek)))

(defn keyword-by-prefix [s]
  (->> (ids-by-prefix s)
       (map (comp :db/key seek))))

(defn keyword-by-infix [s]
  (->> (ids-by-infix s)
       (map (comp :db/key seek))))

(defn ids-by-prefix-phrase [s]
  (->> (keyword->words s)
       (map ids-by-prefix)
       (filter (comp not nil?))
       (apply int-sets/intersection)))

(defn ids-by-infix-phrase [s]
  (->> (keyword->words s)
       (map ids-by-infix)
       (filter (comp not nil?))
       (apply int-sets/intersection)))

(defn by-prefix-phrase [s]
  (->> (ids-by-prefix-phrase s)
       (map seek)))

(defn by-infix-phrase [s]
  (->> (ids-by-infix-phrase s)
       (map seek)))

(defn keywords-by-prefix-phrase [s]
  (->> (ids-by-prefix-phrase s)
       (map (comp :db/key seek))))

(defn keywords-by-infix-phrase [s]
  (->> (ids-by-infix-phrase s)
       (map (comp :db/key seek))))

(defn project-prefix-phrase [phrase]
  (let [cols (->> (ids-by-prefix-phrase phrase)
                  (map seek)
                  (filter #(not (nil? (:db/ids %)))))
        keys (mapv :db/key cols)
        _ (println keys)]
    (->> (map :db/ids cols)
         (apply int-sets/union)
         (project keys))))
