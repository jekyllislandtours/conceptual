(ns conceptual.word-fixes
  (:require [conceptual.core :as c])
  (:require [conceptual.int-sets :as int-sets])
  (:import [conceptual.util IntegerSets]))

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
  (loop [result nil
         start 0
         end 1]
    (if (= start cnt)
      result
      (recur (cons [start end] result)
             (if (= end cnt) (inc start) start)
             (if (= end cnt)
               (+ start 2)
               (inc end))))))

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
  [fix-fn the-set on-kv-fn]
  (->> the-set
       (map #(vector % (keyword->words (c/id->key %))))
       (mapcat #(map (fn [w] (vector w (% 0))) (% 1)))
       (group-by-txfm first second)
       (map #(vector (% 0) (IntegerSets/sortAndFilterDuplicates (int-array (% 1)))))
       (mapcat #(map (fn [fix] (vector fix (% 1))) (fix-fn (% 0))))
       (group-by-txfm first second)
       (map #(vector (% 0) (apply int-sets/union (% 1))))
       (map (fn [[k v]] (on-kv-fn k v)))
       dorun))

(defn build-prefix-index
  ([]
   (let [m (volatile! {})]
     (build-prefix-index (fn [k v] (vswap! m assoc k v)))
     @m))
  ([on-kv-fn]
   (keyword-fix-index word->prefixes
                      (int-sets/difference (c/ids :db/key)
                                           (c/ids :db/dont-index))
                      on-kv-fn)))

(comment (time (build-prefix-index)))

(defn build-infix-index
  ([]
   (let [m (volatile! {})]
     (build-infix-index (fn [k v] (vswap! m assoc k v)))
     @m))
  ([on-kv-fn]
   (keyword-fix-index word->infixes
                      (int-sets/difference (c/ids :db/key)
                                           (c/ids :db/dont-index))
                      on-kv-fn)))

(defn build-key->id-index
  "`on-kv-fn` is a 2 arg fn which receives the key and value"
  ([]
   (let [m (volatile! {})]
     (build-key->id-index (fn [k v] (vswap! m assoc k v)))
     @m))
  ([on-kv-fn]
   (->> (c/ids :db/key)
        (map c/seek)
        (map #(vector (c/nsname (:db/key %)) (:db/id %)))
        (sort-by first)
        (map (fn [[k v]] (on-kv-fn k v)))
        (dorun))))

(defn ids-by-prefix-phrase
  "Returns the intersection of ids matching the prefix `s`.
  `ids-by-prefix-fn` is a 1 arity fn that receives a string
  and returns an integer set."
  [ids-by-prefix-fn s]
  (->> (keyword->words s)
       (map ids-by-prefix-fn)
       (filter some?)
       (apply int-sets/intersection)))

(defn ids-by-infix-phrase
  "Returns the intersection of ids matching the infix `s`.
  `ids-by-infix-fn` is a 1 arity fn that receives a string
  and returns an integer set."
  [ids-by-infix-fn s]
  (->> (keyword->words s)
       (map ids-by-infix-fn)
       (filter some?)
       (apply int-sets/intersection)))

(defn project-prefix-phrase
  [ids-by-prefix-fn phrase]
  (let [cols (->> (ids-by-prefix-phrase ids-by-prefix-fn phrase)
                  (map c/seek)
                  (filter :db/ids))
        keys (mapv :db/key cols)]
    (->> (map :db/ids cols)
         (apply int-sets/union)
         (c/project keys))))
