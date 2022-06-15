(ns conceptual.triples
  (:use [conceptual.core]
        [conceptual.arrays])
  (:import [java.io Writer]
           [conceptual.core DB DBMap$DBMapEntry]))

(defn- map-entry->triples [e a v]
  (if (instance? int-array-class v)
    (map #(vector e a (seek % :db/key)) v)
    (vector e a v)))

(defn- ->triple-reducer [s t]
  (if (seq t)
    (reduce cons s t)
    (cons t s)))

(defn ->triples [id]
  (let [m (seek id)
        e (:db/key m)]
    (loop [kvs (seq m)
           result []]
      (if (seq kvs)
        (let [entry (first kvs)
              k (.key ^DBMap$DBMapEntry entry)
              v (.val ^DBMap$DBMapEntry entry)]
          (if (not= k :db/ids)
            (recur (rest kvs)
                   (if (instance? int-array-class v)
                     (reduce conj result (map-entry->triples e k v))
                     (conj result (map-entry->triples e k v))))
            (recur (rest kvs) result)))
        result))))

(defn- triple->str [[e a v]]
  (str (nsname e) "\t"
       (nsname a) "\t"
       (if (keyword? v) (nsname v) v) "\r\n"))

(defn write! [^java.io.Writer w ^String s]
  (.write ^java.io.Writer w ^String s))

(defn ->triple-file [the-file the-set]
  (with-open [w (clojure.java.io/writer the-file)]
    (loop [ts the-set]
      (when-let [a (first ts)]
        (write! w (->> a ->triples (mapcat triple->str) (apply str)))
        (recur (rest ts))))))

(comment (->triple-file "reports/triples.tsv" (range (inc (max-id)))))
