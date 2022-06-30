(ns conceptual.format
  (:require [conceptual.core :as c])
  (:import [conceptual.core DBMap]))

(defn format-to-many-relation [r]
  (if (c/value :db/key (get r 0))
    (mapv (partial c/value :db/key) r)
    (into [] r)))

(defn format-to-one-relation [r]
  (if (c/value :db/key r)
    (c/value :db/key r)
    r))

(defn format-value [k v]
  (if (c/value :db/relation? k)
    (if (c/value :db/to-one-relation? k)
      (format-to-one-relation v)
      (format-to-many-relation v))
    (if (instance? Class v)
      (str v)
      v)))

(defn format-map [m]
  (let [ks (keys m)
        -ks (.getKeys ^DBMap m)
        vs (.getValues ^DBMap m)]
    (into {}
          (map #(vector %1 (format-value %2 %3)) ks -ks vs))))
