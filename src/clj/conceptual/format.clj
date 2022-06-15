(ns conceptual.format
  (:require [conceptual.arrays :refer :all]
            [conceptual.core :refer :all]
            ;;[flatland.ordered.map :refer :all]
            )
  (:import [clojure.lang Keyword]
           [conceptual.core DBMap]))

(defn format-to-many-relation [r]
  (if (value :db/key (get r 0))
    (mapv (partial value :db/key) r)
    (into [] r)))

(defn format-to-one-relation [r]
  (if (value :db/key r)
    (value :db/key r)
    r))

(defn format-value [k v]
  (if (value :db/relation? k)
    (if (value :db/to-one-relation? k)
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
