(ns conceptual.faceting
  (:require [conceptual.core :refer [*db*]])
  (:import [conceptual.core DB]))

(defn keys-by-frequency
  ([ids] (.getKeysByFrequency ^DB @*db* ids))
  ([db ids] (.getKeysByFrequency ^DB db ids)))

(defn keys-by-frequency-with-skip
  ([skip-keys ids]
   (.getKeysByFrequency ^DB @*db* ids skip-keys))
  ([db skip-keys ids]
   (.getKeysByFrequency ^DB db ids skip-keys)))

(defn relations-by-frequency
  ([relation-key ids]
   (.getRelationsByFrequency ^DB @*db* ids relation-key))
  ([db relation-key ids]
   (.getRelationsByFrequency ^DB db ids relation-key)))

(defn relations-by-frequency-with-skip
  ([relation-key skip-keys ids]
   (.getRelationsByFrequency ^DB @*db* ids relation-key skip-keys))
  ([db relation-key skip-keys ids]
   (.getRelationsByFrequency ^DB db ids relation-key skip-keys)))
