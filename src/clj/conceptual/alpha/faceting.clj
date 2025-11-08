(ns conceptual.alpha.faceting
  "This is an experimental namespace and is subject to change or removal."
  (:require
   [conceptual.core :as c :refer [*db*]])
  (:import
   (conceptual.alpha FacetsAlpha)))

(defn to-many-relations-by-frequency
  "An implementation that does not use an `IntArrayPool`."
  (^int/1 [relation-key min-id max-id ids]
   (to-many-relations-by-frequency @*db* relation-key min-id max-id ids))
  (^int/1 [db relation-key min-id max-id ids]
   (FacetsAlpha/getToManyRelationsByFrequency db ids relation-key min-id max-id)))

(defn to-one-relations-by-frequency
  "An implementation that does not use an `IntArrayPool`."
  (^int/1 [relation-key min-id max-id ids]
   (to-one-relations-by-frequency @*db* relation-key min-id max-id ids))
  (^int/1 [db relation-key min-id max-id ids]
   (FacetsAlpha/getToOneRelationsByFrequency db ids relation-key min-id max-id)))
