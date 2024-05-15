(ns conceptual.alpha.debug
  (:require [conceptual.core :as c]
            [conceptual.int-sets :as i]
            [clojure.data :as data]))

(defn unknown-keys
  "Returns `nil` if no unknown keys are found else a map with keys `:pickle-only-keys`
  and `:input-only-keys`. You probably want to call this immediately after loading
  a pickle. `tag` is a tag such as `:db/tag?`or `:db/property?` or `:db/relation?`.
  `ks` is a seq of keywords that are expected to be tagged with `tag`."
  ([ks]
   (unknown-keys (c/db) ks))
  ([db ks]
   (unknown-keys db ks [:db/tag? :db/property? :db/relation?]))
  ([db ks tags]
   (let [found (->> tags
                    (map c/ids)
                    (apply i/union)
                    (map (partial c/id->key db))
                    (remove #(= "db" (namespace %)))
                    set)
         [pickle-only input-only _] (data/diff found (set ks))]
     (when (or (seq pickle-only)
               (seq input-only))
       {:pickle-only-keys (into (sorted-set) pickle-only)
        :input-only-keys (into (sorted-set) input-only)}))))
