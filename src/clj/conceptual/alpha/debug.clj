(ns conceptual.alpha.debug
  (:require [conceptual.core :as c]
            [conceptual.int-sets :as i]
            [clojure.data :as data]))

(defn known-keys
  "Returns all known keys that are not of the `:db` namespace."
  ([]
   (known-keys (c/db)))
  ([db]
   (known-keys db [:db/tag? :db/property? :db/relation?]))
  ([db tags]
   (->> tags
        (map c/ids)
        (apply i/union)
        (map (partial c/id->key db))
        (remove #(= "db" (namespace %)))
        set)))

(defn unknown-keys
  "Returns `nil` if no unknown keys are found else a map with keys `:conceptual-only-keys`
  and `:input-only-keys`. You probably want to call this immediately after loading
  a pickle. `tag` is a tag such as `:db/tag?`or `:db/property?` or `:db/relation?`.
  `ks` is a seq of keywords that are expected to be tagged with `tag`."
  ([ks]
   (unknown-keys (c/db) ks))
  ([db ks]
   (unknown-keys db ks [:db/tag? :db/property? :db/relation?]))
  ([db ks tags]
   (let [[conceptual-only input-only _] (data/diff (known-keys db tags) (set ks))]
     (when (or (seq conceptual-only)
               (seq input-only))
       {:conceptual-only-keys (into (sorted-set) conceptual-only)
        :input-only-keys (into (sorted-set) input-only)}))))
