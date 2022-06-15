(ns conceptual.datomic-compat
  (:use [conceptual.core])
  (:import [conceptual.core DBMap]))

;; datomic sucks - in a primus kinda way
;; datomic flattery
(def entid key->id)
;;(def ident id->key)

(def entity seek)
(def attribute seek)

(defn touch [^DBMap entity]
  (into {} entity))
