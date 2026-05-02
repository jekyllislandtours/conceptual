(ns walkthrough
  (:require
   [conceptual.core :as c]
   [conceptual.alpha.debug :as debug]))

;; create a new :default db
(c/create-db!)

;; get :default db
(c/db)

;; dumps db to console... only use when db is small
(debug/dump)

(c/seek 1)
(c/seek :db/key)
(c/seek :db/ids)
