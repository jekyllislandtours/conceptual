(ns walkthrough
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import [java.net URL]
           [java.io File BufferedInputStream]
           [java.util.zip GZIPInputStream]
           [java.nio.file Files StandardCopyOption]))

;; create a new :default db
(c/create-db!)

;; get :default db
(c/db)

;; dumps db to console... only use when db is small
(c/dump)

;; prints out the following
;; {:db/id 0, :db/key :db/id, :db/type java.lang.Integer, :db/property? true}
;; {:db/id 1, :db/key :db/key, :db/type clojure.lang.Keyword, :db/property? true}
;; {:db/id 2, :db/key :db/type, :db/type java.lang.Class, :db/property? true}
;; {:db/id 3, :db/key :db/property?, :db/type java.lang.Boolean, :db/property? true}

;; further bootstraps the db with some basic pre-requisites
(c/prime-db!)

;; this will recreate and prime the db
(c/reset-db!)

;; check the db now
(c/dump)

;; {:db/id 0, :db/key :db/id, :db/type java.lang.Integer, :db/property? true,
;;  :db/ids #object["[I" 0xa758e2a "[I@a758e2a"]}
;; {:db/id 1, :db/key :db/key, :db/type clojure.lang.Keyword, :db/property? true,
;;  :db/ids #object["[I" 0x476001f7 "[I@476001f7"]}
;; {:db/id 2, :db/key :db/type, :db/type java.lang.Class, :db/property? true,
;;  :db/ids #object["[I" 0x441f2e56 "[I@441f2e56"]}
;; {:db/id 3, :db/key :db/property?, :db/type java.lang.Boolean, :db/property? true, :db/tag? true,
;;  :db/ids #object["[I" 0x3b0b45c "[I@3b0b45c"]}
;; {:db/id 4, :db/key :db/fn, :db/type clojure.lang.IFn, :db/property? true}
;; {:db/id 5, :db/key :db/dont-index, :db/type java.lang.Boolean, :db/property? true}
;; {:db/id 6, :db/key :db/tag?, :db/type java.lang.Boolean, :db/property? true,
;;  :db/ids #object["[I" 0x1b96dfdd "[I@1b96dfdd"]}
;; {:db/id 7, :db/key :db/fn?, :db/type java.lang.Boolean, :db/property? true,
;;  :db/tag? true}
;; {:db/id 8, :db/key :db/relation?, :db/type java.lang.Boolean, :db/property? true,
;;  :db/tag? true, :db/ids #object["[I" 0x417b55db "[I@417b55db"]}
;; {:db/id 9, :db/key :db/to-many-relation?, :db/type java.lang.Boolean,
;;  :db/property? true, :db/tag? true, :db/ids #object["[I" 0x197a825d "[I@197a825d"]}
;; {:db/id 10, :db/key :db/to-one-relation?, :db/type java.lang.Boolean,
;;  :db/property? true, :db/tag? true, :db/ids #object["[I" 0x5590a806 "[I@5590a806"]}
;; {:db/id 11, :db/key :db/inverse-relation, :db/type java.lang.Integer,
;;  :db/property? true, :db/relation? true, :db/to-one-relation? true}
;;{:db/id 12, :db/key :db/ids, :db/type [I, :db/property? true, :db/relation? true, :db/to-many-relation? true}

;; take a look at concepts by integer id or by keyword

(c/seek 12)
(c/seek :db/ids)
