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

(c/seek 1)
(c/seek :db/key)
(c/seek :db/ids)
