(ns conceptual.test.core
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Keyword PersistentHashMap PersistentVector]
           [java.util Date]
           [java.time Instant]
           [conceptual.core DBTranscoder RDB]))

(defn setup []
  (println "  conceptual test setup:"))


(defn teardown []
  (println "  conceptual test teardown:"))


(defn kaocha-pre-hook! [config]
  (println "Koacha pre hook")
  (setup)
  config)


(defn kaocha-post-hook! [result]
  (println "Koacha post hook")
  (teardown)
  result)


(defn declare-test-schema! []
  (s/declare-properties!
   [[:test/string String]
    [:test/id Keyword]
    [:test/keyword Keyword]
    [:test/int Integer]
    [:test/long Long]
    [:test/float Float]
    [:test/double Double]
    [:test/character Character]
    [:test/boolean Boolean]
    [:test/date Date]
    [:test/instant Instant]
    [:test/class Class]
    [:test/edn PersistentHashMap]
    [:test/collection PersistentVector]
    [:test/external-id String]

    [:sf/captain-id String]
    [:sf/id String {:db/unique? true}]
    [:sf/member-ids PersistentVector]
    [:sf/name String]
    [:sf/position String]
    [:sf/rank String]
    [:sf/registry String]
    [:sf/starship-id String]
    [:sf/team-ids PersistentVector]
    [:sf/type Keyword]])
  (s/declare-to-one-relations! [[:test/parent]])
  (s/declare-to-many-relations! [[:test/children]])
  (s/declare-tags! [[:test/tag?]
                    [:test/nice?]]))

(def sf-test-data
  [;; USS Enterprise
   ;; NB. not that you'd model teams this way but just to help test hierarchical pull
   ;;     team should probably have :sf/starship-id
   {:sf/id "uss-e"
    :sf/type :sf.type/starship
    :sf/name "USS Enterprise"
    :sf/registry "NCC-1701-D"
    :sf/captain-id "picard"
    :sf/team-ids ["uss-e-bridge-team" "uss-e-security-team" "uss-e-eng-team" "uss-e-away-team" "uss-e-med-team"]}

   ;; USS Enterprise Crew
   {:sf/id "picard"
    :sf/type :sf.type/crew
    :sf/name "Jean-Luc Picard"
    :sf/rank "Captain"
    :sf/starship-id "uss-e"}
   {:sf/id "riker"
    :sf/type :sf.type/crew
    :sf/name "William T. Riker"
    :sf/rank "Commander"
    :sf/starship-id "uss-e"}
   {:sf/id "troi"
    :sf/type :sf.type/crew
    :sf/name "Deanna Troi"
    :sf/rank "Lieutenant Commander"
    :sf/starship-id "uss-e"}
   {:sf/id "data"
    :sf/type :sf.type/crew
    :sf/name "Data"
    :sf/rank "Lieutenant Commander"
    :sf/starship-id "uss-e"}
   {:sf/id "yar"
    :sf/type :sf.type/crew
    :sf/name "Tasha Yar"
    :sf/rank "Lieutenant"
    :sf/position "Chief Tactical Officer"
    :sf/starship-id "uss-e"}
   {:sf/id "worf"
    :sf/type :sf.type/crew
    :sf/name "Worf"
    :sf/rank "Lieutenant Junior Grade"
    :sf/starship-id "uss-e"}
   {:sf/id "la-forge"
    :sf/type :sf.type/crew
    :sf/name "Geordi La Forge"
    :sf/rank "Lieutenant Junior Grade"
    :sf/starship-id "uss-e"}
   {:sf/id "bev-crusher"
    :sf/type :sf.type/crew
    :sf/name "Beverly Crusher"
    :sf/position "Chief Medical Officer"
    :sf/rank "Commander"
    :sf/starship-id "uss-e"}
   {:sf/id "wes-crusher"
    :sf/type :sf.type/crew
    :sf/name "Wesley Crusher"
    :sf/rank "Civilian"
    :sf/starship-id "uss-e"}

   ;; USS Enterprise Teams
   {:sf/id "uss-e-bridge-team"
    :sf/type :sf.type/team
    :sf/name "Bridge Team"
    :sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
   {:sf/id "uss-e-security-team"
    :sf/type :sf.type/team
    :sf/name "Security Team"
    :sf/member-ids ["yar" "worf"]}
   {:sf/id "uss-e-eng-team"
    :sf/type :sf.type/team
    :sf/name "Engineering Team"
    :sf/member-ids ["la-forge"]}
   {:sf/id "uss-e-away-team"
    :sf/type :sf.type/team
    :sf/name "Away Team"
    :sf/member-ids ["riker" "data" "worf"]}
   {:sf/id "uss-e-med-team"
    :sf/type :sf.type/team
    :sf/name "Medical Team"
    :sf/member-ids ["bev-crusher" "troi"]}


   ;; USS Defiant
   {:sf/id "uss-d"
    :sf/type :sf.type/starship
    :sf/name "USS Defiant"
    :sf/captain-id "sisko"
    :sf/team-ids ["uss-d-med-team" "uss-d-bridge-team"]}

   ;; USS Defiant Crew
   {:sf/id "sisko"
    :sf/type :sf.type/crew
    :sf/name "Benjamin Sisko"
    :sf/rank "Captain"
    :sf/starship-id "uss-d"}
   {:sf/id "nerys"
    :sf/type :sf.type/crew
    :sf/name "Kira Nerys"
    :sf/rank "Major"
    :sf/starship-id "uss-d"}
   {:sf/id "odo"
    :sf/type :sf.type/crew
    :sf/name "Odo"
    :sf/rank "Constable"
    :sf/starship-id "uss-d"}
   {:sf/id "quark"
    :sf/type :sf.type/crew
    :sf/name "Quark"
    :sf/rank "Civilian"
    :sf/starship-id "uss-d"}


   ;; USS Defiant Teams
   {:sf/id "uss-d-med-team"
    :sf/type :sf.type/team
    :sf/name "Medical Team"
    :sf/member-ids ["odo"]}
   {:sf/id "uss-d-bridge-team"
    :sf/type :sf.type/team
    :sf/name "Bridge Team"
    :sf/member-ids ["sisko" "nerys"]}])

(def +test-data+
  [{:db/key :hello/world
    :test/id :hello/world
    :test/string "World"
    :test/keyword :hello/world
    :test/int 1234
    :test/long 12345678910
    :test/float 3.14159
    :test/double 2.71828
    :test/character \a
    :test/boolean true
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "world" 1 [2 3]}
    :test/tag? true
    :test/collection [100 101 102]
    :test/external-id "external-id-4-world"}
   {:db/key :hello/there
    :test/id :hello/there
    :test/string "There"
    :test/keyword :hello/there
    :test/int 2345
    :test/long 23456789101
    :test/float 3.14159
    :test/double 2.71828
    :test/character \b
    :test/boolean false
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "there" 2 [3 4]}
    :test/tag? true
    :test/collection [200 201 202]}
   {:db/key :hello/dude
    :test/id :hello/dude
    :test/string "Dude"
    :test/keyword :hello/dude
    :test/int 3456
    :test/long 34567891012
    :test/float 3.14159
    :test/double 2.71828
    :test/character \c
    :test/boolean true
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "dude" 1 [5 6]}
    :test/tag? true
    :test/nice? true
    :test/collection [300 201 202]
    :test/external-id "external-id-4-dude"}
   {:db/key :hello/friend
    :test/id :hello/friend
    :test/string "Friend"
    :test/keyword :hello/friend
    :test/int 3456
    :test/long 44444444444
    :test/float 3.14159
    :test/double 2.71828
    :test/character \d
    :test/boolean false
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "friend" 2 [9 7]}
    :test/collection ["abc" "def" "ghi"]}])

(defn insert-test-data! []
  (c/with-aggr [aggr]
    (doseq [data +test-data+]
      (c/insert! aggr data))
    (doseq [data sf-test-data]
      (c/insert! aggr data)))
  (c/with-aggr [aggr]
    (c/update! {:db/key :hello/there
                :test/parent (c/value :db/id :hello/world)})
    (c/update! {:db/key :hello/dude
                :test/parent (c/value :db/id :hello/world)})
    (c/update! {:db/key :hello/world
                :test/children (int-array [(c/value :db/id :hello/there)
                                           (c/value :db/id :hello/dude)])})))

;;(c/create-db!)
;;(declare-test-schema!)
(defn with-persistentdb [f]
  (c/create-db!)
  (declare-test-schema!)
  (insert-test-data!)
  (f))

(defn with-rdb [f]
  (c/create-db!)
  (declare-test-schema!)
  (insert-test-data!)
  (c/compact!)
  (f))
