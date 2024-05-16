(ns conceptual.alpha.debug-test
  (:require [clojure.test :refer [deftest use-fixtures]]
            [conceptual.test.core :as test.core]
            [conceptual.alpha.debug :as debug]
            [expectations.clojure.test :refer [expect]]))

(use-fixtures :each test.core/with-rdb)

(deftest known-keys-test
  (expect #{:test/string :test/keyword :test/id :test/edn
            :test/boolean :test/collection :test/character :test/parent
            :test/class :test/external-id :test/long :test/double :test/children
            :test/tag? :test/float :test/instant :test/int :test/date}
          (debug/known-keys)))

(deftest unknown-keys-test
  (expect #{} (-> #{:test/string :test/id :test/edn}
                  debug/unknown-keys
                  :input-only-keys))
  (expect #{:missing/key} (-> #{:test/string :test/id :test/edn :missing/key}
                              debug/unknown-keys
                              :input-only-keys)))
