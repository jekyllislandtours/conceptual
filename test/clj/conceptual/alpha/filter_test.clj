(ns conceptual.alpha.filter-test
  (:require [clojure.test :refer [deftest use-fixtures testing]]
            [conceptual.test.core :as test.core]
            [conceptual.core :as c]
            [clojure.spec.alpha :as s]
            [conceptual.alpha.filter :as f]
            [expectations.clojure.test :refer [expect]])
  (:import (clojure.lang ExceptionInfo)))



(use-fixtures :each test.core/with-rdb)


(deftest op-sexp-test
  (expect [:sexp/op-field-val
           #:filter{:op [:op/comparison '=]
                    :field 'foo/bar
                    :value [:type/number 23]}]
          (s/conform ::f/op-sexp '(= foo/bar 23)))

  (expect [:sexp/op-val-field
           #:filter{:op [:op/comparison '=], :value [:type/number 23], :field 'foo/bar}]
          (s/conform ::f/op-sexp '(= 23 foo/bar)))

  (expect [:sexp/op-field-val
           #:filter{:op [:op/set 'in],
                    :field 'foo/bar,
                    :value [:type/numbers-coll #{23 42}]}]
          (s/conform ::f/op-sexp '(in foo/bar [23 42])))

  (expect false (s/valid? ::f/op-sexp '(-> 23 foo/bar)))
  (expect false (s/valid? ::f/op-sexp '(= 23 foo/bar 45))))

(deftest sexp-test
  (expect [:sexp/op
           [:sexp/op-field-val
            #:filter{:op [:op/set 'in]
                     :field 'life/answer
                     :value [:type/numbers-coll #{99 42}]}]]
          (s/conform ::f/sexp '(in life/answer [42 99])))

  (expect [:sexp/logical
           {:op/boolean 'and
            :list/sexp
            [[:sexp/logical
              {:op/boolean 'or
               :list/sexp
               [[:sexp/op
                 [:sexp/op-val-field
                  #:filter{:op [:op/comparison '=]
                           :value [:type/number 23],
                           :field 'foo/bar}]]
                [:sexp/op
                 [:sexp/op-val-field
                  #:filter{:op [:op/comparison '=]
                           :value [:type/boolean true]
                           :field 'some/tag?}]]]}]
             [:sexp/op
              [:sexp/op-field-val
               #:filter{:op [:op/comparison '>]
                        :field 'life/answer
                        :value [:type/number 42]}]]]}]
          (s/conform ::f/sexp '(and (or (= 23 foo/bar)
                                        (= true some/tag?))
                                    (> life/answer 42)))))


(defn eval-sexp
  ([sexp] (eval-sexp sexp (c/ids :test/id)))
  ([sexp init-ids]
   (->> (f/evaluate sexp init-ids)
        (map (partial c/value :test/id))
        set)))


(deftest simple-evaluate-numbers-test
  (binding [f/*enable-index-scan* true]
    (testing "= op field value"
      (expect #{:hello/friend :hello/dude}
              (eval-sexp '(= test/int 3456))))
    (testing "= op value field"
      (expect #{:hello/friend :hello/dude}
              (eval-sexp '(= 3456 test/int))))


    (testing "> op field value"
      (expect #{:hello/friend :hello/dude :hello/there}
              (eval-sexp '(> test/int 2000))))

    (testing "> op value field"
      (expect #{:hello/world}
              (eval-sexp '(> 2000 test/int))))

    (testing ">= op field value"
      (expect #{:hello/friend :hello/there :hello/dude}
              (eval-sexp '(>= test/int 2345))))

    (testing ">= op value field"
      (expect #{:hello/world :hello/there}
              (eval-sexp '(>= 2345 test/int))))))

(deftest simple-evaluate-strings-test
  (binding [f/*enable-index-scan* true]
    (testing "= op field value"
      (expect #{:hello/dude}
              (eval-sexp '(= test/string "Dude"))))

    (testing "= op value field"
      (expect #{:hello/dude}
              (eval-sexp '(= "Dude" test/string))))

    (testing "> throws"
      (expect ExceptionInfo
              (eval-sexp '(> "Dude" test/string))))

    (testing ">= throws"
      (expect ExceptionInfo
              (eval-sexp '(>= "Dude" test/string))))

    (testing "< throws"
      (expect ExceptionInfo
              (eval-sexp '(< "Dude" test/string))))

    (testing "<= throws"
      (expect ExceptionInfo
              (eval-sexp '(<= "Dude" test/string))))))


(deftest simple-tag-test
  ;; NB doesn't require f/*enable-index-scan* binding to be set
  (testing "true op field value"
    (expect #{:hello/world :hello/there :hello/dude}
            (eval-sexp '(= test/tag? true))))

  (testing "true op value field"
    (expect #{:hello/world :hello/there :hello/dude}
            (eval-sexp '(= true test/tag?))))

  (testing "false op field value"
    (expect #{:hello/friend}
            (eval-sexp '(= test/tag? false))))

  (testing "false op value field"
    (expect #{:hello/friend}
            (eval-sexp '(= false test/tag?)))))


(deftest and-test
  (binding [f/*enable-index-scan* true]
    (testing "degenerate case"
      (expect #{:hello/dude :hello/there :hello/world}
              (eval-sexp '(and (= test/tag? true)))))

    (testing "tag with comparison"
      (expect #{:hello/dude :hello/there}
              (eval-sexp '(and (= test/tag? true)
                               (> test/int 2000)))))

    (testing "tag with comparison"
      (expect #{:hello/dude}
              (eval-sexp '(and (= test/tag? true)
                               (> test/int 2000)
                               (= test/string "Dude")))))

    (testing "order indifferent"
      (expect true
              (= #{:hello/dude}
                 (eval-sexp '(and (= test/int 3456)
                                  (= test/string "Dude")))
                 (eval-sexp '(and (= test/string "Dude")
                                  (= test/int 3456))))))

    (testing "oddly structured"
      (expect #{:hello/dude :hello/there}
              (eval-sexp '(and (= test/tag? true)
                               (and (> test/int 2000)))))

      (expect #{:hello/dude}
              (eval-sexp '(and (= test/tag? true)
                               (and (> test/int 2000)
                                    (and (= test/string "Dude"))))))

      (expect #{:hello/dude}
              (eval-sexp '(and (= test/tag? true)
                               (and (> test/int 2000)
                                    (= test/string "Dude"))))))))


(deftest or-test
  (binding [f/*enable-index-scan* true]
    (testing "degenerate case"
      (expect #{:hello/dude :hello/there :hello/world}
              (eval-sexp '(or (= test/tag? true)))))

    (testing "tag with comparison"
      (expect #{:hello/dude :hello/there :hello/world :hello/friend}
              (eval-sexp '(or (= test/tag? true)
                              (= test/int 3456)))))

    (testing "> and ="
      (expect #{:hello/friend :hello/world}
              (eval-sexp '(or (= test/string "World")
                              (= test/string "Friend")))))

    (testing "order indifferent"
      (expect true
              (= #{:hello/friend :hello/world}
                 (eval-sexp '(or (= test/string "World")
                                 (= test/string "Friend")))
                 (eval-sexp '(or (= test/string "Friend")
                                 (= test/string "World"))))))

    (testing "oddly structured"
      (expect #{:hello/friend :hello/world}
              (eval-sexp '(or (= test/string "World")
                              (or (= test/string "Friend"))))))))


(deftest and-or-test
  (binding [f/*enable-index-scan* true]
    (testing "base"
      (expect #{:hello/friend}
              (eval-sexp '(and (= test/int 3456)
                               (or (= test/string "World")
                                   (= test/string "Friend"))))))

    (testing "order indifferent"
      (expect true
              (= #{:hello/friend}
                 (eval-sexp '(and (= test/int 3456)
                                  (or (= test/string "World")
                                      (= test/string "Friend"))))
                 (eval-sexp '(and (or (= test/string "World")
                                      (= test/string "Friend"))
                                  (= test/int 3456))))))))


(deftest or-and-test
  (binding [f/*enable-index-scan* true]
    (testing "base"
      (expect #{:hello/friend :hello/dude :hello/there}
              (eval-sexp '(or (= test/int 3456) ;; friend and dude
                               (and (= test/boolean false) ;; there
                                    (= test/string "There"))))))))
