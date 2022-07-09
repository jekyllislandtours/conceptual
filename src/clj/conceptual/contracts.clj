(ns conceptual.contracts
  (:require [clojure.set :as cset]
            ;;[clojure.test :as t]
            ))

(defmacro defn-checked
  "If a function destructures its arguments, this checks the first level
   assumes that anything without a default specified with :or is required"
  [fn-name fn-args & body]
  (let [fn-args (mapv #(if (and (map? %)
                                (not (:as %)))
                         (assoc % :as (gensym "as")) %)
                      fn-args)
        key-dicts (filter map? fn-args)
        arg-keys (set
                  (mapcat #(or (:keys %)
                               (filter (comp not (set [:or :as :keys])) (keys %)))
                          key-dicts))
        defaults (apply merge (map #(:or %) key-dicts))
        ases (map :as key-dicts)
        required (cset/difference arg-keys
                                  (set (keys defaults)))
        contracts {:pre [(list 'clojure.test/is (list 'not-any? 'nil? (vec required)))
                         (list 'clojure.test/is (list 'clojure.set/subset?
                                                      (list 'set (list 'mapcat 'keys (vec ases)))
                                                      (set (map keyword arg-keys))))]}
        existing-meta (if (and (> (count body) 1)
                               (map? (first body)))
                        (first body)
                        {})
        contracts (merge-with concat existing-meta contracts)]
    (concat (list 'defn fn-name fn-args contracts)
            body)))

;; (def test-fn (defn-checked testfn [& {:keys [arg1 arg2]
;;                                       :or {arg1 1}}]
;;                (+ arg1 arg2)))

;; (defn defn-checked-test [] ;; Can't seem to use clojure.test to test a fn that uses clojure.test/is
;;   (and (t/is (thrown? AssertionError (test-fn :arg1 1)))
;;        (t/is (thrown? AssertionError (test-fn :arg3 1)))
;;        (t/is (= 2 (test-fn :arg2 1))))
;;   )

;;(defn-checked-test)
;; => true (but prints the errors... :\)
