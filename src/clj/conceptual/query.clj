(ns conceptual.query
  (:require [conceptual.arrays :refer [int-array?]]
            [conceptual.core :refer [seek value]])
  (:import [conceptual.core DBMap]))

(defn db-map? [arg]
  (instance? DBMap arg))

(defn ^:private path-with-crumbs [result path]
  (if result
    (cond
     (db-map? result) (if (:db/ids result)
                        (map #(vector % (value path %)) (:db/ids result))
                        (result path))
     (map? result) (result path)
     (seq? result) (map #(vector % (let [l (second %)]
                                    (cond
                                     (number? l) (value path l)
                                     (int-array? l) (map (partial value path) l)))) result)
     :else :else-case)
    (seek path)))

(defn ^:private path-with-no-crumbs [result path]
  (if result
    (cond
     (number? result) (value path result)
     (:db/ids result) (map #(value path %) (:db/ids result))
     (map? result) (result path)
     (seq? result) (map #(let [l (second %)]
                           (cond
                            (number? l) (value path l)
                            ;;(int-array? l) (map (partial value path) l)
                            ))
                        result)
     ;;(map #(value path %) result)
     :else :else-case)
    (seek path)))

(defn ^:private path-0
  ([result paths crumbs?]
   (if (seq paths)
     (if crumbs?
       (recur (path-with-crumbs result (first paths)) (rest paths) crumbs?)
       (recur (path-with-no-crumbs result (first paths)) (rest paths) crumbs?))
     (cond (int-array? result) (map seek result)
           :else result))))

(defn path
  ([paths & {:keys [crumbs?] :or {crumbs? true}}]
   (path-0 nil paths crumbs?)))

(comment (path [:employee? :manager :reports :manager] :crumbs? true))

;;; TODO

;; render functions
;; query language with "pathing"
;; optimize methods above
;; consider argument ordering
;; datalog'ish query language

(defn- clause-type? [arg]
  (case arg
    :find true
    :where true
    false))

;;(contains? #{:find :where} :find)

;;(clause-type? 1)

(defn- update-result [result curr-type arg]
  (assoc result curr-type (conj (result curr-type) arg)))

(defn- parse-q [arg]
  (loop [remainder arg
         curr-type nil
         result {:find []
                 :where []}]
    (if-not (seq remainder)
      result
      (let [next-arg (first remainder)
            clause? (clause-type? next-arg)
            curr-type (if clause? next-arg curr-type)
            result (if clause? result (update-result result curr-type next-arg))]
        (recur (rest remainder) curr-type result)))))

(defn q [arg]
  (parse-q arg))
