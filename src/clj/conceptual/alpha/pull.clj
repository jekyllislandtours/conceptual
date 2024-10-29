(ns conceptual.alpha.pull
  (:require [conceptual.core :as c]
            [conceptual.arrays :as a]))

;; inspiration from https://docs.datomic.com/query/query-pull.html

(def known-opts #{:limit :as})

(declare pull)


(defn validate-limit
  [k limit]
  (when (and (some? limit) (not (pos-int? limit)))
    (throw (ex-info "`limit` must be a positive integer"
                    {::error ::invalid-limit-option-value
                     :pull/key k}))))

(defn validate-as
  [k as]
  (when (some? as)
    (when-not (or (string? as)
                  (keyword? as)
                  (symbol? as))
      (throw (ex-info "`as` must be a string, symbol or keyword"
                      {::error ::invalid-as-option-value
                       :pull/key k})))))

(defn vector->key+opts
  [[k & kv-pairs]]
  (let [{:keys [limit as] :as opts} (if (even? (count kv-pairs))
                                      (apply hash-map kv-pairs)
                                      (throw (ex-info "Expected 0 or an even number of options for key"
                                                      {::error ::not-enough-input-for-options
                                                       :pull/key k
                                                       :pull/opts kv-pairs})))]
    (when-let [unk-ks (->> (keys opts)
                           (remove known-opts)
                           set
                           not-empty)]
      (throw (ex-info "Unknown options"
                      {::error ::unknown-options
                       :pull/key k
                       :pull/unknown-opts unk-ks})))
    (validate-limit k limit)
    (validate-as k as)
    [k opts]))

(defn parse
  [pattern]
  (let [f (fn [x]
            (cond
              (keyword? x) :kws
              (vector? x) :kw+opts
              (map? x) :relations))
        {:keys [kws kw+opts relations]} (group-by f pattern)
        kw+opts (mapv vector->key+opts kw+opts)]
    {:pull/ks (into (set kws) (map first kw+opts))
     :pull/k+opts kw+opts
     :pull/relations relations}))

(defn apply-key-opts
  ([c [k :as v]]
   ;; NB `c` must already have key `k`
   (apply-key-opts c v (get c k)))
  ([c [k {:keys [limit as]}] v]
   (let [v (cond->> v
             (and limit (or (coll? v)
                            (a/int-array? v))) (take limit))]
     (cond-> (assoc c (or as k) v)
       as (dissoc k)))))

(defn apply-all-opts
  [k+opts c]
  (loop [c c
         [k+opt & more] k+opts]
    (if-not k+opt
      c
      (recur (apply-key-opts c k+opt) more))))

(defn reify-relations*
  [{:keys [pull/relation-value] :as ctx} relation c]
  (loop [c c
         [[k-or-v pattern] & more] relation]
    (if-not k-or-v
      c
      (let [v (cond-> k-or-v
                (keyword? k-or-v) vector)
            attr-vec? (and (vector? v) (keyword? (first v)))]
        (when-not attr-vec?
          (throw (ex-info "relation must be a keyword or a vector"
                          {::error ::invalid-relation
                           :pull/key (first v)})))
        (let [[k :as k+opts] (vector->key+opts v)
              ids (relation-value {:pull/ctx ctx
                                   :pull/key k
                                   :db/id (:db/id c)})
              [k' id+] (first (apply-key-opts {} k+opts ids)) ;; output is a map with 1 entry
              xs (pull ctx pattern id+)]
          (-> c
              (assoc k' xs)
              (recur more)))))))

(defn reify-relations
  [ctx relations c]
  (loop [c c
         [r & more] relations]
    (if-not r
      c
      (recur (reify-relations* ctx r c) more))))

(defn pull
  "`id+` can be an `int`, `int-array` or any seq of int.
  `ctx` is a map and must include `:pull/relation-value` fn which takes in a
  map of keys `:pull/ctx`, `:pull/key`, `:db/id` and returns either a db id or a
  seq or int-array of db ids."
  [ctx pattern id+]
  (let [{:keys [pull/ks pull/k+opts pull/relations] :as _parsed} (parse pattern)
        rm-db-id? (not (contains? ks :db/id))
        xform (comp (map (partial apply-all-opts k+opts))
                 (map (partial reify-relations ctx relations))
                 (map (fn [c]
                        (cond-> c
                          rm-db-id? (dissoc c :db/id)))))
        one? (int? id+)
        ids (if one? [id+] id+)
        cs (into [] xform (c/project-map (conj ks :db/id) ids))]
    (cond-> cs
      one? first)))
