(ns conceptual.alpha.pull
  (:require [conceptual.core :as c]
            [conceptual.arrays :as a]
            [conceptual.int-sets :as i]
            [conceptual.alpha.filter :as c.filter]))

;; inspiration from https://docs.datomic.com/query/query-pull.html

;; TODO:
;; - wildcard support
;;

(def known-opts #{:limit :as :filter})

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

(def default-key+opts-fn (juxt :pull/key :pull/key-opts))

(defn vector->key+opts
  [{:keys [pull/key+opts-fn]
    :or {key+opts-fn default-key+opts-fn}:as ctx} [k & kv-pairs]]
  (let [{:keys [limit as] :as opts} (if (even? (count kv-pairs))
                                      (apply hash-map kv-pairs)
                                      (throw (ex-info "Expected 0 or an even number of options for key"
                                                      {::error ::not-enough-input-for-options
                                                       :pull/key k
                                                       :pull/opts kv-pairs})))
        f-sexp (:filter opts)
        opts (cond-> opts
               f-sexp (update :filter c.filter/conform))]
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
    (key+opts-fn (cond-> {:pull/ctx ctx :pull/key k :pull/key-opts opts}
                   f-sexp (assoc :pull/filter f-sexp)))))

(defn parse
  [ctx pattern]
  (let [f (fn [x]
            (cond
              (keyword? x) :kws
              (vector? x) :kw+opts
              (map? x) :relations))
        {:keys [kws kw+opts relations]} (group-by f pattern)
        kw+opts (mapv (partial vector->key+opts ctx) kw+opts)]
    {:pull/ks (into (set kws) (map first kw+opts))
     :pull/k+opts kw+opts
     :pull/relations relations}))

(defn apply-key-opts
  [c [k {:keys [limit as]}]]
  ;; NB `c` must already have key `k`
  (let [v (get c k)
        many? (coll? v)
        v' (cond->> v
             (and limit many?) (take limit))]
    (cond-> (assoc c (or as k) v')
      as (dissoc k))))

(defn apply-relation-key-opts
  [[k {:keys [limit as] f-sexp :filter}] id+]
  (let [many? (a/int-array? id+)
        v (if (and many? f-sexp)
            (c.filter/evaluate-conformed f-sexp id+)
            id+)
        v' (cond->> v
             (and limit many?) (take limit))]
    (cond-> {:k (or as k)
             :v v'}
      (and limit many?) (assoc :v-pre-limit v))))

(defn apply-all-opts
  [k+opts c]
  (loop [c c
         [k+opt & more] k+opts]
    (if-not k+opt
      c
      (recur (apply-key-opts c k+opt) more))))

(defn default-relation-value
  [m]
  (c/value (:pull/key m) (:db/id m)))


(defn reify-relations*
  [{:keys [pull/relation-value
           pull/relation-finalizer]
    :or {relation-value default-relation-value
         relation-finalizer :pull/concept} :as ctx} relation c]
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
        (let [[k k-opts :as k+opts] (vector->key+opts ctx v)
              rel-opts {:pull/ctx ctx
                        :pull/key k
                        :pull/key-opts k-opts
                        :pull/concept c
                        :db/id (:db/id c)}
              id+ (relation-value rel-opts)
              ids (if (int? id+) id+ (i/set id+))
              {k' :k id+ :v pre-limit-ids :v-pre-limit} (apply-relation-key-opts k+opts ids)
              rel-opts (cond-> rel-opts
                         pre-limit-ids (assoc :pull/pre-limit-ids pre-limit-ids))
              xs (pull ctx pattern id+)
              c (-> c
                    (dissoc k)
                    (assoc k' xs))
              c (relation-finalizer (assoc rel-opts :pull/concept c :pull/renamed-key k'))]
          (recur c more))))))

(defn reify-relations
  [ctx relations c]
  (loop [c c
         [r & more] relations]
    (if-not r
      c
      (recur (reify-relations* ctx r c) more))))


(defn map-invert+
  [m]
  (persistent!
   (reduce-kv (fn [ans k v]
             (let [ks (ans v #{})]
               (assoc! ans v (conj ks k))))
           (transient {})
           m)))

(def default-finalizer :pull/concept)

(defn pull
  "`id+` can be an `int`, `int-array` or any seq of int.
  `ctx` is a map and can include fns to customize behavior. All the fns
  receive a map with key `:pull/ctx`.

  `:pull/relation-value`
     - fn which takes in a map and returns either a conceptual db/id or a sequence or int array of db/ids
     - input map has keys `:pull/ctx`, `:pull/key`, `:pull/key-opts`, `:pull/concept` and `:db/id`
     - `:pull/concept` is the current concept being built up

  `:pull/relation-finalizer`
     - fn which takes in a map and returns either a conceptual db/id or a sequence or int array of db/ids
     - input map has keys `:pull/ctx`, `:pull/key`, `:pull/key-opts`, `:pull/concept`, `:db/id` and
       optionally `:pull/pre-limit-ids`
     - `:pull/concept` is the current concept being built up

  `:pull/concept-finalizer`
     - fn for apply custom business logic to a concept before it is added to output. Returns the concept.
     - input map has keys `:pull/ctx`, `:pull/as->k`, `:pull/k->as`, and `:pull/concept`
     - `:pull/concept` is the current concept being built up"
  [{:keys [pull/concept-finalizer] :or {concept-finalizer default-finalizer} :as ctx} pattern id+]
  (let [{:keys [pull/ks pull/k+opts pull/relations] :as _parsed} (parse ctx pattern)
        rm-db-id? (not (contains? ks :db/id))
        as->k (into {} (for [[k {:keys [as]}] k+opts
                             :when as]
                         [as k]))
        k->as (map-invert+ as->k)
        cb-opts {:pull/ctx ctx
                 :pull/as->k as->k
                 :pull/k->as k->as}
        xform (comp (map (partial apply-all-opts k+opts))
                 (map (partial reify-relations ctx relations))
                 (map #(concept-finalizer (assoc cb-opts :pull/concept %)))
                 (map (fn [c]
                        (cond-> c
                          rm-db-id? (dissoc c :db/id)))))
        one? (int? id+)
        ids (if one? [id+] id+)
        cs (into [] xform (c/project-map (conj ks :db/id) ids))]
    (cond-> cs
      one? first)))
