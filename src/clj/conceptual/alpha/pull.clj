(ns conceptual.alpha.pull
  (:require [conceptual.core :as c]
            [conceptual.arrays :as a]
            [conceptual.int-sets :as i]
            [conceptual.alpha.filter :as c.filter]))

;; inspiration from https://docs.datomic.com/query/query-pull.html

(set! *warn-on-reflection* true)

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

(defn variable?
  [x]
  (and (symbol? x)
       (-> x name (.charAt 0) (= \$))))


(defn vector->key+opts
  [{:keys [pull/variable-value]} [k & kv-pairs]]
  (let [{:keys [limit as] :as opts} (if (even? (count kv-pairs))
                                      (apply hash-map kv-pairs)
                                      (throw (ex-info "Expected 0 or an even number of options for key"
                                                      {::error ::not-enough-input-for-options
                                                       :pull/key k
                                                       :pull/opts kv-pairs})))
        f-sexp|var (:filter opts)
        f-sexp (if (variable? f-sexp|var)
                 (get variable-value f-sexp|var)
                 f-sexp|var)
        opts (cond-> opts
               f-sexp (assoc :filter (c.filter/conform f-sexp)))]
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
  [ctx pattern]
  (let [ks (java.util.ArrayList.)
        rels (java.util.ArrayList.)]
    (loop [[x & more] pattern]
      (if-not x
        {:pull/key+opts (vec ks)
         :pull/relations (vec rels)}
        (do
          (cond
            (keyword? x) (.add ks (vector->key+opts ctx [x]))
            (vector? x) (.add ks (vector->key+opts ctx x))
            (map? x) (.add rels x))
          (recur more))))))

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
                        :pull/depth (::depth ctx)
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
  (let [ctx (update ctx ::depth (fnil inc 0))]
    (loop [c c
           [r & more] relations]
      (if-not r
        c
        (recur (reify-relations* ctx r c) more)))))


(defn map-invert+
  [m]
  (persistent!
   (reduce-kv (fn [ans k v]
                (let [ks (ans v #{})]
                  (assoc! ans v (conj ks k))))
              (transient {})
              m)))

(def default-finalizer :pull/concept)

(defn default-pattern-finalizer
  [{:keys [pull/key+opts pull/relations]}]
  {:pull/key+opts key+opts
   :pull/relations relations})

(defn pull
  "`id+` can be an `int`, `int-array` or any seq of int.
  `ctx` is a map and can include fns to customize behavior. All the fns
  receive a map with key `:pull/ctx`.

  `:pull/pattern-finalizer`
     - fn which takes in a map and returns a map with keys `:pull/key+opts` and `:pull/relations`
     - input map has keys `:pull/ctx`, `:pull/key+opts` and `:pull/relations`
     - opportunity to do any validation checks or transformations

  `:pull/variable-value`
     - a map of symbol variables to values. variables must begin with a `$` char
     - only the `:filter` option on an expanded relation supports variables

  `:pull/relation-value`
     - fn which takes in a map and returns either a conceptual db/id or a sequence or int array of db/ids
     - input map has keys `:pull/ctx`, `:pull/key`, `:pull/key-opts`, `:pull/concept`, `:pull/depth` and `:db/id`
     - `:pull/concept` is the current concept being built up
     - `:pull/depth` is a zero based integer indicating the depth of the graph at this point
     - an opportunity to enforce depth and return relation values based on authorization etc

  `:pull/relation-finalizer`
     - fn which takes in a map and returns either a conceptual db/id or a sequence or int array of db/ids
     - input map has keys `:pull/ctx`, `:pull/key`, `:pull/key-opts`, `:pull/concept`, `:db/id` and
       optionally `:pull/pre-limit-ids`.
     - `:pull/concept` is the current concept being built up
     - an opportunity to associate a metadata ie to convey total count when limit is used, etc
     - returns the concept

  `:pull/concept-finalizer`
     - fn for apply custom business logic to a concept before it is added to output. Returns the concept.
     - input map has keys `:pull/ctx`, `:pull/as->k`, `:pull/k->as`, and `:pull/concept`
     - `:pull/concept` is the current concept being built up"
  [{:keys [pull/concept-finalizer
           pull/pattern-finalizer] :or {concept-finalizer default-finalizer
                                        pattern-finalizer default-pattern-finalizer} :as ctx} pattern id+]
  (let [{:keys [pull/key+opts pull/relations]} (-> (parse ctx pattern)
                                                   (assoc :pull/ctx ctx)
                                                   pattern-finalizer)
        ks (set (map first key+opts))
        rm-db-id? (not (contains? ks :db/id))
        as->k (into {} (for [[k {:keys [as]}] key+opts
                             :when as]
                         [as k]))
        k->as (map-invert+ as->k)
        cb-opts {:pull/ctx ctx
                 :pull/as->k as->k
                 :pull/k->as k->as}
        xform (comp (map (partial apply-all-opts key+opts))
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
