(ns conceptual.alpha.pull
  (:require [conceptual.core :as c]
            [conceptual.arrays :as a]
            [conceptual.alpha.int-sets :as i]
            [conceptual.alpha.filter :as c.filter]
            [clojure.set :as set]))

;; inspiration from https://docs.datomic.com/query/query-pull.html

(defonce ^:dynamic ^:private *store* (atom {}))

(set! *warn-on-reflection* true)

(def known-opts #{:limit :as :filter})

(declare pull)

(defn ident-like?
  [x]
  (or (symbol? x)
      (keyword? x)
      (string? x)))

(defn attr?
  [x]
  (or (keyword? x) (symbol? x)))

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
  (when (ident-like? x)
    (-> x symbol str (.charAt 0) (= \$))))

(defn vector->key-info
  "Returns a map"
  [{:keys [pull/variables]} [k opts]]
  (let [k (keyword k)
        opts (cond
               (map? opts) (update-keys
                            opts
                            (fn [opts-k]
                              (when-not (ident-like? opts-k)
                                (throw (ex-info "Invalid opts map key"
                                                {::error ::invalid-opts-map-key
                                                 :pull/key k
                                                 :pull/opts opts
                                                 :pull/opts-key opts-k})))
                              (keyword opts-k)))
               (not opts) {}
               :else (throw (ex-info "Expected map for opts"
                                     {::error ::invalid-opts-map
                                      :pull/key k
                                      :pull/opts opts})))
        {:keys [limit as]} opts
        _ (validate-limit k limit)
        _ (validate-as k as)
        f-sexp|var (:filter opts)
        f-sexp (if (variable? f-sexp|var)
                 (get variables (keyword f-sexp|var))
                 f-sexp|var)

        ;; TODO: don't conform the same filter repeatedly
        opts (cond-> opts
               f-sexp (assoc :filter (c.filter/conform f-sexp))
               as (update :as keyword))]
    (when-let [unk-ks (->> (keys opts)
                           (remove known-opts)
                           set
                           not-empty)]
      (throw (ex-info "Unknown options"
                      {::error ::unknown-options
                       :pull/key k
                       :pull/unknown-opts unk-ks})))
    (cond-> {:pull/key k}
      (seq opts) (assoc :pull/key-opts opts))))


(defn default-pattern-finalizer
  [m]
  (select-keys m [:pull/key-infos :pull/relations :pull/k->as]))


(defn parse
  "Returns a data structure for processing
  `:pull/relation?`
     - predicate to determine if the specifed key is a relation
     - useful when there is a set of non-conceptual ids that should be treated as a relation
     - receives a map with key  `:pull/ctx` and `:pull/key`

  `:pull/variables`
     - a map of keyword variables to values. variables must begin with a `$` char
     - only the `:filter` option on an expanded relation supports variables

  `:pull/pattern-finalizer`
     - fn which takes in a map and returns a map with keys `:pull/key-infos`, `:pull/relations`, and `pull/k->as`
     - input map has keys `:pull/ctx`, `:pull/key-infos` and `:pull/relations`
     - opportunity to do any validation checks or transformations
     - items in `pull/key-infos` can be tagged with `:pull/relation?` if it is managed as a public relation
     - will be called for each nested pattern and the outermost pattern

  `:pull/virtual-attributes-info`
     - map of virtual attribute keys to maps.
     - each value map has keys `:pull.virtual-attribute/resolver-fn`,

"
  [{:keys [pull/relation?
           pull/pattern-finalizer]
    keyword->virtual-attribute-info :pull/virtual-attributes-info
    :or {relation? (constantly false)
         pattern-finalizer default-pattern-finalizer} :as ctx} pattern]
  (let [keyword->virtual-attribute-info (or keyword->virtual-attribute-info
                                            (:pull/virtual-attributes @*store*)
                                            {})
        ks (java.util.ArrayList.)
        rels (java.util.ArrayList.)
        rel-rfn (fn [ans k sub-pattern]
                  (when-not (or (attr? k) (vector? k))
                    (throw (ex-info "relation must be a keyword or a vector"
                                    {::error ::invalid-relation
                                     :pull/key k})))
                  (let [kw (if (attr? k) (keyword k) (-> k first keyword))
                        virtual-info (get keyword->virtual-attribute-info kw)
                        key-info (if (attr? k)
                                   {:pull/key kw}
                                   (vector->key-info ctx k))]
                    (when-not (or (relation? key-info)
                                  (:pull.virtual-attribute/relation? virtual-info))
                      (throw (ex-info "not a relation" (assoc key-info ::error ::not-a-relation))))
                    (conj ans (assoc (merge key-info virtual-info) :pull/pattern (parse ctx sub-pattern)))))]
    (loop [[x & more] pattern]
      (if-not x
        (let [key-infos (vec ks)
              rels (reduce into [] rels)
              k->as (java.util.HashMap.)]
          (doseq [{:keys [pull/key pull/key-opts]} key-infos
                  :let [{:keys [as]} key-opts]
                  :when as]
            (.put k->as key as))
          (doseq [{:keys [pull/key pull/key-opts]} rels
                  :let [{:keys [as]} key-opts]
                  :when as]
            (.put k->as key as))
          (let [k->as (into {} k->as)]
            (pattern-finalizer
             {:pull/key-infos key-infos
              :pull/relations rels
              :pull/k->as k->as})))
        (do
          (cond
            (attr? x) (let [x (keyword x)
                            virtual-info (get keyword->virtual-attribute-info x)]
                        (cond
                          (and virtual-info (:pull.virtual-attribute/relation? virtual-info))
                          (.add rels [(assoc virtual-info :pull/key x)])

                          virtual-info
                          (.add ks (assoc virtual-info :pull/key x))

                          (relation? {:pull/key x})
                          (.add rels [{:pull/key x}])

                          :else
                          (.add ks {:pull/key x})))
            (vector? x) (let [k (-> x first keyword)
                              virtual-info (get keyword->virtual-attribute-info k)]
                          (cond
                            (and virtual-info (:pull.virtual-attribute/relation? virtual-info))
                            (.add rels [(assoc virtual-info :pull/key k)])

                            virtual-info
                            (.add ks (merge (vector->key-info ctx x) virtual-info))

                            (relation? {:pull/key k}) ;; case when relation is in a vector without a pattern
                            (.add rels [(vector->key-info ctx x)])

                            :else
                            (.add ks (vector->key-info ctx x))))
            (map? x) (.add rels (reduce-kv rel-rfn [] x)))
          (recur more))))))


(defn default-value
  [m]
  (c/value (:pull/key m) (:db/id m)))

(defn apply-key-info
  [ctx key-info c]
  ;; NB `c` must already have key `key`
  (let [k (:pull/key key-info)
        {:keys [limit]} (:pull/key-opts key-info)
        v (if-let [resolver-fn (:pull.virtual-attribute/resolver-fn key-info)]
            (resolver-fn (assoc ctx :pull/concept c))
            (get c k))
        many? (coll? v)
        v' (cond->> v
             (and limit many?) (take limit))]
    (cond-> c
      (some? v') (assoc k v'))))

(defn apply-all-key-infos
  [ctx key-infos c]
  (loop [c c
         [key-info & more] key-infos]
    (if-not key-info
      c
      (recur (apply-key-info ctx key-info c) more))))

(defn apply-relation-key-info
  [{:keys [pull/key pull/key-opts]} id+]
  (let [{:keys [limit as] f-sexp :filter} key-opts
        many? (a/int-array? id+)
        v (if (and many? f-sexp)
            (c.filter/evaluate-conformed f-sexp id+)
            id+)
        too-many? (and limit many? (> (alength ^int/1 id+) limit))
        v' (cond->> v
             too-many? (take limit))]
    (cond-> {:k (or as key)
             :v v'}
      many? (assoc :v-all v))))

(defn reify-relations*
  [{:keys [pull/relation-value pull/relation-finalizer]
    :or {relation-value default-value
         relation-finalizer :pull/concept} :as ctx}
   {:keys [pull/key pull/key-opts pull/pattern pull.virtual-attribute/resolver-fn] :as relation} c]
  (let [depth (cond-> (::depth ctx)
                (not pattern) dec)
        rel-opts {:pull/ctx ctx
                  :pull/key key
                  :pull/key-opts key-opts
                  :pull/concept c
                  :pull/depth depth
                  :pull/reified-relation? (some? pattern)
                  :db/id (:db/id c)}
        id+ (if resolver-fn
              (resolver-fn rel-opts)
              (relation-value rel-opts))
        ids (if (int? id+) id+ (i/set id+))
        {k' :k id+ :v all-ids :v-all} (apply-relation-key-info relation ids)
        rel-opts (cond-> rel-opts
                   all-ids (assoc :pull/all-ids all-ids))
        xs (if pattern
             (pull ctx pattern id+)
             id+)
        c (assoc c key xs) ;; use original key until ready to finalize to make it easier to validate actual attrs
        c (relation-finalizer (assoc rel-opts :pull/concept c :pull/output-key k'))]
    c))

(defn reify-relations
  [ctx relations c]
  (let [ctx (update ctx ::depth (fnil inc 0))]
    (loop [c c
           [r & more] relations]
      (if-not r
        c
        (recur (reify-relations* ctx r c) more)))))


(defn rename-keys
  [{:keys [pull/concept pull/k->as]}]
  (set/rename-keys concept k->as))

(def default-finalizer :pull/concept)

(defn pull
  "`id+` can be an `int`, `int-array` or any seq of int.
  `ctx` is a map and can include fns to customize behavior. All the fns
  receive a map with key `:pull/ctx`.

  `:pull/relation-value`
     - fn which takes in a map and returns either a conceptual db/id or a sequence or int array of db/ids
     - input map has keys `:pull/ctx`, `:pull/key`, `:pull/key-opts`, `:pull/concept`, `:pull/depth` and `:db/id`
     - `:pull/concept` is the current concept being built up
     - `:pull/depth` is a zero based integer indicating the depth of the graph at this point
     - an opportunity to enforce depth and return relation values based on authorization etc

  `:pull/relation-finalizer`
     - fn which takes in a map and returns either a conceptual db/id or a sequence or int array of db/ids
     - input map has keys `:pull/ctx`, `:pull/key`, `:pull/key-opts`, `:pull/concept`, `:db/id` and
       `:pull/all-ids`.
     - `:pull/concept` is the current concept being built up
     - an opportunity to associate a metadata ie to convey total count when limit is used, etc
     - returns the concept

  `:pull/concept-finalizer`
     - fn for apply custom business logic to a concept before it is added to output. Returns the concept.
     - input map has keys `:pull/ctx`, `:pull/k->as`, and `:pull/concept`
     - `:pull/concept` is the current concept being built up"
  [{:keys [pull/concept-finalizer] :or {concept-finalizer default-finalizer} :as ctx} parsed-pattern id+]
  (let [{:keys [pull/key-infos pull/relations pull/k->as]} parsed-pattern
        ks (set (map :pull/key key-infos))
        rm-db-id? (not (contains? ks :db/id))
        key-ids (c/keys->ids (conj ks :db/id))
        cb-opts {:pull/ctx ctx
                 :pull/k->as k->as}
        xform (comp (map (partial apply-all-key-infos ctx key-infos))
                    (map (partial reify-relations ctx relations))
                    (map #(concept-finalizer (assoc cb-opts :pull/concept %)))
                    (map #(rename-keys (assoc cb-opts :pull/concept %)))
                 (map (fn [c]
                        (cond-> c
                          rm-db-id? (dissoc c :db/id)))))
        one? (int? id+)
        ids (if one? [id+] id+)
        ;; TODO: consider looking up the full concept instead of project-map which might be slower
        cs (into [] xform (c/project-map key-ids ids))]
    (cond-> cs
      one? first)))



;;----------------------------------------------------------------------
;; Virtual Attributes
;;----------------------------------------------------------------------
(defn register-virtual-attribute!
  "The `resolver-fn` will get the context with at least the key `:pull/concept`
  which will have all the keys in the pattern as well as `:db/id`.  Therefore,
  the resolver should use the `:db/id` to lookup the full concept."
  [& {:keys [pull/virtual-attribute pull.virtual-attribute/resolver-fn
             pull.virtual-attribute/relation?]}]
  (assert (qualified-keyword? virtual-attribute))
  (assert (ifn? resolver-fn))
  (let [path [:pull/virtual-attributes virtual-attribute]
        info (cond-> {:pull.virtual-attribute/resolver-fn resolver-fn}
               (true? relation?) (assoc :pull.virtual-attribute/relation? relation?))]
    (swap! *store* assoc-in path info)))
