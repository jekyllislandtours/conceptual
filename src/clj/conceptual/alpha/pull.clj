(ns conceptual.alpha.pull
  (:require
   [conceptual.core :as c]
   [conceptual.arrays :as a]
   [conceptual.int-sets :as i]
   [conceptual.alpha.filter :as c.filter]
   [clojure.set :as set]))

;; inspiration from https://docs.datomic.com/query/query-pull.html

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
  [{:keys [pull/variables]} [input-key opts] & {:keys [key-parse-fn]
                                              :or {key-parse-fn keyword}}]
  (let [k (key-parse-fn input-key)
        _ (when (nil? k) (throw (ex-info "Unparseable input key"
                                         {::error ::key-parse-error
                                          :pull/key input-key
                                          :pull/opts opts})))
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


(defn data-many-coll?
  [x]
  (or (coll? x)
      (a/array? )))

(defn parse-data-selection-pattern
  [ctx pattern]
  (vec
   (for [x pattern]
     (do
       (when (map? x) ;; not allowing sub selections on data
         (throw (ex-info "map not allowed in data selection" {::error ::invalid-data-selection-pattern})))
       (cond
         (vector? x) (let [m (vector->key-info ctx x :key-parse-fn #(or (keyword %) %))]
                       (when (get-in m [:pull/key-opts :filter])
                         (throw (ex-info "filter not supported in data selection"
                                         {::error ::data-selection-pattern-filter-not-allowed})))
                       m)
         :else {:pull/key x})))))

(defn default-pattern-finalizer
  [m]
  (select-keys m [:pull/key-infos :pull/relations :pull/data :pull/k->as]))


(defn parse
  "Returns a data structure for processing
  `:pull/relation?`
     - predicate to determine if the specifed key is a relation
     - useful when there is a set of non-conceptual ids that should be treated as a relation
     - receives a map with key `:pull/key` and `:pull/ctx`

  `:pull/data?`
     - predicate to determine if the specified key is a map or a list of maps
     - useful when there is a map that you want selections on
     - receives a map with key `:pull/key` and `:pull/ctx`

  `:pull/variables`
     - a map of keyword variables to values. variables must begin with a `$` char
     - only the `:filter` option on an expanded relation supports variables

  `:pull/pattern-finalizer`
     - fn which takes in a map and returns a map with keys `:pull/key-infos`, `:pull/relations`, and `pull/k->as`
     - input map has keys `:pull/ctx`, `:pull/key-infos` and `:pull/relations`
     - opportunity to do any validation checks or transformations
     - items in `pull/key-infos` can be tagged with `:pull/relation?` if it is managed as a public relation
     - will be called for each nested pattern and the outermost pattern"
  [{:keys [pull/relation?
           pull/pattern-finalizer
           pull/data?]
    :or {relation? (constantly false)
         data? (constantly false)
         pattern-finalizer default-pattern-finalizer} :as ctx} pattern]
  (let [keys-list (java.util.ArrayList.)
        rels-list (java.util.ArrayList.)
        data-list (java.util.ArrayList.)
        rel-rfn (fn [ans k sub-pattern]
                  (when-not (or (attr? k) (vector? k))
                    (throw (ex-info "relation must be a keyword or a vector"
                                    {::error ::invalid-relation
                                     :pull/key k})))
                  (let [key-info (if (attr? k)
                                   {:pull/key (keyword k)}
                                   (vector->key-info ctx k))]
                    (cond
                      (relation? key-info)
                      (update ans :relations conj (assoc key-info :pull/pattern (parse ctx sub-pattern)))

                      (data? key-info)
                      (cond
                        (not (or (sequential? sub-pattern) (set? sub-pattern)))
                        (throw (ex-info "data selection keys must be a list"
                                        (assoc key-info ::error ::data-selection-keys-not-a-list)))

                        :else
                        (update ans :data conj (assoc key-info :pull/key-infos (parse-data-selection-pattern ctx sub-pattern))))

                      :else
                      (throw (ex-info "not a relation" (assoc key-info ::error ::not-a-relation))))))]
    (loop [[x & more] pattern]
      (if-not x
        (let [key-infos (vec keys-list)
              rels-list (reduce into [] rels-list)
              data-list (reduce into [] data-list)
              k->as (java.util.HashMap.)]
          (doseq [{:keys [pull/key pull/key-opts]} key-infos
                  :let [{:keys [as]} key-opts]
                  :when as]
            (.put k->as key as))
          (doseq [{:keys [pull/key pull/key-opts]} rels-list
                  :let [{:keys [as]} key-opts]
                  :when as]
            (.put k->as key as))
          (let [k->as (into {} k->as)]
            (pattern-finalizer
             {:pull/key-infos key-infos
              :pull/relations rels-list
              :pull/data data-list
              :pull/k->as k->as})))
        (do
          (cond
            (attr? x) (if (relation? {:pull/key (keyword x)})
                        (.add rels-list [{:pull/key (keyword x)}])
                        (.add keys-list {:pull/key (keyword x)}))
            (vector? x) (if (relation? {:pull/key (keyword (first x))}) ;; case when relation is in a vector without a pattern
                          (.add rels-list [(vector->key-info ctx x)])
                          (.add keys-list (vector->key-info ctx x)))
            (map? x) (let [{:keys [relations data]} (reduce-kv rel-rfn {:relations [] :data []} x)]
                       (.add rels-list relations)
                       (.add data-list data)))
          (recur more))))))


(defn default-value
  [m]
  (c/value (:pull/key m) (:db/id m)))

(defn apply-key-info
  [source dest key-info & {:keys [rename-key?]
                           :or {rename-key? false}}]
  (let [k (:pull/key key-info)
        {:keys [limit as]} (:pull/key-opts key-info)
        k' (if (and rename-key? (some? as)) as k)
        v (get source k)
        many? (coll? v)
        v' (cond->> v
             (and limit many?) (take limit))]
    (cond-> dest
      (some? v') (assoc k' v'))))

(defn apply-all-key-infos
  ([key-infos c]
   (apply-all-key-infos key-infos c c {:rename-key? false}))
  ([key-infos source dest opts]
   (loop [dest dest
          [key-info & more] key-infos]
     (if-not key-info
       dest
       (recur (apply-key-info source dest key-info opts) more)))))

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
      too-many? (assoc :v-pre-limit v))))

(defn reify-relations*
  [{:keys [pull/relation-value pull/relation-finalizer]
    :or {relation-value default-value
         relation-finalizer :pull/concept} :as ctx}
   {:keys [pull/key pull/key-opts pull/pattern] :as relation} c]
  (let [depth (cond-> (::depth ctx)
                (not pattern) dec)
        rel-opts {:pull/ctx ctx
                  :pull/key key
                  :pull/key-opts key-opts
                  :pull/concept c
                  :pull/depth depth
                  :pull/reified-relation? (some? pattern)
                  :db/id (:db/id c)}
        id+ (relation-value rel-opts)
        ids (if (int? id+) id+ (i/set id+))
        {k' :k id+ :v pre-limit-ids :v-pre-limit} (apply-relation-key-info relation ids)
        rel-opts (cond-> rel-opts
                   pre-limit-ids (assoc :pull/pre-limit-ids pre-limit-ids))
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


(defn apply-data-key-info
  [dest source key-info]
  (println "<apply-data-key-info> " source  dest  key-info)
  (let [k (:pull/key key-info)
        {:keys [limit as]} (:pull/key-opts key-info)
        k' (if (some? as) as k)
        v (get source k)
        many? (coll? v)
        v' (cond->> v
             (and limit many?) (take limit))]
    (cond-> dest
      (some? v') (assoc k' v'))))


(defn apply-all-data-key-infos-for-map
  "restricts/transforms `v` as per `key-infos`"
  [key-infos v]
  (loop [ans {}
         [key-info & more] key-infos]
    (if-not key-info
      ans
      (recur (apply-data-key-info ans v key-info) more))))
;; if map

(defn apply-all-data-key-infos
  "restricts/transforms `v` as per `key-infos`"
  [key-infos v]
  (if (map? v)
    (apply-all-data-key-infos-for-map key-infos v)
    (for [x v]
      (if (map? x)
        (apply-all-data-key-infos-for-map key-infos x)
        (throw (ex-info "unhandled case" {:x x :type-x (type x)}))))))


(defn select-data
  [_ctx data-specs c]
  (loop [c c
         [datum-spec & more] data-specs]
    (if-not datum-spec
      c
      (let [{:keys [pull/key pull/key-opts pull/key-infos]} datum-spec
            v (c/value key (:db/id c))
            ;; TODO What about an array? that might work
            _ (when-not (or (map? v)
                            (set? v)
                            (sequential? v))
                (throw (ex-info "expected map for selecting on data" {::error ::data-selection-expected-collection
                                                                      :v v
                                                                      :type-v (type v)})))
            c (cond-> c
                (some? v) (assoc key (apply-all-data-key-infos key-infos v)))
            c (apply-data-key-info c c datum-spec)
            c (cond-> c
                (:as key-opts) (dissoc key))]
        (recur c more)))))


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
       optionally `:pull/pre-limit-ids`.
     - `:pull/concept` is the current concept being built up
     - an opportunity to associate a metadata ie to convey total count when limit is used, etc
     - returns the concept

  `:pull/concept-finalizer`
     - fn for apply custom business logic to a concept before it is added to output. Returns the concept.
     - input map has keys `:pull/ctx`, `:pull/k->as`, and `:pull/concept`
     - `:pull/concept` is the current concept being built up"
  [{:keys [pull/concept-finalizer] :or {concept-finalizer default-finalizer} :as ctx} parsed-pattern id+]
  (let [{:keys [pull/key-infos pull/relations pull/k->as pull/data]} parsed-pattern
        ks (set (map :pull/key key-infos))
        rm-db-id? (not (contains? ks :db/id))
        key-ids (c/keys->ids (conj ks :db/id))
        cb-opts {:pull/ctx ctx
                 :pull/k->as k->as}
        xform (comp (map (partial apply-all-key-infos key-infos))
                    (map (partial reify-relations ctx relations))
                    (map (partial select-data ctx data))
                    (map #(concept-finalizer (assoc cb-opts :pull/concept %)))
                    (map #(rename-keys (assoc cb-opts :pull/concept %)))
                 (map (fn [c]
                        (cond-> c
                          rm-db-id? (dissoc c :db/id)))))
        one? (int? id+)
        ids (if one? [id+] id+)
        cs (into [] xform (c/project-map key-ids ids))]
    (cond-> cs
      one? first)))
