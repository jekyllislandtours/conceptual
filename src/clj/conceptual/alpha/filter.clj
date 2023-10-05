(ns conceptual.alpha.filter
  (:require [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [conceptual.core :as c]
            [conceptual.int-sets :as i]))

;; prevent slow index scans by default
(def ^:dynamic *index-scan-enabled?* false)

(defn set-index-scan-enabled!
  [enabled?]
  (alter-var-root #'*index-scan-enabled?* (constantly enabled?)))

(def +comparison-operators+
  '#{= > >= < <=})

(def +set-operators+
  '#{in intersects? subset? superset?})

(def +operators+ (set/union +comparison-operators+ +set-operators+))

(s/def ::op (s/or :op/comparison +comparison-operators+
                  :op/set +set-operators+))

(def +boolean-operators+ '#{and or})
(s/def ::boolean-op +boolean-operators+)

(s/def ::field qualified-symbol?)

(s/def ::value
  (s/or :type/string string?
        :type/boolean boolean?
        :type/number number?
        :type/strings-coll (s/coll-of string? :kind vector? :into #{})
        :type/numbers-coll (s/coll-of number? :kind vector? :into #{})
        :type/strings-set (s/coll-of string? :kind set? :into #{})
        :type/numbers-set (s/coll-of number? :kind set? :into #{})))


(s/def ::op-field-val-form
  (s/and list? (s/cat :filter/op ::op
                      :filter/field ::field
                      :filter/value ::value)))

(s/def ::op-val-field-form
  (s/and list? (s/cat :filter/op ::op
                      :filter/value ::value
                      :filter/field ::field)))

(s/def ::op-sexp
  (s/or :sexp/op-field-val ::op-field-val-form
        :sexp/op-val-field ::op-val-field-form))

(s/def ::op-sexps
  (s/and list? (s/+ ::op-sexp)))


(s/def ::logical-sexp
  (s/and list? (s/cat :op/boolean ::boolean-op
                      :list/sexp (s/+ (s/or :sexp/logical ::logical-sexp
                                            :sexp/op ::op-sexp)))))

(s/def ::sexp
  (s/or :sexp/logical ::logical-sexp
        :sexp/op ::op-sexp))

(def +comparison-operator->fn+
  {'= = '> > '>= >= '< < '<= <=})

(defn conform
  [sexp]
  (let [ans (s/conform ::sexp sexp)]
    (when (= ::s/invalid ans)
      (throw (ex-info "Conform failed"
                      {:sexp sexp
                       :spec ::sexp
                       :explanation (s/explain-data ::sexp sexp)})))
    ans))


(defn filter-expr-value-collection?
  [{[val-type] :filter/value}]
  (contains? (#{:type/strings-coll
                :type/strings-set
                :type/numbers-coll
                :type/numbers-set}) val-type))


(defn index-scan-filter
  "`init-ids` is the starting sorted int set, could be nil"
  ([pred filter-info] (index-scan-filter pred filter-info nil))
  ([pred {field :filter/field [_val-type -value] :filter/value sexp-type :filter/sexp-type anding? :anding?} init-ids]
   (when-not *index-scan-enabled?*
     (throw (ex-info "Index scan not enabled." {:field field})))
   (let [field (keyword field)]
     (loop [[id & ids] (cond-> (c/ids field)
                         anding? (i/intersection init-ids))
            ans (transient [])]
       (if-not id
         (-> ans persistent! sort int-array)
         (if-some [found (c/value field id)]
           (let [outcome? (case sexp-type
                            :sexp/op-field-val (pred found -value)
                            :sexp/op-val-field (pred -value found))
                 ans (cond-> ans
                       outcome? (conj! id))]
             (recur ids ans))
           (recur ids ans)))))))


(defn in-reducer
  [{field :filter/field [_v-type] :filter/value :as filter-info} ids]
  (when-not (filter-expr-value-collection? filter-info)
    (throw (ex-info "`in` requires a collection" {:field field})))
  (index-scan-filter contains? filter-info ids))

(defn subset-reducer
  [filter-info _ids]
  (throw (ex-info "subset? not yet implemented" filter-info)))

(defn superset-reducer
  [filter-info _ids]
  (throw (ex-info "superset? not yet implemented" filter-info)))


(defn intersection-reducer
  [filter-info _ids]
  (throw (ex-info "intersect? not yet implemented" filter-info)))

(defn comparison-reducer
  [{[_op-type op] :filter/op field :filter/field [val-type] :filter/value :as filter-info} ids]
  (let [op-fn (+comparison-operator->fn+ op)]
    (assert op-fn (str "No fn for op: " op))
    (when (and (= val-type :type/string)
               (not= op '=))
      (throw (ex-info "Strings support only `=`" {:field field :op op})))
    (index-scan-filter op-fn filter-info ids)))

(defn tag-reducer
  [{[_op-type op] :filter/op field :filter/field [val-type -value] :filter/value anding? :anding?} ids]
  (when (not= :type/boolean val-type)
    (throw (ex-info "tag comparison values must be a boolean true or false" {:field field :op op})))
  (when (not= '= op)
    (throw (ex-info "tag comparison values use `=`" {:field field :op op})))
  (let [tagged-ids (c/ids (keyword field))
        set-op (if -value i/intersection i/difference)]
    (if (or anding? (nil? anding?)) ;; anding? is nil for lone sexp
      (set-op ids tagged-ids)
      tagged-ids)))

(def +set-op->reducer-fn+
  {'in in-reducer
   'intersects? intersection-reducer
   'subset? subset-reducer
   'superset? superset-reducer})

(defn lookup-reducer
  [{[op-type op] :filter/op field :filter/field :as filter-expr}]
  (cond
    (-> field keyword c/seek :db/tag?) tag-reducer
    (= :op/comparison op-type) comparison-reducer
    (= :op/set op-type) (+set-op->reducer-fn+ op)
    :else
    (throw (ex-info "Can't handle filter-expr" filter-expr))))



(defmulti evaluate-sexp (fn [conformed-sexp _anding? _ids] (first conformed-sexp)))

(defmethod evaluate-sexp :sexp/op
  [[_ [op-sexp-type filter-info]] anding? ids]
  (assert #{:sexp/op-field-val :sexp/op-val-field} op-sexp-type)
  (let [filter-info (assoc filter-info :filter/sexp-type op-sexp-type :anding? anding?)
        reducer (lookup-reducer filter-info)]
    (reducer filter-info ids)))

(defn and-sexps
  [sexp-info init-ids]
  (loop [[sexp & more] (:list/sexp sexp-info)
         ids init-ids]
    (if-not sexp
      ids
      ;; need this intersection because sexp might be an or expr for example
      (recur more (i/intersection ids (evaluate-sexp sexp true ids))))))

(defn or-sexps
  [sexp-info init-ids]
  (loop [[sexp & more] (:list/sexp sexp-info)
         ids-coll []]
    (if-not sexp
      (apply i/union ids-coll)
      (recur more (conj ids-coll (evaluate-sexp sexp false init-ids))))))

(defmethod evaluate-sexp :sexp/logical
  [[_ sexp-info] _anding? init-ids]
  (let [{boolean-op :op/boolean} sexp-info]
    (case boolean-op
      and (and-sexps sexp-info init-ids)
      or (or-sexps sexp-info init-ids))))


(defn evaluate
  [sexp init-ids]
  (evaluate-sexp (conform sexp) nil init-ids))
