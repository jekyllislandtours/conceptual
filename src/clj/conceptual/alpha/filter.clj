(ns conceptual.alpha.filter
  (:require [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [conceptual.core :as c]
            [conceptual.int-sets :as i]))

;; prevent slow index scans by default
(def ^:dynamic *enable-index-scan* false)

(defn enable-index-scan!
  [enabled?]
  (alter-var-root #'*enable-index-scan* (constantly enabled?)))

(def +comparison-operators+
  '#{= > >= < <= not=})

(def +set-operators+
  '#{in not-in intersects? subset? superset?})

(def +operators+ (set/union +comparison-operators+ +set-operators+))

(s/def ::op (s/or :op/comparison +comparison-operators+
                  :op/set +set-operators+))

(def +boolean-operators+ '#{and or})
(s/def ::boolean-op +boolean-operators+)

(s/def ::field qualified-symbol?)

(s/def ::value
  (s/or :type/string string?
        :type/keyword keyword?
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
  {'= = '> > '>= >= '< < '<= <= 'not= not=})


(defn normalize
  [sexp]
  (if (#{'or 'and} (first sexp))
    sexp
    (list 'and sexp)))

(defn conform
  [sexp]
  (let [norm-sexp (normalize sexp)
        ans (s/conform ::sexp norm-sexp)]
    (when (= ::s/invalid ans)
      (throw (ex-info "Conform failed"
                      {:sexp sexp
                       :spec ::sexp
                       :explanation (s/explain-data ::sexp sexp)})))
    ans))

(defn filter-expr-value-collection?
  [{[val-type] :filter/value}]
  (contains? #{:type/strings-coll
               :type/strings-set
               :type/numbers-coll
               :type/numbers-set} val-type))

(defn filter-ids
  "Checks to see if concepts represented by `init-ids` with field have a value that
  satisfies `pred` and returns a subset of `init-ids`. NB. `init-ids` must be sorted."
  [pred
   {field :filter/field [_val-type the-value] :filter/value sexp-type :filter/sexp-type
    :keys [field-xform]
    :or {field-xform identity}}
   init-ids]
  (let [field (keyword field)]
    (loop [[id & ids] init-ids
           ans (transient [])]
      (if-not id
        (-> ans persistent! int-array)
        (if-some [found (c/value field id)]
          (let [found (field-xform found)
                outcome? (case sexp-type
                           :sexp/op-field-val (pred found the-value)
                           :sexp/op-val-field (pred the-value found))
                ans (cond-> ans
                      outcome? (conj! id))]
            (recur ids ans))
          (recur ids ans))))))


(defn index-scan-filter
  "`init-ids` is the starting sorted int set, could be nil.
  NB negations ie not= only operate on concepts that have the field
  for performance reasons."
  [{::keys [anding?] :as _ctx}
   pred
   {field :filter/field :as filter-info}
   init-ids]
  (when-not *enable-index-scan*
    (throw (ex-info "Index scan not enabled." {:field field})))
  ;; looks up all concept ids that have said field
  (let [ids (cond-> (c/ids (keyword field))
              anding? (i/intersection init-ids))]
    (filter-ids pred filter-info ids)))


(def +scalar-types+
  #{Boolean Byte Character Short Integer Long Double BigDecimal BigInteger String
    java.time.Instant java.util.Date java.util.UUID})

(defn scalar?
  [field]
  (-> field keyword c/seek :db/type +scalar-types+ some?))


(defn collection?
  [field]
  (->> field keyword c/seek :db/type class (.isAssignableFrom (class clojure.lang.IPersistentCollection))))

(defn- in-reducer*
  [ctx
   pred
   {[_op-type op] :filter/op field :filter/field [_v-type] :filter/value
    sexp-type :filter/sexp-type :as filter-info} ids]
  (when-not (filter-expr-value-collection? filter-info)
    (throw (ex-info (format "op `%s` requires a collection" op) {:field field})))
  (when (and (= :sexp/op-field-val sexp-type)
             (scalar? field))
    (throw (ex-info "Scalar field in wrong position" {:field field})))
  ;; field is a scalar and it is in :sexp/op-field-val then throw
  (index-scan-filter ctx pred filter-info ids))

(defn in-reducer
  [ctx filter-info ids]
  (in-reducer* ctx contains? filter-info ids))


(defn not-in-reducer
  [ctx filter-info ids]
  (in-reducer* ctx (complement contains?) filter-info ids))

(defn- ensure-set
  [x]
  (cond
    (set? x) x
    (coll? x) (set x)
    :else #{x}))

(defn- set-op-reducer
  [ctx pred {[_op-type op] :filter/op field :filter/field [_v-type] :filter/value :as filter-info} ids]
  (when-not (filter-expr-value-collection? filter-info)
    (throw (ex-info (format "op `%s` requires a collection" op) {:field field})))
  (when-not (collection? field)
    (throw (ex-info (format "field `%s` is not a collection" field) {:field field})))
  (index-scan-filter ctx pred (assoc filter-info :field-xform ensure-set) ids))


(defn subset-reducer
  [ctx filter-info ids]
  (set-op-reducer ctx set/subset? filter-info ids))

(defn superset-reducer
  [ctx filter-info ids]
  (set-op-reducer ctx set/superset? filter-info ids))


(defn intersection-reducer
  [ctx filter-info ids]
  (set-op-reducer ctx (comp not-empty set/intersection) filter-info ids))

(defn comparison-reducer
  [ctx
   {[_op-type op] :filter/op field :filter/field [val-type] :filter/value :as filter-info}
   ids]
  (let [op-fn (+comparison-operator->fn+ op)]
    (assert op-fn (str "No fn for op: " op))
    (when (and (= val-type :type/string)
               (not (#{'= 'not=} op)))
      (throw (ex-info "Strings support only `=` or `not=`" {:field field :op op})))
    (index-scan-filter ctx op-fn filter-info ids)))

(defn tag-reducer
  [{::keys [anding?] :as _ctx}
   {[_op-type op] :filter/op field :filter/field [val-type the-value] :filter/value}
   ids]
  (when (not= :type/boolean val-type)
    (throw (ex-info "tag comparison values must be a boolean true or false" {:field field :op op})))
  (when-not (#{'= 'not=} op)
    (throw (ex-info "tag comparison values may only use `=` or `not=`" {:field field :op op})))
  (let [tagged-ids (c/ids (keyword field))
        the-value (if (= '= op) the-value (not the-value))
        set-op (if the-value i/intersection i/difference)]
    (if (or anding? (nil? anding?)) ;; anding? is nil for lone sexp
      (set-op ids tagged-ids)
      tagged-ids)))

(def +set-op->reducer-fn+
  {'in in-reducer
   'not-in not-in-reducer
   'intersects? intersection-reducer
   'subset? subset-reducer
   'superset? superset-reducer})


(defmulti custom-reducer
  "Dispatch is off of a tuple [op-symbol field-symbol] or just a `field-symbol`.
  This should return either `nil` or a fn that takes in a `ctx`, `filter-info` and `ids`
  and returns modified `ids`. `nil` return value implies using the default logic.
  The `ctx` has key `::sexp` which is the conformed s-expression."
  (fn [_ctx tuple-or-sym] tuple-or-sym))

(defmethod custom-reducer :default [_ _] nil)

(defn lookup-reducer
  [ctx {[op-type op] :filter/op field :filter/field :as filter-expr}]
  (let [ctx (assoc ctx ::sexp filter-expr)]
    (or (custom-reducer ctx [op field])
        (custom-reducer ctx field)
        (cond
          (-> field keyword c/seek :db/tag?) tag-reducer
          (= :op/comparison op-type) comparison-reducer
          (= :op/set op-type) (+set-op->reducer-fn+ op)
          :else
          (throw (ex-info "Can't handle filter-expr" filter-expr))))))



(defmulti evaluate-sexp (fn [_ctx conformed-sexp _ids] (first conformed-sexp)))

(defmethod evaluate-sexp :sexp/op
  [ctx [_ [op-sexp-type filter-info]] ids]
  (assert #{:sexp/op-field-val :sexp/op-val-field} op-sexp-type)
  (let [filter-info (assoc filter-info :filter/sexp-type op-sexp-type)]
    ((lookup-reducer ctx filter-info) ctx filter-info ids)))

(defn and-sexps
  [ctx sexp-info init-ids]
  (loop [[sexp & more] (:list/sexp sexp-info)
         ids init-ids]
    (if-not sexp
      ids
      ;; need this intersection because sexp might be an or expr for example
      (recur more (i/intersection ids (evaluate-sexp (assoc ctx ::anding? true) sexp ids))))))

(defn or-sexps
  [ctx sexp-info init-ids]
  (loop [[sexp & more] (:list/sexp sexp-info)
         ids-coll []]
    (if-not sexp
      (apply i/union ids-coll)
      (recur more (conj ids-coll (evaluate-sexp (dissoc ctx ::anding?) sexp init-ids))))))

(defmethod evaluate-sexp :sexp/logical
  [ctx [_ sexp-info] init-ids]
  (let [{boolean-op :op/boolean} sexp-info]
    (case boolean-op
      and (and-sexps ctx sexp-info init-ids)
      or (or-sexps ctx sexp-info init-ids))))


(defn evaluate-conformed
  ([conformed-sexp init-ids]
   (evaluate-sexp {} conformed-sexp init-ids))
  ([ctx conformed-sexp init-ids]
   (evaluate-sexp ctx conformed-sexp init-ids)))

(defn evaluate
  "`sexp` is an s-expression. `init-ids` is a sorted int array"
  ([sexp init-ids]
   (evaluate {} sexp init-ids))
  ([ctx sexp init-ids]
   (evaluate-conformed ctx (conform sexp) init-ids)))
