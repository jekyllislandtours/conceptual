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
  '#{contains? not-contains? intersects? subset? superset? exists?})

(def +operators+ (set/union +comparison-operators+ +set-operators+))


(defmulti custom-op? identity)
(defmethod custom-op? :default [_] false)

(s/def ::op (s/or :op/comparison +comparison-operators+
                  :op/set +set-operators+
                  :op/custom custom-op?))

(def +boolean-operators+ '#{and or})
(s/def ::boolean-op +boolean-operators+)

(s/def ::field qualified-symbol?)


(def +scalar-types+
  #{:type/string :type/keyword :type/boolean :type/number})

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

(s/def ::op-val-form
  (s/and list? (s/cat :filter/op ::op
                      :filter/value ::value)))

(s/def ::op-field-form
  (s/and list? (s/cat :filter/op ::op
                      :filter/field ::field)))

(s/def ::op-form
  (s/and list? (s/cat :filter/op ::op)))

(s/def ::op-sexp
  (s/or :sexp/op-field-val ::op-field-val-form
        :sexp/op-val-field ::op-val-field-form
        :sexp/op-val ::op-val-form
        :sexp/op-field ::op-field-form
        :sexp/op ::op-form))

(s/def ::op-sexps
  (s/and list? (s/+ ::op-sexp)))


(s/def ::logical-sexp
  (s/and list? (s/cat :op/boolean ::boolean-op
                      :list/sexp (s/+ (s/or :sexp/logical ::logical-sexp
                                            :sexp/op ::op-sexp
                                            :sexp/field ::field)))))

(s/def ::sexp
  (s/or :sexp/logical ::logical-sexp
        :sexp/op ::op-sexp))

(def +comparison-operator->fn+
  {'= = '> > '>= >= '< < '<= <= 'not= not=})


(defn normalize
  [sexp]
  (cond
    (qualified-symbol? sexp) (list 'and sexp)
    (#{'or 'and} (first sexp)) sexp
    :else (list 'and sexp)))

(defn conform
  [sexp]
  (let [norm-sexp (normalize sexp)
        ans (s/conform ::sexp norm-sexp)]
    (when (= ::s/invalid ans)
      (throw (ex-info "Conform failed"
                      {::error ::invalid-syntax
                       :sexp sexp
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
    (throw (ex-info "Index scan not enabled." {::error ::index-scan-disabled
                                               :field field})))
  ;; looks up all concept ids that have said field
  (let [ids (cond-> (c/ids (keyword field))
              anding? (i/intersection init-ids))]
    (filter-ids pred filter-info ids)))


(defn collection?
  [field]
  (some->> field keyword c/seek :db/type class (.isAssignableFrom (class clojure.lang.IPersistentCollection))))

(defn- ensure-set
  [x]
  (cond
    (set? x) x
    (coll? x) (set x)
    :else #{x}))

(defn- contains?-reducer-coll-literal-sym
  [ctx pred filter-info ids]
  ;; make sure the value is a collection literal
  (when-not (filter-expr-value-collection? filter-info)
    (let [[_op-type op] (:filter/op filter-info)
          field (:filter/field filter-info)]
      (throw (ex-info (format "op `%s` requires a collection" op) {::error ::collection-required
                                                                   :op op
                                                                   :field field}))))
  (index-scan-filter ctx pred filter-info ids))

(defn- contains?-reducer-coll-sym-scalar-val
  [ctx pred filter-info ids]
  ;; make sure the field is a collection and value is a scalar
  (let [field (:filter/field filter-info)
        [v-type] (:filter/value filter-info)]
    (when-not (collection? field)
      (throw (ex-info (format "field %s is not a collection" field) {::error ::field-not-a-collection
                                                                     :field field})))
    (when-not (contains? +scalar-types+ v-type)
      (throw (ex-info (format "value for field %s must be a scalar" field) {::error ::scalar-value-required
                                                                            :field field
                                                                            :value-type v-type}))))
  (index-scan-filter ctx pred (assoc filter-info :field-xform ensure-set) ids))


(defn contains?-reducer
  ([ctx filter-info ids]
   (contains?-reducer ctx contains? filter-info ids))
  ([ctx pred filter-info ids]
   (let [in-fn (case (:filter/sexp-type filter-info)
                 :sexp/op-val-field contains?-reducer-coll-literal-sym
                 :sexp/op-field-val contains?-reducer-coll-sym-scalar-val)]
     (in-fn ctx pred filter-info ids))))


(defn not-contains?-reducer
  [ctx filter-info ids]
  (contains?-reducer ctx (complement contains?) filter-info ids))

(defn- set-op-reducer
  [ctx pred {[_op-type op] :filter/op field :filter/field [_v-type] :filter/value :as filter-info} ids]
  (when-not (filter-expr-value-collection? filter-info)
    (throw (ex-info (format "op `%s` requires a collection" op) {::error ::collection-required
                                                                 :op op
                                                                 :field field})))
  (when-not (collection? field)
    (throw (ex-info (format "field `%s` is not a collection" field) {::error ::field-not-a-collection
                                                                     :field field})))
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
      (throw (ex-info "Strings support only `=` or `not=`" {::error ::unsupported-operator
                                                            :op op
                                                            :field field})))
    (index-scan-filter ctx op-fn filter-info ids)))

(defn tag-reducer
  [{::keys [anding?] :as _ctx}
   {[_op-type op] :filter/op field :filter/field [val-type the-value] :filter/value}
   ids]
  (when (not= :type/boolean val-type)
    (throw (ex-info "tag comparison values must be a boolean true or false" {::error ::invalid-tag-comparison-value
                                                                             :op op
                                                                             :field field})))
  (when-not (#{'= 'not=} op)
    (throw (ex-info "tag comparison values may only use `=` or `not=`" {::error ::unsupported-operator
                                                                        :op op
                                                                        :field field})))
  (let [tagged-ids (c/ids (keyword field))
        the-value (if (= '= op) the-value (not the-value))
        set-op (if the-value i/intersection i/difference)]
    (if (or anding? (nil? anding?)) ;; anding? is nil for lone sexp
      (set-op ids tagged-ids)
      tagged-ids)))

(defn exists-reducer
  [_ctx
   {field :filter/field [_val-type the-value] :filter/value}
   ids]
  (when the-value
    (throw (ex-info "value not allowed for exists? " {::error ::unexpected-value
                                                      :op 'exists?
                                                      :field field})))
  (i/intersection ids (c/ids (keyword field))))

(def +set-op->reducer-fn+
  {'contains? contains?-reducer
   'not-contains? not-contains?-reducer
   'intersects? intersection-reducer
   'subset? subset-reducer
   'superset? superset-reducer
   'exists? exists-reducer})


(defmulti custom-reducer
  "Dispatch is off of a tuple [op-symbol field-symbol] or just a `field-symbol`.
  This should return either `nil` or a fn that takes in a `ctx`, `filter-info` and `ids`
  and returns modified `ids`. `nil` return value implies using the default logic.
  The `ctx` has key `::sexp` which is the conformed s-expression."
  (fn [_ctx tuple-or-sym] tuple-or-sym))

(defmethod custom-reducer :default [_ _] nil)


(defmulti custom-op-reducer (fn [_ctx op-sym] op-sym))
(defmethod custom-op-reducer :default [_ op-sym]
  (throw (ex-info "no custom-op-reducer method for dispatch op" {:op op-sym})))

(defn lookup-reducer
  [ctx {[op-type op] :filter/op field :filter/field :as filter-expr}]
  (let [ctx (assoc ctx ::sexp filter-expr)]
    (or (custom-reducer ctx [op field])
        (custom-reducer ctx field)
        (when (= :op/custom op-type) (custom-op-reducer ctx op))
        (cond
          (= :op/set op-type) (+set-op->reducer-fn+ op)
          (-> field keyword c/seek :db/tag?) tag-reducer
          (= :op/comparison op-type) comparison-reducer
          :else
          (throw (ex-info "Can't handle filter-expr" {::error ::unsupported-expression
                                                      :expr filter-expr}))))))



(defmulti evaluate-sexp (fn [_ctx conformed-sexp _ids] (first conformed-sexp)))

(defmethod evaluate-sexp :sexp/op
  [ctx [_ [op-sexp-type filter-info]] ids]
  (let [filter-info (assoc filter-info :filter/sexp-type op-sexp-type)]
    ((lookup-reducer ctx filter-info) ctx filter-info ids)))

(defmethod evaluate-sexp :sexp/field
  [{::keys [anding?] :as _ctx} [_ field] ids]
  (let [op (if anding? i/intersection i/union)]
    (op ids (c/ids (keyword field)))))

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


(defn error-code
  [ex]
  (::error (ex-data ex)))

(defn error?
  [ex]
  (some? (error-code ex)))
