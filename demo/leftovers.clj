;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Code to gather enumerable types/categories/etc. above.
;; Not really relevant to this demo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; NOTE: this is only used to find the enumerated sets used in this demo
(defn enumerate-values
  [table-key enumeration-set-atom extract-set-fn]
  (with-open [is (io/input-stream (local-filename table-key))
              gzis (GZIPInputStream. is)
              rdr (io/reader gzis)]
    (let [columns (read-table-columns table-key)]
      (doseq [set (cond->> (line-seq rdr)
                    true rest
                    (not (nil? *row-limit*)) (take *row-limit*)
                    true (map (comp extract-set-fn
                                    (partial row->concept table-key columns)
                                    #(str/split % #"\t"))))]
        (swap! enumeration-set-atom clojure.set/union set))))
  @enumeration-set-atom)

(comment
  (def -genres
    (->> (enumerate-values
          :title.basics
          (atom {})
          (fn [{:keys [genres]}]
            (split-on-comma genres)))
         ;;(binding [*row-limit* 1000]) ;; use to sample
         ))
  (->> -genres sort clojure.pprint/pprint))

(comment
  (def -regions
    (->> (enumerate-values
          :title.akas
          (atom {})
          (fn [{:keys [region]}] (split-on-comma region)))
         ;;(binding [*row-limit* 1000]) ;; use to sample
         ))
  (->> -regions sort str println))

(comment
  (def -languages
    (->> (enumerate-values
          :title.akas
          (atom {})
          (fn [{:keys [language]}] (split-on-comma language)))
         ;;(binding [*row-limit* 1000]) ;; use to sample
         ))
  (->> -languages sort str println))

(comment
  (def -types
    (->> (enumerate-values
          :title.akas
          (atom {})
          (fn [{:keys [types]}]
            (if (and types
                     (not= "\\N" types))
              ;;#{types} ;;(set types)
              (set (split-on-ctrl-b types))
              #{})))
         ;;(binding [*row-limit* 1000]) ;; use to sample
         ))
  (-> -types sort str println))

(comment
  (def -attributes
    (->> (enumerate-values
          :title.akas
          (atom {})
          (fn [{:keys [attributes]}]
            (if (and attributes
                     (not= "\\N" attributes))
              (set (split-on-ctrl-b attributes))
              #{})))
         ;;(binding [*row-limit* 1000]) ;; use to sample
         ))
  (->> -attributes sort str println))

(comment
  (def -primary-professions
    (->> (enumerate-values
          :name.basics
          (atom {})
          (fn [{:keys [primary-profession]}]
            (if (and primary-profession
                     (not= "\\N" primary-profession))
              (some->> (str/split primary-profession #",")
                       (filter (comp not str/blank?))
                       (map #(str/replace % #"_" "-"))
                       set)
              #{})))
         ;;(binding [*row-limit* 1000]) ;; use to sample
         ))
  (->> -primary-professions sort str println))
