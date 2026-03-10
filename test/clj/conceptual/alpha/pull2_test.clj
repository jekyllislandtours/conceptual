(ns conceptual.alpha.pull2-test
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest use-fixtures testing]]
   [conceptual.test.core :as test.core]
   [conceptual.core :as c]
   [conceptual.int-sets :as i]
   [conceptual.alpha.pull2 :as pull]
   [expectations.clojure.test :refer [expect]]))


(set! *print-namespace-maps* false)

(use-fixtures :each test.core/with-rdb)


(deftest variable?-test
  (expect true (pull/variable? '$foo))
  (expect true (pull/variable? '$foo/bar))
  (expect true (pull/variable? :$foo/bar))
  (expect true (pull/variable? :$foo))
  (expect true (pull/variable? "$foo"))
  (expect false (pull/variable? "foo"))
  (expect false (pull/variable? 'foo))
  (expect false (pull/variable? :foo))
  (expect false (pull/variable? 'foo/bar))
  (expect false (pull/variable? :foo/bar)))


(deftest paginate-test
  (let [f (fn [ids page page-size]
            (vec (pull/paginate (int-array ids) page page-size)))]
    (expect [] (f [] 0 0))
    (expect [] (f [] 0 1))
    (expect [] (f [] 0 2))
    (expect [] (f [1 2 3] 0 0))

    (expect [1] (f [1 2 3] 0 1))
    (expect [1 2] (f [1 2 3] 0 2))
    (expect [1 2 3] (f [1 2 3] 0 3))

    ;; step by odd page size
    (expect [1 2 3] (f [1 2 3 4 5 6 7] 0 3))
    (expect [4 5 6] (f [1 2 3 4 5 6 7] 1 3))
    (expect [7] (f [1 2 3 4 5 6 7] 2 3))
    (expect [] (f [1 2 3 4 5 6 7] 3 3))

    ;; step by even page size
    (expect [1 2] (f [1 2 3 4 5 6 7] 0 2))
    (expect [3 4] (f [1 2 3 4 5 6 7] 1 2))
    (expect [5 6] (f [1 2 3 4 5 6 7] 2 2))
    (expect [7] (f [1 2 3 4 5 6 7] 3 2))
    (expect [] (f [1 2 3 4 5 6 7] 4 2))))


(defn ->db-id
  [id]
  (c/lookup-id :sf/id id))


(deftest validate-limit-test
  (let [f (fn [k limit]
            (try
              (pull/validate-limit k limit)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect nil (f :foo/bar 2))

    ;; errors
    (expect {::pull/error ::pull/invalid-limit-option-value
             :pull/key :foo/bar}
            (f :foo/bar -1))

    (expect {::pull/error ::pull/invalid-limit-option-value
             :pull/key :foo/bar}
            (f :foo/bar 0))
    (expect {::pull/error ::pull/invalid-limit-option-value
             :pull/key :foo/bar}
            (f :foo/bar "hello"))))

(deftest validate-as-test
  (let [f (fn [k as]
            (try
              (pull/validate-as k as)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect nil (f :foo/bar :meow))
    (expect nil (f :foo/bar "meow"))
    (expect nil (f :foo/bar 'meow))
    (expect nil (f :foo/bar :fu/bar))
    (expect nil (f :foo/bar 'fu/bar))
    (expect nil (f :foo/bar "fu/bar"))

    ;; errors
    (expect {::pull/error ::pull/invalid-as-option-value
             :pull/key :foo/bar}
            (f :foo/bar -1))


    (expect {::pull/error ::pull/invalid-as-option-value
             :pull/key :foo/bar}
            (f :foo/bar 42))

    (expect {::pull/error ::pull/invalid-as-option-value
             :pull/key :foo/bar}
            (f :foo/bar [:a :b]))

    (expect {::pull/error ::pull/invalid-as-option-value
             :pull/key :foo/bar}
            (f :foo/bar {:a :b}))))


(deftest validated-page-params
  (let [f (fn [k page max-page-size]
            (try
              (pull/validate-page-params k page max-page-size)
              (catch Exception ex
                (ex-data ex))))]
    (expect nil (f :foo/bar 0 10))
    (expect nil (f :foo/bar 1 10))
    (expect {::pull/error ::pull/invalid-page-option-value
             :pull/key :foo/bar}
            (f :foo/bar -1 10))
    ;; more than max-page-size is ok, because code does (min max-page-size *max-relation-page-size*)
    (expect nil (f :foo/bar 1 (inc pull/*max-relation-page-size*)))
    (expect {::pull/error ::pull/invalid-max-page-size-value
             :pull/key :foo/bar}
            (f :foo/bar 1 "42"))))

(deftest vector->key-info-test
  (let [f (fn [v]
            (try
              (pull/vector->key-info {:pull/variables {'$x 'sf/human?}} v)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect {:pull/key :sf/id
             :pull/key-id (c/key->id :sf/id)}
            (f [:sf/id]))

    (expect {:pull/key :sf/id
             :pull/key-id (c/key->id :sf/id)}
            (f [:sf/id {}]))

    (expect {:pull/key :sf/id
             :pull/key-id (c/key->id :sf/id)
             :pull/key-opts {:as :hello/world}}
            (f [:sf/id {:as :hello/world}]))

    (expect {:pull/key :sf/member-ids
             :pull/key-id (c/key->id :sf/member-ids)
             :pull/key-opts {:as :hello/world :max-page-size 3}}
            (f [:sf/member-ids {:as :hello/world :limit 3}]))

    (expect {:pull/key :sf/member-ids
             :pull/key-id (c/key->id :sf/member-ids)
             :pull/key-opts {:as :hello/world :max-page-size 3}}
            (f [:sf/member-ids {'as "hello/world" 'limit 3}]))

    (expect {:pull/key :sf/member-ids
             :pull/key-id (c/key->id :sf/member-ids)
             :pull/key-opts {:max-page-size 3
                             :as :hello/world
                             :filter [:sexp/logical {:op/boolean 'and
                                                     :list/sexp [[:sexp/field 'sf/human?]]}]}}
            (pull/vector->key-info {:pull/variables {:$x 'sf/human?}}
                                   [:sf/member-ids {:as :hello/world :limit 3 :filter '$x}]))

    ;; errors
    (expect {::pull/error ::pull/unknown-options
             :pull/key :sf/id
             :pull/unknown-opts #{:meow}}
            (f [:sf/id {:meow 23}]))

    (expect {::pull/error ::pull/invalid-opts-map
             :pull/key :sf/id
             :pull/opts :meow}
            (f [:sf/id :meow]))

    (expect {::pull/error ::pull/invalid-opts-map-key
             :pull/key :sf/id
             :pull/opts {12 22}
             :pull/opts-key 12}
            (f [:sf/id {12 22}]))))

(deftest parse-test
  (let [relation? (fn [{:keys [pull/key]}]
                    (#{:sf/team-ids} key))
        f (fn [pattern]
            (try
              (pull/parse {:pull/relation? relation?} pattern)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect {:pull/key-infos [{:pull/key :sf/id
                               :pull/key-id (c/key->id :sf/id)}]
             :pull/relations []}
            (f [:sf/id]))

    (expect {:pull/key-infos [{:pull/key :sf/id
                               :pull/key-id (c/key->id :sf/id)}
                              {:pull/key :sf/rank
                               :pull/key-id (c/key->id :sf/rank)}]
             :pull/relations []}
            (f [:sf/id :sf/rank]))

    (expect {:pull/key-infos [{:pull/key :sf/id
                               :pull/key-id (c/key->id :sf/id)}
                              {:pull/key :sf/rank
                               :pull/key-id (c/key->id :sf/rank)}]
             :pull/relations [{:pull/key :sf/team-ids
                               :pull/key-id (c/key->id :sf/team-ids)
                               :pull/key-opts {:max-page-size 3}}]}
            (f [:sf/id :sf/rank [:sf/team-ids {:limit 3}]]))

    (expect {:pull/key-infos [{:pull/key :sf/id
                               :pull/key-id (c/key->id :sf/id)}
                              {:pull/key :sf/rank
                               :pull/key-id (c/key->id :sf/rank)}]
             :pull/relations [{:pull/key :sf/team-ids
                               :pull/key-id (c/key->id :sf/team-ids)
                               :pull/key-opts {:max-page-size 3}
                               :pull/pattern {:pull/key-infos [{:pull/key :sf/id
                                                                :pull/key-id (c/key->id :sf/id)}
                                                               {:pull/key :sf/name
                                                                :pull/key-id (c/key->id :sf/name)}]
                                              :pull/relations []}}]}
            (f [:sf/id :sf/rank {[:sf/team-ids {:limit 3}] [:sf/id :sf/name]}]))

    (expect {:pull/key-infos
             [{:pull/key :sf/id
               :pull/key-id (c/key->id :sf/id)}
              {:pull/key :sf/rank
               :pull/key-id (c/key->id :sf/rank)}]
             :pull/relations
             [{:pull/key :sf/team-ids
               :pull/key-id (c/key->id :sf/team-ids)
               :pull/key-opts {:max-page-size 3}}
              {:pull/key :sf/team-ids
               :pull/key-id (c/key->id :sf/team-ids)
               :pull/key-opts {:as :sf/team-members}
               :pull/pattern {:pull/key-infos [{:pull/key :sf/id
                                                :pull/key-id (c/key->id :sf/id)}
                                               {:pull/key :sf/name
                                                :pull/key-id (c/key->id :sf/name)}]
                              :pull/relations []}}]}
            (f [:sf/id :sf/rank
                [:sf/team-ids {:limit 3}]
                {[:sf/team-ids {:as :sf/team-members}] [:sf/id :sf/name]}]))

    ;; error
    (expect {::pull/error ::pull/invalid-opts-map
             :pull/key :sf/id
             :pull/opts :as}
            (f [[:sf/id :as]]))))

(deftest assoc-kv-test
  (let [db-id (c/lookup-id :sf/id "uss-e-bridge-team")]
    ;; just the limit
    (expect {:db/id db-id
             :sf/member-ids ["picard"]}
            (pull/assoc-kv {}
                           {:pull/key :sf/member-ids
                            :pull/key-id (c/key->id :sf/member-ids)
                            :pull/key-opts {:max-page-size 1}}
                           {:db/id db-id}))

    (expect {:db/id db-id
             :sf/member-ids ["picard" "riker"]}
            (pull/assoc-kv {}
                           {:pull/key :sf/member-ids
                            :pull/key-id (c/key->id :sf/member-ids)
                            :pull/key-opts {:max-page-size 2}}
                           {:db/id db-id}))

    ;; as is applied here so we can support 1 key aliased many times
    (expect {:db/id db-id
             :sf/members ["picard" "riker"]}
            (pull/assoc-kv {:db/id db-id}
                           {:pull/key :sf/member-ids
                            :pull/key-id (c/key->id :sf/member-ids)
                            :pull/key-opts {:max-page-size 2 :as :sf/members}}
                           {:db/id db-id}))))


(deftest basic-pull-test
  (expect {:sf/id "picard"
           :sf/name "Jean-Luc Picard"
           :sf/rank "Captain"}
          (pull/pull {}
                     (pull/parse {} [:sf/id :sf/name :sf/rank])
                     (->db-id "picard"))))

(deftest basic-pull-with-symbols-test
  (expect {:sf/id "picard"
           :sf/name "Jean-Luc Picard"
           :sf/rank "Captain"}
          (pull/pull {}
                     (pull/parse {} '[sf/id sf/name sf/rank])
                     (->db-id "picard"))))


(deftest basic-pull-limit-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]
    (testing "baseline"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain-id "picard"
                :sf/team-ids ["uss-e-bridge-team" "uss-e-security-team" "uss-e-eng-team" "uss-e-away-team" "uss-e-med-team"]}]
              (pull/pull {} (pull/parse {} [:sf/id :sf/name :sf/captain-id :sf/team-ids]) ids)))


    (testing "limit"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain-id "picard"
                :sf/team-ids ["uss-e-bridge-team" "uss-e-security-team"]}]
              (pull/pull {} (pull/parse {} [:sf/id :sf/name :sf/captain-id [:sf/team-ids {:limit 2}]]) ids)))))

(deftest basic-as-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]
    (expect [{:starfleet/id "uss-e"
              :sf/name "USS Enterprise"
              :sf/captain-id "picard"}]
            (pull/pull {}
                       (pull/parse {} [[:sf/id {:as :starfleet/id}] :sf/name :sf/captain-id])
                       ids))))





(defn id-resolver
  [{:keys [pull/key db/id] :as m}]
  (let [v (c/value key id)]
    (if (coll? v)
      (mapv ->db-id v)
      (->db-id v))))

;; NB for the relations the data is returned in sorted :db/id order

(defn sf-relation?
  [{:keys [pull/key]}]
  (#{:sf/captain-id :sf/starship-id :sf/member-ids :sf/team-ids} key))

(deftest relations-test
  (testing "one"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain-id {:sf/id "picard"
                             :sf/name "Jean-Luc Picard"}}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?} [:sf/id :sf/name {:sf/captain-id [:sf/id :sf/name]}])
                       (->db-id "uss-e"))))

  (testing "many"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain-id {:sf/id "picard"
                             :sf/name "Jean-Luc Picard"}
             :sf/team-ids [{:sf/id "uss-e-bridge-team"
                            :sf/name "Bridge Team"
                            :sf/member-ids [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                            {:sf/id "riker" :sf/name "William T. Riker"}
                                            {:sf/id "troi" :sf/name "Deanna Troi"}
                                            {:sf/id "data" :sf/name "Data"}
                                            {:sf/id "worf" :sf/name "Worf"}]}
                           {:sf/id "uss-e-security-team"
                            :sf/name "Security Team"
                            :sf/member-ids [{:sf/id "yar" :sf/name "Tasha Yar"}
                                            {:sf/id "worf" :sf/name "Worf"}]}
                           {:sf/id "uss-e-eng-team"
                            :sf/name "Engineering Team"
                            :sf/member-ids [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}
                           {:sf/id "uss-e-away-team"
                            :sf/name "Away Team"
                            :sf/member-ids [{:sf/id "riker" :sf/name "William T. Riker"}
                                            {:sf/id "data" :sf/name "Data"}
                                            {:sf/id "worf" :sf/name "Worf"}]}
                           {:sf/id "uss-e-med-team"
                            :sf/name "Medical Team"
                            :sf/member-ids [{:sf/id "troi" :sf/name "Deanna Troi"}
                                            {:sf/id "bev-crusher" :sf/name "Beverly Crusher"}]}]}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id
                                    :sf/name
                                    {:sf/captain-id [:sf/id :sf/name]}
                                    {:sf/team-ids [:sf/id
                                                   :sf/name
                                                   {:sf/member-ids [:sf/id :sf/name]}]}])
                       (->db-id "uss-e")))))

(deftest many-relations-test
  (testing "one"
    (expect [{:sf/id "uss-e"
              :sf/name "USS Enterprise"
              :sf/captain-id {:sf/id "picard"
                              :sf/name "Jean-Luc Picard"}}
             {:sf/id "uss-d"
              :sf/name "USS Defiant"
              :sf/captain-id {:sf/id "sisko"
                              :sf/name "Benjamin Sisko"}}]
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?} [:sf/id :sf/name {:sf/captain-id [:sf/id :sf/name]}])
                       (mapv ->db-id ["uss-e" "uss-d"]))))

  (testing "many"
    (expect [{:sf/id "uss-e"
              :sf/name "USS Enterprise"
              :sf/captain-id {:sf/id "picard"
                              :sf/name "Jean-Luc Picard"}
              :sf/team-ids [{:sf/id "uss-e-bridge-team"
                             :sf/name "Bridge Team"
                             :sf/member-ids [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                             {:sf/id "riker" :sf/name "William T. Riker"}
                                             {:sf/id "troi" :sf/name "Deanna Troi"}
                                             {:sf/id "data" :sf/name "Data"}
                                             {:sf/id "worf" :sf/name "Worf"}]}
                            {:sf/id "uss-e-security-team"
                             :sf/name "Security Team"
                             :sf/member-ids [{:sf/id "yar" :sf/name "Tasha Yar"}
                                             {:sf/id "worf" :sf/name "Worf"}]}
                            {:sf/id "uss-e-eng-team"
                             :sf/name "Engineering Team"
                             :sf/member-ids [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}
                            {:sf/id "uss-e-away-team"
                             :sf/name "Away Team"
                             :sf/member-ids [{:sf/id "riker" :sf/name "William T. Riker"}
                                             {:sf/id "data" :sf/name "Data"}
                                             {:sf/id "worf" :sf/name "Worf"}]}
                            {:sf/id "uss-e-med-team"
                             :sf/name "Medical Team"
                             :sf/member-ids [{:sf/id "troi" :sf/name "Deanna Troi"}
                                             {:sf/id "bev-crusher" :sf/name "Beverly Crusher"}]}]}
             {:sf/id "uss-d"
              :sf/name "USS Defiant"
              :sf/captain-id {:sf/id "sisko"
                              :sf/name "Benjamin Sisko"}
              :sf/team-ids [{:sf/id "uss-d-med-team"
                             :sf/name "Medical Team"
                             :sf/member-ids [{:sf/id "odo" :sf/name "Odo"}]}
                            {:sf/id "uss-d-bridge-team"
                             :sf/name "Bridge Team"
                             :sf/member-ids [{:sf/id "sisko" :sf/name "Benjamin Sisko"}
                                             {:sf/id "nerys" :sf/name "Kira Nerys"}]}]}]
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id
                                    :sf/name
                                    {:sf/captain-id [:sf/id :sf/name]}
                                    {:sf/team-ids [:sf/id
                                                   :sf/name
                                                   {:sf/member-ids [:sf/id :sf/name]}]}])
                       (mapv ->db-id ["uss-e" "uss-d"])))))


(deftest relations-as-test
  (testing "one"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain {:sf/id "picard"
                          :sf/name "Jean-Luc Picard"}}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id :sf/name {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}])
                       (->db-id "uss-e"))))

  (testing "many"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain {:sf/id "picard"
                          :sf/name "Jean-Luc Picard"}
             :sf/teams [{:sf/id "uss-e-bridge-team"
                         :sf/name "Bridge Team"
                         :sf/members [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                      {:sf/id "riker" :sf/name "William T. Riker"}
                                      {:sf/id "troi" :sf/name "Deanna Troi"}
                                      {:sf/id "data" :sf/name "Data"}
                                      {:sf/id "worf" :sf/name "Worf"}]}
                        {:sf/id "uss-e-security-team"
                         :sf/name "Security Team"
                         :sf/members [{:sf/id "yar" :sf/name "Tasha Yar"}
                                      {:sf/id "worf" :sf/name "Worf"}]}
                        {:sf/id "uss-e-eng-team"
                         :sf/name "Engineering Team"
                         :sf/members [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}
                        {:sf/id "uss-e-away-team"
                         :sf/name "Away Team"
                         :sf/members [{:sf/id "riker" :sf/name "William T. Riker"}
                                      {:sf/id "data" :sf/name "Data"}
                                      {:sf/id "worf" :sf/name "Worf"}]}
                        {:sf/id "uss-e-med-team"
                         :sf/name "Medical Team"
                         :sf/members [{:sf/id "troi" :sf/name "Deanna Troi"}
                                      {:sf/id "bev-crusher" :sf/name "Beverly Crusher"}]}]}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id
                                    :sf/name
                                    {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                                    {[:sf/team-ids {:as :sf/teams}] [:sf/id
                                                                     :sf/name
                                                                     {[:sf/member-ids {:as :sf/members}] [:sf/id :sf/name]}]}])
                       (->db-id "uss-e")))))

(deftest relations-as-limit-test
  (testing "one"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain {:sf/id "picard"
                          :sf/name "Jean-Luc Picard"}}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id :sf/name {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}])
                       (->db-id "uss-e"))))

  (testing "many"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain {:sf/id "picard"
                          :sf/name "Jean-Luc Picard"}
             :sf/teams [{:sf/id "uss-e-bridge-team"
                         :sf/name "Bridge Team"
                         :sf/members [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                      {:sf/id "riker" :sf/name "William T. Riker"}]}
                        {:sf/id "uss-e-security-team"
                         :sf/name "Security Team"
                         :sf/members [{:sf/id "yar" :sf/name "Tasha Yar"}
                                      {:sf/id "worf" :sf/name "Worf"}]}
                        {:sf/id "uss-e-eng-team"
                         :sf/name "Engineering Team"
                         :sf/members [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}]}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id
                                    :sf/name
                                    {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                                    {[:sf/team-ids {:as :sf/teams :limit 3}]
                                     [:sf/id
                                      :sf/name
                                      {[:sf/member-ids {:as :sf/members :limit 2}] [:sf/id :sf/name]}]}])
                       (->db-id "uss-e")))))



(defn depth-id-resolver
  [{:keys [pull/depth pull/key db/id]}]
  (when (> depth 1)
    (throw (ex-info "depth exceeded" {:depth depth})))
  (let [v (c/value key id)]
    (if (coll? v)
      (mapv ->db-id v)
      (->db-id v))))

(defn internal->external-relation-finalizer
  [{:keys [pull/concept pull/key pull/reified-relation?] :as m}]
  (cond-> concept
    (not reified-relation?)
    (assoc key (mapv (partial c/value :sf/id) (get concept key)))))

(deftest relation-depth-test
  (let [f (fn [pattern]
            (try
              (pull/pull {:pull/relation-value depth-id-resolver
                          :pull/relation-finalizer internal->external-relation-finalizer}
                         (pull/parse {:pull/relation? sf-relation?} pattern)
                         (->db-id "uss-e"))
              (catch Exception ex
                {:ex/message (ex-message ex)
                 :ex/data (ex-data ex)})))]

    (testing "depth NOT exceeded"
      (expect {:sf/id "uss-e"
               :sf/name "USS Enterprise"
               :sf/captain {:sf/id "picard" :sf/name "Jean-Luc Picard"}
               :sf/teams [{:sf/id "uss-e-bridge-team"
                           :sf/member-ids ["picard" "riker" "troi" "data" "worf"]
                           :sf/name "Bridge Team"}
                          {:sf/id "uss-e-security-team"
                           :sf/member-ids ["yar" "worf"]
                           :sf/name "Security Team"}
                          {:sf/id "uss-e-eng-team"
                           :sf/member-ids ["la-forge"]
                           :sf/name "Engineering Team"}]}
              (f [:sf/id :sf/name
                  {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                  {[:sf/team-ids {:as :sf/teams :limit 3}] [:sf/id :sf/name :sf/member-ids]}])))

    (expect {:ex/message "depth exceeded"
             :ex/data {:depth 2}}
            (f [:sf/id
                :sf/name
                {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                {[:sf/team-ids {:as :sf/teams :limit 3}]
                 [:sf/id
                  :sf/name
                  {[:sf/member-ids {:as :sf/members :limit 2}] [:sf/id :sf/name]}]}]))))


(defn restricted-keys-finalizer
  [{:keys [pull/concept pull/parsed-pattern] :as m}]
  (let [{:keys [pull/k->as]} parsed-pattern
        all-restricted (set/union #{:sf/id} (set (get k->as :sf/id)))]
    (apply dissoc concept all-restricted)))

(defn restricted-pattern-finalizer
  [{:keys [pull/key-infos pull/relations] :as m}]
  (let [k->as (into {}
                    (for [{:keys [pull/key pull/key-opts]} key-infos
                          :let [{:keys [as]} key-opts]
                          :when as]
                      [key #{as}]))
        r->as (into {}
                    (for [{:keys [pull/key pull/key-opts]} relations
                          :let [{:keys [as]} key-opts]
                          :when as]
                      [key #{as}]))]
    (assoc m :pull/k->as (merge-with set/union k->as r->as))))


(deftest finalizer-test
  (testing "one"
    (expect {:sf/name "USS Enterprise"
             :sf/captain {:sf/name "Jean-Luc Picard"}}
            (pull/pull {:pull/relation-value id-resolver
                        :pull/concept-finalizer restricted-keys-finalizer}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id :sf/name {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}])
                       (->db-id "uss-e"))))

  (testing "one renamed"
    (expect {:sf/name "USS Enterprise"
             :sf/captain {:sf/name "Jean-Luc Picard"}}
            (pull/pull {:pull/relation-value id-resolver
                        :pull/concept-finalizer restricted-keys-finalizer}
                       (pull/parse {:pull/relation? sf-relation?
                                    :pull/pattern-finalizer restricted-pattern-finalizer}
                                   [:sf/id :sf/name {[:sf/captain-id {:as :sf/captain}] [[:sf/id {:as :sneaky/id}] :sf/name]}])
                       (->db-id "uss-e"))))

  (testing "many"
    (expect {:sf/name "USS Enterprise"
             :sf/captain {:sf/name "Jean-Luc Picard"}
             :sf/teams [{:sf/name "Bridge Team"
                         :sf/members [{:sf/name "Jean-Luc Picard"}
                                      {:sf/name "William T. Riker"}]}
                        {:sf/name "Security Team"
                         :sf/members [{:sf/name "Tasha Yar"}
                                      {:sf/name "Worf"}]}
                        {:sf/name "Engineering Team"
                         :sf/members [{:sf/name "Geordi La Forge"}]}]}
            (pull/pull {:pull/relation-value id-resolver
                        :pull/concept-finalizer restricted-keys-finalizer}
                       (pull/parse {:pull/relation? sf-relation?
                                    :pull/pattern-finalizer restricted-pattern-finalizer}
                                   [:sf/id
                                    :sf/name
                                    {[:sf/captain-id {:as :sf/captain}] [[:sf/id {:as :captain/id}] :sf/name]}
                                    {[:sf/team-ids {:as :sf/teams :limit 3}]
                                     [[:sf/id {:as :team/id}]
                                      :sf/name
                                      {[:sf/member-ids {:as :sf/members :limit 2}] [[:sf/id {:as :member/id}] :sf/name]}]}])
                       (->db-id "uss-e")))))


(deftest relation-filter-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain {:sf/id "picard"
                        :sf/name "Jean-Luc Picard"}
           :sf/teams [{:sf/id "uss-e-bridge-team"
                       :sf/name "Bridge Team"
                       :sf/members [{:sf/id "troi" :sf/name "Deanna Troi"} ;; troi has a lower db/id so is first here
                                    {:sf/id "data" :sf/name "Data"}]}
                      {:sf/id "uss-e-security-team"
                       :sf/name "Security Team"
                       :sf/members []}
                      {:sf/id "uss-e-eng-team"
                       :sf/name "Engineering Team"
                       :sf/members []}]}
          (pull/pull {:pull/relation-value id-resolver}
                     (pull/parse {:pull/relation? sf-relation?}
                                 [:sf/id
                                  :sf/name
                                  {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                                  {[:sf/team-ids {:as :sf/teams :limit 3}]
                                   [:sf/id
                                    :sf/name
                                    {[:sf/member-ids
                                      {:as :sf/members
                                       :filter '(or sf/betazoid? sf/android?)}]
                                     [:sf/id :sf/name]}]}])
                     (->db-id "uss-e")))

  (testing "limit"
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain {:sf/id "picard"
                          :sf/name "Jean-Luc Picard"}
             :sf/teams [{:sf/id "uss-e-bridge-team"
                         :sf/name "Bridge Team"
                         ;; troi has a lower db/id so is first here
                         :sf/members [{:sf/id "troi" :sf/name "Deanna Troi"}]}
                        {:sf/id "uss-e-security-team"
                         :sf/name "Security Team"
                         :sf/members []}
                        {:sf/id "uss-e-eng-team"
                         :sf/name "Engineering Team"
                         :sf/members []}]}
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/relation? sf-relation?}
                                   [:sf/id
                                    :sf/name
                                    {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                                    {[:sf/team-ids {:as :sf/teams :limit 3}]
                                     [:sf/id
                                      :sf/name
                                      {[:sf/member-ids
                                        {:as :sf/members
                                         :limit 1
                                         :filter '(or sf/betazoid? sf/android?)}]
                                       [:sf/id :sf/name]}]}])
                       (->db-id "uss-e")))))


(deftest relation-filter-variable-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain {:sf/id "picard"
                        :sf/name "Jean-Luc Picard"}
           :sf/teams [{:sf/id "uss-e-bridge-team"
                       :sf/name "Bridge Team"
                       :sf/members [{:sf/id "troi" :sf/name "Deanna Troi"} ;; troi has a lower db/id so is first here
                                    {:sf/id "data" :sf/name "Data"}]}
                      {:sf/id "uss-e-security-team"
                       :sf/name "Security Team"
                       :sf/members []}
                      {:sf/id "uss-e-eng-team"
                       :sf/name "Engineering Team"
                       :sf/members []}]}
          (pull/pull {:pull/relation-value id-resolver}
                     (pull/parse {:pull/relation? sf-relation?
                                  :pull/variables {:$x '(or sf/betazoid? sf/android?)}}
                                 [:sf/id
                                  :sf/name
                                  {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                                  {[:sf/team-ids {:as :sf/teams :limit 3}]
                                   [:sf/id
                                    :sf/name
                                    {[:sf/member-ids
                                      {:as :sf/members
                                       :filter '$x}]
                                     [:sf/id :sf/name]}]}])
                     (->db-id "uss-e"))))


(defn metadata-finalizer
  [{:keys [pull/concept pull/all-ids pull/output-key]}]
  (if-not all-ids
    concept
    (let [metadata-key (keyword (str (symbol output-key) ":metadata"))]
      (assoc concept metadata-key {:summary/estimated-count (alength all-ids)}))))

(deftest relation-filter-finalizer-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain {:sf/id "picard"
                        :sf/name "Jean-Luc Picard"}
           :sf/teams:metadata {:summary/estimated-count 5}
           :sf/teams [{:sf/id "uss-e-bridge-team"
                       :sf/name "Bridge Team"
                       :sf/members:metadata {:summary/estimated-count 4}
                       :sf/members [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                    {:sf/id "riker" :sf/name "William T. Riker"}]}
                      {:sf/id "uss-e-security-team"
                       :sf/name "Security Team"
                       :sf/members:metadata {:summary/estimated-count 2}
                       :sf/members [{:sf/id "yar" :sf/name "Tasha Yar"}
                                    {:sf/id "worf" :sf/name "Worf"}]}
                      {:sf/id "uss-e-eng-team"
                       :sf/name "Engineering Team"
                       :sf/members:metadata {:summary/estimated-count 1}
                       :sf/members [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}]}
          (pull/pull {:pull/relation-value id-resolver
                      :pull/relation-finalizer metadata-finalizer}
                     (pull/parse
                      {:pull/relation? sf-relation?}
                      [:sf/id
                       :sf/name
                       {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                       {[:sf/team-ids {:as :sf/teams :limit 3}]
                        [:sf/id
                         :sf/name
                         {[:sf/member-ids
                           {:as :sf/members
                            :limit 2
                            :filter '(or sf/human? sf/klingon?)}]
                          [:sf/id :sf/name]}]}])
                     (->db-id "uss-e"))))



(def external-relation->internal-relation
  {:sf/member-ids :sf/-member-ids
   :sf/team-ids :sf/-team-ids
   :sf/captain-id :sf/-captain-id})

(defn relation?
  [{:keys [pull/key]}]
  (some? (external-relation->internal-relation key)))


(defn pre-indexed-relation-value
  [{:keys [pull/key db/id]}]
  (some-> key
          external-relation->internal-relation
          (c/value id)))


(defn unexpanded-finalizer
  [{:keys [pull/concept pull/all-ids pull/output-key] :as m}]
  (let [id+ (get concept output-key)
        ->external-id  (partial c/value :sf/id)
        v (if (int? id+)
            (->external-id id+)
            (mapv ->external-id id+))
        concept (assoc concept output-key v)]
    (if-not all-ids
      concept
      (let [metadata-key (keyword (str (symbol output-key) ":metadata"))]
        (assoc concept metadata-key {:summary/estimated-count (alength all-ids)})))))


(deftest unexpanded-relation-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain "picard"
           :sf/teams:metadata {:summary/estimated-count 5}
           :sf/teams ["uss-e-bridge-team"
                      "uss-e-security-team"
                      "uss-e-eng-team"]}
          (pull/pull {:pull/relation-value pre-indexed-relation-value
                      :pull/relation-finalizer unexpanded-finalizer}
                     (pull/parse
                      {:pull/relation? relation?}
                      [:sf/id
                       :sf/name
                       [:sf/captain-id {:as :sf/captain}]
                       [:sf/team-ids {:as :sf/teams :limit 3}]])
                     (->db-id "uss-e")))

  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain {:sf/id "picard"
                        :sf/name "Jean-Luc Picard"}
           :sf/teams:metadata {:summary/estimated-count 5}
           :sf/teams [{:sf/id "uss-e-bridge-team"
                       :sf/name "Bridge Team"
                       :sf/members:metadata {:summary/estimated-count 4}
                       :sf/members [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                    {:sf/id "riker" :sf/name "William T. Riker"}]}
                      {:sf/id "uss-e-security-team"
                       :sf/name "Security Team"
                       :sf/members:metadata {:summary/estimated-count 2}
                       :sf/members [{:sf/id "yar" :sf/name "Tasha Yar"}
                                    {:sf/id "worf" :sf/name "Worf"}]}
                      {:sf/id "uss-e-eng-team"
                       :sf/name "Engineering Team"
                       :sf/members:metadata {:summary/estimated-count 1}
                       :sf/members [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}]}
          (pull/pull {:pull/relation-value pre-indexed-relation-value
                      :pull/relation-finalizer metadata-finalizer}
                     (pull/parse
                      {:pull/relation? sf-relation?}
                      [:sf/id
                       :sf/name
                       {[:sf/captain-id {:as :sf/captain}] [:sf/id :sf/name]}
                       {[:sf/team-ids {:as :sf/teams :limit 3}]
                        [:sf/id
                         :sf/name
                         {[:sf/member-ids
                           {:as :sf/members
                            :limit 2
                            :filter '(or sf/human? sf/klingon?)}]
                          [:sf/id :sf/name]}]}])
                     (->db-id "uss-e"))))


(deftest gh-109--unknown-field-NPE-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain-id {:sf/id "picard"
                           :sf/name "Jean-Luc Picard"}}
          (pull/pull {:pull/relation-value id-resolver}
                     (pull/parse {:pull/relation? sf-relation?} [:sf/id :sf/name {:sf/captain-id [:sf/id :sf/name :undefined/field-kd83jj]}])
                     (->db-id "uss-e"))))


;;------------------------------------------------------------------------
;; Virtual Attributes
;;------------------------------------------------------------------------
(defn team-assigned-crew-ids-resolver
  [{:keys [pull/concept] :as _rel-opts}]
  (->> concept
       :db/id
       (c/value :sf/team-ids)
       (map ->db-id)
       (map #(c/value :sf/member-ids %))
       (apply set/union)
       (map ->db-id)
       i/set))


(defn teams-count-resolver
  [{:keys [pull/concept] :as _ctx}]
  ;; :pull/concept is guaranteed to only have `:db/id` the
  ;; rest of the fields are dependent on the pull pattern specifed
  (->> concept :db/id (c/value :sf/team-ids) count))

(def virtual-attributes-info
  {:sf/team-assigned-crew-ids
   {:pull.virtual-attribute/resolver-fn team-assigned-crew-ids-resolver
    :pull.virtual-attribute/relation? true}
   :sf/teams-count
   {:pull.virtual-attribute/resolver-fn teams-count-resolver}})

(deftest parse-virtual-attribute-test
  (expect {:pull/key-infos [{:pull/key :sf/id
                             :pull/key-id (c/key->id :sf/id)}
                            {:pull/key :sf/type
                             :pull/key-id (c/key->id :sf/type)}
                            {:pull/key :sf/name
                             :pull/key-id (c/key->id :sf/name)}
                            {:pull/key :sf/team-ids
                             :pull/key-id (c/key->id :sf/team-ids)}
                            {:pull/key :sf/teams-count
                             :pull.virtual-attribute/resolver-fn teams-count-resolver}]
           :pull/relations []}
          (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                      [:sf/id :sf/type :sf/name :sf/team-ids :sf/teams-count]))

  (expect {:pull/key-infos [{:pull/key :sf/id
                             :pull/key-id (c/key->id :sf/id)}
                            {:pull/key :sf/type
                             :pull/key-id (c/key->id :sf/type)}
                            {:pull/key :sf/name
                             :pull/key-id (c/key->id :sf/name)}
                            {:pull/key :sf/team-ids
                             :pull/key-id (c/key->id :sf/team-ids)}
                            {:pull/key :sf/teams-count
                             :pull/key-opts {:as :sf/virtual-attr}
                             :pull.virtual-attribute/resolver-fn teams-count-resolver}]
           :pull/relations []}
          (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                      [:sf/id :sf/type :sf/name :sf/team-ids [:sf/teams-count {:as :sf/virtual-attr}]])))

(deftest parse-virtual-attribute-relation-test
  (expect {:pull/key-infos [{:pull/key :sf/id
                             :pull/key-id (c/key->id :sf/id)}
                            {:pull/key :sf/type
                             :pull/key-id (c/key->id :sf/type)}
                            {:pull/key :sf/name
                             :pull/key-id (c/key->id :sf/name)}
                            {:pull/key :sf/team-ids
                             :pull/key-id (c/key->id :sf/team-ids)}]
           :pull/relations [{:pull/key :sf/team-assigned-crew-ids
                             :pull/key-opts {:as :sf/virtual-attr}
                             :pull.virtual-attribute/relation? true
                             :pull.virtual-attribute/resolver-fn team-assigned-crew-ids-resolver
                             :pull/pattern {:pull/key-infos [{:pull/key :sf/id
                                                              :pull/key-id (c/key->id :sf/id)}
                                                             {:pull/key :sf/name
                                                              :pull/key-id (c/key->id :sf/name)}]
                                            :pull/relations []}}]}
          (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                      [:sf/id :sf/type :sf/name :sf/team-ids
                       {[:sf/team-assigned-crew-ids {:as :sf/virtual-attr}] [:sf/id :sf/name]}])))


(deftest virtual-attr-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain-id "picard"
           :sf/team-ids ["uss-e-bridge-team"
                         "uss-e-security-team"
                         "uss-e-eng-team"
                         "uss-e-away-team"
                         "uss-e-med-team"]
           :sf/teams-count 5}
          (pull/pull {:pull/relation-value id-resolver}
                     (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                                 [:sf/id :sf/name :sf/captain-id :sf/team-ids :sf/teams-count])
                     (->db-id "uss-e")))

  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain-id "picard"
           :sf/teams-count 5}
          (pull/pull {:pull/relation-value id-resolver}
                     (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                                 [:sf/id :sf/name :sf/captain-id :sf/teams-count])
                     (->db-id "uss-e"))))

(deftest virtual-attr-relation-test
  (let [result (pull/pull {:pull/relation-value id-resolver}
                          (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                                      [:sf/id :sf/name :sf/captain-id :sf/team-ids
                                       {[:sf/team-assigned-crew-ids {:as :sf/team-assigned-crew}]
                                        [:sf/id]}])
                          (->db-id "uss-e"))]
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain-id "picard"
             :sf/team-ids ["uss-e-bridge-team"
                           "uss-e-security-team"
                           "uss-e-eng-team"
                           "uss-e-away-team"
                           "uss-e-med-team"]
             :sf/team-assigned-crew [{:sf/id "picard"}
                                     {:sf/id "riker"}
                                     {:sf/id "troi"}
                                     {:sf/id "data"}
                                     {:sf/id "yar"}
                                     {:sf/id "worf"}
                                     {:sf/id "la-forge"}
                                     {:sf/id "bev-crusher"}]})
    result))

(deftest virtual-attr-relation-pagination-test
  (let [f (fn [page max-page-size]
            (pull/pull {:pull/relation-value id-resolver}
                       (pull/parse {:pull/virtual-attributes-info virtual-attributes-info}
                                   [:sf/id :sf/name :sf/captain-id :sf/team-ids
                                    {[:sf/team-assigned-crew-ids {:as :sf/team-assigned-crew
                                                                  :page page
                                                                  :max-page-size max-page-size}]
                                     [:sf/id]}])
                       (->db-id "uss-e")))
        g (comp :sf/team-assigned-crew f)
        result (f 0 2)]
    (expect {:sf/id "uss-e"
             :sf/name "USS Enterprise"
             :sf/captain-id "picard"
             :sf/team-ids ["uss-e-bridge-team"
                           "uss-e-security-team"
                           "uss-e-eng-team"
                           "uss-e-away-team"
                           "uss-e-med-team"]
             :sf/team-assigned-crew [{:sf/id "picard"}
                                     {:sf/id "riker"}]}
            result)

    ;;----------------------------------------------------------------------
    ;; Even page size
    ;;----------------------------------------------------------------------
    ;; page 0, max-page-size 2
    (expect [{:sf/id "picard"}
             {:sf/id "riker"}]
            (g 0 2))

    ;; page 1, max-page-size 2
    (expect [{:sf/id "troi"}
             {:sf/id "data"}]
            (g 1 2))


    ;; page 2, max-page-size 2
    (expect [{:sf/id "yar"}
             {:sf/id "worf"}]
            (g 2 2))

    ;; page 3, max-page-size 2
    (expect [{:sf/id "la-forge"}
             {:sf/id "bev-crusher"}]
            (g 3 2))

    ;; page 4, max-page-size 2
    (expect [] (g 4 2))

    ;;----------------------------------------------------------------------
    ;; Odd page size
    ;;----------------------------------------------------------------------
    ;; page 0, max-page-size 5
    (expect [{:sf/id "picard"}
             {:sf/id "riker"}
             {:sf/id "troi"}
             {:sf/id "data"}
             {:sf/id "yar"}]
            (g 0 5))

    ;; page 1, max-page-size 5
    (expect [{:sf/id "worf"}
             {:sf/id "la-forge"}
             {:sf/id "bev-crusher"}]
            (g 1 5))

    ;; page 2, max-page-size 5
    (expect [] (g 2 5))))


(defn multiple-aliases-relation-finalizer
  [{:keys [pull/key pull/output-key pull/concept pull/reified-relation?] :as m}]
  (if reified-relation?
    concept
    (assoc concept output-key (c/value key (:db/id concept)))))

(deftest field-multiple-aliases-test
  (expect {:sf/id "picard"
           :starfleet/id "picard"
           :system/id "picard"
           :federation/id "picard"
           :sf/name "Jean-Luc Picard"
           :sf/rank "Captain"}
          (pull/pull {:pull/relation-value id-resolver
                      :pull/relation-finalizer multiple-aliases-relation-finalizer}
                     (pull/parse {:pull/relation? sf-relation?}
                                 [:sf/id
                                  [:sf/id {:as :starfleet/id}]
                                  [:sf/id {:as :system/id}]
                                  [:sf/id {:as :federation/id}]
                                  :sf/name :sf/rank])
                     (->db-id "picard"))))

(deftest to-one-relation-mutliple-aliases-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain-id "picard"
           :federation/captain-id "picard"}
          (pull/pull {:pull/relation-value id-resolver
                      :pull/relation-finalizer multiple-aliases-relation-finalizer}
                     (pull/parse {:pull/relation? sf-relation?}
                                 [:sf/id
                                  :sf/name
                                  :sf/captain-id
                                  [:sf/captain-id {:as :federation/captain-id}]])
                     (->db-id "uss-e"))))

(deftest to-many-relation-mutliple-aliases-test
  (expect {:sf/id "uss-e"
           :sf/name "USS Enterprise"
           :sf/captain-id "picard"
           :sf/team-ids ["uss-e-bridge-team"
                         "uss-e-security-team"
                         "uss-e-eng-team"
                         "uss-e-away-team"
                         "uss-e-med-team"]
           :sf/teams [{:sf/id "uss-e-bridge-team"
                       :sf/name "Bridge Team"}
                      {:sf/id "uss-e-security-team"
                       :sf/name "Security Team"}]}
          (pull/pull {:pull/relation-value id-resolver
                      :pull/relation-finalizer multiple-aliases-relation-finalizer}
                     (pull/parse {:pull/relation? sf-relation?}
                                 [:sf/id
                                  :sf/name
                                  :sf/captain-id
                                  :sf/team-ids
                                  {[:sf/team-ids {:as :sf/teams :limit 2}] [:sf/id :sf/name]}])
                     (->db-id "uss-e"))))
