(ns conceptual.alpha.pull-test
  (:require
   [clojure.test :refer [deftest use-fixtures testing]]
   [conceptual.test.core :as test.core]
   [conceptual.core :as c]
   [conceptual.int-sets :as i]
   [conceptual.alpha.pull :as pull]
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

(deftest vector->key-info-test
  (let [f (fn [v]
            (try
              (pull/vector->key-info {:pull/variables {'$x 'sf/human?}} v)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect {:pull/key :foo/bar} (f [:foo/bar]))
    (expect {:pull/key :foo/bar} (f [:foo/bar {}]))
    (expect {:pull/key :foo/bar :pull/key-opts {:as :hello/world}} (f [:foo/bar {:as :hello/world}]))
    (expect {:pull/key :foo/bar :pull/key-opts {:as :hello/world :limit 3}} (f [:foo/bar {:as :hello/world :limit 3}]))
    (expect {:pull/key :foo/bar :pull/key-opts {:as :hello/world :limit 3}} (f [:foo/bar {'as "hello/world" 'limit 3}]))
    (expect {:pull/key :foo/bar
             :pull/key-opts {:limit 3
                             :as :hello/world
                             :filter [:sexp/logical {:op/boolean 'and
                                                     :list/sexp [[:sexp/field 'sf/human?]]}]}}
            (pull/vector->key-info {:pull/variables {:$x 'sf/human?}}
                                   [:foo/bar {:as :hello/world :limit 3 :filter '$x}]))

    ;; errors
    (expect {::pull/error ::pull/unknown-options
             :pull/key :foo/bar
             :pull/unknown-opts #{:meow}}
            (f [:foo/bar {:meow 23}]))

    (expect {::pull/error ::pull/invalid-opts-map
             :pull/key :foo/bar
             :pull/opts :meow}
            (f [:foo/bar :meow]))

    (expect {::pull/error ::pull/invalid-opts-map-key
             :pull/key :foo/bar
             :pull/opts {12 22}
             :pull/opts-key 12}
            (f [:foo/bar {12 22}]))))

(deftest parse-test
  (let [relation? (fn [{:keys [pull/key]}]
                    (#{:x/rel} key))
        f (fn [pattern]
            (try
              (pull/parse {:pull/relation? relation?} pattern)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect {:pull/key-infos [{:pull/key :foo/bar}]
             :pull/relations []
             :pull/data []
             :pull/k->as {}}
            (f [:foo/bar]))

    (expect {:pull/key-infos [{:pull/key :foo/bar} {:pull/key :foo/baz}]
             :pull/relations []
             :pull/data []
             :pull/k->as {}}
            (f [:foo/bar :foo/baz]))

    (expect {:pull/key-infos [{:pull/key :a}
                              {:pull/key :b}
                              {:pull/key :c :pull/key-opts {:limit 3}}]
             :pull/relations []
             :pull/data []
             :pull/k->as {}}
            (f [:a :b [:c {:limit 3}]]))

    (expect {:pull/key-infos [{:pull/key :x/a}
                              {:pull/key :x/b}
                              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations []
             :pull/data []
             :pull/k->as {}}
            (f [:x/a :x/b [:x/c {:limit 3}]]))


    (expect {:pull/key-infos [{:pull/key :x/a}
                              {:pull/key :x/b}
                              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations [{:pull/key :x/rel
                               :pull/pattern {:pull/key-infos [{:pull/key :foo/a}
                                                               {:pull/key :foo/b}]
                                              :pull/relations []
                                              :pull/data []
                                              :pull/k->as {}}}]
             :pull/data []
             :pull/k->as {}}
            (f [:x/a :x/b [:x/c {:limit 3}] {:x/rel [:foo/a :foo/b]}]))

    (expect {:pull/key-infos
             [{:pull/key :x/a}
              {:pull/key :x/b}
              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations
             [{:pull/key :x/rel
               :pull/key-opts {:as :x/foos}
               :pull/pattern
               {:pull/key-infos [{:pull/key :foo/a} {:pull/key :foo/b}]
                :pull/relations []
                :pull/data []
                :pull/k->as {}}}]
             :pull/data []
             :pull/k->as {:x/rel :x/foos}}
            (f [:x/a :x/b [:x/c {:limit 3}] {[:x/rel {:as :x/foos}] [:foo/a :foo/b]}]))

    (expect {:pull/key-infos
             [{:pull/key :x/a}
              {:pull/key :x/b}
              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations [{:pull/key :x/rel}]
             :pull/data []
             :pull/k->as {}}
            (f '[x/a x/b  x/rel [x/c {:limit 3}]]))

    (expect {:pull/key-infos
             [{:pull/key :x/a}
              {:pull/key :x/b}
              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations [{:pull/key :x/rel :pull/key-opts {:as :x/foo}}]
             :pull/data []
             :pull/k->as {:x/rel :x/foo}}
            (f '[x/a x/b  [x/rel {:as x/foo}] [x/c {:limit 3}]]))

    ;; error
    (expect {::pull/error ::pull/invalid-opts-map
             :pull/key :foo/bar
             :pull/opts :as}
            (f [[:foo/bar :as]]))))

(deftest parse-data-test
  (let [relation? (fn [{:keys [pull/key] :as m}]
                    (#{:x/rel} key))
        data? (fn [{:keys [pull/key] :as m}]
                (#{:x/simple-map :x/list-of-maps} key))
        f (fn [pattern]
            (try
              (pull/parse {:pull/relation? relation?
                           :pull/data? data?} pattern)
              (catch Exception ex
                (ex-data ex))))]

    (expect {:pull/key-infos [{:pull/key :foo/bar}]
             :pull/relations []
             :pull/data [{:pull/key :x/simple-map
                          :pull/key-infos [{:pull/key :k1} {:pull/key :k2}]}]
             :pull/k->as {}}
            (f [:foo/bar {:x/simple-map [:k1 :k2]}]))

    (expect {:pull/key-infos [{:pull/key :foo/bar}]
             :pull/relations []
             :pull/data [{:pull/key :x/simple-map
                          :pull/key-opts {:as :the-simple-map}
                          :pull/key-infos [{:pull/key :k1} {:pull/key :k2}]}]
             :pull/k->as {}}
            (f [:foo/bar {[:x/simple-map {:as :the-simple-map}] [:k1 :k2]}]))

    (expect {:pull/key-infos
             [{:pull/key :x/a}
              {:pull/key :x/b}
              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations [{:pull/key :x/rel :pull/key-opts {:as :x/foo}}]
             :pull/data [{:pull/key :x/list-of-maps
                          :pull/key-infos [{:pull/key :k1}
                                                {:pull/key :k3
                                                 :pull/key-opts {:limit 10
                                                                 :as :k3-alias}}
                                                {:pull/key :k9}]}]
             :pull/k->as {:x/rel :x/foo}}
            (f '[x/a x/b  [x/rel {:as x/foo}] [x/c {:limit 3}] {x/list-of-maps [:k1 [:k3 {as k3-alias limit 10}] :k9]}]))

    (expect {:pull/key-infos
             [{:pull/key :x/a}
              {:pull/key :x/b}
              {:pull/key :x/c :pull/key-opts {:limit 3}}]
             :pull/relations [{:pull/key :x/rel :pull/key-opts {:as :x/foo}}]
             :pull/data [{:pull/key :x/list-of-maps
                          :pull/key-infos [{:pull/key true}
                                                {:pull/key 42
                                                 :pull/key-opts {:as :alias-42}}
                                                {:pull/key "hello"}
                                                {:pull/key :k3
                                                 :pull/key-opts {:limit 10
                                                                 :as :k3-alias}}
                                                {:pull/key :k9}]}]
             :pull/k->as {:x/rel :x/foo}}
            (f '[x/a x/b  [x/rel {:as x/foo}] [x/c {:limit 3}] {x/list-of-maps [true [42 {as alias-42}] "hello" [:k3 {as k3-alias limit 10}] :k9]}]))

    (testing "vector is ok with various data types that are not collections"
      (expect {:pull/key-infos [{:pull/key :foo/bar}]
               :pull/relations []
               :pull/data [{:pull/key :x/simple-map
                            :pull/key-infos
                            [{:pull/key :k1}
                             {:pull/key true}
                             {:pull/key 1}
                             {:pull/key "hi"}]}]
               :pull/k->as {}}
              (f [:foo/bar {:x/simple-map [:k1 true 1 "hi"]}])))

    (testing "[1] is ok as selection defaults to [1 {}]"
      (expect {:pull/key-infos [{:pull/key :foo/bar}]
               :pull/relations []
               :pull/data [{:pull/key :x/simple-map
                            :pull/key-infos
                            [{:pull/key :k1}
                             {:pull/key true}
                             {:pull/key 1}
                             {:pull/key "hi"}]}]
               :pull/k->as {}}
              (f [:foo/bar {:x/simple-map [:k1 true 1 "hi"]}]))
      (f [:foo/bar {:x/simple-map [:k1 true [1] "hi"]}]))

    (testing "set is ok"
      (expect {:pull/key-infos [{:pull/key :foo/bar}]
               :pull/relations []
               :pull/data [{:pull/key :x/simple-map
                            :pull/key-infos
                            [{:pull/key :k1}
                             {:pull/key :k2}]}]
               :pull/k->as {}}
              (f [:foo/bar {:x/simple-map #{:k1 :k2}}])))))


(deftest parse-error-when-data-not-list-like-test
  (let [relation? (fn [{:keys [pull/key] :as m}]
                    (#{:x/rel} key))
        data? (fn [{:keys [pull/key] :as m}]
                (#{:x/simple-map :x/list-of-maps} key))
        f (fn [pattern]
            (try
              (pull/parse {:pull/relation? relation?
                           :pull/data? data?} pattern)
              (catch Exception ex
                (ex-data ex))))]

    (testing "map results in error"
      (expect {:pull/key :x/simple-map
               ::pull/error ::pull/data-selection-keys-not-a-list}
              (f [:foo/bar {:x/simple-map {:k1 :k2}}])))

    (testing "int results in error"
      (expect {:pull/key :x/simple-map
               ::pull/error ::pull/data-selection-keys-not-a-list}
              (f [:foo/bar {:x/simple-map 42}])))

    (testing "string results in error"
      (expect {:pull/key :x/simple-map
               ::pull/error ::pull/data-selection-keys-not-a-list}
              (f [:foo/bar {:x/simple-map "hello"}])))))




(deftest parse-external-relation-test
  (let [relation? (fn [{:keys [pull/key]}]
                    (some? (#{:external/relation} key)))
        f (fn [pattern]
            (try
              (pull/parse {:pull/relation? relation?} pattern)
              (catch Exception ex
                (ex-data ex))))]

    (expect {:pull/key-infos [{:pull/key :foo/bar}
                              {:pull/key :foo/baz}]
             :pull/relations [{:pull/key :external/relation}]
             :pull/data []
             :pull/k->as {}}
            (f [:foo/bar :foo/baz :external/relation]))))

(deftest apply-key-info-test
  ;; just the limit
  (testing "limit"
    (expect {:sf/member-ids ["picard" "riker"]}
            (pull/apply-key-info {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {:pull/key :sf/member-ids
                                  :pull/key-opts {:limit 2}})))

  (testing "as NOT applied"
    (expect {:sf/member-ids ["picard" "riker"]}
            (pull/apply-key-info {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {:pull/key :sf/member-ids :pull/key-opts {:limit 2 :as :sf/members}})))

  (testing "as and limit ARE applied on same dest"
    (expect {:sf/members ["picard" "riker"]
             :sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
            (pull/apply-key-info {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {:pull/key :sf/member-ids :pull/key-opts {:limit 2 :as :sf/members}}
                                 :rename-key? true)))

  (testing "as and limit ARE applied on different dest"
    (expect {:sf/members ["picard" "riker"]}
            (pull/apply-key-info {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                                 {}
                                 {:pull/key :sf/member-ids :pull/key-opts {:limit 2 :as :sf/members}}
                                 :rename-key? true))))


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
  [{:keys [pull/key db/id]}]
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
  [{:keys [pull/concept pull/key pull/reified-relation?]}]
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
  [{:keys [pull/k->as pull/concept]}]
  (let [all-restricted (conj #{:sf/id} (k->as :sf/id))]
    (apply dissoc concept all-restricted)))


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
                       (pull/parse {:pull/relation? sf-relation?}
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
                       (pull/parse {:pull/relation? sf-relation?}
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
  [{:keys [pull/concept pull/pre-limit-ids pull/output-key]}]
  (if-not pre-limit-ids
    concept
    (let [metadata-key (keyword (str (symbol output-key) ":metadata"))]
      (assoc concept metadata-key {:summary/estimated-count (count pre-limit-ids)}))))

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
                       :sf/members [{:sf/id "yar" :sf/name "Tasha Yar"}
                                    {:sf/id "worf" :sf/name "Worf"}]}
                      {:sf/id "uss-e-eng-team"
                       :sf/name "Engineering Team"
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
  [{:keys [pull/concept pull/pre-limit-ids pull/output-key pull/key]}]
  (let [id+ (get concept key)
        ->external-id  (partial c/value :sf/id)
        v (if (int? id+)
            (->external-id id+)
            (mapv ->external-id id+))
        concept (assoc concept key v)]
    (if-not pre-limit-ids
      concept
      (let [metadata-key (keyword (str (symbol output-key) ":metadata"))]
        (assoc concept metadata-key {:summary/estimated-count (count pre-limit-ids)})))))

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
                       :sf/members [{:sf/id "yar" :sf/name "Tasha Yar"}
                                    {:sf/id "worf" :sf/name "Worf"}]}
                      {:sf/id "uss-e-eng-team"
                       :sf/name "Engineering Team"
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



(deftest pull-map-attrs
  (let [data? (fn [{:keys [pull/key]}]
                (#{:sf/kvs :sf/addresses} key))]

    (testing "baseline data structures"
      (expect {:sf/id "uss-e"
               :sf/name "USS Enterprise"
                                        ;:sf/captain-id "picard"
               :sf/captain-id {:sf/name  "Jean-Luc Picard"
                               :sf/addresses [{:street "409 W Water St"
                                               :city "Santa Fe"
                                               :state "NM"
                                               :postal-code "87501"}
                                              {:street "799 Poho Pl"
                                               :city "Paia"
                                               :state "HI"
                                               :postal-code "96779"}]
                               :sf/kvs {:university/name "Starfleet Academy"
                                        :university/degree "Engineering"
                                        :favorite-foods ["pizza" "burritos" "curry"]}}}
              (pull/pull {:pull/relation-value id-resolver}
                         (pull/parse
                          {:pull/relation? sf-relation?}
                          [:sf/id
                           :sf/name
                           {:sf/captain-id [:sf/name :sf/addresses :sf/kvs]}])
                         (->db-id "uss-e"))))


    (testing "data selections for 1 map and seq of maps"
      (expect {:sf/id "uss-e"
               :sf/name "USS Enterprise"
               :sf/captain-id {:sf/name  "Jean-Luc Picard"
                               :sf/addresses [{:city "Santa Fe"
                                               :state "NM"}
                                              {:city "Paia"
                                               :state "HI"}]
                               :sf/kvs {:university/name "Starfleet Academy"
                                        :favorite-foods ["pizza" "burritos" "curry"]}}}
              (pull/pull {:pull/relation-value id-resolver}
                         (pull/parse
                          {:pull/relation? sf-relation?
                           :pull/data? data?}
                          [:sf/id
                           :sf/name
                           {:sf/captain-id [:sf/name
                                            {:sf/addresses [:city :state]}
                                            {:sf/kvs [:university/name :favorite-foods]}]}])
                         (->db-id "uss-e"))))))

(deftest pull-map-attrs-opts
  (let [data? (fn [{:keys [pull/key]}]
                (#{:sf/kvs :sf/addresses} key))]
    (testing "data selections for 1 map and seq of maps"
      (expect {:sf/id "uss-e"
               :sf/name "USS Enterprise"
               :sf/captain-id {:sf/name  "Jean-Luc Picard"
                               :addrs [{:ville "Santa Fe"
                                        :department "NM"}]
                               :sf/kvs {:uni "Starfleet Academy"
                                        :favs ["pizza" "burritos"]}}}
              (pull/pull {:pull/relation-value id-resolver}
                         (pull/parse
                          {:pull/relation? sf-relation?
                           :pull/data? data?}
                          [:sf/id
                           :sf/name
                           {:sf/captain-id
                            [:sf/name
                             {[:sf/addresses {:as :addrs :limit 1}]
                              [[:city {:as :ville}] [:state {:as :department}]]}
                             {:sf/kvs [[:university/name {:as :uni}]
                                       [:favorite-foods {:as :favs :limit 2}]]}]}])
                         (->db-id "uss-e"))))))



;; TODO metadata for data specs
