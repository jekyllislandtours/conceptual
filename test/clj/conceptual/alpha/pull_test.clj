(ns conceptual.alpha.pull-test
  (:require [clojure.test :refer [deftest use-fixtures testing]]
            [conceptual.test.core :as test.core]
            [conceptual.core :as c]
            [conceptual.int-sets :as i]
            [conceptual.alpha.pull :as pull]
            [expectations.clojure.test :refer [expect]]))



(use-fixtures :each test.core/with-rdb)

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


(deftest vector->key-opts-test
  (let [f (fn [v]
            (try
              (pull/vector->key+opts v)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect [:foo/bar {}] (f [:foo/bar]))
    (expect [:foo/bar {:as :hello/world}] (f [:foo/bar :as :hello/world]))
    (expect [:foo/bar {:as :hello/world :limit 3}] (f [:foo/bar :as :hello/world :limit 3]))

    ;; errors
    (expect {::pull/error ::pull/unknown-options
             :pull/key :foo/bar
             :pull/unknown-opts #{:meow}}
            (f [:foo/bar :meow 23]))

    (expect {::pull/error ::pull/not-enough-input-for-options
             :pull/key :foo/bar
             :pull/opts [:meow]}
            (f [:foo/bar :meow]))))


(deftest parse-test
  (let [f (fn [pattern]
            (try
              (pull/parse pattern)
              (catch Exception ex
                (ex-data ex))))]

    ;; success
    (expect {:pull/ks #{:foo/bar}
             :pull/k+opts []
             :pull/relations nil}
            (f [:foo/bar]))

    (expect {:pull/ks #{:foo/bar :foo/baz}
             :pull/k+opts []
             :pull/relations nil}
            (f [:foo/bar :foo/baz]))

    (expect {:pull/ks #{:a :b :c}
             :pull/k+opts [[:c {:limit 3}]]
             :pull/relations nil}
            (f [:a :b [:c :limit 3]]))

    (expect {:pull/ks #{:x/a :x/b :x/c}
             :pull/k+opts [[:x/c {:limit 3}]]
             :pull/relations nil}
            (f [:x/a :x/b [:x/c :limit 3]]))

    (expect {:pull/ks #{:x/a :x/b :x/c}
             :pull/k+opts [[:x/c {:limit 3}]]
             :pull/relations [{:x/rel [:foo/a :foo/b]}]}
            (f [:x/a :x/b [:x/c :limit 3] {:x/rel [:foo/a :foo/b]}]))

    (expect {:pull/ks #{:x/a :x/b :x/c}
             :pull/k+opts [[:x/c {:limit 3}]]
             :pull/relations [{[:x/rel :as :x/foos] [:foo/a :foo/b]}]}
            (f [:x/a :x/b [:x/c :limit 3] {[:x/rel :as :x/foos] [:foo/a :foo/b]}]))

    ;; error
    (expect {::pull/error ::pull/not-enough-input-for-options
             :pull/key :foo/bar
             :pull/opts [:as]}
            (f [[:foo/bar :as]]))))

(deftest apply-key-opts-test
  ;; just the limit
  (expect {:sf/member-ids ["picard" "riker"]}
          (pull/apply-key-opts {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                               [:sf/member-ids {:limit 2}]))

  ;; limit and as
  (expect {:sf/members ["picard" "riker"]}
          (pull/apply-key-opts {:sf/member-ids ["picard" "riker" "data" "troi" "worf"]}
                               [:sf/member-ids {:limit 2 :as :sf/members}])))


(deftest basic-pull-test
  (expect [{:sf/id "picard"
            :sf/name "Jean-Luc Picard"
            :sf/rank "Captain"}]
          (pull/pull {} [:sf/id
                         :sf/name
                         :sf/rank]
                     (->> ["picard"]
                          (map ->db-id)
                          i/set))))


(deftest basic-pull-limit-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]
    (testing "baseline"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain-id "picard"
                :sf/team-ids ["uss-e-bridge-team" "uss-e-security-team" "uss-e-eng-team" "uss-e-away-team" "uss-e-med-team"]}]
              (pull/pull {} [:sf/id :sf/name :sf/captain-id :sf/team-ids] ids)))


    (testing "limit"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain-id "picard"
                :sf/team-ids ["uss-e-bridge-team" "uss-e-security-team"]}]
              (pull/pull {} [:sf/id :sf/name :sf/captain-id [:sf/team-ids :limit 2]] ids)))))

(deftest basic-as-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]
    (expect [{:starfleet/id "uss-e"
              :sf/name "USS Enterprise"
              :sf/captain-id "picard"}]
            (pull/pull {}
                       [[:sf/id :as :starfleet/id] :sf/name :sf/captain-id]
                       ids))))





(defn id-resolver
  [{:keys [pull/key db/id]}]
  (let [v (c/value key id)]
    (if (coll? v)
      (mapv ->db-id v)
      (->db-id v))))


(deftest relations-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]
    (testing "one"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain-id {:sf/id "picard"
                                :sf/name "Jean-Luc Picard"}}]
              (pull/pull {:pull/relation-value id-resolver}
                         [:sf/id :sf/name {:sf/captain-id [:sf/id :sf/name]}]
                         ids)))

    (testing "many"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain-id {:sf/id "picard"
                                :sf/name "Jean-Luc Picard"}
                :sf/team-ids [{:sf/id "uss-e-bridge-team"
                               :sf/name "Bridge Team"
                               :sf/member-ids [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                               {:sf/id "riker" :sf/name "William T. Riker"}
                                               {:sf/id "data" :sf/name "Data"}
                                               {:sf/id "troi" :sf/name "Deanna Troi"}
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
                               :sf/member-ids [{:sf/id "bev-crusher" :sf/name "Beverly Crusher"}
                                               {:sf/id "troi" :sf/name "Deanna Troi"}]}]}]
              (pull/pull {:pull/relation-value id-resolver}
                         [:sf/id
                          :sf/name
                          {:sf/captain-id [:sf/id :sf/name]}
                          {:sf/team-ids [:sf/id
                                         :sf/name
                                         {:sf/member-ids [:sf/id :sf/name]}]}]
                         ids)))))


(deftest relations-as-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]

    (testing "one"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain {:sf/id "picard"
                             :sf/name "Jean-Luc Picard"}}]
              (pull/pull {:pull/relation-value id-resolver}
                         [:sf/id :sf/name {[:sf/captain-id :as :sf/captain] [:sf/id :sf/name]}] ids)))

    (testing "many"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain {:sf/id "picard"
                             :sf/name "Jean-Luc Picard"}
                :sf/teams [{:sf/id "uss-e-bridge-team"
                            :sf/name "Bridge Team"
                            :sf/members [{:sf/id "picard" :sf/name "Jean-Luc Picard"}
                                         {:sf/id "riker" :sf/name "William T. Riker"}
                                         {:sf/id "data" :sf/name "Data"}
                                         {:sf/id "troi" :sf/name "Deanna Troi"}
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
                            :sf/members [{:sf/id "bev-crusher" :sf/name "Beverly Crusher"}
                                         {:sf/id "troi" :sf/name "Deanna Troi"}]}]}]
              (pull/pull {:pull/relation-value id-resolver}
                         [:sf/id
                          :sf/name
                          {[:sf/captain-id :as :sf/captain] [:sf/id :sf/name]}
                          {[:sf/team-ids :as :sf/teams] [:sf/id
                                                         :sf/name
                                                         {[:sf/member-ids :as :sf/members] [:sf/id :sf/name]}]}]
                         ids)))))

(deftest relations-as-limit-test
  (let [ids (->> ["uss-e"]
                 (map ->db-id)
                 i/set)]

    (testing "one"
      (expect [{:sf/id "uss-e"
                :sf/name "USS Enterprise"
                :sf/captain {:sf/id "picard"
                             :sf/name "Jean-Luc Picard"}}]
              (pull/pull {:pull/relation-value id-resolver}
                         [:sf/id :sf/name {[:sf/captain-id :as :sf/captain] [:sf/id :sf/name]}]
                         ids)))

    (testing "many"
      (expect [{:sf/id "uss-e"
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
                            :sf/members [{:sf/id "la-forge" :sf/name "Geordi La Forge"}]}]}]
              (pull/pull {:pull/relation-value id-resolver}
                         [:sf/id
                          :sf/name
                          {[:sf/captain-id :as :sf/captain] [:sf/id :sf/name]}
                          {[:sf/team-ids :as :sf/teams :limit 3]
                           [:sf/id
                            :sf/name
                            {[:sf/member-ids :as :sf/members :limit 2] [:sf/id :sf/name]}]}]
                         ids)))))
