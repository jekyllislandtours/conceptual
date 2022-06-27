(ns imdb-demo
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import [java.net URL]
           [java.io File BufferedInputStream]
           [java.util.zip GZIPInputStream]
           [java.nio.file Files StandardCopyOption]))


;; Let's populate conceptual with some data.
;; let's use imdb free movie datasets available here: https://datasets.imdbws.com/
;; data definitions are found here: https://www.imdb.com/interfaces/

;; name.basics (11632330) 30018.759614 msecs
;; title.akas (32038715) 86085.431627 msecs
;; title.basics (8925038) 48549.834477
;; title.crew (8928653) 17003.08025
;; title.episode (6701217) 13111.277693
;; title.principals (50355311) 147980.33101
;; title.ratings (1244712) 2323.48901

;; 15.3 GB heap storing as maps in a vector by table-key

;; a little bit of code

(def +imdb-base-url+ "https://datasets.imdbws.com/")
(def +imdb-file-suffix+ ".tsv.gz")
(def +imdb-demo-path+ "demo/")

(def +imdb-table-keys+ [:name.basics
                        :title.akas
                        :title.basics
                        :title.crew
                        :title.episode
                        :title.principals
                        :title.ratings])

(def ^:dynamic *initial-tables* {:name.basics []
                                 :title.akas []
                                 :title.basics []
                                 :title.crew []
                                 :title.episode []
                                 :title.principals []
                                 :title.ratings []})

(def ^:dynamic *row-limit* nil) ;; use this to limit records loaded

(def ^:dynamic *imdb-tables* (atom *initial-tables*))

;; TODO: remove
(def total-titles 8925038)
;; TODO: remove
(def total-time-minutes (/ (/ 178027.379424 1000.0) 60.0))

(def title-count (atom 0))
(def runner-thread (atom nil))
(def title-set (atom #{}))
(def ^:dynamic *const->id* (atom {}))
(def ^:dynamic *ratings* (atom []))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- snakify [s]
  (-> (->> s
           (map #(if (Character/isUpperCase %) (str "-" (str/lower-case %)) %))
           (apply str))
      (str/replace #"\s" "-")))

;;(snakify "primaryName")
;;(snakify "16mm release title")

(defn- split-on-ctrl-b [s]
  (if (and s (not= "\\N" s))
    (str/split s #"\cB")
    #{}))

(defn- split-on-comma [s]
  ;; dataset has "\\N" where there is no data
  (if (and s (not= "\\N" s))
    (some->> (str/split s #",") set)
    #{}))

(defn- split-primary-professions [s]
  (if (and s (not= "\\N" s))
    (->> (str/split s #",")
         (filter (comp not str/blank?))
         (mapv #(str/replace % #"_" "-")))
    []))

(defn- local-filename [key]
  (str +imdb-demo-path+ (name key) +imdb-file-suffix+))

;;(local-filename :title.basics)

(defn- read-table-columns [table-key]
  (with-open [is (io/input-stream (local-filename table-key))
              gzis (GZIPInputStream. is)
              rdr (io/reader gzis)]
    (->> (-> (line-seq rdr) first (str/split #"\t"))
         (map (comp keyword snakify)))))

;;(read-table-columns :name.basics)

(defn percent-complete []
  (* 100.0 (/ @title-count total-titles)))

(defn tally []
  [@title-count (count @title-set) (- (c/max-id) 904)])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Download files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn download-url [key] (str +imdb-base-url+ (name key) +imdb-file-suffix+))

;;(download-url :title.basics)

;; Do some crazy stuff so we don't add dependencies
(defn download-file [key]
  (let [url (download-url key)
        is (-> url URL. (.openConnection) (.getInputStream))
        local-filename (local-filename key)
        target-file (File. local-filename)]
    (io/make-parents local-filename)
    (Files/copy is (.toPath target-file)
                (into-array [StandardCopyOption/REPLACE_EXISTING]))
    (.close is)))

;;(download-file "name.basics")
;;(download-file "title.crew")
;;(download-file "title.principals")

(defn download-all-files []
  (println "downloading files:")
  (doseq [key +imdb-table-keys+]
    (println (str "  " +imdb-demo-path+ (name key) +imdb-file-suffix+))
    (download-file (name key))))

;;(download-all-files)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Work
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def genres
  ["Action"
   "Adult"
   "Adventure"
   "Animation"
   "Biography"
   "Comedy"
   "Crime"
   "Documentary"
   "Drama"
   "Family"
   "Fantasy"
   "Film-Noir"
   "Game-Show"
   "History"
   "Horror"
   "Music"
   "Musical"
   "Mystery"
   "News"
   "Reality-TV"
   "Romance"
   "Sci-Fi"
   "Short"
   "Sport"
   "Talk-Show"
   "Thriller"
   "War"
   "Western"])

(defn genre-tag [label]
  (keyword "imdb" (str "genre." (str/lower-case label) "?")))

(defn declare-genres! []
  (s/declare-tags!
   (->> genres
        (mapv #(vector (genre-tag %) {:imdb/genre? true
                                      :imdb/name %})))))

(def regions
  ["AD" "AE" "AF" "AG" "AI" "AL" "AM" "AN" "AO" "AQ" "AR" "AS" "AT" "AU" "AW" "AZ" "BA" "BB" "BD" "BE" "BF" "BG" "BH" "BI" "BJ" "BM" "BN" "BO" "BR" "BS" "BT" "BUMM" "BW" "BY" "BZ" "CA" "CD" "CF" "CG" "CH" "CI" "CK" "CL" "CM" "CN" "CO" "CR" "CSHH" "CSXX" "CU" "CV" "CW" "CY" "CZ" "DDDE" "DE" "DJ" "DK" "DM" "DO" "DZ" "EC" "EE" "EG" "EH" "ER" "ES" "ET" "FI" "FJ" "FM" "FO" "FR" "GA" "GB" "GD" "GE" "GF" "GH" "GI" "GL" "GM" "GN" "GP" "GQ" "GR" "GT" "GU" "GW" "GY" "HK" "HN" "HR" "HT" "HU" "ID" "IE" "IL" "IM" "IN" "IQ" "IR" "IS" "IT" "JE" "JM" "JO" "JP" "KE" "KG" "KH" "KI" "KM" "KN" "KP" "KR" "KW" "KY" "KZ" "LA" "LB" "LC" "LI" "LK" "LR" "LS" "LT" "LU" "LV" "LY" "MA" "MC" "MD" "ME" "MG" "MH" "MK" "ML" "MM" "MN" "MO" "MP" "MQ" "MR" "MS" "MT" "MU" "MV" "MW" "MX" "MY" "MZ" "NA" "NC" "NE" "NG" "NI" "NL" "NO" "NP" "NR" "NU" "NZ" "OM" "PA" "PE" "PF" "PG" "PH" "PK" "PL" "PR" "PS" "PT" "PW" "PY" "QA" "RE" "RO" "RS" "RU" "RW" "SA" "SB" "SC" "SD" "SE" "SG" "SH" "SI" "SK" "SL" "SM" "SN" "SO" "SR" "ST" "SUHH" "SV" "SY" "SZ" "TC" "TD" "TG" "TH" "TJ" "TL" "TM" "TN" "TO" "TR" "TT" "TV" "TW" "TZ" "UA" "UG" "US" "UY" "UZ" "VA" "VC" "VDVN" "VE" "VG" "VI" "VN" "VU" "WS" "XAS" "XAU" "XEU" "XKO" "XKV" "XNA" "XPI" "XSA" "XSI" "XWG" "XWW" "XYU" "YE" "YUCS" "ZA" "ZM" "ZRCD" "ZW"])

(defn region-tag [label]
  (keyword "imdb" (str "region." (str/lower-case label) "?")))

(defn declare-regions! []
  (s/declare-tags!
   (->> regions
        (mapv #(vector (region-tag %) {:imdb/region? true
                                       :imdb/name %})))))

(def languages
  ["af" "am" "ar" "az" "be" "bg" "bn" "br" "bs" "ca" "cmn" "cr" "cs" "cy" "da" "de" "eka" "el" "en" "es" "et" "eu" "fa" "fi" "fr" "fro" "ga" "gd" "gl" "gsw" "gu" "haw" "he" "hi" "hr" "hu" "hy" "id" "is" "it" "iu" "ja" "jv" "ka" "kk" "kn" "ko" "ku" "ky" "la" "lb" "lo" "lt" "lv" "mi" "mk" "ml" "mn" "mr" "ms" "my" "myv" "ne" "nl" "no" "pa" "pl" "prs" "ps" "pt" "qac" "qal" "qbn" "qbo" "qbp" "rm" "rn" "ro" "roa" "ru" "sd" "sk" "sl" "sq" "sr" "st" "su" "sv" "ta" "te" "tg" "th" "tk" "tl" "tn" "tr" "uk" "ur" "uz" "vi" "wo" "xh" "yi" "yue" "zh" "zu"])

(defn language-tag [label]
  (keyword "imdb" (str "language." (str/lower-case label) "?")))

(defn declare-languages! []
  (s/declare-tags!
   (->> languages
        (mapv #(vector (language-tag %) {:imdb/language? true
                                         :imdb/name %})))))

(def ^:dynamic *start-year* 1800)
(def ^:dynamic *end-year* 2050)

(defn year-tag [year]
  (keyword "imdb" (str "year." year "?")))

;;(year-tag 1984)

(defn coerce-int [v]
  (let [v (str v)]
    (when (and v (not= "\\N" v))
      (try
        (Integer/parseInt v)
        (catch Exception e)))))

;;(coerce-int "1")
;;(coerce-int "\\N")
;;(coerce-int nil)

(defn declare-years! []
  ;; import year-tags
  (->> (range *start-year* (inc *end-year*))
       (map #(vector (year-tag %) {:imdb/year? true
                                   :imdb/name (str %)
                                   :imdb/year %}))
       s/declare-tags!))

(def version-types
  ["alternative"
   "dvd"
   "festival"
   "imdbDisplay"
   "original"
   "tv"
   "video"
   "working"])

(defn version-type-tag [label]
  (keyword "imdb" (str "version-type." (snakify label) "?")))

(defn declare-version-types! []
  (s/declare-tags!
   (->> version-types
        (mapv #(vector (version-type-tag %)
                       {:imdb/version-type? true
                        :imdb/name %})))))

(def version-attributes ["16mm release title" "16mm rental title" "3-D version" "3-D video title" "8mm release title" "American Mutoscope & Biograph catalog title" "Bable dialect title" "Berlin film festival title" "Bilbao festival title" "Cannes festival title" "DVD box title" "DVD menu title" "English translation of working title" "GameCube version" "Hakka dialect title" "IMAX version" "LD title" "Locarno film festival title" "Los Angeles premiere title" "Los Angeles première title" "MIFED title" "PC version" "POLart" "Pay-TV title" "R-rated version" "TV listings title" "Venice film festival title" "X-rated version" "YIVO translation" "Yiddish dubbed" "added framing sequences and narration in Yiddish" "alternative spelling" "alternative transliteration" "anthology series" "approximation of original mirrored title" "armed forces circuit title" "bootleg title" "bowdlerized title" "cable TV title" "censored version" "closing credits title" "complete title" "copyright title" "correct transliteration" "cut version" "daytime version title" "director's cut" "dubbed version" "eighteenth season title" "eighth season title" "eleventh season title" "english transliteration" "expansion title" "fake working title" "fifteenth season title" "fifth part title" "fifth season title" "first episode title" "first episodes title" "first four episodes title" "first part title" "first season title" "first segment title" "first three episodes title" "first two episodes title" "fortieth season title" "fourteenth season title" "fourth part title" "fourth season title" "game box title" "incorrect title" "informal English title" "informal alternative title" "informal literal English title" "informal literal title" "informal short title" "informal title" "last season title" "late Sunday edition" "literal English title" "literal French title" "literal title" "literal translation of working title" "long new title" "long title" "longer version" "modern translation" "new syndication title" "new title" "nineteenth season title" "ninth season title" "non-modified Hepburn romanization" "original pilot title" "original script title" "original subtitled version" "orthographically correct title" "poster title" "pre-release title" "premiere title" "première title" "promotional abbreviation" "promotional title" "racier version" "recut version" "redubbed comic version" "reissue title" "rerun title" "restored version" "review title" "rumored" "second copyright title" "second part title" "second season title" "second segment title" "segment title" "series title" "seventeenth season title" "seventh season title" "short title" "short version" "silent version" "sixteenth season title" "sixth season title" "soft porn version" "subtitle" "summer title" "syndication title" "teaser title" "tenth season title" "theatrical title" "third and fourth season title" "third part title" "third season title" "third segment title" "thirteenth season title" "thirtieth season title" "thirtyeighth season title" "thirtyfifth season title" "thirtyfirst season title" "thirtyfourth season title" "thirtyninth season title" "thirtysecond season title" "thirtyseventh season title" "thirtysixth season title" "thirtythird season title" "title for episodes with guest hosts" "trailer title" "transliterated title" "twelfth season title" "twentieth season title" "twentyeighth season title" "twentyfifth season title" "twentyfirst season title" "twentyfourth season title" "twentyninth season title" "twentysecond season title" "twentyseventh season title" "twentysixth season title" "twentythird season title" "unauthorized video title" "uncensored intended title" "video CD title" "video box title" "video catalogue title" "videogame episode" "weekend title"])

(defn version-attribute-tag [label]
  (keyword "imdb" (str "version-attribute." (snakify label) "?")))

(defn declare-version-attributes! []
  (s/declare-tags!
   (->> version-attributes
        (mapv #(vector (version-attribute-tag %)
                       {:imdb/version-attribute? true
                        :imdb/name %})))))

(def primary-professions
  ["actor" "actress" "animation-department" "art-department" "art-director" "assistant" "assistant-director" "camera-department" "casting-department" "casting-director" "choreographer" "cinematographer" "composer" "costume-department" "costume-designer" "director" "editor" "editorial-department" "electrical-department" "executive" "legal" "location-management" "make-up-department" "manager" "miscellaneous" "music-department" "producer" "production-department" "production-designer" "production-manager" "publicist" "script-department" "set-decorator" "sound-department" "soundtrack" "special-effects" "stunts" "talent-agent" "transportation-department" "visual-effects" "writer"])

(defn primary-profession-tag [label]
  (keyword "imdb" (str "primary-profession." (snakify label) "?")))

(defn declare-primary-professions! []
  (->> primary-professions
       (mapv #(vector (primary-profession-tag %)
                      {:imdb/primary-profession? true
                       :imdb/name %}))
       s/declare-tags!))

(defn declare-schema! []
  (s/declare-properties!
   [[:imdb/nconst String]
    [:imdb/tconsts clojure.lang.PersistentHashSet] ;; there can be multiple records for one title...
    [:imdb/parent-tconst String]
    [:imdb/name String]
    [:imdb/primary-name String]
    [:imdb/primary-title String]
    [:imdb/original-title String]
    [:imdb/average-rating Double]
    [:imdb/num-votes Integer]
    [:imdb/runtime-minutes Integer]
    [:imdb/season-number Integer]
    [:imdb/episode-number Integer]
    [:imdb/ordering Integer]
    [:imdb/year Integer]
    [:imdb/start-year Integer]
    [:imdb/end-year Integer]
    [:imdb/birth-year Integer]
    [:imdb/death-year Integer]
    [:imdb/genres clojure.lang.PersistentHashSet]])

  (s/declare-relations!
   [[:imdb/start-year]
    [:imdb/end-year]]
   :type :to-one)

  (s/declare-relations!
   [[:imdb/titles]
    [:imdb/versions]
    [:imdb/directors]
    [:imdb/writers]
    [:imdb/principles]
    [:imdb/professions]
    [:imdb/known-for-titles]]
   :type :to-many)

  (s/declare-tags!
   (concat
    [[:imdb/year?]
     [:imdb/title?]
     [:imdb/title-type?]
     [:imdb/version?]
     [:imdb/version-type?]
     [:imdb/version-attribute?]
     [:imdb/original-title?]
     [:imdb/region?]
     [:imdb/language?]
     [:imdb/person?]
     [:imdb/crew?]
     [:imdb/writer?]
     [:imdb/director?]
     [:imdb/actor?]
     [:imdb/season?]
     [:imdb/episode?]
     [:imdb/adult?]
     [:imdb/genre?]
     [:imdb/job?]
     [:imdb/job-category?]
     [:imdb/primary-profession?]]))

  (declare-genres!)
  (declare-regions!)
  (declare-languages!)
  (declare-years!)
  (declare-version-types!)
  (declare-version-attributes!)
  (declare-primary-professions!))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; title ratings - :title.ratings
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#:imdb{:tconst "tt0000001", :average-rating "5.7", :num-votes "1879"}

(defn make-rating [{:keys [tconst average-rating num-votes]}]
  {:imdb/tconsts #{tconst}
   :imdb/title? true
   :imdb/average-rating (try (Double/parseDouble average-rating)
                             (catch Exception e 0.0))
   :imdb/num-votes (try (Integer/parseInt num-votes)
                        (catch Exception e 0.0))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; title basics - :title.basics
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; edn'ified record
#:imdb{:primary-title "Carmencita",
       :tconst "tt0000001",
       :end-year "\\N",
       :title-type "short",
       :original-title "Carmencita",
       :runtime-minutes "1",
       :genres "Documentary,Short",
       :start-year "1894",
       :is-adult "0"}

(defn title-key [primary-title start-year end-year]
  (keyword "imdb" (str/replace primary-title #"\s" "_")))

(defn qualified-title-key [primary-title start-year end-year]
  (keyword "imdb" (str (str/replace primary-title #"\s" "_")
                       (when start-year (str "." start-year))
                       (when end-year (str "." end-year)))))

(defn title-name [primary-title start-year end-year]
  (str primary-title
       (if start-year
         (if end-year
           (str " (" start-year "-" end-year")")
           (str " (" start-year ")"))
         (when end-year
           (str " (" end-year ")")))))

(defn assoc-year-tags [c start-year end-year]
  (->> (range (or start-year end-year)
              (inc (or end-year start-year)))
       (map year-tag)
       (reduce (fn [c' k] (assoc c' k true)) c)))

;;(assoc-year-tags {} 2015 2022)

(defn create-title-key [primary-title start-year-int end-year-int]
  (let [short-key (title-key primary-title start-year-int end-year-int)
        qualified-key (qualified-title-key primary-title start-year-int end-year-int)
        existing-title (c/seek short-key)]
    ;; check - start / end year
    (if (and existing-title
             (= (:imdb/start-year existing-title) start-year-int)
             (= (:imdb/end-year existing-title) end-year-int))
      short-key
      (if existing-title
        qualified-key
        short-key))))

;; prepare title edn for inserting
(defn make-title
  [{:keys [tconst primary-title original-title
           start-year end-year runtime-minutes genres is-adult]}]
  (let [start-year-int (coerce-int start-year)
        end-year-int (coerce-int end-year)
        runtime-minutes (coerce-int runtime-minutes)
        genres (split-on-comma genres)
        genre-tag-map (some->> genres
                               (mapv #(vector (genre-tag %) true))
                               (into {}))]
    (cond->
        (merge {:db/key (create-title-key primary-title start-year-int end-year-int)
                :imdb/title? true
                ;;:imdb/name (title-name primary-title start-year-int end-year-int)
                :imdb/tconsts #{tconst}
                :imdb/primary-title primary-title
                :imdb/original-title original-title}
               genre-tag-map)
      start-year-int (assoc :imdb/start-year start-year-int)
      end-year-int (assoc :imdb/end-year end-year-int)
      (or start-year-int end-year-int) (assoc-year-tags start-year-int end-year-int)
      runtime-minutes (assoc :imdb/runtime-minutes runtime-minutes)
      (= is-adult "1") (assoc :imdb/adult? true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; names - :name.basics
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#:imdb{:nconst "nm0000001",
       :primary-name "Fred Astaire",
       :birth-year "1899",
       :death-year "1987",
       :primary-profession "soundtrack,actor,miscellaneous",
       :known-for-titles "tt0031983,tt0072308,tt0053137,tt0050419"}

(defn name-key [primary-name]
  ;;(println "primary-name:" primary-name)
  (keyword "imdb" (str "person." (str/replace primary-name #"\s" "_"))))

;;(name-key "Fred Astaire")

(defn qualified-name-key [primary-name birth-year death-year]
  (keyword "imdb" (str "person." (str/replace primary-name #"\s" "_")
                       (when birth-year (str "." birth-year))
                       (when death-year (str "." death-year)))))


(defn create-person-key [primary-name birth-year-int death-year-int]
  (let [short-key (name-key primary-name)
        qualified-key (qualified-name-key primary-name birth-year-int death-year-int)
        existing-name (c/seek short-key)]
    ;; check - birth / death year
    (if (and existing-name
             (= (:imdb/birth-year existing-name) birth-year-int)
             (= (:imdb/death-year existing-name) death-year-int))
      short-key
      (if existing-name
        qualified-key
        short-key))))

;;(c/seek :imdb/Fred_Astaire)

(defn make-person [{:keys [nconst primary-name
                           birth-year death-year primary-profession
                           known-for-titles] :as person}]
  ;;(clojure.pprint/pprint person)
  ;;(println primary-name)
  (let [birth-year-int (coerce-int birth-year)
        death-year-int (coerce-int death-year)
        known-for-titles (some->> known-for-titles
                                  split-on-comma
                                  (map @*const->id*)
                                  (int-array))
        primary-professions (split-primary-professions primary-profession)
        primary-profession-tag-map (some->> primary-professions
                                            (mapv #(vector (primary-profession-tag %) true))
                                            (into {}))]
    (cond->
        (merge {;;:db/key (create-person-key primary-name birth-year-int death-year-int)
                :imdb/person? true
                :imdb/nconst nconst
                :imdb/primary-name primary-name}
               primary-profession-tag-map)
      birth-year-int (assoc :imdb/birth-year death-year-int)
      death-year-int (assoc :imdb/death-year death-year-int)
      known-for-titles (assoc :imdb/known-for-titles known-for-titles))))

(comment
  (c/with-aggr [aggr]
    (upsert-row!
     :name.basics
     aggr
     (make-person
      {:nconst "nm0000001",
       :primary-name "Fred Astaire",
       :birth-year "1899",
       :death-year "1987",
       :primary-profession "soundtrack,actor,miscellaneous",
       :known-for-titles "tt0031983,tt0072308,tt0053137,tt0050419"})))
  (->> (make-person
        {:nconst "nm0000001",
         :primary-name "Fred Astaire",
         :birth-year "1899",
         :death-year "1987",
         :primary-profession "soundtrack,actor,miscellaneous",
         :known-for-titles "tt0031983,tt0072308,tt0053137,tt0050419"})
       keys
       ;;(map c/key->id)
       )
  )

;;(c/seek :imdb/title?)
;;(c/seek :imdb/person?)

;;   * add known-for-titles relation

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; akas - :title.akas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-aka [])
;; * load akas - :title.akas
;;   add region tag
;;   add language tag
;;   add types tags
;;   add attribute tags
#:imdb{:title-id "tt0000001",
       :ordering "1",
       :title "Карменсіта",
       :region "UA",
       :language "\\N",
       :types "imdbDisplay",
       :attributes "\\N",
       :is-original-title "0"}

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; title - :title.crew
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#:imdb{:tconst "tt0000001", :directors "nm0005690", :writers "\\N"}

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; episode - :title.episode
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#:imdb{:tconst "tt0020666",
       :parent-tconst "tt15180956",
       :season-number "1",
       :episode-number "2"}

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; principals - :title.principals
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;   * create tags for category
;;   * create tags for jobs
#:imdb{:tconst "tt0000001",
       :ordering "1",
       :nconst "nm1588970",
       :category "self",
       :job "\\N",
       :characters "[\"Self\"]"}

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Initialize DB Stuff
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn row->concept [table-key columns row]
  (cond->> (zipmap columns row)
    (= :title.basics table-key) make-title
    (= :name.basics table-key) make-person
    (= :title.ratings table-key) make-rating))

(defn merge-concepts [table-key prev new]
  (cond-> (merge prev new)
    (= :title.basics table-key) (assoc :imdb/tconsts
                                       (set/union (:imdb/tconsts prev)
                                                  (:imdb/tconsts new)))))

(defn upsert-row! [table-key aggr row]
  (if-let [prev (or (some-> row :imdb/tconsts first (@*const->id*) c/seek c/->persistent-map)
                    (some-> row :db/id c/seek c/->persistent-map)
                    ;;(some-> row :db/key c/seek c/->persistent-map)
                    )]
    ;; yeah there are multiple records for the same thing
    (do ;;(println "\n" (merge-concepts table-key prev row))
        (c/update! aggr (merge-concepts table-key prev row)))
    (do (c/insert! aggr row)
        (when-let [const (or (some-> row :imdb/tconsts first)
                             (some-> row :imdb/nconst))]
          (swap! *const->id* assoc const (c/max-id))))))

(defn load-table!
  ([table-key] (load-table! table-key upsert-row!))
  ([table-key row-handler-fn]
   (with-open [is (io/input-stream (local-filename table-key))
               gzis (GZIPInputStream. is)
               rdr (io/reader gzis)]
     (let [columns (read-table-columns table-key)]
       (c/with-aggr [aggr]
         (doseq [row (cond->> (rest (line-seq rdr))
                       (not (nil? *row-limit*)) (take *row-limit*)
                       true (map #(str/split % #"\t")))]
           (try
             (let [row (row->concept table-key columns row)]
               (row-handler-fn table-key aggr row))
             (catch Throwable t
               (println "problem with row:" row)))))))))

(defn load-title-basics! []
  (print "loading title basics... ")
  (load-table! :title.basics upsert-row!)
  (println "done."))

(defn load-name-basics! []
  (print "loading name basics... ")
  (load-table! :name.basics upsert-row!)
  (println "done."))

(defn load-ratings! []
  (load-table! :title.ratings
               (fn [table-key aggr row]
                 (swap! *ratings* conj row)))
  (print "inserting title stubs... ")
  (c/with-aggr [aggr]
    (doseq [title (->> @*ratings*
                       (sort-by (comp (partial apply *)
                                      (juxt :imdb/average-rating :imdb/num-votes)) >))]
      (c/insert! aggr title)
      (swap! *const->id* assoc (-> title :imdb/tconsts first) (c/max-id))))
  (reset! *ratings* [])
  (println "done."))

(defn run-it! [table-key]
  (let [t (Thread. (fn [] (time
                           ;;binding [*row-limit* 10]
                           (load-table! table-key))))]
    (.start t)
    (reset! runner-thread t)))

(defn reset-db! []
  (reset! *ratings* [])
  (reset! *const->id* {})
  (c/reset-db!)
  (reset! title-count 0)
  (reset! title-set #{})
  (declare-schema!))

(comment
  ;; download all files
  (download-all-files)

  ;; reset the db so you can start over
  (reset-db!)

  ;; dump entire db to stdout - don't use if db is too big :D
  (c/dump)

  ;; load the ratings
  (time (load-ratings!))

  (load-table! :title.basics)
  ;; load the basic titles in a thread
  ;; do this instead if you don't want to lockup the repl
  ;;(run-it! :title.basics)

  ;; NOTE: will print out some duplicate error message
  (load-table! :name.basics)
  ;; load the basic names in a thread, wait for previous step to finish
  ;; to use this
  ;; (run-it! :name.basics)

  ;; check the max id
  (println (c/max-id))

  ;; check out last entry
  (clojure.pprint/pprint (c/seek (c/max-id)))

  ;; check out a title
  (c/seek :imdb/The_Shawshank_Redemption)

  ;; some titles have the year in the key to disabiguate
  (c/seek :imdb/The_Dark_Knight.2008)

  ;; check title count
  (count (c/ids :imdb/title?))

  ;; print top 50 titles
  (->> (c/idents :imdb/title?)
       (take 50)
       clojure.pprint/pprint)

  ;; print out the available genres
  (->> (c/idents :imdb/genre?)
       (clojure.pprint/pprint))

  ;; print out the top 50 action filmns
  (->> (c/idents :imdb/genre.action?)
       (take 50)
       clojure.pprint/pprint)

  ;; how about the action thrillers
  (->> (i/intersection
        (c/ids :imdb/genre.action?)
        (c/ids :imdb/genre.thriller?))
       c/idents
       (take 50)
       clojure.pprint/pprint)

  ;; add the year and the ratings
  (->> [:imdb/genre.action? :imdb/genre.thriller?]
       (map c/ids)
       (apply i/intersection)
       (map c/seek)
       (filter #(>= 1975 (:imdb/start-year %)))
       (map (juxt :imdb/primary-title :imdb/start-year :imdb/average-rating))
       (take 50)
       time
       clojure.pprint/pprint)

  (->> (c/ids :imdb/person?)
       count)

  (->> (c/ids :imdb/primary-profession.actor?)
       count)

  (->> [:imdb/primary-profession.actor?
        :imdb/primary-profession.director?
        :imdb/primary-profession.writer?]
       (map c/ids)
       (apply i/intersection)
       (map c/seek)
       (map (juxt :imdb/primary-name :imdb/birth-year))
       (take 50)
       time
       clojure.pprint/pprint))
