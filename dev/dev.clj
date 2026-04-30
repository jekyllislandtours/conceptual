(ns dev
  (:require
   [portal.api]))

(defn portal!
  []
  ;; make *portal* available in all namespaces for debugging
  (intern 'clojure.core '*portal* (portal.api/open {:window-title (gensym "Portal-")}))
  (add-tap #'portal.api/submit))
