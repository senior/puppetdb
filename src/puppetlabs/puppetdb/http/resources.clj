(ns puppetlabs.puppetdb.http.resources
  (:require [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.query.paging :as paging]
            [net.cgrand.moustache :refer [app]]))

(defn resources-app
  ([version] (resources-app version true))
  ([version restrict-to-active-nodes & optional-handlers]
   (let [handler (if restrict-to-active-nodes
                   http-q/restrict-query-to-active-nodes
                   identity)
         handlers (cons handler optional-handlers)
         param-spec {:optional paging/query-params}
         query-route (partial http-q/query-route-from' "resources" version param-spec)]
     [["" (query-route handlers)]

      [["/" :type "/" [#".*" :title]]
       (fn [{:keys [route-params] :as req}]
         (do
           (println "req is")
           (clojure.pprint/pprint req)
           ((query-route (concat handlers
                                 [(partial http-q/restrict-resource-query-to-type (:type route-params))
                                  (partial http-q/restrict-resource-query-to-title (:title route-params))]))
            req)))]
      
      [["/" :type]
       (fn [{:keys [route-params] :as req}]
         ((query-route (concat handlers
                               [(partial http-q/restrict-resource-query-to-type (:type route-params))]))
          req))]])))
