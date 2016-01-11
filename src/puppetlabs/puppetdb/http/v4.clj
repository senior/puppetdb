(ns puppetlabs.puppetdb.http.v4
  (:require [puppetlabs.puppetdb.http :as http]
            [bidi.bidi :as bidi]
            [bidi.ring :as bring]
            [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.query-eng :refer [produce-streaming-body
                                                   stream-query-result]]
            [puppetlabs.puppetdb.middleware :refer [validate-no-query-params
                                                    validate-query-params
                                                    wrap-with-parent-check'']]
            [puppetlabs.comidi :as cmdi]
            [schema.core :as s]
            [puppetlabs.puppetdb.catalogs :refer [catalog-query-schema]]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.utils :refer [assoc-when]]
            [clojure.walk :refer [keywordize-keys]]))

(def version :v4)

(defn extract-query
  ([routes]
   (extract-query {:optional paging/query-params} routes))
  ([param-spec routes]
   (cmdi/wrap-routes routes #(http-q/extract-query % param-spec))))

(defn append-handler [route handler-to-append]
  (cmdi/wrap-routes route #(comp % handler-to-append)))

(defn create-handler
  [version entity & handler-fns]
  (apply comp
         (http-q/query-handler version)
         (http-q/restrict-query-to-entity entity)
         handler-fns))

(defn add-parent-check [route version entity]
  (cmdi/wrap-routes route
                    #(wrap-with-parent-check'' % version entity entity)))

(defn experimental-index-app
  [version]
  (extract-query
   {:optional paging/query-params
    :required ["query"]}
   (cmdi/ANY "" []
             (-> (http-q/query-handler version)
                 (http/experimental-warning "The root endpoint is experimental")))))

(defn report-data-responder
  "Respond with either metrics or logs for a given report hash.
   `entity` should be either :metrics or :logs."
  [version entity]
  (fn [{:keys [globals route-params] :as foo}]
    (let [query ["from" entity ["=" "hash" (:hash route-params)]]]
      (produce-streaming-body version {:query query}
                              (select-keys globals [:scf-read-db
                                                    :url-prefix
                                                    :pretty-print
                                                    :warn-experimental])))))

(defn events-app
  "Ring app for querying events"
  [version]
  (extract-query
   {:optional (concat
               ["query"
                "distinct_resources"
                "distinct_start_time"
                "distinct_end_time"]
               paging/query-params)}
   (cmdi/ANY "" []
             (create-handler version "events"))))

(defn reports-app
  [version]
  (cmdi/routes
   (extract-query
    (cmdi/ANY "" []
              (create-handler version "reports")))
   (cmdi/ANY ["/" :hash "/events"] []
             (wrap-with-parent-check'' (comp (events-app version) http-q/restrict-query-to-report') version :report :hash))
       
   (cmdi/ANY ["/" :hash "/metrics"] []
             (-> (report-data-responder version "report_metrics")
                 validate-no-query-params
                 (wrap-with-parent-check'' version :report :hash)))
       
   (cmdi/ANY ["/" :hash "/logs"] []
             (-> (report-data-responder version "report_logs")
                 validate-no-query-params
                 (wrap-with-parent-check'' version :report :hash)))))

(defn url-decode [x]
  (java.net.URLDecoder/decode x))

(defn resources-app
  [version]
  (extract-query
   {:optional paging/query-params}
   (cmdi/routes
    (cmdi/ANY "" []
              (create-handler version "resources"  http-q/restrict-query-to-active-nodes))
    
    (cmdi/context ["/" :type]
                  (cmdi/ANY "" []
                            (create-handler version "resources"
                                            (fn [{:keys [route-params] :as req}]
                                              (http-q/restrict-resource-query-to-type (:type route-params) req))
                                            http-q/restrict-query-to-active-nodes))
                  (cmdi/ANY ["/" [#".*" :title]] []
                            (create-handler version "resources"
                                            (fn [{:keys [route-params] :as req}]
                                              (http-q/restrict-resource-query-to-title (url-decode (:title route-params)) req))
                                            (fn [{:keys [route-params] :as req}]
                                              (http-q/restrict-resource-query-to-type (:type route-params) req))
                                            http-q/restrict-query-to-active-nodes))))))

(defn narrow-globals [globals]
  (select-keys globals [:scf-read-db :warn-experimental :url-prefix]))

(defn status-response [version query options found-fn not-found-response]
  (if-let [query-result (first (stream-query-result version query {} options))]
    (http/json-response (found-fn query-result))
    not-found-response))

(defn catalog-status
  [version]
  (fn [{:keys [globals route-params]}]
    (let [node (:node route-params)]
      (status-response version
                       ["from" "catalogs" ["=" "certname" node]]
                       (narrow-globals globals)
                       #(s/validate catalog-query-schema
                                    (kitchensink/mapvals sutils/parse-db-json [:edges :resources] %))
                       (http/status-not-found-response "catalog" node)))))

(defn factset-status
  "Produces a response body for a request to retrieve the factset for `node`."
  [api-version]
  (fn [{:keys [globals route-params]}]
    (let [node (:node route-params)]
      (status-response version
                       ["from" "factsets" ["=" "certname" node]]
                       (narrow-globals globals)
                       identity
                       (http/status-not-found-response "factset" node)))))

(defn node-status
  "Produce a response body for a single environment."
  [api-version]
  (fn [{:keys [globals route-params]}]
    (let [node (:node route-params)]
      (status-response version
                       ["from" "nodes" ["=" "certname" node]]
                       (narrow-globals globals)
                       identity
                       (http/status-not-found-response "node" node)))))

(defn environment-status
  "Produce a response body for a single environment."
  [api-version]
  (fn [{:keys [globals route-params]}]
    (let [environment (:environment route-params)]
      (status-response version
                       ["from" "environments" ["=" "name" environment]]
                       (narrow-globals globals)
                       identity
                       (http/status-not-found-response "environment" environment)))))

(defn catalog-app
  [version]
  (extract-query
   {:optional paging/query-params}
   (cmdi/routes

    (cmdi/ANY "" []
              (create-handler version "catalogs"))

    (cmdi/context ["/" :node]
                  (cmdi/ANY "" []
                            (catalog-status version))

                  (cmdi/ANY "/edges" []
                            (-> (create-handler version "edges" http-q/restrict-query-to-node')
                                (wrap-with-parent-check'' version :catalog :node)))
                                
                  (cmdi/ANY "/resources" []
                            (-> (comp (resources-app version)
                                      http-q/restrict-query-to-node')
                                (wrap-with-parent-check'' version :catalog :node)))))))

(defn facts-app
  [version]
  (let [param-spec {:optional paging/query-params}]
    (extract-query
     {:optional paging/query-params}
     (cmdi/routes
      (cmdi/ANY "" []
                (create-handler version "facts" http-q/restrict-query-to-active-nodes))

      (cmdi/context ["/" :fact]
                                  
                    (cmdi/ANY "" []
                              (create-handler version "facts"
                                              http-q/restrict-fact-query-to-name 
                                              http-q/restrict-query-to-active-nodes))

                    (cmdi/ANY ["/" :value] []
                              (create-handler version "facts"
                                              http-q/restrict-fact-query-to-name
                                              http-q/restrict-fact-query-to-value
                                              http-q/restrict-query-to-active-nodes)))))))

(defn factset-app
  [version]
  (let [param-spec {:optional paging/query-params}]
    (extract-query
     {:optional paging/query-params}
     (cmdi/routes
      (cmdi/ANY "" []
                (create-handler version "factsets" http-q/restrict-query-to-active-nodes))

      (cmdi/context ["/" :node]
                    (cmdi/ANY "" []
                              (factset-status version))

                    (cmdi/ANY "/facts" []
                              (-> (create-handler version "factsets" http-q/restrict-query-to-node')
                                  (wrap-with-parent-check'' version :factset :node))))))))

(defn fact-names-app
  [version]
  (extract-query {:optional paging/query-params}
                 (cmdi/ANY "" []
                           (comp
                            (fn [{:keys [params globals puppetdb-query]}]
                              (let [puppetdb-query (assoc-when puppetdb-query :order_by [[:name :ascending]])]
                                (produce-streaming-body
                                 version
                                 (http-q/validate-distinct-options! (merge (keywordize-keys params) puppetdb-query))
                                 (narrow-globals globals))))
                            (http-q/restrict-query-to-entity "fact_names")))))

(defn node-app
  [version]
  (let [param-spec {:optional paging/query-params}]
    (extract-query
     {:optional paging/query-params}
     (cmdi/routes
      (cmdi/ANY "" []
                (create-handler version "nodes" http-q/restrict-query-to-active-nodes))
      (cmdi/context ["/" :node]
                    (cmdi/ANY "" []
                              (validate-no-query-params (node-status version)))
                    
                    (cmdi/context "/facts"
                                  (-> (facts-app version)
                                      (append-handler http-q/restrict-query-to-node')
                                      (cmdi/wrap-routes #(wrap-with-parent-check'' % version :node :node))))
                    (cmdi/context "/resources"
                                  (-> (resources-app version)
                                      (append-handler http-q/restrict-query-to-node')
                                      (cmdi/wrap-routes #(wrap-with-parent-check'' % version :node :node)))))))))

(defn environments-app
  [version & optional-handlers]
  (let [param-spec {:optional paging/query-params}]
    (cmdi/routes
     (extract-query
      (cmdi/ANY "" []
                (create-handler version "environments")))
     (cmdi/context ["/" :environment]
                   (cmdi/ANY "" []
                             (validate-no-query-params (environment-status version)))
                   
                   (add-parent-check
                    (cmdi/routes
                     (extract-query
                      (cmdi/context "/facts"
                                    (-> (facts-app version)
                                        (append-handler http-q/restrict-query-to-environment'))))
                     (extract-query
                      (cmdi/context "/resources"
                                    (-> (resources-app version)
                                        (append-handler http-q/restrict-query-to-environment'))))

                     (extract-query
                      (cmdi/context "/reports"
                                    (-> (reports-app version)
                                        (append-handler http-q/restrict-query-to-environment'))))

                     (extract-query
                      {:optional (concat
                                  ["query"
                                   "distinct_resources"
                                   "distinct_start_time"
                                   "distinct_end_time"]
                                  paging/query-params)}

                      (cmdi/context "/events"
                                    (-> (events-app version)
                                        (append-handler http-q/restrict-query-to-environment')))))
                    
                    version :environment)))))

(def v4-app
  (cmdi/routes
   (cmdi/context ""
                 (experimental-index-app version))
   (cmdi/context "/facts" (facts-app version))
   (cmdi/context "/edges"
                 (extract-query
                  (cmdi/ANY "" []
                            (create-handler version "edges" http-q/restrict-query-to-active-nodes))))
   (cmdi/context "/factsets"
                 (factset-app version))
   (cmdi/context "/fact-names" (fact-names-app version))
   (cmdi/context "/fact-contents"
                 (extract-query
                  (cmdi/ANY "" []
                            (create-handler version "fact_contents" http-q/restrict-query-to-active-nodes))))
   (cmdi/context "/fact-paths"
                 (extract-query
                  (cmdi/ANY "" []
                            (create-handler version
                                            "fact_paths"))))
   
   (cmdi/context "/nodes" (node-app version))
   (cmdi/context "/environments" (environments-app version))
   (cmdi/context "/resources" (resources-app version))
   (cmdi/context "/catalogs" (catalog-app version))
   (cmdi/context "/events" (events-app version))
   (cmdi/context "/event-counts"
                 (extract-query
                  {:required ["summarize_by"]
                   :optional (concat ["counts_filter" "count_by"
                                      "distinct_resources" "distinct_start_time"
                                      "distinct_end_time"]
                                     paging/query-params)}
                  (cmdi/ANY "" []
                            (create-handler version "event_counts"))))
   (cmdi/context "/aggregate-event-counts"
                 (extract-query
                  {:required ["summarize_by"]
                   :optional ["query" "counts_filter" "count_by"
                              "distinct_resources" "distinct_start_time"
                              "distinct_end_time"]}
                  (cmdi/ANY "" []
                            (create-handler version
                                            "aggregate_event_counts")))) 
   (cmdi/context "/reports" (reports-app version))))
