(ns puppetlabs.puppetdb.http.v4
  (:require [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.http.catalogs :as catalogs]
            [puppetlabs.puppetdb.http.reports :as reports]
            [puppetlabs.puppetdb.http.fact-names :as fact-names]
            [puppetlabs.puppetdb.http.fact-paths :as fact-paths]
            [puppetlabs.puppetdb.http.facts :as facts]
            [puppetlabs.puppetdb.http.edges :as edges]
            [puppetlabs.puppetdb.http.factsets :as factsets]
            [puppetlabs.puppetdb.http.fact-contents :as fact-contents]
            [puppetlabs.puppetdb.http.resources :as resources]
            [puppetlabs.puppetdb.http.nodes :as nodes]
            [puppetlabs.puppetdb.http.environments :as envs]
            [puppetlabs.puppetdb.http.index :as index]
            [bidi.bidi :as bidi]
            [bidi.ring :as bring]
            [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.query-eng :refer [produce-streaming-body]]
            [puppetlabs.puppetdb.middleware :refer [validate-no-query-params
                                                    wrap-with-parent-check
                                                    wrap-with-parent-check'
                                                    wrap-with-parent-check'']]
            [puppetlabs.comidi :as cmdi]))

(def version :v4)

(defn experimental-index-app
  [version]
  (bring/wrap-middleware (index/index-app version)
                         (fn [app]
                           (partial http/experimental-warning app  "The root endpoint is experimental"))))

(defn report-data-responder
  "Respond with either metrics or logs for a given report hash.
   `entity` should be either :metrics or :logs."
  [version entity]
  (fn [{:keys [globals route-params] :as foo}]
    (println "here")
    (clojure.pprint/pprint foo)
    (let [query ["from" entity ["=" "hash" (:hash route-params)]]]
      (produce-streaming-body version {:query query}
                              (select-keys globals [:scf-read-db
                                                    :url-prefix
                                                    :pretty-print
                                                    :warn-experimental])))))

(defn events-app
  "Ring app for querying events"
  [version]
  (let [param-spec {:optional (concat
                               ["query"
                                "distinct_resources"
                                "distinct_start_time"
                                "distinct_end_time"]
                               paging/query-params)}]
    (http-q/query-route-from' "events" version param-spec)))

(defn reports-app
  [version]
  
  [["" (http-q/query-route-from' "reports" version {:optional paging/query-params})]
   
   [["/" :hash "/events"]
    (wrap-with-parent-check'' (comp (events-app version) http-q/restrict-query-to-report') version :report :hash)]
   
   [["/" :hash "/metrics"]
    (-> (report-data-responder version "report_metrics")
        validate-no-query-params
        (wrap-with-parent-check'' version :report :hash))]
  
   [["/" :hash "/logs"]
    (-> (report-data-responder version "report_logs")
        validate-no-query-params
        (wrap-with-parent-check'' version :report :hash))]] )

(def v4-app
  {"" (experimental-index-app version)
   "/facts" (facts/facts-app version)
   "/edges" (edges/edges-app version)
   "/factsets" (factsets/factset-app version)
   "/fact-names" (fact-names/fact-names-app version)
   "/fact-contents" (fact-contents/fact-contents-app version)
   "/fact-paths" (fact-paths/fact-paths-app version)
   "/nodes" (nodes/node-app version)
   "/environments" (envs/environments-app version)
   "/resources" (resources/resources-app version)
   "/catalogs" (catalogs/catalog-app version)
   "/events" (events-app version)
   "/event-counts" (http-q/query-route-from' "event_counts" version {:required ["summarize_by"]
                                                                     :optional (concat ["counts_filter" "count_by"
                                                                                        "distinct_resources" "distinct_start_time"
                                                                                        "distinct_end_time"]
                                                                                       paging/query-params)})
   "/aggregate-event-counts" (http-q/query-route-from' "aggregate_event_counts" version {:required ["summarize_by"]
                                                                                         :optional ["query" "counts_filter" "count_by"
                                                                                                    "distinct_resources" "distinct_start_time"
                                                                                                    "distinct_end_time"]})
   "/reports" (reports-app version)})
