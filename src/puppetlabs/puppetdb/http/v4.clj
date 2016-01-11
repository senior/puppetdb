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
            [clojure.walk :refer [keywordize-keys]]
            [puppetlabs.puppetdb.http.handlers :as handlers]))

(def version :v4)

(def v4-app
  (apply cmdi/routes
         (map (fn [[route-str handler]]
                (cmdi/context route-str (handler version)))
              {"" handlers/experimental-index-routes
               "/facts" handlers/facts-routes
               "/edges" handlers/edge-routes
               "/factsets" handlers/factset-routes
               "/fact-names" handlers/fact-names-routes
               "/fact-contents" handlers/fact-contents-routes
               "/fact-paths" handlers/fact-path-routes
               "/nodes" handlers/node-routes
               "/environments" handlers/environments-routes
               "/resources" handlers/resources-routes
               "/catalogs" handlers/catalog-routes
               "/events" handlers/events-routes
               "/event-counts" handlers/event-counts-routes
               "/aggregate-event-counts" handlers/agg-event-counts-routes 
               "/reports" handlers/reports-routes})))
