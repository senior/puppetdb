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
  (cmdi/routes
   (cmdi/context ""
                 (handlers/experimental-index-routes version))
   (cmdi/context "/facts"
                 (handlers/facts-routes version))
   (cmdi/context "/edges"
                 (handlers/edge-routes version))
   (cmdi/context "/factsets"
                 (handlers/factset-routes version))
   (cmdi/context "/fact-names"
                 (handlers/fact-names-routes version))
   (cmdi/context "/fact-contents"
                 (handlers/fact-contents-routes version))
   (cmdi/context "/fact-paths"
                 (handlers/fact-path-routes version))
   
   (cmdi/context "/nodes"
                 (handlers/node-routes version))
   (cmdi/context "/environments"
                 (handlers/environments-routes version))
   (cmdi/context "/resources"
                 (handlers/resources-routes version))
   (cmdi/context "/catalogs"
                 (handlers/catalog-routes version))
   (cmdi/context "/events"
                 (handlers/events-routes version))
   (cmdi/context "/event-counts"
                 (handlers/event-counts-routes version))
   (cmdi/context "/aggregate-event-counts"
                 (handlers/agg-event-counts-routes version)) 
   (cmdi/context "/reports" (handlers/reports-routes version))))
