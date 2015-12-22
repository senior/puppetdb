(ns puppetlabs.puppetdb.http.server
  "REST server

   Consolidates our disparate REST endpoints into a single Ring
   application."
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.http.v4 :refer [v4-app]]
            [puppetlabs.puppetdb.middleware :refer [wrap-with-globals
                                                    wrap-with-metrics
                                                    wrap-with-illegal-argument-catch
                                                    verify-accepts-json
                                                    make-pdb-handler]]
            [net.cgrand.moustache :refer [app]]
            [ring.util.response :as rr]
            [bidi.bidi :as bidi]
            [bidi.ring :as bring]))

(defn- refuse-retired-api
  [version]
  (constantly
   (http/error-response
    (format "The %s API has been retired; please use v4" version)
    404)))

(def routes
  ["" {"/v1" [[true (refuse-retired-api "v1")]]
       "/v2" [[true (refuse-retired-api "v2")]]
       "/v3" [[true (refuse-retired-api "v3")]]
       "/v4" v4-app}])

(defn build-app
  "Generates a Ring application that handles PuppetDB requests.
   If get-authorizer is nil or false, all requests will be accepted.
   Otherwise it must accept no arguments and return an authorize
   function that accepts a request.  The request will be allowed only
   if authorize returns :authorized.  Otherwise, the return value
   should be a message describing the reason that access was denied."
  [get-shared-globals]
  (fn [req]
    (let [handler (-> (make-pdb-handler routes identity)
                      wrap-with-illegal-argument-catch
                      verify-accepts-json
                      (wrap-with-metrics (atom {}) http/leading-uris)
                      (wrap-with-globals get-shared-globals))]
      (handler req))))
