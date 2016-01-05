(ns puppetlabs.puppetdb.metrics.server
  (:require [puppetlabs.puppetdb.metrics.core :as metrics]
            [net.cgrand.moustache :refer [app]]
            [puppetlabs.puppetdb.middleware :refer [wrap-with-puppetdb-middleware]]
            [puppetlabs.puppetdb.middleware :as mid]))

(def routes
  ["/v1/mbeans" [[[""] metrics/list-mbeans]
                 [["/" [#".*" :names]] (fn [{:keys [route-params] :as req}]
                                         ((metrics/mbean [(java.net.URLDecoder/decode (:names route-params))]) req))]]])

(defn build-app
  "Generates a Ring application that handles metrics requests.
  If get-authorizer is nil or false, all requests will be accepted.
  Otherwise it must accept no arguments and return an authorize
  function that accepts a request.  The request will be allowed only
  if authorize returns :authorized.  Otherwise, the return value
  should be a message describing the reason that access was denied."
  [cert-whitelist]
  (-> routes
      mid/make-pdb-handler 
      mid/verify-accepts-json
      mid/validate-no-query-params
      (wrap-with-puppetdb-middleware cert-whitelist)))
