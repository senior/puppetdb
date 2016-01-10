(ns puppetlabs.puppetdb.http.index
  (:require [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.comidi :as cmdi]))

(defn index-app
  [version & optional-handlers]
  (let [param-spec {:optional paging/query-params
                    :required ["query"]}]
    (cmdi/ANY "" [] (http-q/query-route' version param-spec optional-handlers))))
