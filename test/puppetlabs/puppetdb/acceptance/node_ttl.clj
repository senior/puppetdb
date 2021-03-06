(ns puppetlabs.puppetdb.acceptance.node-ttl
  (:require [clojure.test :refer :all]
            [puppetlabs.puppetdb.testutils.services :as svc-utils]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.testutils.db :refer [*db* with-test-db]]
            [puppetlabs.puppetdb.testutils.http :as tuhttp]
            [puppetlabs.puppetdb.examples :refer [wire-catalogs]]
            [clj-time.core :refer [now]]
            [puppetlabs.puppetdb.testutils :as tu]
            [puppetlabs.puppetdb.test-protocols :refer [called?]]))

(deftest test-node-ttl
  (tu/with-coordinated-fn run-purge-nodes puppetlabs.puppetdb.cli.services/purge-nodes!
    (tu/with-coordinated-fn run-expire-nodes puppetlabs.puppetdb.cli.services/auto-expire-nodes!
      (with-test-db
        (svc-utils/call-with-puppetdb-instance
         (-> (svc-utils/create-temp-config)
             (assoc :database *db*)
             (assoc-in [:database :node-ttl] "1s")
             (assoc-in [:database :node-purge-ttl] "1s"))
         (fn []
           (let [certname "foo.com"
                 catalog (-> (get-in wire-catalogs [8 :empty])
                             (assoc :certname certname
                                    :producer_timestamp (now)))]
             (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) certname
                                          "replace catalog" 8 catalog)

             (is (= 1 (count (:body (tuhttp/pdb-get (svc-utils/pdb-query-url) "/nodes")))))
             (is (nil? (:expired (:body (tuhttp/pdb-get (svc-utils/pdb-query-url) "/nodes/foo.com")))))
             (Thread/sleep 1000)
             (run-expire-nodes)

             (is (= 0 (count (:body (tuhttp/pdb-get (svc-utils/pdb-query-url) "/nodes")))))
             (is (:expired (:body (tuhttp/pdb-get (svc-utils/pdb-query-url) "/nodes/foo.com"))))
             (Thread/sleep 1000)
             (run-purge-nodes)

             (is (= 0 (count (:body (tuhttp/pdb-get (svc-utils/pdb-query-url) "/nodes")))))
             (is (= {:error "No information is known about node foo.com"}
                    (:body (tuhttp/pdb-get (svc-utils/pdb-query-url) "/nodes/foo.com")))))))))))

(deftest test-zero-gc-interval
  (with-redefs [puppetlabs.puppetdb.cli.services/purge-nodes! (tu/mock-fn)]
    (with-test-db
      (svc-utils/call-with-puppetdb-instance
       (-> (svc-utils/create-temp-config)
           (assoc :database *db*)
           (assoc-in [:database :node-ttl] "0s")
           (assoc-in [:database :report-ttl] "0s")
           (assoc-in [:database :node-purge-ttl] "1s")
           (assoc-in [:database :gc-interval] 0))
       (fn []
         (Thread/sleep 1500)
         (is (not (called? puppetlabs.puppetdb.cli.services/purge-nodes!))))))))
