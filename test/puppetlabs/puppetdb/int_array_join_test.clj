(ns puppetlabs.puppetdb.int-array-join-test
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.kitchensink.core :refer [uuid]]
            [clj-time.core :refer [now plus minutes secs]]
            [puppetlabs.puppetdb.time :refer [to-timestamp]]
            [puppetlabs.puppetdb.scf.hash :as hash]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.scf.storage :as st]
            [clojure.java.jdbc :as sql]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]))

#_ (def a (json/parse-string (slurp "/Users/ryan/work/test-data/pe-3k-data/puppetdb-bak/reports/upydjllvrl.vaqevzxtnnxemvi.mos-7f7607393c62f919cded4d0fc9f0161d26f0f7a4.json")))

(def base-report
{:configuration_version "1426532010",
;; :end-time "2015-03-16T18:54:44.139Z",
;; :start-time "2015-03-16T18:54:03.592Z",
;; :certname
 :status "unchanged",
;; :transaction_uuid (uuid)
 :environment "PROD",
 :report_format 4,
 :puppet_version "3.6.2 (Puppet Enterprise 3.3.1)"})

(def base-resource
  {:type "File"
   :title "/foo/bar"
   :tags ["a" "b" "c"]
   :line 10
   :file "/foo/bar"})

(defn hash-it [key m]
  (assoc m key (hash/generic-identity-hash m)))

(defn create-resource [index]
  (hash-it :resource_hash (-> base-resource
                              (assoc :id index)
                              (update-in [:title] str index))))

(def base-report-resource
  {:skipped false
   :status "success"
   :containing_class "foo"
   :containment-path ["foo" "bar"]})

(defn create-report-resource [index resource]
  (assoc base-report-resource
    :resource_id (:id resource)
    :hash (hash/generic-identity-hash (assoc base-report-resource :resource resource))))

(defn create-report [certname config-version start-time]
  (hash-it :hash (assoc base-report
                   :certname certname
                   :configuration_version config-version
                   :transaction_uuid (uuid)
                   :start_time start-time
                   :end_time (plus start-time (minutes 1)))))

(defn create-certname [index]
  (format "upydjllvrl%s.vaqevzxtnnxemvi.mos" index))

(defn reports-seq [num]
  (let [report-time (now)]
    (map (fn [idx]
           (create-report (create-certname idx) idx (plus report-time (secs idx))))
         (range 0 num))))

(defn prep-resource [resource]
  (-> resource
      st/convert-tags-array
      (update-in [:resource_hash] sutils/munge-hash-for-storage)))

(defn resources-seq [num]
  (map (comp prep-resource create-resource) (range 0 num)))

(defn prep-report-for-storage [report]
  (-> report
      (update-in [:start_time] to-timestamp)
      (update-in [:end_time] to-timestamp)
      (update-in [:configuration_version] to-timestamp)))

(def pg-conn
  {:classname   "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname     "puppetdb_test"
   :user        "puppetdb"
   :password    "puppetdb"})

(defn insert-resources []
  (jdbc/with-transacted-connection pg-conn
    (let [resources (resources-seq 10)]
      (apply sql/insert-records :ur_resources resources))))
