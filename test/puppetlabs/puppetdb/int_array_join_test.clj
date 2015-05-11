(ns puppetlabs.puppetdb.int-array-join-test
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.kitchensink.core :refer [uuid]]
            [clj-time.core :refer [now plus minutes secs minutes]]
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
;; :status "unchanged",
;; :transaction_uuid (uuid)
;; :environment "PROD",
 :report_format 4,
 :puppet_version "3.6.2 (Puppet Enterprise 3.3.1)"})

(def base-resource
  {:type "File"
   :title "/foo/bar"
   :tags ["a" "b" "c"]
   :line 10
   :file "/foo/bar"})

(def base-report-resource
  {;;:id index
   ;;:resource_id resource-id
   :containment_path ["foo" "bar" "baz"]
   :containing_clas "foo"
   :status "success"
   :skipped "false"})

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
   :containment_path ["foo" "bar"]})

(defn create-report-resource [resource]
  (hash-it :hash
           (assoc base-report-resource
             :id (:id resource)
             :resource_id (:id resource))))

(defn create-report [certname start-time resource-id]
  (assoc base-report
    :certname certname
    :report_resource_set_id resource-id
;;    :configuration_version config-version
    :transaction_uuid (java.util.UUID/randomUUID)
    :receive_time start-time
    :start_time start-time
    :end_time (plus start-time (minutes 1))))

(defn create-certname [index]
  (format "upydjllvrl%s.vaqevzxtnnxemvi.mos" index))

#_(defn reports-seq [num]
  (let [report-time (now)]
    (map (fn [idx]
           (create-report (create-certname idx) idx (plus report-time (secs idx))))
         (range 0 num))))

(defn reports-for-node-seq [node-name report-time resources]
  (for [{:keys [id]} (take 14 resources)
        puppet-run (range 0 48)]
    (hash-it :hash
             (create-report node-name (plus report-time (minutes (* 30 puppet-run))) id))))

(defn natural-numbers []
  (iterate inc 1))

(defn all-reports [node-names resources]
  (let [cyclic-resources (cycle resources)
        report-time (now)]
    (map (fn [id report]
           (assoc report
             :id id
             :configuration_version (str id)))
         (natural-numbers)
     (mapcat (fn [node node-index]
               (reports-for-node-seq node (plus report-time (secs node-index)) (drop node-index cyclic-resources)))
             node-names (natural-numbers)))))

(defn prep-resource [resource]
  (-> resource
      st/convert-tags-array
      (update-in [:resource_hash] sutils/munge-hash-for-storage)))

(defn prep-report-resource [report-resource]
  (-> report-resource
      (update-in [:hash]  sutils/munge-hash-for-storage)
      (update-in [:containment_path] sutils/to-jdbc-varchar-array)))

(defn resources-seq [num]
  (map (comp prep-resource create-resource) (range 1 num)))

(defn prep-report-for-storage [report]
  (-> report
      (update-in [:start_time] to-timestamp)
      (update-in [:receive_time] to-timestamp)
      (update-in [:end_time] to-timestamp)
      (update-in [:configuration_version] to-timestamp)
      (update-in [:hash] sutils/munge-hash-for-storage)))

(def pg-conn
  {:classname   "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname     "puppetdb_test"
   :user        "puppetdb"
   :password    "puppetdb"})

#_(defn create-resource-set [index report-resource-coll]
  (map (fn [report-resource]
         )))

(defn resource-set-permutations [num-permutations resources]
  (let [num-resources (count resources)
        res-coll (mapv :id resources)
        rand-resource-start #(rand num-resources)
        rand-resource-set-size #(rand 1000)]
    (loop [idx 0
           resource-set-coll []
           resource-hashes #{}
           start-num (rand num-resources)
           total-selected (rand 1000)]
      (let [resource-set-ids (sort (set (take total-selected (repeatedly #(rand-nth res-coll)))))
            h (hash/generic-identity-hash resource-set-ids)]
        (cond
         (contains? resource-hashes h)
         (recur idx resource-set-coll resource-hashes (rand-resource-start) (rand-resource-set-size))

         (<= idx num-permutations)
         (recur
          (inc idx)
          (conj resource-set-coll
                {:id idx
                 :hash h
                 :resources resource-set-ids})
          (conj resource-hashes h)
          (rand-resource-start)
          (rand-resource-set-size))

         :else
         resource-set-coll)))))

(defn create-resource-set-to-resources [total-resource-sets resources]


  )

(defn to-jdbc-int-array
  "Takes the supplied collection and transforms it into a
  JDBC-appropriate VARCHAR array."
  [coll]
  (let [connection (sql/find-connection)]
    (->> coll
         (into-array Object)
         (.createArrayOf connection "int"))))


(defn prep-resource-set [resource-set]
  (-> resource-set
      (update-in [:hash] sutils/munge-hash-for-storage)
      (update-in [:resources] to-jdbc-int-array)))

(defn clear-tables []
  (try
    (sql/do-commands
     "truncate reports cascade"
     "alter sequence reports_id_seq start 1"
     "truncate certnames  cascade"
     "truncate ur_report_resource_set_to_resources_array  cascade"
     "truncate ur_report_resource_set_to_resources  cascade"
     "truncate ur_report_resource_set  cascade"
     "alter sequence ur_report_resource_set_id_seq start 1"
     "truncate ur_report_resources cascade "
     "alter sequence ur_report_resources_id_seq start 1"
     "truncate ur_resources cascade"
     "alter sequence ur_resources_id_seq start 1")
    (catch Exception e
      (println "here?")
      (clojure.repl/pst (.getNextException e)))))

(defn insert-reports [total-nodes total-resources]
  (jdbc/with-transacted-connection pg-conn
    (clear-tables))
  (jdbc/with-transacted-connection pg-conn
    (let [node-names (map create-certname (range 0 total-nodes))
          _ (println "Certname seq")
          resources (resources-seq total-resources)
          _ (println "Resource seq")
          report-resources (map (comp prep-report-resource create-report-resource) resources)
          _ (println "report resources seq")
          resource-sets (resource-set-permutations (* 10 total-resources) resources)
          _ (println "resource sets seq")
          reports (all-reports node-names resource-sets)
          _ (println "reports seq")]
      (jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :certnames (map (fn [node-name] {:certname node-name}) node-names)))
      (println "Done with certnames")
      (jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :ur_resources resources))
      (println "Done with resources")
      (jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :ur_report_resources report-resources))
      (println "Done with report resources")
      (jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :ur_report_resource_set
               (map (comp #(select-keys % [:id :hash])
                          prep-resource-set)
                    resource-sets)))
      (println "Done with report resource set")
      (jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :ur_report_resource_set_to_resources_array
               (map (comp (fn [{:keys [id resources]}]
                            {:report_resource_set_id id
                             :report_resource_ids resources})
                          prep-resource-set)
                    resource-sets)))
      (println "Done with report resource set to array")
      #_(jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :ur_report_resource_set_to_resources
               (mapcat (fn [{:keys [id resources]}]
                         (map (fn [resource]
                                {:report_resource_set_id id
                                 :report_resource_id resource})
                              resources))
                       resource-sets)))
      #_(println "Done with report resource set join")
      (jdbc/with-transacted-connection pg-conn
        (apply sql/insert-records :reports
               (map prep-report-for-storage reports)))
      (println "Done with reports")
      {:resources resources
       :report-resources report-resources
       :resource-sets resource-sets
       :reports reports})))



#_(defn insert-reports []
  (jdbc/with-transacted-connection pg-conn
    (sql/do-commands
     "delete from reports"
     "alter sequence reports_id_seq start 1")
    (let [reports ])

    ))
