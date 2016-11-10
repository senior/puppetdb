(ns puppetlabs.puppetdb.scf.scratch
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.scf.storage :as scf-storage]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.testutils.db :as tudb]
            [clj-time.core :refer [now]]
            [clj-time.format :as tformat]))



(defn ral-package-seq []
  (mapv (fn [[package-name package-map]]
          {:package-name package-name
           :version (get package-map "ensure")
           :provider "yum"})
        (-> (slurp "packages.json")
            json/parse-string
            (get "package"))))

(defn create-node-specific-packages [node-name packages]
  (map (fn [{:keys [id name version]}]
         {:package-name name
          :package-id id
          :version version})
       (apply jdbc/insert!
              :packages
              (map (fn [{:keys [package-name version]}]
                     {:name (format "%s-%s" package-name node-name)
                      :version version
                      :provider "yum"})
                   (repeatedly 5 #(rand-nth packages))))) )

(defn populate-common-packages [db]
  (jdbc/with-db-connection db
    (jdbc/with-db-transaction []
      (apply jdbc/insert! :packages (map (fn [{:keys [package-name version provider]} ]
                                           {:name package-name
                                            :version (if (coll? version)
                                                       (first version)
                                                       version)
                                            :provider provider})
                                         (ral-package-seq))))))

(defn all-packages [db]
  (jdbc/with-db-connection db
    (mapv (fn [{:keys [id name version]}]
              {:package-name name
               :package-id id
               :version version})
            (jdbc/query-to-vec "select id, name, version from packages"))))

(defn create-fake-nodes [db]
  (jdbc/with-db-connection db
    (jdbc/with-db-transaction []
      (doseq [node-name (map #(str "node-" %) (range 0 5000))]
        (scf-storage/add-certname! node-name)))))

(defn node-info [db]
  (jdbc/with-db-connection db
    (jdbc/with-db-transaction []
      (reduce (fn [acc {:keys [id name]}]
                (assoc acc id name))
              (sorted-map)
              (jdbc/query-to-vec "select id, certname from certnames")))))

(defn munge-range [date-time]
  (sutils/str->pgobject
   "tstzrange"
   (format "[%s,]" (tformat/unparse (:basic-date-time tformat/formatters) date-time))))

(defn insert-packages-for-certname [db certname-id certname packages]
  (jdbc/with-db-connection db
    (jdbc/with-db-transaction []
      (let [current-time (now)
            node-specific-packages (create-node-specific-packages certname packages)]
        (apply jdbc/insert! :package_lifetimes
               (map (fn [{:keys [package-id]}]
                      {:package_id package-id
                       :certname_id certname-id
                       :time_range (munge-range current-time)})
                    (concat packages node-specific-packages)))))))

(defn insert-all-the-things [db]
  (jdbc/with-db-connection db
    (jdbc/with-db-transaction []
      (create-fake-nodes db)
      (populate-common-packages db)
      (let [node-map (node-info db)
            packages (all-packages db)]
        (doseq [[node-id node-name] node-map]
          (insert-packages-for-certname db node-id node-name packages))))))

#_(def db (tudb/init-db (tudb/create-temp-db) false))
#_(insert-all-the-things db)
