(ns generator-integ-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :as cct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [schema-gen.core :as sg]
            [puppetlabs.puppetdb.scf.storage :as st]
            [puppetlabs.puppetdb.schema :as pls]
            [clj-time.coerce :as c]
            [schema.core :as s]
            [puppetlabs.puppetdb.cli.import-export-roundtrip-test :as rt]
            [puppetlabs.puppetdb.testutils.jetty :as jutils]
            [puppetlabs.puppetdb.cli.export :as export]
            [clojure.test.check :as tc]
            [clojure.java.jdbc :as sql]))

(def facts-schema
  {:name #"^((([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9]))$"

   :values {s/Any s/Any}
   :timestamp pls/Timestamp
   :environment (s/maybe s/Str)
   :producer-timestamp (s/either (s/maybe s/Str) pls/Timestamp)})

(def ten-years-in-ms (* 1000 60 60 24 365 10))
(def now (System/currentTimeMillis))
(def ten-years-ago (- now ten-years-in-ms))
(def ten-years-ahead (+ now ten-years-in-ms))

(def date-generator
  (gen/fmap
   c/to-string
   (gen/fmap c/from-long
             (gen/such-that #(> ten-years-ahead %)
                            (gen/fmap #(+ ten-years-ago %) gen/nat)))))



(defmethod sg/schema->gen pls/Timestamp
  [e]
  date-generator
  )

(def hostname'
  (gen/such-that (complement clojure.string/blank?) (gen/resize 20 gen/string-alphanumeric)))

(def hostname
  (gen/bind (gen/tuple hostname' hostname')
            (fn [[k v]]
              (gen/return (str k "." v)))))

(def compound
  (fn [leaf-gen]
    (gen/one-of [(gen/list leaf-gen)
                 (gen/map gen/string
                          leaf-gen)])))

(def non-blank-string
  (gen/such-that (complement clojure.string/blank?) gen/string))

(def scalars
  (gen/one-of [gen/int non-blank-string gen/int gen/boolean]))



(def value-map
  (gen/map gen/string
           (gen/one-of [non-blank-string gen/int gen/boolean (gen/recursive-gen compound scalars)])))

(def the-command
  (gen/tuple (gen/return :name) hostname
             ;;             (gen/return :timestamp) date-generator
             (gen/return :producer-timestamp) date-generator
             (gen/return :environment) non-blank-string
             (gen/return :values) (gen/such-that seq value-map)))

(def the-real-command
  (gen/fmap #(apply hash-map %) the-command))

(defn submit-facts [payload]
  (rt/submit-command :replace-facts 3 payload))

(defn do-commands'
  "Executes SQL commands on the open database connection."
  [& commands]
  (with-open [stmt (let [con (sql/connection)] (.createStatement con))]
    (doseq [^String cmd commands]
      (.addBatch stmt cmd))
    (seq (.executeBatch stmt))))

(defn reset-db [f]
  (binding [puppetlabs.puppetdb.fixtures/*db* {:classname   "org.postgresql.Driver"
                                               :subprotocol "postgresql"
                                               :subname     "//localhost:5432/puppetdb_gen_template"
                                               :user        "pdbgen"
                                               :password    "pdbgen"}]
    (sql/with-connection puppetlabs.puppetdb.fixtures/*db*
      (try
        #spy/d (do-commands' "drop database puppetdb_gen")
        #spy/d (do-commands' "create database puppetdb_gen with owner pdbgen template puppetdb_gen_template")
        (catch Exception e (.printStackTrace (.getNextException e))))
      (println "done"))
    (f)))

(def the-test
  (prop/for-all [{node-name :name :as m} the-real-command]
                (reset-db
                 (fn []
                   (println "submitting...")
                   (submit-facts m)
                   @(rt/block-until-results 100 (export/facts-for-node "localhost" jutils/*port* node-name))
                   (let [res (export/facts-for-node "localhost" jutils/*port* node-name)]
                     (if (= m res)
                       true
                       (do
                         (println "Originally submitted:")
                         (clojure.pprint/pprint m)
                         (println "-----------------------------------------------------------------")
                         (println "Received as a response")
                         (clojure.pprint/pprint res)
                         (println "-----------------------------------------------------------------")
                         (println "Difference:")
                         (clojure.pprint/pprint (clojure.data/diff m res))
                         nil)))))))

#_ (sql/with-connection (puppetlabs.puppetdb.fixtures/init-db {:classname   "org.postgresql.Driver"
                                                               :subprotocol "postgresql"
                                                               :subname     "//localhost:5432/puppetdb_gen_template"
                                                               :user        "puppetdb"
                                                               :password    "puppetdb"}
                                                              false))




(deftest test-em
  (jutils/puppetdb-instance
   (assoc-in (jutils/create-config) [:database] {:classname   "org.postgresql.Driver"
                                                 :subprotocol "postgresql"
                                                 :subname     "//localhost:5432/puppetdb_gen"
                                                 :user        "pdbgen"
                                                 :password    "pdbgen"})
   (fn []
     (tc/quick-check 20 the-test))))

#_(defn basic-test []
    (jutils/puppetdb-instance
     (assoc-in (jutils/create-config) [:database] {:classname   "org.postgresql.Driver"
                                                   :subprotocol "postgresql"
                                                   :subname     "puppetdb_gen"
                                                   :user        "puppetdb"
                                                   :password    "puppetdb"})
     (fn []
       (let [{node-name :name :as m} {:name "c1OY29yhOLu1p9.0H" :values {"y" false}, :environment "lgDyu5KrAFS6Siz" :producer-timestamp "2004-12-06T21:37:56.680Z"}]
         #spy/d (submit-facts m)
         (Thread/sleep 1000)
         #spy/d @(rt/block-until-results 100 (export/facts-for-node "localhost" jutils/*port* node-name))
         (is (= m
                (export/facts-for-node "localhost" jutils/*port* :v4 node-name)))))))


#_  (binding [puppetlabs.puppetdb.fixtures/*db* {:classname   "org.postgresql.Driver"
                                                 :subprotocol "postgresql"
                                                 :subname     "//localhost:5432/puppetdb_gen_template"
                                                 :user        "pdbgen"
                                                 :password    "pdbgen"}]
      (try (sql/with-connection puppetlabs.puppetdb.fixtures/*db*
             (do-commands' "drop database puppetdb_gen")
             (do-commands' "create database puppetdb_gen template puppetdb_gen_template")
             (println "done"))
           (catch Exception e (clojure.repl/pst (.getNextException e)))))

#_(binding [puppetlabs.puppetdb.fixtures/*db* {:classname   "org.postgresql.Driver"
                                               :subprotocol "postgresql"
                                               :subname     "//localhost:5432/puppetdb_gen"
                                               :user        "pdbgen"
                                               :password    "pdbgen"}]
    (sql/with-connection puppetlabs.puppetdb.fixtures/*db*
      (puppetlabs.puppetdb.scf.migrate/migrate!)
      (println "done")))
