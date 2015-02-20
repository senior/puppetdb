(ns puppetlabs.puppetdb.query-eng.honey
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as sqlh]
            [honeysql.format :as fmt]))

(defn coalesce [& args]
  (apply sql/call :coalesce args))

(defn scast [col cast-to]
  (sql/call :cast col cast-to))

(def facts-table
  {:select [:fs.certname
            :fp.path
            :fp.name
            :fp.depth
            :fv.value_integer
            :fv.value_float
            :fv.value_hash
            :fv.value_string
            :vt.type
            [:env.name :environment]]
   :from [[:factsets :fs]]
   :join [[:facts :f] [:= :fs.id :f.factset_id]
          [:fact_values :fv] [:= :f.fact_value_id :fv.id]
          [:fact_paths :fp] [:= :fv.path_id :fp.id]
          [:value_types :vt] [:= :vt.id :fv.value_type_id]]
   :left-join [[:environments :env] [:= :fs.environment_id :env.id]]})

(def facts-query
  (-> facts-table
      (sqlh/merge-select [(coalesce :fv.value_string
                                    :fv.value_json
                                    (scast :fv.value_boolean :text))
                          :value])
      (sqlh/merge-where [:= :depth 0])
      sql/format
      first))

(def fact-contents
  (-> facts-table
      (sqlh/merge-select [(coalesce :fv.value_string
                                    (scast :fv.value_boolean :text))
                          :value])
      (sqlh/merge-where [:!= :fp.value_type_id 5])
      sql/format
      first))

(def factset
  (-> facts-table
      (sqlh/merge-select [(coalesce :fv.value_string
                                    :fv.value_json
                                    (scast :fv.value_boolean :text))
                          :value]
                         :fs.hash
                         :fs.producer_timestamp
                         :fs.timestamp)
      (sqlh/merge-where [:= :depth 0])
      (sqlh/merge-order-by :fs.certname)
      sql/format
      first))
