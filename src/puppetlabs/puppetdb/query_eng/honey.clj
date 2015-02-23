(ns puppetlabs.puppetdb.query-eng.honey
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as sqlh]
            [honeysql.format :as fmt]
            [clojure.string :as str]))

(defn coalesce [& args]
  (apply sql/call :coalesce args))

(defn scast [col cast-to]
  (sql/call :cast col cast-to))

(defrecord Column [field alias queryable? col-type]
  honeysql.format.ToSql
  (-to-sql [col]
    (fmt/to-sql [field alias])))

(defn col
  ([field type]
   (let [alias-name (nth (str/split (name field) #"\.") 1)]
     (col field (keyword alias-name) type)))
  ([field alias type]
   (col field alias true type))
  ([field alias queryable? type]
   (Column. field alias queryable? type)))

(def facts-table
  {:select [(col :fs.certname :string)
            (col :fp.path :string)
            (col :fp.depth :depth false :integer)
            (col :fp.name :string)
            (col :fv.value_integer :value_integer false :number)
            (col :fv.value_float :value_float false :number)
            (col :fv.value_hash :value_hash false :string)
            (col :fv.value_string :value_string false :string)
            (col :vt.type :type false :string)
            (col :env.name :environment :string)]
   :from [[:factsets :fs]]
   :join [[:facts :f] [:= :fs.id :f.factset_id]
          [:fact_values :fv] [:= :f.fact_value_id :fv.id]
          [:fact_paths :fp] [:= :fv.path_id :fp.id]
          [:value_types :vt] [:= :vt.id :fv.value_type_id]]
   :left-join [[:environments :env] [:= :fs.environment_id :env.id]]})


(def facts-query
  (-> facts-table
      (sqlh/merge-select (col (coalesce :fv.value_string
                                        :fv.value_json
                                        (scast :fv.value_boolean :text))
                              :value
                              :multi))
      ;;      (sqlh/un-select (col :fv.value_hash :value_hash false :string))
      (sqlh/merge-where [:= :depth 0])))

(def fact-contents
  (-> facts-table
      (sqlh/merge-select (col (coalesce :fv.value_string
                                        (scast :fv.value_boolean :text))
                              :value
                              :multi))
      (sqlh/merge-where [:!= :fp.value_type_id 5])
      sql/format
      first))

(def factset
  (-> facts-table
      (sqlh/merge-select (col (coalesce :fv.value_string
                                        :fv.value_json
                                        (scast :fv.value_boolean :text))
                              :value
                              :multi)
                         (col :fs.hash :string)
                         (col :fs.producer_timestamp :timestamp)
                         (col :fs.timestamp :timestamp))
      (sqlh/merge-where [:= :depth 0])
      (sqlh/merge-order-by :fs.certname)
      sql/format
      first))

(defn to-sql-str [sql-map]
  (-> sql-map
      sql/format
      first))
