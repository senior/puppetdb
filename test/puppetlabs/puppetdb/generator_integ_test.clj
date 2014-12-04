(ns generator-integ-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :as cct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [schema-gen.core :as sg]
            [puppetlabs.puppetdb.scf.storage :as st]
            [puppetlabs.puppetdb.schema :as pls]))

(defmethod schema->gen pls/Timestamp
  [e]
  )
