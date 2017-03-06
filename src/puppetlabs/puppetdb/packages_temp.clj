(ns puppetlabs.puppetdb.packages-temp
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [me.raynes.fs :as fs]))

(defn slurp-packages []
  (json/parse-stream
   (java.io.FileReader. "/home/ryan/work/package_inventory_data/packages.json")))

(defn create-inventory-format [[package pkg-map] provider]
  (let [maybe-version-seq (get pkg-map "ensure")]
    (if (coll? maybe-version-seq)
      (map (fn [version]
             {:package_name package
              :version version
              :provider provider})
           maybe-version-seq)
      [{:package_name package
        :version maybe-version-seq
        :provider provider}])))

(defn create-test-set []
  (let [packages (-> (slurp-packages)
                     (get "package"))]
    (concat
     (mapcat #(create-inventory-format % "apt")
             (take 400 packages))
     (mapcat #(create-inventory-format % "pip")
             (take 50 (drop 400 packages)))
     (mapcat #(create-inventory-format % "gem")
             (take 50 (drop 450 packages))))))

(defn pkgs->fact-format [pkg-list]
  (reduce (fn [acc {:keys [package_name version provider]}]
            (if-let [pkg-map (get acc package_name)]
              (assoc acc
                     package_name
                     (assoc pkg-map
                            version
                            {provider true}))
              (assoc acc package_name {version {provider true}})))
          {} pkg-list))

(defn munge-structured-fact-pkgs [pkg-list]
  (let [file-list (fs/list-dir "/home/ryan/work/package_inventory_data/structured-fact-pkgs/facts")]
    (doseq [file file-list
            :let [facts (json/parse-stream (java.io.FileReader. file))]]
      (with-open [fw (java.io.FileWriter. file)]
        (json/generate-stream
         (update facts "values" assoc "package_inventory" (pkgs->fact-format pkg-list))
         fw)))))

(defn munge-inventory-fact-pkgs [pkg-list]
  (let [file-list (fs/list-dir "/home/ryan/work/package_inventory_data/inventory-pkgs/facts")]
    (doseq [file file-list
            :let [facts (json/parse-stream (java.io.FileReader. file))]]
      (with-open [fw (java.io.FileWriter. file)]
        (json/generate-stream
         (assoc facts "inventory" pkg-list)
         fw)))))
