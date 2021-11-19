(defproject io.kosong.egeria/egeria-crux-omrs "0.1.0-SNAPSHOT"
  :description "Egeria open metadata repository implemented in Crux"
  :url "https://github.com/keytiong/egeria-crux-orms"
  :license {:name "The MIT License"
            :url  "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.odpi.egeria/open-metadata-types "3.3"]
                 [org.odpi.egeria/repository-services-apis "3.3"]
                 [org.odpi.egeria/repository-services-implementation "3.3" :exclusions
                  [org.odpi.egeria/graph-repository-connector
                   org.odpi.egeria/inmemory-repository-connector]]
                 [juxt/crux-core "21.04-1.16.0-beta"]]
  :target-path "target/%s"
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :resource-paths ["src/main/resources"]
  :test-paths ["src/test/clojure"]
  :aot [io.kosong.egeria.omrs.crux-metadata-store]
  :profiles {:dev     {:plugins        [[lein-midje "3.2.1"]]
                       :dependencies   [[clojure-complete "0.2.5"]
                                        [midje "1.9.9"]
                                        [org.clojure/tools.namespace "1.1.0"]
                                        [integrant/repl "0.3.1"]
                                        [nrepl "0.6.0"]
                                        [org.testcontainers/kafka "1.15.2"]
                                        [clj-http "2.3.0"]
                                        [org.clojure/data.json "2.0.1"]
                                        [juxt/crux-rocksdb "21.02-1.15.0-beta"]]
                       :source-paths   ["dev"]
                       :resource-paths ["dev-resources"
                                        "src/test/resources"]
                       }
             :uberjar {:aot      :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
