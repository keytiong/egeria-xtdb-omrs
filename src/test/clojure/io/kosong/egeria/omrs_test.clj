(ns io.kosong.egeria.omrs-test
  (:use [midje.sweet])
  (:require [io.kosong.egeria.omrs :as omrs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances EntityDetail InstanceProperties PrimitivePropertyValue)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs PrimitiveDefCategory)))

(defn- new-repo-helper []
  (let [user-id               "test-user"
        server-name           "my-server-name"
        server-type           "my-server-type"
        organization          "my-organization"
        component-id          0
        component-name        "my-component-name"
        component-description "my-component-description"
        component-wiki-url    "my-component-wiki-url"
        audit-log-store       (omrs/->audit-log-store)
        audit-log-destination (omrs/->audit-log-destination
                                {:audit-log-stores [audit-log-store]
                                 :server-name      server-name
                                 :server-type      server-type
                                 :organization     organization})
        audit-log             (omrs/->audit-log
                                {:audit-log-destination audit-log-destination
                                 :component-id          component-id
                                 :component-name        component-name
                                 :component-description component-description
                                 :component-wiki-url    component-wiki-url})
        archive-type-store    (omrs/->openmetadata-archive)
        content-manager       (omrs/->repository-content-manager
                                {:user-id   user-id
                                 :audit-log audit-log
                                 :archive   archive-type-store})
        repo-helper           (omrs/->repository-helper
                                {:content-manager content-manager})]
    repo-helper))

(def ^:dynamic *repo-helper* (new-repo-helper))

(let [^EntityDetail entity-detail (omrs/->EntityDetail *repo-helper* "repl" "1234" "alice" "CSVFile")
      csv-file                    (omrs/EntityDetail->map *repo-helper* entity-detail)]
  (fact "EntityDetail object"
    entity-detail =not=> nil?)
  (fact "EntityDetail type def name"
    (-> entity-detail (.getType) (.getTypeDefName)) => "CSVFile"))

(facts "Create InstanceProperties object from map"
  (let [^InstanceProperties obj (omrs/->InstanceProperties *repo-helper* "CSVFile"
                                  {:name                 "Week 1: Drop Foot Clinical Trial Measurements"
                                   :description          "One week's data covering foot angle, hip displacement and mobility measurements."
                                   :qualifiedName        "file://secured/research/clinical-trials/drop-foot/DropFootMeasurementsWeek1.csv"
                                   :fileType             "csv"
                                   :ownerType            "UserId"
                                   :additionalProperties {"a" "1"
                                                          "b" "2"}})]
    (fact "should have expected property count"
      (some-> (.getPropertyCount obj) => 15))
    (fact "name property should have primitive string category"
      (some-> (.getPropertyValue obj "name") (.getPrimitiveDefCategory)) => PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
    (fact "property name value"
      (some-> (.getPropertyValue obj "name") (.valueAsObject)) => "Week 1: Drop Foot Clinical Trial Measurements")))

(facts "Build EntityDetail from Clojure map"
  (let [entity-detail (omrs/map->EntityDetail *repo-helper*
                        (-> (io/resource "DataSet/iris.edn")
                          (slurp)
                          (edn/read-string)))]
    (fact "instance guid"
      (.getGUID entity-detail) =>
      "9bfbf0ca-3fb0-4980-a8b2-be903da4d1cf")
    (fact "type def guid"
      (some-> entity-detail (.getType) (.getTypeDefGUID)) =>
      "1449911c-4f44-4c22-abc0-7540154feefb")
    (fact "type def name"
      (some-> entity-detail (.getType) (.getTypeDefName)) =>
      "DataSet")
    (fact "data set name"
      (some-> entity-detail (.getProperties) (.getPropertyValue "name") (.valueAsObject)) =>
      "Iris Plants Database")
    (fact "data set description"
      (some-> entity-detail (.getProperties) (.getPropertyValue "description") (.valueAsObject)) =>
      #(str/starts-with? % "Multivariate data set"))))

