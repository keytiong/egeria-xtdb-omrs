(ns io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection-test
  (:use [midje.sweet])
  (:require [io.kosong.egeria.omrs :as om]
            [io.kosong.egeria.omrs.datafy :as om-datafy]
            [integrant.core :as ig]
            [dev]
            [xtdb.api :as xt]
            [clojure.datafy :refer [datafy]])
  (:import (java.util UUID Date)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceStatus EntityDetail PrimitivePropertyValue)))

(def system
  (ig/init dev/dev-config))

(om/set-context! (dev/context system))

(def test-user "garygeeke")

(def metadata-collection-id
  (-> system :dev/repository-config :metadata-collection-id))

(def metadata-collection-name
  (-> system :dev/repository-config :metadata-collection-name))

(def server-name
  (-> system :dev/repository-config :server-name))

;; Easy reference to the metadata collection test subject
(def metadata-collection
  (dev/metadata-collection system))

;; We will test metadata collection entity lifecycle operations with CSVFile metadata
(def csvfile-type-def (om/find-type-def-by-name "CSVFile"))

;; Prepares initial values of entity metadata
(def cvsfile-v1-properties-map
  {:openmetadata.Referenceable/qualifiedName "/omrs/test-file.csv"
   :openmetadata.DataFile/fileType "csv"
   :openmetadata.CSVFile/delimiterCharacter ","
   :openmetadata.CSVFile/quoteCharacter "\""})

;; Converts CSVFile properties map into InstanceProperties for addEntity()
(def csvfile-v1-instance-properties
  (om-datafy/map->InstanceProperties csvfile-type-def cvsfile-v1-properties-map))

;; Invoke addEntity() and save the returned EntityDetails
(def add-entity-result
  (.addEntity metadata-collection
    test-user
    (:openmetadata.TypeDef/guid csvfile-type-def)
    csvfile-v1-instance-properties
    []
    InstanceStatus/ACTIVE))

(fact "addEntity() returns EntityDetail"
  (type add-entity-result) => EntityDetail)

(fact "add entity result version equals 1"
  (.getVersion add-entity-result) => 1)

(fact "add entity result created by"
  (.getCreatedBy add-entity-result) => test-user)

(fact "add entity result create time"
  (type (.getCreateTime add-entity-result)) => Date)

;; Save the GUID of newly added Entity for later tests
(def csvfile-guid (.getGUID add-entity-result))

(fact "add entity result has a GUID"
  csvfile-guid = =test=> (fn [x] (UUID/fromString x)))

;; Invoke isEntityKnown() with the newly created entity GUID
(def is-entity-known-result
  (.isEntityKnown metadata-collection
    test-user
    csvfile-guid))

(fact "isEntityKnwon() returns EntityDetail"
  (type is-entity-known-result) => EntityDetail)

;; Invoke getEntityDetail() with the same GUID
(def get-entity-result
  (.getEntityDetail metadata-collection
    test-user
    csvfile-guid))

(fact "getEntityDetail() returns EntityDetail"
  (type get-entity-result) => EntityDetail)

(fact "addEntity() getEntityDetail() and isEntityKnown() results should equals"
  (= add-entity-result is-entity-known-result get-entity-result) => truthy)

;; Save get entity result as version 1
(def csvfile-entity-v1 get-entity-result)

;; Prepare instance properties to add description to csvfile metadata
(def cvsfile-v2-properties-map
  {:openmetadata.Referenceable/qualifiedName "/omrs/test-file.csv"
   :openmetadata.DataFile/fileType "csv"
   :openmetadata.CSVFile/delimiterCharacter ","
   :openmetadata.CSVFile/quoteCharacter "\""
   :openmetadata.Asset/description "csvfile description v2"})

(def csvfile-v2-instance-properties
  (om-datafy/map->InstanceProperties csvfile-type-def cvsfile-v2-properties-map))

;; Invoke updateEntityProperties()

(def update-entity-properties-result
  (.updateEntityProperties metadata-collection
    test-user
    csvfile-guid
    csvfile-v2-instance-properties))

(fact "updateEntityProperties() returns EntityDetail"
  (type update-entity-properties-result) => EntityDetail)

(fact "entity version is now 2"
  (.getVersion update-entity-properties-result) => 2)

(fact "entity description is updated"
  (some-> update-entity-properties-result
    .getProperties
    .getInstanceProperties
    (.get "description")
    .valueAsString)
  => "csvfile description v2")

(->> (om/list-type-defs)
  #_(filter #(< 2 (count (:openmetadata.TypeDef/validInstanceStatusList %))))
  (map #(select-keys % [#_:openmetadata.TypeDef/validInstanceStatusList
                        :openmetadata.TypeDef/name
                        :openmetadata.TypeDef/description])))




#_(def anchor-classification
  (-> (om/skeleton-classification
        {:user-name                test-user
         :type-name                "Anchors"
         :metadata-collection-id   metadata-collection-id
         :metadata-collection-name metadata-collection-name
         :instance-provenance-type "LOCAL_COHORT"
         })
    (assoc :openmetadata.Anchors/anchorGUID (str (UUID/randomUUID)))))

#_(def confidence-classification
  (-> (om/skeleton-classification
        {:user-naem                test-user
         :type-name                "Confidence"
         :metadata-collection-id   metadata-collection-id
         :metadata-collection-name metadata-collection-name
         :instance-provenance-type "LOCAL_COHORT"})
    (assoc :openmetadata.Confidence/confidence 39)))


(om/set-context! nil)

(ig/halt! system)