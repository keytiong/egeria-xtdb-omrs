(ns dev
  (:require [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]
            [io.kosong.egeria.omrs :as omrs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [crux.rocksdb]
            [crux.api :as crux])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceProperties PrimitivePropertyValue)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs PrimitiveDefCategory)
           (java.util Collections)
           (java.nio.file Path Paths)
           (java.net URI)))

(import io.kosong.egeria.omrs.CruxOMRSRepositoryConnector)

(def metadata-collection-id "b2718e10-9aa0-4944-8849-e856959cbbaa")

(def ^:dynamic *repo-helper*
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
        openmetadata-archive  (omrs/->openmetadata-archive)
        content-manager       (omrs/->repository-content-manager
                                {:user-id   user-id
                                 :audit-log audit-log
                                 :archive   openmetadata-archive})
        repo-helper           (omrs/->repository-helper
                                {:content-manager content-manager})]
    repo-helper))




(defmethod ig/init-key ::openmetadata-archive [_ _]
  (omrs/->openmetadata-archive))

(defmethod ig/init-key ::audit-log-store
  [_ _]
  (omrs/->audit-log-store))

(defmethod ig/init-key ::audit-log-destination
  [_ config]
  (omrs/->audit-log-destination config))

(defmethod ig/init-key ::audit-log
  [_ config]
  (omrs/->audit-log config))

(defmethod ig/init-key ::repository-content-manager
  [_ config]
  (omrs/->repository-content-manager config))

(defmethod ig/init-key ::repository-helper [_ config]
  (omrs/->repository-helper config))

(defmethod ig/init-key ::repository-validator [_ config]
  (omrs/->repository-validator config))

(defmethod ig/init-key ::crux-repository-connector [_ {:keys [metadata-collection-id
                                                              repository-validator
                                                              repository-helper
                                                              audit-log]}]
  (doto (CruxOMRSRepositoryConnector.)
    (.setRepositoryHelper repository-helper)
    (.setRepositoryValidator repository-validator)
    (.setAuditLog audit-log)
    (.setMetadataCollectionId metadata-collection-id)
    (.start)))

(defmethod ig/halt-key! ::crux-repository-connector [_ connector]
  (.disconnect connector))

(defmethod ig/init-key ::crux-node [_ crux-config]
  (crux/start-node crux-config))

(def db-dir (str "file://" (System/getProperty "user.dir") "/data/rocksdb"))

(def crux-config
  {::crux-node
   {:my-rocksdb          {:crux/module crux.rocksdb/->kv-store
                          :db-dir      (Paths/get (URI. db-dir))}
    :crux/index-store    {:kv-store :my-rocksdb}
    :crux/tx-log         {:kv-store :my-rocksdb}
    :crux/document-store {:kv-store :my-rocksdb}}})


(def egeria-config
  {::openmetadata-archive       {}

   ::audit-log-store            {}

   ::audit-log-destination      {:server-name      "my-server-name"
                                 :server-type      "my-server-type"
                                 :organization     "my-organization"
                                 :audit-log-stores [(ig/ref ::audit-log-store)]}

   ::audit-log                  {:destination           (ig/ref ::audit-log-destination)
                                 :component-id          1
                                 :component-name        "my-component-name"
                                 :component-description "my-component-description"
                                 :component-wiki-url    "my-component-wiki-url"}

   ::repository-content-manager {:user-id   "alice"
                                 :audit-log (ig/ref ::audit-log)
                                 :archive   (ig/ref ::openmetadata-archive)}

   ::repository-helper          {:content-manager (ig/ref ::repository-content-manager)}

   ::repository-validator       {:content-manager (ig/ref ::repository-content-manager)}

   ::crux-repository-connector  {:metadata-collection-id "b2718e10-9aa0-4944-8849-e856959cbbaa"
                                 :repository-helper      (ig/ref ::repository-helper)
                                 :repository-validator   (ig/ref ::repository-validator)
                                 :audit-log              (ig/ref ::audit-log)}})

(integrant.repl/set-prep! (constantly crux-config))

(defn dataset-instance []
  (let [instance-props (doto (InstanceProperties.)
                         (.setProperty "name" (doto (PrimitivePropertyValue.)
                                                (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
                                                (.setPrimitiveValue "iris"))))]
    (omrs/->EntityDetail *repo-helper*
      "repl"
      metadata-collection-id
      "alice"
      "DataSet"
      instance-props
      Collections/EMPTY_LIST)))

(defn csv-file-instance []
  (let [instance-props (doto (InstanceProperties.)
                         (.setProperty "name" (doto (PrimitivePropertyValue.)
                                                (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
                                                (.setPrimitiveValue "Week 1: Drop Foot Clinical Trial Measurements")))
                         (.setProperty "description" (doto (PrimitivePropertyValue.)
                                                       (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
                                                       (.setPrimitiveValue "One week's data covering foot angle, hip displacement and mobility measurements.")))
                         (.setProperty "qualifiedName" (doto (PrimitivePropertyValue.)
                                                         (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
                                                         (.setPrimitiveValue "file://secured/research/clinical-trials/drop-foot/DropFootMeasurementsWeek1.csv")))
                         (.setProperty "fileType" (doto (PrimitivePropertyValue.)
                                                    (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
                                                    (.setPrimitiveValue "csv")))
                         (.setProperty "delimiterCharacter" (doto (PrimitivePropertyValue.)
                                                              (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_CHAR)
                                                              (.setPrimitiveValue \,)))
                         (.setProperty "quoteCharacter" (doto (PrimitivePropertyValue.)
                                                          (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_CHAR)
                                                          (.setPrimitiveValue \")))
                         )]
    (omrs/->EntityDetail *repo-helper*
      "repl"
      metadata-collection-id
      "alice"
      "CSVFile"
      instance-props
      Collections/EMPTY_LIST)))

(defn crux-node []
  (::crux-node integrant.repl.state/system))

(defn metadata-collection []
  (some->
    (::crux-repository-connector integrant.repl.state/system)
    (.getMetadataCollection)))


(defn load-entity-detail [edn]
  (->> (io/resource edn)
    (slurp)
    (edn/read-string)
    (omrs/map->EntityDetail *repo-helper*)))
