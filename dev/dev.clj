(ns dev
  (:refer-clojure)
  (:require [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]
            [io.kosong.egeria.omrs :as omrs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [xtdb.rocksdb]
            [xtdb.api :as xtdb])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceProperties PrimitivePropertyValue)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs PrimitiveDefCategory)
           (io.kosong.egeria.omrs.xtdb XtdbOMRSRepositoryConnector)
           (java.util Collections)
           (java.nio.file Path Paths)
           (java.net URI)
           (org.odpi.openmetadata.frameworks.connectors.properties ConnectionProperties)
           (org.odpi.openmetadata.frameworks.connectors.properties.beans Connection)))


(def metadata-collection-id "b2718e10-9aa0-4944-8849-e856959cbbaa")

(defmethod ig/init-key ::openmetadata-archive [_ _]
  (omrs/->openmetadata-types-archive))

(defmethod ig/init-key ::audit-log-store
  [_ _]
  (omrs/->console-audit-log-store))

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

(defmethod ig/init-key ::xtdb-connection [_ {:keys [configuration-properties]}]
  (doto (Connection.)
    (.setConfigurationProperties configuration-properties)))

(defmethod ig/init-key ::xtdb-repository-connector [_ {:keys [connection
                                                              repository-name
                                                              metadata-collection-id
                                                              repository-validator
                                                              repository-helper
                                                              audit-log]}]
  (let [connection-props (ConnectionProperties. connection)
        instance-id      (str (java.util.UUID/randomUUID))]
    (println connection)
    (doto (XtdbOMRSRepositoryConnector.)
      (.setRepositoryName repository-name)
      (.setRepositoryHelper repository-helper)
      (.setRepositoryValidator repository-validator)
      (.setAuditLog audit-log)
      (.setMetadataCollectionId metadata-collection-id)
      (.initialize instance-id connection-props)
      (.start))))

(defmethod ig/halt-key! ::xtdb-repository-connector [_ connector]
  (.disconnect connector))

(defmethod ig/init-key ::xtdb-node [_ xtdb-config]
  (xtdb/start-node xtdb-config))

(def db-dir (str "file://" (System/getProperty "user.dir") "/data/rocksdb"))

(def xtdb-config
  {::xtdb-node
   {:my-rocksdb          {:xtdb/module xtdb.rocksdb/->kv-store
                          :db-dir      (Paths/get (URI. db-dir))}
    :xtdb/index-store    {:kv-store :my-rocksdb}
    :xtdb/tx-log         {:kv-store :my-rocksdb}
    :xtdb/document-store {:kv-store :my-rocksdb}}})


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
                                 :audit-log (ig/ref ::audit-log)}

   ::repository-helper          {:content-manager (ig/ref ::repository-content-manager)}

   ::repository-validator       {:content-manager (ig/ref ::repository-content-manager)}

   ::xtdb-connection            {:configuration-properties {"xtdbConfigPath" "dev-resources/xtdb-node.edn"}}

   ::xtdb-repository-connector  {:connection             (ig/ref ::xtdb-connection)
                                 :repository-name        "dev"
                                 :metadata-collection-id "b2718e10-9aa0-4944-8849-e856959cbbaa"
                                 :repository-helper      (ig/ref ::repository-helper)
                                 :repository-validator   (ig/ref ::repository-validator)
                                 :audit-log              (ig/ref ::audit-log)
                                 :config-properties      {"xtdbConfig" "data/xtdb-node.edn"}}})

(integrant.repl/set-prep! (constantly egeria-config))

(defn dataset-instance []
  (let [instance-props (doto (InstanceProperties.)
                         (.setProperty "name" (doto (PrimitivePropertyValue.)
                                                (.setPrimitiveDefCategory PrimitiveDefCategory/OM_PRIMITIVE_TYPE_STRING)
                                                (.setPrimitiveValue "iris"))))]
    (omrs/->EntityDetail
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
    (omrs/->EntityDetail
      "repl"
      metadata-collection-id
      "alice"
      "CSVFile"
      instance-props
      Collections/EMPTY_LIST)))

(defn repo-helper []
  (::repository-helper integrant.repl.state/system))

(defn connector []
  (::xtdb-repository-connector integrant.repl.state/system))

(defn metadata-collection []
  (some-> (connector)
    (.getMetadataCollection)))

(defn xtdb-node []
  (some-> (connector)
    (.getXtdbNode)
    :node))

(defn load-type-defs
  []
  (let [user-id              "garygeek"
        metadata-collection  (metadata-collection)
        archive              (::openmetadata-archive integrant.repl.state/system)
        repo-content-manager (::repository-content-manager integrant.repl.state/system)
        archive-type-store   (.getArchiveTypeStore archive)
        archive-guid         (some-> (.getArchiveProperties archive)
                               (.getArchiveGUID))]
    (.setOpenMetadataTypesOriginGUID repo-content-manager archive-guid)
    (doseq [attribute-type-def (.getAttributeTypeDefs archive-type-store)]
      (.addAttributeTypeDef metadata-collection user-id attribute-type-def))
    (doseq [type-def (.getNewTypeDefs archive-type-store)]
      (.addTypeDef metadata-collection user-id type-def))
    (doseq [^TypeDefPatch patch (.getTypeDefPatches archive-type-store)]
      (.updateTypeDef metadata-collection user-id patch))))


(defn load-entity-detail [edn]
  (->> (io/resource edn)
    (slurp)
    (edn/read-string)
    (omrs/map->EntityDetail)))
