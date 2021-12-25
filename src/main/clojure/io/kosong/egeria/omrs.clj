(ns io.kosong.egeria.omrs
  (:require [clojure.string :as str]
            [clojure.core.protocols :as p]
            [clojure.datafy :refer [datafy]]
            [clojure.tools.logging :as log])
  (:import (java.util LinkedList UUID)
           (org.odpi.openmetadata.repositoryservices.localrepository.repositorycontentmanager OMRSRepositoryContentHelper OMRSRepositoryContentManager OMRSRepositoryContentValidator)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDefPatch)
           (org.odpi.openmetadata.opentypes OpenMetadataTypesArchive)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.auditlogstore OMRSAuditLogRecord OMRSAuditLogStore)
           (org.odpi.openmetadata.repositoryservices.auditlog OMRSAuditLogDestination OMRSAuditLog)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.archivestore.properties OpenMetadataArchiveTypeStore OpenMetadataArchive)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.utilities OMRSRepositoryPropertiesUtilities)))

(defonce ^:dynamic *repo-helper* nil)

(defn set-repo-helper! [repo-helper]
  (alter-var-root #'*repo-helper* (constantly repo-helper)))

(defn find-type-def-by-guid
  ([type-def-guid]
   (find-type-def-by-guid *repo-helper* type-def-guid))
  ([^OMRSRepositoryContentHelper repo-helper type-def-guid]
   (if-let [type-def (.getTypeDef repo-helper "omrs" "guid" type-def-guid "find-type-def-by-guid")]
     (datafy type-def))))

(defn find-type-def-by-name
  ([type-def-name]
   (find-type-def-by-name *repo-helper* type-def-name))
  ([^OMRSRepositoryContentHelper repo-helper type-def-name]
   (if-let [type-def (.getTypeDefByName repo-helper "omrs" type-def-name)]
     (datafy type-def))))

(defn find-attribute-type-def-by-guid
  ([attribute-type-guid]
   (find-attribute-type-def-by-guid *repo-helper* attribute-type-guid))
  ([^OMRSRepositoryContentHelper repo-helper attribute-type-guid]
   (datafy (.getAttributeTypeDef repo-helper
             "local"
             attribute-type-guid
             "find-attribute-type-def-by-guid"))))

(defn find-type-def-ancestors
  ([type-def]
   (loop [as [] t type-def]
     (let [parent-type-def (some-> t
                             :openmetadata.TypeDef/superType
                             find-type-def-by-guid)]
       (if parent-type-def
         (recur (conj as parent-type-def) parent-type-def)
         as)))))

(defn list-type-def-attributes
  "Resolves properties of the given type-def and its super type. Returns a sequence of type def attributes."
  ([type-def]
   (list-type-def-attributes *repo-helper* type-def))
  ([^OMRSRepositoryContentHelper repo-helper type-def]
   (let [type-def-guid   (:openmetadata.TypeDef/guid type-def)
         type-def-name   (:openmetadata.TypeDef/name type-def)
         super-type-guid (:openmetadata.TypeDef/superType type-def)
         super-type-def  (when super-type-guid
                           (find-type-def-by-guid repo-helper super-type-guid))
         attrs           (->> (:openmetadata.TypeDef/propertiesDefinition type-def)
                           (map #(assoc % :openmetadata.TypeDef/name type-def-name))
                           (map #(assoc % :openmetadata.TypeDef/guid type-def-guid)))]
     (if super-type-def
       (concat (list-type-def-attributes repo-helper super-type-def) attrs)
       attrs))))

(defn find-entity-by-guid [guid]
  nil)

(defn qualify-property-key [attr]
  (let [ns   (str "openmetadata." (:openmetadata.TypeDef/name attr))
        name (:openmetadata.TypeDefAttribute/attributeName attr)]
    (keyword ns name)))

(defn list-type-def-property-keys
  ([type-def]
   (list-type-def-property-keys *repo-helper* type-def))
  ([^OMRSRepositoryContentHelper repo-helper type-def]
   (let [attrs (list-type-def-attributes repo-helper type-def)]
     (map qualify-property-key attrs))))

(defn list-type-defs
  ([]
   (list-type-defs *repo-helper*))
  ([^OMRSRepositoryContentHelper repo-helper]
   (map datafy (.getKnownTypeDefs repo-helper))))

(def find-enum-element-def
  (memoize
    (fn [enum-type-def enum-name]
      (some->> (:openmetadata.EnumDef/elementDefs enum-type-def)
        (filter (fn [x] (= (:openmetadata.EnumElementDef/value x) enum-name)))
        (first)))))

(defn ^OpenMetadataArchive ->openmetadata-types-archive
  []
  (-> (OpenMetadataTypesArchive.)
    (.getOpenMetadataArchive)))

(defn ->console-audit-log-store
  []
  (proxy [OMRSAuditLogStore] []
    (storeLogRecord [^OMRSAuditLogRecord record]
      (log/info record))))

(defn ->null-audit-log-store
  []
  (proxy [OMRSAuditLogStore] []))

(defn ->audit-log-destination
  [{:keys [audit-log-stores
           server-name
           server-type
           organization]}]
  (OMRSAuditLogDestination. server-name server-type organization (LinkedList. audit-log-stores)))

(defn ->audit-log
  [{:keys [audit-log-destination
           component-id
           component-name
           component-description
           component-wiki-url]}]
  (OMRSAuditLog. audit-log-destination
    component-id
    component-name
    component-description
    component-wiki-url))

(defn ->repository-validator
  [{:keys [content-manager]}]
  (OMRSRepositoryContentValidator. content-manager))

(defn ->repository-helper
  [{:keys [content-manager]}]
  (let [helper (OMRSRepositoryContentHelper. content-manager)]
    helper))

(defn init-repo-content-manager
  [^OMRSRepositoryContentManager repo-content-manager archive user-id]
  (let [archive-type-store (.getArchiveTypeStore archive)
        repo-util          (OMRSRepositoryPropertiesUtilities.)
        archive-guid       (some-> (.getArchiveProperties archive)
                             (.getArchiveGUID))
        name-type-def-map  (transient {})]
    (.setOpenMetadataTypesOriginGUID repo-content-manager archive-guid)
    (doseq [attribute-type-def (.getAttributeTypeDefs archive-type-store)]
      (.addAttributeTypeDef repo-content-manager user-id attribute-type-def))
    (doseq [type-def (.getNewTypeDefs archive-type-store)]
      (.addTypeDef repo-content-manager user-id type-def)
      (assoc! name-type-def-map (.getName type-def) type-def))
    (doseq [^TypeDefPatch patch (.getTypeDefPatches archive-type-store)]
      (let [type-def-name     (.getTypeDefName patch)
            original-type-def (get name-type-def-map type-def-name)]
        (if original-type-def
          (let [updated-type-def (.applyPatch repo-util "" original-type-def patch "")]
            (assoc! name-type-def-map type-def-name updated-type-def)
            (.updateTypeDef repo-content-manager "" updated-type-def)))))))

(defn ^OMRSRepositoryContentManager ->repository-content-manager
  [{:keys [user-id
           audit-log]}]
  (OMRSRepositoryContentManager. user-id audit-log))
