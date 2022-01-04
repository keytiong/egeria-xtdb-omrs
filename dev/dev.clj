(ns dev
  (:refer-clojure)
  (:require [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]
            [io.kosong.egeria.omrs :as om]
            [clojure.tools.logging :as log])
  (:import
    (io.kosong.egeria.omrs.xtdb XtdbOMRSRepositoryConnectorProvider)
    (org.odpi.openmetadata.frameworks.connectors.properties ConnectionProperties)
    (org.odpi.openmetadata.frameworks.connectors.properties.beans Connection ConnectorType)
    (org.odpi.openmetadata.repositoryservices.localrepository.repositoryconnector LocalOMRSConnectorProvider LocalOMRSRepositoryConnector)
    (org.odpi.openmetadata.adminservices.configuration.properties OpenMetadataExchangeRule)
    (org.odpi.openmetadata.repositoryservices.eventmanagement OMRSRepositoryEventExchangeRule OMRSRepositoryEventManager)
    (org.odpi.openmetadata.repositoryservices.auditlog OMRSAuditLog OMRSAuditLogDestination)
    (java.util UUID)
    (org.odpi.openmetadata.repositoryservices.localrepository.repositorycontentmanager OMRSRepositoryContentHelper OMRSRepositoryContentValidator OMRSRepositoryContentManager)
    (org.odpi.openmetadata.repositoryservices.archivemanager OMRSArchiveManager)
    (org.odpi.openmetadata.frameworks.connectors ConnectorBroker)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.auditlogstore OMRSAuditLogStore OMRSAuditLogRecord OMRSAuditLogStoreConnectorBase)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector OMRSRepositoryConnector)
    (org.odpi.openmetadata.frameworks.auditlog AuditLog)))

;;
;; Hack to access private OMRSContentManager memeber in OMRSRepositoryHelper
;;
(defn- real-local-connector [local-connector]
  (let [m (.. local-connector getClass (getDeclaredField "realLocalConnector"))]
    (.setAccessible m true)
    (.get m local-connector)))

(defmethod ig/init-key ::server-config
  [_ config]
  config)

(defmethod ig/init-key ::repository-config
  [_ config]
  config)

(defmethod ig/init-key ::archive-manager
  [_ {:keys [repository-config
             archive-connectors
             audit-log
             local-repository-connector
             repository-content-manager]}]
  (let [metadata-collection-id   (:metadata-collection-id repository-config)
        instance-event-processor (some-> local-repository-connector
                                   .getIncomingInstanceEventProcessor)
        archive-manager          (OMRSArchiveManager. nil audit-log)]
    (try
      (.setLocalRepository archive-manager
        metadata-collection-id repository-content-manager instance-event-processor)
      (catch Throwable t
        (println t)))
    archive-manager))

(defmethod ig/init-key ::audit-log-store
  [_ _]
  #_(ConsoleAuditLogStoreConnector.)
  (reify
    OMRSAuditLogStore
    (^String storeLogRecord [_ ^OMRSAuditLogRecord logRecord]
      (let [log-record (bean logRecord)]
        ;;(log/info log-record)
        (:GUID logRecord)))))

(defmethod ig/init-key ::audit-log-destination
  [_ {:keys [server-config repository-config audit-log-stores]}]
  (let [{:keys [organization
                server-name
                server-type]} server-config
        {:keys [metadata-collection-id]} repository-config]
    (doto (OMRSAuditLogDestination. server-name server-type organization audit-log-stores)
      (.setLocalMetadataCollectionId metadata-collection-id))))

(defmethod ig/init-key ::audit-log
  [_ {:keys [audit-log-destination component-id component-name component-description component-wiki-url]}]
  (OMRSAuditLog. audit-log-destination
    component-id
    component-name
    component-description
    component-wiki-url))

(defmethod ig/init-key ::repository-content-manager
  [_ {:keys [server-config audit-log]}]
  (let [server-user-id (:server-user-id server-config)]
    (OMRSRepositoryContentManager. server-user-id audit-log)))

(defmethod ig/init-key ::repository-content-helper
  [_ {:keys [repository-content-manager]}]
  (OMRSRepositoryContentHelper. repository-content-manager))

(defmethod ig/init-key ::repository-content-validator
  [_ {:keys [repository-content-manager]}]
  (OMRSRepositoryContentValidator. repository-content-manager))

(defmethod ig/init-key ::repository-event-manager
  [_ {:keys [name repository-content-validator audit-log]}]
  (let [exchange-rule (OMRSRepositoryEventExchangeRule. OpenMetadataExchangeRule/ALL [])
        event-manager (OMRSRepositoryEventManager. name
                        exchange-rule
                        repository-content-validator
                        audit-log)]
    event-manager))

(defmethod ig/init-key ::xtdb-repository-connection
  [_ {:keys [configuration-properties]}]
  (let [provider-class (.getName XtdbOMRSRepositoryConnectorProvider)
        connector-type (doto (ConnectorType.)
                         (.setConnectorProviderClassName provider-class))
        connection     (doto (Connection.)
                         (.setConnectorType connector-type)
                         (.setConfigurationProperties configuration-properties))]
    connection))

(defmethod ig/init-key ::xtdb-repository-connector
  [_ {:keys [connection
             repository-config
             repository-content-validator
             repository-content-helper
             audit-log]}]
  (let [{:keys [metadata-collection-id
                metadata-collection-name
                repository-name]} repository-config
        broker           (ConnectorBroker. audit-log)
        connector        (.getConnector broker ^Connection connection)
        connection-props (ConnectionProperties. ^Connection connection)
        instance-id      (str (UUID/randomUUID))]
    (doto ^OMRSRepositoryConnector connector
      (.setRepositoryName repository-name)
      (.setRepositoryHelper repository-content-helper)
      (.setRepositoryValidator repository-content-validator)
      (.setAuditLog audit-log)
      (.setMetadataCollectionId metadata-collection-id)
      (.setMetadataCollectionName metadata-collection-name)
      (.initialize instance-id connection-props)
      (.start))))

(defmethod ig/init-key ::local-repository-connector
  [_ {:keys [server-config repository-config connection repository-event-manager repository-content-manager
             repository-content-helper repository-content-validator audit-log]}]
  (let [{:keys [server-name
                server-type
                server-user-id]} server-config
        {:keys [metadata-collection-id
                metadata-collection-name
                repository-name]} repository-config
        provider  (LocalOMRSConnectorProvider.
                    metadata-collection-id
                    connection
                    nil
                    repository-event-manager
                    repository-content-manager
                    (OMRSRepositoryEventExchangeRule. OpenMetadataExchangeRule/ALL []))
        connector (.getConnector provider ^Connection connection)]
    (doto ^LocalOMRSRepositoryConnector connector
      (.setRepositoryHelper ^OMRSRepositoryContentHelper repository-content-helper)
      (.setRepositoryValidator ^OMRSRepositoryContentValidator repository-content-validator)
      (.setRepositoryName repository-name)
      (.setServerName server-name)
      (.setServerType server-type)
      (.setServerUserId server-user-id)
      (.setAuditLog ^OMRSAuditLog audit-log))
    (doto repository-content-manager
      (.setupEventProcessor connector repository-event-manager))
    (doto ^LocalOMRSRepositoryConnector connector
      (.setMetadataCollectionName metadata-collection-name)
      (.setMetadataCollectionId metadata-collection-id))
    (.start connector)
    connector))

(defmethod ig/init-key ::xtdb-metadata-collection
  [_ {:keys [connector]}]
  (some-> connector
    real-local-connector
    .getMetadataCollection))

(defmethod ig/init-key ::xtdb-node
  [_ {:keys [connector]}]
  (some-> connector
    real-local-connector
    .getXtdbNode
    :node))

(defmethod ig/init-key ::context
  [_ {:keys [type-store instance-store]}]
  (om/->context {:type-store     type-store
                 :instance-store instance-store}))

(defmethod ig/halt-key! ::archive-manager [_ archive-manager]
  (.close archive-manager))

(defmethod ig/halt-key! ::xtdb-repository-connector [_ connector]
  (.disconnect connector))

(defmethod ig/halt-key! ::local-repository-connector [_ connector]
  (.disconnect connector))

(def dev-config
  {
   ::server-config                {:server-name    "Developer local server"
                                   :server-user-id "system"
                                   :server-type    "Open Metadata and Governance Server"
                                   :organization   "Egeria"}

   ::repository-config            {:metadata-collection-id   "b2718e10-9aa0-4944-8849-e856959cbbaa"
                                   :metadata-collection-name "Dev Metadata Collection"
                                   :repository-name          "Dev Metadata Repository"}

   ::audit-log-store              {}

   ::audit-log-destination        {:server-config     (ig/ref ::server-config)
                                   :repository-config (ig/ref ::repository-config)
                                   :audit-log-stores  [(ig/ref ::audit-log-store)]}

   ::audit-log                    {:destination           (ig/ref ::audit-log-destination)
                                   :component-id          1
                                   :component-name        "XTDB OMRS Repository"
                                   :component-description "XTDB OMRS Repository"
                                   :component-wiki-url    "http://github.com/keytiong"}

   ::repository-content-manager   {:server-config (ig/ref ::server-config)
                                   :audit-log     (ig/ref ::audit-log)}

   ::repository-content-helper    {:repository-content-manager (ig/ref ::repository-content-manager)}

   ::repository-content-validator {:repository-content-manager (ig/ref ::repository-content-manager)}

   ::repository-event-manager     {:name                         "Local Repository Outbound"
                                   :repository-content-validator (ig/ref ::repository-content-validator)
                                   :audit-log                    (ig/ref ::audit-log)}

   ::xtdb-repository-connection   {:configuration-properties {"xtdbConfigPath" nil}}

   #_::xtdb-repository-connector    #_{:connection                   (ig/ref ::xtdb-repository-connection)
                                       :repository-config            (ig/rref ::repository-config)
                                       :repository-content-validator (ig/ref ::repository-content-validator)
                                       :repository-content-helper    (ig/ref ::repository-content-helper)
                                       :audit-log                    (ig/ref ::audit-log)}

   ::xtdb-metadata-collection     {:connector (ig/ref ::local-repository-connector)}

   ::xtdb-node                    {:connector (ig/ref ::local-repository-connector)}

   ::local-repository-connector   {:server-config                (ig/ref ::server-config)
                                   :repository-config            (ig/ref ::repository-config)
                                   :connection                   (ig/ref ::xtdb-repository-connection)
                                   :repository-event-manager     (ig/ref ::repository-event-manager)
                                   :repository-content-manager   (ig/ref ::repository-content-manager)
                                   :repository-content-helper    (ig/ref ::repository-content-helper)
                                   :repository-content-validator (ig/ref ::repository-content-validator)
                                   :audit-log                    (ig/ref ::audit-log)}

   ::archive-manager              {:repository-config          (ig/ref ::repository-config)
                                   :archive-connectors         []
                                   :repository-content-manager (ig/ref ::repository-content-manager)
                                   :local-repository-connector (ig/ref ::local-repository-connector)
                                   :audit-log                  (ig/ref ::audit-log)}

   ::context                      {:type-store     (ig/ref ::repository-content-helper)
                                   :instance-store (ig/ref ::xtdb-node)}})

(defn repository-content-helper
  ([]
   (repository-content-helper integrant.repl.state/system))
  ([system]
   (::repository-content-helper system)))

(defn local-repository-connector
  ([]
   (local-repository-connector integrant.repl.state/system))
  ([system]
   (::local-repository-connector system)))

(defn xtdb-node
  ([]
   (xtdb-node integrant.repl.state/system))
  ([system]
   (::xtdb-node system)))

(defn metadata-collection
  ([]
   (metadata-collection integrant.repl.state/system))
  ([system]
   (::xtdb-metadata-collection system)))

(defn context
  ([]
   (context integrant.repl.state/system))
  ([system]
   (::context system)))

(defn load-openmetadata-types
  [system]
  (let [metadata-collection-id     (some-> system
                                     ::repository-config
                                     :metadata-collection-id)
        repository-content-manager (::repository-content-manager system)
        local-repository-connector (local-repository-connector system)
        instance-event-processor   (.getIncomingInstanceEventProcessor local-repository-connector)
        audit-log                  (some-> system ::audit-log)
        archive-manager            (OMRSArchiveManager. [] audit-log)]
    (.setLocalRepository archive-manager
      metadata-collection-id
      repository-content-manager
      instance-event-processor)))