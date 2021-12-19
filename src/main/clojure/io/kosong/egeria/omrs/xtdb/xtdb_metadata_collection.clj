(ns io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection
  (:require [clojure.datafy :refer [datafy]]
            [io.kosong.egeria.omrs :as omrs]
            [xtdb.api :as xt])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector OMRSRepositoryConnector OMRSRepositoryHelper OMRSRepositoryValidator)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDef TypeDefPatch AttributeTypeDef TypeDefSummary)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances EntityDetail InstanceProperties EntitySummary InstanceStatus EntityProxy ClassificationOrigin Classification Relationship)
           (java.util Date Collections List)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties HistorySequencingOrder SequencingOrder MatchCriteria)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.search SearchProperties SearchClassifications)
           (org.odpi.openmetadata.frameworks.auditlog AuditLog)))

;;
;; Hack to access private OMRSContentManager memeber in OMRSRepositoryHelper
;;
(defn- content-manager [repo-helper]
  (let [m (.. repo-helper getClass (getDeclaredField "repositoryContentManager"))]
    (.setAccessible m true)
    (.get m repo-helper)))

;;
;; Source: https://groups.google.com/g/clojure/c/-sypb2Djhio/m/r9AzRpwTgRkJ
;; Credit: John Harrop
;;

(defn- find-a-node [deps already-have-nodes]
  (some (fn [[k v]] (if (empty? (remove already-have-nodes v)) k)) deps))

(defn- order-nodes [deps]
  (loop [deps deps already-have-nodes #{} output []]
    (if (empty? deps)
      output
      (if-let [item (find-a-node deps already-have-nodes)]
        (recur
          (dissoc deps item)
          (conj already-have-nodes item)
          (conj output item))
        (throw (Exception. "Circular dependency."))))))

(defn- type-def-deps-graph [type-defs]
  (let [f (fn [m type-def]
            (let [n  (:openmetadata.TypeDef/guid type-def)
                  e  (:openmetadata.TypeDef/superType type-def)
                  es (or (get m n) #{})]
              (if e
                (assoc m n (conj es e))
                (assoc m n es))))]
    (reduce f {} type-defs)))

(defn- topology-sort [type-defs]
  (let [g            (type-def-deps-graph type-defs)
        sorted-guids (order-nodes g)
        type-def-map (reduce (fn [a type-def]
                               (let [k (:openmetadata.TypeDef/guid type-def)]
                                 (assoc a k type-def)))
                       {}
                       type-defs)]
    (reduce (fn [coll guid] (conj coll (type-def-map guid))) [] sorted-guids)))

(defn- random-uuid-str []
  (str (java.util.UUID/randomUUID)))

(defn- ensure-xt-id [doc]
  (if (:xt/id doc)
    doc
    (assoc doc :xt/id (random-uuid-str))))

(defn- same-document? [doc-1 doc-2]
  (= (dissoc doc-1 :xt/id) (dissoc doc-2 :xt/id)))

(defn- fetch-entity-by-key-attribute-value [db key-attribute key-value]
  (let [q   `{:find  [e]
              :where [[e ~key-attribute ~key-value]]}
        rs  (xt/q db q)
        eid (ffirst rs)]
    (when eid
      (xt/entity db eid))))

(defn fetch-type-def-by-guid [db guid]
  (fetch-entity-by-key-attribute-value db :openmetadata.TypeDef/guid guid))

(defn fetch-attribute-type-def-by-guid [db guid]
  (fetch-entity-by-key-attribute-value db :openmetadata.AttributeTypeDef/guid guid))

(defn- store-type-def [xtdb-node type-def]
  (let [type-def   (ensure-xt-id type-def)
        valid-time (or (:openmetadata.TypeDef/updateTime type-def)
                     (:openmetadata.TypeDef/createTime type-def)
                     (Date.))]
    (xt/submit-tx xtdb-node [[::xt/put type-def valid-time]])))

(defn- store-attribute-type-def [xtdb-node attribute-type-def]
  (let [attribute-type-def (ensure-xt-id attribute-type-def)]
    (xt/submit-tx xtdb-node [[::xt/put attribute-type-def]])))

(defn fetch-entities-by-key-attribute [db key-attribute]
  (let [q   `{:find  [e]
              :where [[e ~key-attribute]]}
        rs  (xt/q db q)
        ids (map first rs)]
    (map #(xt/entity db %) ids)))

(defn fetch-type-defs [db]
  (fetch-entities-by-key-attribute db :openmetadata.TypeDef/guid))

(defn fetch-attribute-type-defs [db]
  (fetch-entities-by-key-attribute db :openmetadata.AttributeTypeDef/guid))

(defn init-content-manager-from-store [metadata-collection]
  (let [{:keys [xtdb-node
                repository-content-manager
                repository-helper]} @(.state metadata-collection)
        db                       (xt/db xtdb-node)
        type-defs                (fetch-type-defs db)
        entity-type-defs         (->> type-defs
                                   (filter #(= (:openmetadata.TypeDef/category %) "ENTITY_DEF"))
                                   topology-sort)
        relationship-type-defs   (->> type-defs
                                   (filter #(= (:openmetadata.TypeDef/category %) "RELATIONSHIP_DEF"))
                                   topology-sort)
        classification-type-defs (->> type-defs
                                   (filter #(= (:openmetadata.TypeDef/category %) "CLASSIFICATION_DEF"))
                                   topology-sort)
        attr-type-defs           (fetch-attribute-type-defs db)]

    (binding [omrs/*repo-helper* repository-helper]
      (doseq [m attr-type-defs]
        (let [obj (omrs/map->AttributeTypeDef m)]
          (.addAttributeTypeDef repository-content-manager "init-content-manager" obj)))
      (doseq [m entity-type-defs]
        (let [obj (omrs/map->TypeDef m)]
          (.addTypeDef repository-content-manager "init-content-manager" obj)))
      (doseq [m relationship-type-defs]
        (let [obj (omrs/map->TypeDef m)]
          (.addTypeDef repository-content-manager "init-content-manager" obj)))
      (doseq [m classification-type-defs]
        (let [obj (omrs/map->TypeDef m)]
          (.addTypeDef repository-content-manager "init-content-manager" obj))))))

(gen-class
  :name io.kosong.egeria.omrs.xtdb.XtdbOMRSMetadataCollection
  :extends org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSDynamicTypeMetadataCollectionBase
  :init init
  :post-init post-init
  :state state
  :exposes-methods {setAuditLog                 superSetAuditLog
                    addTypeDef                  superAddTypeDef
                    updateTypeDef               superUpdateTypeDef
                    addAttributeTypeDef         superAddAttributeTypeDef
                    deleteTypeDef               superDeleteTypeDef
                    deleteAttribtueTypeDef      superDeleteAttributeTypeDef
                    reIdentifyTypeDef           superDeIdentifyTypeDef
                    reIdentifyAttribtueTypeDef  superDeIdentifyAttribtueTypeDef
                    reportTypeDefAlreadyDefined superReportTypeDefAlreadyDefined}
  :constructors {[io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector ;; parentConnector
                  String                                    ;; repositoryName
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper ;; repositoryHelper
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator ;; repositoryValidator
                  String                                    ;; metadataCollectionId
                  org.odpi.openmetadata.frameworks.auditlog.AuditLog
                  ]
                 [org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector ;; parentConnector
                  String                                    ;; repositoryName
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper ;; repositoryHelper
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator ;; repositoryValidator
                  String                                    ;; metadataCollectionId
                  ]})

(defn -init [^io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector connector
             ^String repositoryName
             ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper repositoryHelper
             ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator repositoryValidator
             ^String metadataCollectionId
             ^org.odpi.openmetadata.frameworks.auditlog.AuditLog auditLog]
  (let [state (atom {:repository-name            repositoryName
                     :metadata-collection-id     metadataCollectionId
                     :repository-helper          repositoryHelper
                     :repository-content-manager (content-manager repositoryHelper)
                     :xtdb-node                  (some-> (.getXtdbNode connector) :node)})]
    [[connector repositoryName repositoryHelper repositoryValidator metadataCollectionId] state]))

(defn -post-init [this
                  ^io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector connector
                  ^String repositoryName
                  ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper repositoryHelper
                  ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator repositoryValidator
                  ^String metadataCollectionId
                  ^AuditLog auditLog]
  (.superSetAuditLog this auditLog)
  (init-content-manager-from-store this))

(defn -addTypeDef [this
                   ^String userId
                   ^TypeDef newTypeDef]
  (let [{:keys [xtdb-node
                repository-content-manager
                repository-name]} @(.state this)
        incoming (datafy newTypeDef)]
    (.superAddTypeDef this userId newTypeDef)
    (store-type-def xtdb-node incoming)
    (.addTypeDef repository-content-manager repository-name newTypeDef)))

(defn -addAttributeTypeDef [this
                            ^String userId
                            ^AttributeTypeDef newAttributeTypeDef]
  (let [{:keys [xtdb-node
                repository-content-manager
                repository-name]} @(.state this)
        incoming (datafy newAttributeTypeDef)]
    (.superAddAttributeTypeDef this userId newAttributeTypeDef)
    (store-attribute-type-def xtdb-node incoming)
    (.addAttributeTypeDef repository-content-manager repository-name newAttributeTypeDef)))

(defn ^TypeDef -updateTypeDef [this
                               ^String userId
                               ^TypeDefPatch typeDefPatch]
  (let [{:keys [xtdb-node
                repository-content-manager
                repository-name]} @(.state this)
        type-def-obj (.superUpdateTypeDef this userId typeDefPatch)
        incoming     (datafy type-def-obj)]
    (store-type-def xtdb-node incoming)
    (.updateTypeDef repository-content-manager repository-name type-def-obj)
    type-def-obj))

(defn -deleteTypeDef [this
                      ^String userId
                      ^String obsoleteTypeDefGUID
                      ^String obsoleteTypeDefName]
  (.superDeleteTypeDef this userId obsoleteTypeDefGUID obsoleteTypeDefName))

(defn -deleteAttributeTypeDef [this
                               ^String userId
                               ^String obsoleteTypeDefGUID
                               ^String obsoleteTypeDefName]
  (.superDeleteAttributeTypeDef this userId obsoleteTypeDefGUID obsoleteTypeDefName))

(defn ^TypeDef -reIdentifyTypeDef [this
                                   ^String userId
                                   ^String originalTypeDefGUID
                                   ^String originalTypeDefName
                                   ^String newTypeDefGUID
                                   ^String newTypeDefName]
  (.superReIdentifyTypeDef this userId originalTypeDefGUID originalTypeDefName
    newTypeDefGUID newTypeDefName))

(defn ^AttributeTypeDef -reIdentifyAttributeTypeDef [this
                                                     ^String userId
                                                     ^String originalAttributeTypeDefGUID
                                                     ^String originalAttributeTypeDefName
                                                     ^String newAttributeTypeDefGUID
                                                     ^String newAttributeTypeDefName]
  (.superReIdentifyAttribtueTypeDef this userId originalAttributeTypeDefGUID
    originalAttributeTypeDefName newAttributeTypeDefGUID newAttributeTypeDefName))

(defn ^EntityDetail -isEntityKnown [this
                                    ^String userId
                                    ^String guid]
  nil)

(defn ^EntitySummary -getEntitySummary [this
                                        ^String userId
                                        ^String guid]
  nil)

(defn ^EntityDetail -getEntityDetail
  ([this ^String userId ^String guid]
   (-getEntityDetail this userId guid nil))
  ([this ^String userId ^String guid ^Date asOfTime]
   (let [valid-time (or asOfTime
                      (Date.))]
     nil)))

(defn -getEntityDetailHistory [this
                               ^String userId
                               ^String guid
                               ^Date fromTime
                               ^Date toTime
                               startFromElement
                               pageSize
                               ^HistorySequencingOrder sequencingOrder]
  (Collections/EMPTY_LIST))

(defn -getRelationshipsForEntity [this
                                  ^String userId
                                  ^String entityGUID
                                  ^String relationshipTypeGUID
                                  fromRelationshipElement
                                  ^List limitResultsByStatus
                                  ^Date asOfTime
                                  ^String sequencingProperty
                                  ^SequencingOrder sequencingOrder
                                  pageSize]
  (Collections/EMPTY_LIST))

(defn -findEntitiesByProperty [this
                               ^String userId
                               ^String entityTypeGUID
                               ^InstanceProperties matchProperties
                               ^MatchCriteria matchCriteria
                               fromEntityElement
                               ^List limitResultsByStatus
                               ^List limitResultsByClassification
                               ^Date asOfTime
                               ^String sequencingProperty
                               ^SequencingOrder sequencingOrder
                               pageSize]
  (Collections/EMPTY_LIST))

(defn -findEntities [this
                     ^String userId
                     ^String entityTypeGUID
                     ^List entitySubtypeGUIDs
                     ^SearchProperties matchProperties
                     fromEntityElement
                     ^List limitResultsByStatus
                     ^SearchClassifications matchClassifications
                     ^Date asOfTime
                     ^String sequencingProperty
                     ^SequencingOrder sequencingOrder
                     pageSize]
  (Collections/EMPTY_LIST))

(defn -findEntitiesByClassification [this
                                     ^String userId
                                     ^String entityTypeGUID
                                     ^String classificationName
                                     ^InstanceProperties matchClassificationProperties
                                     ^MatchCriteria matchCriteria
                                     fromEntityElement
                                     ^List limitResultsByStatus
                                     ^Date asOfTime
                                     ^String sequencingProperty
                                     ^SequencingOrder sequencingOrder
                                     pageSize]
  (Collections/EMPTY_LIST))

(defn -findEntitiesByPropertyValue [this
                                    ^String userId
                                    ^String entityTypeGUID
                                    ^String searchCriteria
                                    fromEntityElement
                                    ^List limitResultsByStatus
                                    ^List limitResultsByClassification
                                    ^Date asOfTime
                                    ^String sequencingProperty
                                    ^SequencingOrder sequencingOrder
                                    pageSize]
  (Collections/EMPTY_LIST))

(defn -isRelationshipKnown [this
                            ^String userId
                            ^String guid]
  nil)

(defn -getRelationship
  ([this ^String userId ^String guid]
   (-getRelationship this userId guid nil))
  ([this ^String userId ^String guid ^Date asOfTime]
   nil))

(defn -getRelationshipHistory [this
                               ^String userId
                               ^String guid
                               ^Date fromTime
                               ^Date toTime
                               startFromElement
                               pageSize
                               ^HistorySequencingOrder sequencingOrder]
  (Collections/EMPTY_LIST))

(defn -findRelationships [this
                          ^String userId
                          ^String relationshipTypeGUID
                          ^List relationshipSubtypeGUIDs
                          ^SearchProperties matchProperties
                          fromRelationshipElement
                          ^List limitResultsByStatus
                          ^Date asOfTime
                          ^String sequencingProperty
                          ^SequencingOrder sequencingOrder
                          pageSize]
  (Collections/EMPTY_LIST))

(defn -findRelationshipsByProperty [this
                                    ^String userId
                                    ^String relationshipTypeGUID
                                    ^InstanceProperties matchProperties
                                    ^MatchCriteria matchCriteria
                                    fromRelationshipElement
                                    ^List limitResultsByStatus
                                    ^Date asOfTime
                                    ^String sequencingProperty
                                    ^SequencingOrder sequencingOrder
                                    pageSize]
  (Collections/EMPTY_LIST))

(defn -findRelationshipsByPropertyValue [this
                                         ^String userId
                                         ^String relationshipTypeGUID
                                         ^String searchCriteria
                                         fromRelationshipElement
                                         ^List limitResultsByStatus
                                         ^Date asOfTime
                                         ^String sequencingProperty
                                         ^SequencingOrder sequencingOrder
                                         pageSize]
  (Collections/EMPTY_LIST))

(defn -getLinkingEntities [this
                           ^String userId
                           ^String startEntityGUID
                           ^String endEntityGUID
                           ^List limitResultsByStatus
                           ^Date asOfTime]
  (Collections/EMPTY_LIST))

(defn -getEntityNeighborhood [this
                              ^String userId
                              ^String entityGUID
                              ^List entityTypeGUIDs
                              ^List relationshipTypeGUIDs
                              ^List limitResultsByStatus
                              ^List limitResultsByClassification
                              ^Date asOfTime
                              level]
  nil)

(defn -getRelatedEntities [this
                           ^String userId
                           ^String startEntityGUID
                           ^List entityTypeGUIDs
                           fromEntityElement
                           ^List limitResultsByStatus
                           ^List limitResultsByClassification
                           ^Date asOfTime
                           ^String sequencingProperty
                           ^SequencingOrder sequencingOrder
                           pageSize]
  (Collections/EMPTY_LIST))

(defn -addEntity [this
                  ^String userId
                  ^String entityTypeGUID
                  ^InstanceProperties initialProperties
                  ^List initialClassifications
                  ^InstanceStatus initialStatus]
  nil)

(defn -addEntityProxy [this
                       ^String userId
                       ^EntityProxy entityProxy]
  nil)

(defn -updateEntityStatus [this
                           ^String userId
                           ^String entityGUID
                           ^InstanceStatus newStatus]
  nil)

(defn -updateEntityProperties [this
                               ^String userId
                               ^String entityGUID
                               ^InstanceProperties properties]
  nil)

(defn -undoEntityUpdate [this
                         ^String userId
                         ^String entityGUID]
  nil)

(defn -deleteEntity [this
                     ^String userId
                     ^String typeDefGUID
                     ^String typeDefName
                     ^String obsoleteEntityGUID]
  nil)

(defn -purgeEntity [this
                    ^String userId
                    ^String typeDefGUID
                    ^String typeDefName
                    ^String deletedEntityGUID]
  nil)

(defn -restoreEntity [this
                      ^String userId
                      ^String deletedEntityGUID]
  nil)

(defn -classifyEntity [this
                       ^String userId
                       ^String entityGUID
                       ^String classificationName
                       ^InstanceProperties classificationProperties]
  nil)

(defn -classifyEntity [this
                       ^String userId
                       ^String entityGUID
                       ^String classificationName
                       ^String externalSourceGUID
                       ^String externalSourceName
                       ^ClassificationOrigin classificationOrigin
                       ^String classificationOriginGUID
                       ^InstanceProperties classificationProperties]
  nil)

(defn -declassifyEntity [this
                         ^String userId
                         ^String entityGUID
                         ^String classificationName]
  nil)

(defn -updateEntityClassification [this
                                   ^String userId
                                   ^String entityGUID
                                   ^String classificationName
                                   ^InstanceProperties properties]
  nil)

(defn -addRelationship [this
                        ^String userId
                        ^String relationshipTypeGUID
                        ^InstanceProperties initialProperties
                        ^String entityOneGUID
                        ^String entityTwoGUID
                        ^InstanceStatus initialStatus]
  nil)

(defn -addExternalRelationship [this
                                ^String userId
                                ^String relationshipTypeGUID
                                ^String externalSourceGUID
                                ^String externalSourceName
                                ^InstanceProperties initialProperties
                                ^String entityOneGUID
                                ^String entityTwoGUID
                                ^InstanceStatus initialStatus]
  nil)

(defn -updateRelationshipStatus [this
                                 ^String userId
                                 ^String relationshipGUID
                                 ^InstanceStatus newStatus]
  nil)

(defn -updateRelationshipProperties [this
                                     ^String userId
                                     ^String relationshipGUID
                                     ^InstanceProperties properties]
  nil)

(defn -undoRelationshipUpdate [this
                               ^String userId
                               ^String relationshipGUID]
  nil)

(defn -deleteRelationship [this
                           ^String userId
                           ^String typeDefGUID
                           ^String typeDefName
                           ^String obsoleteRelationshipGUID]
  nil)

(defn -purgeRelationship [this
                          ^String userId
                          ^String typeDefGUID
                          ^String typeDefName
                          ^String deletedRelationshipGUID]
  nil)

(defn -restoreRelationship [this
                            ^String userId
                            ^String deletedRelationshipGUID]
  nil)

(defn -reIdentifyEntity [this
                         ^String userId
                         ^String typeDefGUID
                         ^String typeDefName
                         ^String entityGUID
                         ^String newEntityGUID]
  nil)

(defn -reTypeEntity [this
                     ^String userId
                     ^String entityGUID
                     ^TypeDefSummary currentTypeDefSummary
                     ^TypeDefSummary newTypeDefSummary]
  nil)

(defn -reHomeEntity [this
                     ^String userId
                     ^String entityGUID
                     ^String typeDefGUID
                     ^String typeDefName
                     ^String homeMetadataCollectionId
                     ^String newHomeMetadataCollectionId
                     ^String newHomeMetadataCollectionName]
  nil)

(defn -reIdentifyRelationship [this
                               ^String userId
                               ^String typeDefGUID
                               ^String typeDefName
                               ^String relationshipGUID
                               ^String newRelationshipGUID]
  nil)

(defn -reTypeRelationship [this
                           ^String userId
                           ^String relationshipGUID
                           ^TypeDefSummary currentTypeDefSummary
                           ^TypeDefSummary newTypeDefSummary]
  nil)

(defn -reHomeRelationship [this
                           ^String userId
                           ^String relationshipGUID
                           ^String typeDefGUID
                           ^String typeDefName
                           ^String homeMetadataCollectionId
                           ^String newHomeMetadataCollectionId
                           ^String newHomeMetadataCollectionName]
  nil)

(defn -saveEntityReferenceCopy [this
                                ^String userId
                                ^EntityDetail entity]
  nil)

(defn -getHomeClassifications [this
                               ^String userId
                               ^String entityGUID]
  (Collections/EMPTY_LIST))

(defn -purgeEntityReferenceCopy [this
                                 ^String userId
                                 ^String entityGUID
                                 ^String typeDefGUID
                                 ^String typeDefName
                                 ^String homeMetadataCollectionId]
  nil)

(defn -saveClassificationReferenceCopy [this
                                        ^String userId
                                        ^EntityDetail entity
                                        ^Classification classification]
  nil)

(defn -purgeClassificationReferenceCopy [this
                                         ^String userId
                                         ^EntityDetail entity
                                         ^Classification classification]
  nil)

(defn -saveRelationshipReferenceCopy [this
                                      ^String userId
                                      ^Relationship relationship]
  nil)

(defn -purgeRelationshipReferenceCopy [this
                                       ^String userId
                                       ^String relationshipGUID
                                       ^String typeDefGUID
                                       ^String typeDefName
                                       ^String homeMetadataCollectionId]
  nil)

(defn -getEntityProxy [this
                       ^String userId
                       ^String entityGUID
                       ^String methodName]
  nil)