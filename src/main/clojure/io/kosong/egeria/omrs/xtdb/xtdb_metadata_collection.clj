(ns io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection
  (:require [clojure.datafy :refer [datafy]]
            [xtdb.api :as xt])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector OMRSRepositoryConnector OMRSRepositoryHelper OMRSRepositoryValidator)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDef TypeDefPatch AttributeTypeDef TypeDefSummary)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances EntityDetail InstanceProperties EntitySummary InstanceStatus EntityProxy ClassificationOrigin Classification Relationship)
           (java.util Date Collections List)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties HistorySequencingOrder SequencingOrder MatchCriteria)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.search SearchProperties SearchClassifications)))

(gen-class
  :name io.kosong.egeria.omrs.xtdb.XtdbOMRSMetadataCollection
  :extends org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSDynamicTypeMetadataCollectionBase
  :init init
  :state state
  :exposes-methods {setAuditLog                superSetAuditLog
                    addTypeDef                 superAddTypeDef
                    addAttributeTypeDef        superAddAttributeTypeDef
                    deleteTypeDef              superDeleteTypeDef
                    deleteAttribtueTypeDef     superDeleteAttributeTypeDef
                    reIdentifyTypeDef          superDeIdentifyTypeDef
                    reIdentifyAttribtueTypeDef superDeIdentifyAttribtueTypeDef}
  :constructors {[io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector ;; parentConnector
                  String                                    ;; repositoryName
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper ;; repositoryHelper
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator ;; repositoryValidator
                  String                                    ;; metadataCollectionId
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
             ^String metadataCollectionId]
  (let [state (atom {:repository-name        repositoryName
                     :metadata-collection-id metadataCollectionId
                     :repository-helper      repositoryHelper
                     :xtdb-node              (some-> (.getXtdbNode connector) :node)})]
    [[connector repositoryName repositoryHelper repositoryValidator metadataCollectionId] state]))


(defn -addTypeDef [this
                   ^String userId
                   ^TypeDef newTypeDef]
  (let [{:keys [xtdb-node]} @(.state this)
        db            (xt/db xtdb-node)
        type-def      (datafy newTypeDef)
        type-def-guid (:openmetadata.TypeDef/guid type-def)
        query-result  (xt/q db
                        `{:find  [e]
                          :in    [guid]
                          :where [[e :openmetadata.TypeDef/guid guid]]}
                        type-def-guid)]
    (when (not-empty query-result)
      (throw (ex-info "TypeDef already exist" {})))
    (let [xt-id (str (java.util.UUID/randomUUID))
          doc   (assoc type-def :xt/id xt-id)]
      (xt/submit-tx xtdb-node [[::xt/put doc]])
      (.superAddTypeDef this userId newTypeDef))))

(defn -addAttributeTypeDef [this
                            ^String userId
                            ^AttributeTypeDef newAttributeTypeDef]
  (.superAddAttributeTypeDef this userId newAttributeTypeDef))

(defn ^TypeDef -updateTypeDef [this
                               ^String userId
                               ^TypeDefPatch typeDefPatch]
  (.superUpdateTypeDef this userId typeDefPatch))

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