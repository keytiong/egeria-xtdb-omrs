(ns io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection
  (:require [clojure.datafy :refer [datafy]]
            [io.kosong.egeria.omrs :as om]
            [io.kosong.egeria.omrs.xtdb.metadata-store :as store]
            [io.kosong.egeria.omrs.datafy :as om-datafy]
            [xtdb.api :as xt])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDef AttributeTypeDef TypeDefSummary)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances EntityDetail InstanceProperties EntitySummary InstanceStatus EntityProxy ClassificationOrigin Classification Relationship InstanceProvenanceType)
           (java.util Date Collections List)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties HistorySequencingOrder SequencingOrder MatchCriteria)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.search SearchProperties SearchClassifications)
           (org.odpi.openmetadata.frameworks.auditlog AuditLog)))

(gen-class
  :name io.kosong.egeria.omrs.xtdb.XtdbOMRSMetadataCollection
  :extends org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSDynamicTypeMetadataCollectionBase
  :init init
  :post-init post-init
  :state state
  :exposes-methods {setAuditLog                             superSetAuditLog
                    addTypeDef                              superAddTypeDef
                    updateTypeDef                           superUpdateTypeDef
                    addAttributeTypeDef                     superAddAttributeTypeDef
                    deleteTypeDef                           superDeleteTypeDef
                    deleteAttribtueTypeDef                  superDeleteAttributeTypeDef
                    reIdentifyTypeDef                       superDeIdentifyTypeDef
                    reIdentifyAttribtueTypeDef              superDeIdentifyAttribtueTypeDef
                    reportTypeDefAlreadyDefined             superReportTypeDefAlreadyDefined
                    addEntityParameterValidation            superAddEntityParameterValidation
                    getInstanceParameterValidation          superGetInstanceParameterValidation
                    updateInstanceStatusParameterValidation superUpdateInstanceStatusParameterValidation
                    }
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

(defn -init
  [^io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector connector
   ^String repositoryName
   ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper repositoryHelper
   ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator repositoryValidator
   ^String metadataCollectionId
   ^org.odpi.openmetadata.frameworks.auditlog.AuditLog auditLog]
  (let [xtdb-node                (some-> (.getXtdbNode connector) :node)
        context                  (om/->context {:type-store repositoryHelper})
        server-name              (.getServerName connector)
        metadata-collection-name (.getMetadataCollectionName connector)
        state                    (atom {:server-name               server-name
                                        :repository-name           repositoryName
                                        :metadata-collection-id    metadataCollectionId
                                        :metadata-collection-name  metadata-collection-name
                                        :repository-content-helper repositoryHelper
                                        :xtdb-node                 xtdb-node
                                        :context                   context})]
    [[connector repositoryName repositoryHelper repositoryValidator metadataCollectionId] state]))

(defn -post-init
  [this
   ^io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector connector
   ^String repositoryName
   ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper repositoryHelper
   ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator repositoryValidator
   ^String metadataCollectionId
   ^AuditLog auditLog]
  (.superSetAuditLog this auditLog))

(defn -addTypeDef
  [this userId newTypeDef]
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (.superAddTypeDef this userId newTypeDef)
    (binding [om/*context* context]
      (store/persist-type-def xtdb-node (datafy newTypeDef)))))

(defn -addAttributeTypeDef
  [this userId newAttributeTypeDef]
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (.superAddAttributeTypeDef this userId newAttributeTypeDef)
    (binding [om/*context* context]
      (store/persist-attribute-type-def xtdb-node (datafy newAttributeTypeDef)))))

(defn ^TypeDef -updateTypeDef
  [this userId typeDefPatch]
  (let [{:keys [xtdb-node
                context]} @(.state this)
        updated (.superUpdateTypeDef this userId typeDefPatch)]
    (binding [om/*context* context]
      (store/persist-type-def xtdb-node (datafy updated)))))

(defn -deleteTypeDef
  [this userId obsoleteTypeDefGUID obsoleteTypeDefName]
  (.superDeleteTypeDef this userId obsoleteTypeDefGUID obsoleteTypeDefName))

(defn -deleteAttributeTypeDef
  [this userId obsoleteTypeDefGUID obsoleteTypeDefName]
  (.superDeleteAttributeTypeDef this userId obsoleteTypeDefGUID obsoleteTypeDefName))

(defn ^TypeDef -reIdentifyTypeDef
  [this userId originalTypeDefGUID originalTypeDefName newTypeDefGUID newTypeDefName]
  (.superReIdentifyTypeDef this userId originalTypeDefGUID originalTypeDefName newTypeDefGUID newTypeDefName))

(defn ^AttributeTypeDef -reIdentifyAttributeTypeDef
  [this userId originalAttributeTypeDefGUID originalAttributeTypeDefName newAttributeTypeDefGUID newAttributeTypeDefName]
  (.superReIdentifyAttribtueTypeDef this userId originalAttributeTypeDefGUID originalAttributeTypeDefName
    newAttributeTypeDefGUID newAttributeTypeDefName))

(defn ^EntityDetail -isEntityKnown
  [this userId guid]
  (.superGetInstanceParameterValidation this userId guid "isEntityKnown")
  (let [{:keys [xtdb-node
                context]} @(.state this)
        entity (store/fetch-entity-by-guid xtdb-node guid)]
    (when entity
      (binding [om/*context* context]
        (om-datafy/map->EntityDetail entity)))))

(defn ^EntitySummary -getEntitySummary
  [this userId guid]
  (.superGetInstanceParameterValidation this userId guid "getEntitySummary")
  (let [{:keys [xtdb-node
                context]} @(.state this)
        entity (store/fetch-entity-by-guid xtdb-node guid)]
    (when entity
      (binding [om/*context* context]
        (om-datafy/map->EntitySummary entity)))))

(defn ^EntityDetail -getEntityDetail
  ([this userId guid]
   (-getEntityDetail this userId guid (Date.)))
  ([this userId guid valid-time]
   (.superGetInstanceParameterValidation this userId guid "getEntityDetail")
   (tap> guid)
   (let [{:keys [xtdb-node context]} @(.state this)]
     (binding [om/*context* context]
       (if-let [entity (store/fetch-entity-by-guid xtdb-node guid valid-time)]
         (do
           (tap> entity)
           (om-datafy/map->EntityDetail entity)))))))

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

(defn- random-uuid-str []
  (str (java.util.UUID/randomUUID)))

(defn add-entity [this user-id type-def properties classifications status]
  (let [{:keys [metadata-collection-id
                metadata-collection-name
                context
                xtdb-node]} @(.state this)
        type-def-guid  (:openmetadata.TypeDef/guid type-def)
        entity-summary {:openmetadata.Entity/type                   type-def-guid
                        :openmetadata.Entity/headerVersion          1
                        :openmetadata.Entity/version                1
                        :openmetadata.Entity/metadataCollectionId   metadata-collection-id
                        :openmetadata.Entity/metadataCollectionName metadata-collection-name
                        :openmetadata.Entity/createdBy              user-id
                        :openmetadata.Entity/createTime             (Date.)
                        :openmetadata.Entity/instanceProvenanceType (.name InstanceProvenanceType/LOCAL_COHORT)
                        :openmetadata.Entity/classifications        classifications
                        :openmetadata.Entity/status                 status
                        :openmetadata.Entity/guid                   (random-uuid-str)}
        entity         (merge entity-summary
                         properties)]
    (binding [om/*context* context]
      (store/persist-entity xtdb-node entity)
      (xt/sync xtdb-node)
      entity)))

(defn ^EntityDetail -addEntity
  [this userId entityTypeGUID properties classifications initialStatus]
  (.superAddEntityParameterValidation this userId, entityTypeGUID, properties, classifications, initialStatus,
    "addEntity")
  (let [{:keys [context]} @(.state this)]
    (binding [om/*context* context]
      (let [type-def            (om/find-type-def-by-guid entityTypeGUID)
            instance-properties (om-datafy/instance-properties->map type-def properties)
            classifications     (mapv datafy classifications)
            status              (.name initialStatus)
            entity              (add-entity this userId type-def instance-properties classifications status)]
        (om-datafy/map->EntityDetail entity)))))

(defn -addEntityProxy [this
                       ^String userId
                       ^EntityProxy entityProxy]
  nil)

(defn -updateEntityStatus
  [this userId entityGUID newStatus]
  (.superUpdateInstanceStatusParameterValidation this userId entityGUID newStatus "updateEntityStatus")
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (binding [om/*context* context]
      (let [entity (some-> (store/fetch-entity-by-guid xtdb-node entityGUID)
                     (assoc :openmetadata.Entity/status (.name newStatus))
                     (assoc :openmetadata.Entity/updateTime (Date.))
                     (assoc :openmetadata.Entity/updatedBy userId)
                     (update-in [:openmetadata.Entity/version] inc))]
        (when entity
          (store/persist-entity xtdb-node (datafy entity))
          (xt/sync xtdb-node)
          (om-datafy/map->EntityDetail entity))))))

(defn -updateEntityProperties
  [this userId entityGUID properties]
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (binding [om/*context* context]
      (let [entity        (store/fetch-entity-by-guid xtdb-node entityGUID)
            type-def-guid (:openmetadata.Entity/type entity)
            type-def      (some-> type-def-guid om/find-type-def-by-guid)
            prop-map      (when type-def
                            (om-datafy/instance-properties->map type-def properties))
            entity        (some-> entity
                            (merge prop-map)
                            (assoc :openmetadata.Entity/updateTime (Date.))
                            (assoc :openmetadata.Entity/updatedBy userId)
                            (update-in [:openmetadata.Entity/version] inc))]
        (tap> prop-map)
        (when entity
          (store/persist-entity xtdb-node (datafy entity))
          (om-datafy/map->EntityDetail entity))))))

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