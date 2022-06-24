(ns io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection
  (:require [clojure.datafy :refer [datafy]]
            [io.kosong.egeria.omrs :as om]
            [io.kosong.egeria.omrs.protocols :as om-p]
            [io.kosong.egeria.omrs.xtdb.metadata-store :as store]
            [io.kosong.egeria.omrs.datafy :as om-datafy]
            [xtdb.api :as xt])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDef AttributeTypeDef TypeDefSummary)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances EntityDetail InstanceProperties EntitySummary InstanceStatus EntityProxy ClassificationOrigin Classification Relationship InstanceProvenanceType InstanceAuditHeader)
           (java.util Date Collections List)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties HistorySequencingOrder SequencingOrder MatchCriteria)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.search SearchProperties SearchClassifications PropertyComparisonOperator)
           (org.odpi.openmetadata.frameworks.auditlog AuditLog)
           (org.odpi.openmetadata.repositoryservices.ffdc.exception EntityNotKnownException)
           (org.odpi.openmetadata.repositoryservices.ffdc OMRSErrorCode)))

(gen-class
  :name io.kosong.egeria.omrs.xtdb.XtdbOMRSMetadataCollection
  :extends org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSDynamicTypeMetadataCollectionBase
  :init init
  :post-init post-init
  :state state
  :exposes-methods {setAuditLog                                superSetAuditLog
                    addAttributeTypeDef                        superAddAttributeTypeDef
                    addEntityParameterValidation               superAddEntityParameterValidation
                    addEntityProxyParameterValidation          superAddEntityProxyParameterValidation
                    addRelationshipParameterValidation         superAddRelationshipParameterValidation
                    addTypeDef                                 superAddTypeDef
                    classifyEntityParameterValidation          superClassifyEntityParameterValidation
                    declassifyEntityParameterValidation        superDeclassifyEntityParameterValidation
                    deleteTypeDef                              superDeleteTypeDef
                    updateTypeDef                              superUpdateTypeDef
                    updateInstanceStatusParameterValidation    superUpdateInstanceStatusParameterValidation
                    deleteAttribtueTypeDef                     superDeleteAttributeTypeDef
                    reIdentifyTypeDef                          superDeIdentifyTypeDef
                    reIdentifyAttribtueTypeDef                 superDeIdentifyAttribtueTypeDef
                    reportTypeDefAlreadyDefined                superReportTypeDefAlreadyDefined
                    getInstanceParameterValidation             superGetInstanceParameterValidation
                    updateInstancePropertiesPropertyValidation superUpdateInstancePropertiesPropertyValidation
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
        server-name              (.getServerName connector)
        metadata-collection-name (.getMetadataCollectionName connector)
        state                    (atom {:server-name               server-name
                                        :repository-name           repositoryName
                                        :metadata-collection-id    metadataCollectionId
                                        :metadata-collection-name  metadata-collection-name
                                        :repository-content-helper repositoryHelper
                                        :xtdb-node                 xtdb-node})]
    [[connector repositoryName repositoryHelper repositoryValidator metadataCollectionId] state]))

(defn -post-init
  [this
   ^io.kosong.egeria.omrs.xtdb.XtdbOMRSRepositoryConnector connector
   ^String repositoryName
   ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper repositoryHelper
   ^org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator repositoryValidator
   ^String metadataCollectionId
   ^AuditLog auditLog]
  (.superSetAuditLog this auditLog)
  (let [context (om/->context {:type-store     repositoryHelper
                               :instance-store this})]
    (swap! (.state this) assoc :context context)))

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
      (store/persist-type-def xtdb-node (datafy updated))
      updated)))

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

(defn- is-entity-known-else-throw
  ([this guid]
   (is-entity-known-else-throw this guid (Date.)))
  ([this guid valid-time]
   (let [{:keys [repository-name
                 xtdb-node]} @(.state this)
         entity (store/fetch-entity-by-guid xtdb-node guid valid-time)]
     (if entity
       entity
       (let [msg-params (into-array String [guid "is-entity-known" repository-name])
             msg        (.getMessageDefinition OMRSErrorCode/ENTITY_NOT_KNOWN msg-params)]
         (throw (EntityNotKnownException. msg
                  (str 'io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection)
                  "")))))))

(defn ^EntityDetail -isEntityKnown
  [this userId guid]
  (.superGetInstanceParameterValidation this userId guid "isEntityKnown")
  (let [{:keys [context]} @(.state this)
        entity (is-entity-known-else-throw this guid)]
    (binding [om/*context* context]
      (om-datafy/map->EntityDetail entity))))

(defn ^EntitySummary -getEntitySummary
  [this userId guid]
  (.superGetInstanceParameterValidation this userId guid "getEntitySummary")
  (let [{:keys [context]} @(.state this)
        entity (is-entity-known-else-throw this guid)]
    (binding [om/*context* context]
      (om-datafy/map->EntitySummary entity))))

(defn ^EntityDetail -getEntityDetail
  ([this userId guid]
   (-getEntityDetail this userId guid (Date.)))
  ([this userId guid valid-time]
   (.superGetInstanceParameterValidation this userId guid "getEntityDetail")
   (let [{:keys [context]} @(.state this)
         entity (is-entity-known-else-throw this guid valid-time)]
     (binding [om/*context* context]
       (om-datafy/map->EntityDetail entity)))))

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

(defn entity-detail [m e]
  (-> m
    (conj [e :openmetadata.Entity/guid])
    (conj '(not [e :openmetadata.Entity/isProxy true]))))

(defn entity-subtypes [m e entity-type-guid entity-subtype-guids]
  (if entity-type-guid
    (let [ds (->> (om/find-type-def-descendants {:openmetadata.TypeDef/guid entity-type-guid})
               (map :openmetadata.TypeDef/guid)
               (cons entity-type-guid)
               (apply hash-set))]
      (conj m [e :openmetadata.Entity/type ds])
      m)))

(defn entity-statuses [m e statuses]
  (let [xs (map (fn [s] (.name s)) statuses)]
    (if (empty? xs)
      (conj m '(not [e :openmetadata.Entity/status "DELETED"]))
      (conj m [e :openmetadata.Entity/status (apply hash-set xs)]))))

(defmulti entity-property-condition (fn [condition]
                                      (.getOperator condition)))

(defmethod entity-property-condition PropertyComparisonOperator/EQ [condition]
  (let [e         (symbol "e")
        prop-name (.getProperty condition)
        val       (some-> condition .getValue .valueAsObject)
        ks        (->> (om/find-type-def-by-property-name prop-name)
                    (filter (fn [x] (= "ENTITY_DEF" (:openmetadata.TypeDef/category x))))
                    (map om/type-def-attribute-key->attribute)
                    (mapcat keys)
                    (filter (fn [x] (= prop-name (name x)))))
        xs        (reduce (fn [a k] (conj a [e k val])) [] ks)]
    (cons 'or (apply list xs))))

(defmethod entity-property-condition PropertyComparisonOperator/NEQ [condition]
  (let [e         (symbol "e")
        prop-name (.getProperty condition)
        val       (some-> condition .getValue .valueAsObject)
        ks        (->> (om/find-type-def-by-property-name prop-name)
                    (filter (fn [x] (= "ENTITY_DEF" (:openmetadata.TypeDef/category x))))
                    (map om/type-def-attribute-key->attribute)
                    (mapcat keys)
                    (filter (fn [x] (= prop-name (name x)))))
        xs        (reduce (fn [a k] (conj a [e k val])) [] ks)]
    `(~(symbol "not") (~(symbol "or") ~@(apply list xs)))))


(defn entity-property-conditions [m e search-properties]
  (let [match-criteria (.getMatchCriteria search-properties)]
    (cond
      (= MatchCriteria/ALL match-criteria)
      (let [conditions (.getConditions search-properties)
            clauses    (map entity-property-condition conditions)]
        (reduce conj m clauses))

      (= MatchCriteria/ANY match-criteria)
      (let [conditions  (.getConditions search-properties)
            clauses     (map entity-property-condition conditions)
            join-clause (->> (apply list clauses)
                          (cons ['e])
                          (cons 'or-join))]
        (conj m join-clause))

      (= MatchCriteria/NONE match-criteria)
      (let [conditions  (.getConditions search-properties)
            clauses     (map entity-property-condition conditions)
            join-clause (->> (apply list clauses)
                          (cons ['e])
                          (cons 'not-join))]
        (conj m join-clause)))))

(defn xtdb-query [entity-type--guid entity-subtype-guids search-properties statuses]
  (let [e       (symbol "e")
        clauses (-> []
                  (entity-detail e)
                  (entity-subtypes e entity-type--guid entity-subtype-guids)
                  (entity-statuses e statuses)
                  (entity-property-conditions e search-properties))]
    {:find  [e]
     :where clauses}))

(defn find-entities
  [xtdb-node
   entity-type--guid
   entity-subtype-guids
   search-properties
   statuses
   valid-time]
  (let [db (xt/db xtdb-node valid-time)
        q  (xtdb-query entity-type--guid entity-subtype-guids search-properties statuses)
        rs (->> (xt/q db q)
             (map first))]
    (when-not (empty? rs)
      (map (fn [guid] (store/fetch-entity-by-guid xtdb-node guid)) rs))))

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
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (binding [om/*context* context]
      (find-entities xtdb-node
        entityTypeGUID
        entitySubtypeGUIDs
        matchProperties
        limitResultsByStatus
        asOfTime))))

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

(defn add-relationship [this user-id type-def entity-one entity-two instance-properties status]
  (let [{:keys [metadata-collection-id
                metadata-collection-name
                context
                xtdb-node]} @(.state this)
        type-def-guid (:openmetadata.TypeDef/guid type-def)
        relationship  (merge
                        {:openmetadata.Relationship/type                   type-def-guid
                         :openmetadata.Relationship/headerVersion          InstanceAuditHeader/CURRENT_AUDIT_HEADER_VERSION
                         :openmetadata.Relationship/version                1
                         :openmetadata.Relationship/metadataCollectionId   metadata-collection-id
                         :openmetadata.Relationship/metadataCollectionName metadata-collection-name
                         :openmetadata.Relationship/createdBy              user-id
                         :openmetadata.Relationship/createTime             (Date.)
                         :openmetadata.Relationship/instanceProvenanceType (.name InstanceProvenanceType/LOCAL_COHORT)
                         :openmetadata.Relationship/status                 status
                         :openmetadata.Relationship/guid                   (random-uuid-str)
                         :openmetadata.Relationship/entityOne              entity-one
                         :openmetadata.Relationship/entityTwo              entity-two}
                        instance-properties)]
    (binding [om/*context* context]
      (store/persist-relationship xtdb-node relationship)
      (xt/sync xtdb-node)
      relationship)))


(defn ^EntityDetail -addEntity
  [this userId entityTypeGUID properties classifications initialStatus]
  (.superAddEntityParameterValidation this userId, entityTypeGUID, properties, classifications, initialStatus,
    "addEntity")
  (let [{:keys [context]} @(.state this)]
    (binding [om/*context* context]
      (let [type-def            (om/find-type-def-by-guid entityTypeGUID)
            instance-properties (om-datafy/InstanceProperties->map type-def properties)
            classifications     (mapv datafy classifications)
            status              (.name initialStatus)
            entity              (add-entity this userId type-def instance-properties classifications status)]
        (om-datafy/map->EntityDetail entity)))))

(defn -addEntityProxy
  [this
   ^String userId
   ^EntityProxy entityProxy]
  (.superAddEntityProxyParameterValidation this userId entityProxy)
  (let [{:keys [context xtdb-node]} @(.state this)]
    (binding [om/*context* context]
      (let [entity-proxy (datafy entityProxy)]
        (store/persist-entity xtdb-node entity-proxy)))))

(defn ^EntityDetail -updateEntityStatus
  [this userId entityGUID newStatus]
  (.superUpdateInstanceStatusParameterValidation this userId entityGUID newStatus "updateEntityStatus")
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (binding [om/*context* context]
      (let [entity (some-> (is-entity-known-else-throw this entityGUID)
                     (assoc :openmetadata.Entity/status (.name newStatus))
                     (assoc :openmetadata.Entity/updateTime (Date.))
                     (assoc :openmetadata.Entity/updatedBy userId)
                     (update-in [:openmetadata.Entity/version] inc))]
        (when entity
          (store/persist-entity xtdb-node (datafy entity))
          (xt/sync xtdb-node)
          (om-datafy/map->EntityDetail entity))))))

(defn ^EntityDetail -updateEntityProperties
  [this userId entityGUID properties]
  (.superUpdateInstancePropertiesPropertyValidation this userId entityGUID properties "updateEntityProperties")
  (let [{:keys [xtdb-node
                context]} @(.state this)]
    (binding [om/*context* context]
      (let [entity        (is-entity-known-else-throw this entityGUID)
            type-def-guid (:openmetadata.Entity/type entity)
            type-def      (some-> type-def-guid om/find-type-def-by-guid)
            prop-map      (when type-def
                            (om-datafy/InstanceProperties->map type-def properties))
            entity        (some-> entity
                            (merge prop-map)
                            (assoc :openmetadata.Entity/updateTime (Date.))
                            (assoc :openmetadata.Entity/updatedBy userId)
                            (update-in [:openmetadata.Entity/version] inc))]
        (when entity
          (store/persist-entity xtdb-node entity)
          (xt/sync xtdb-node)
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

(defn ^EntityDetail -classifyEntity
  ([this
    userId
    entityGUID
    classificationName
    classificationProperties]
   (.classifyEntity this userId entityGUID classificationName nil nil ClassificationOrigin/ASSIGNED nil
     classificationProperties))
  ([this
    ^String userId
    ^String entityGUID
    ^String classificationName
    ^String externalSourceGUID
    ^String externalSourceName
    ^ClassificationOrigin classificationOrigin
    ^String classificationOriginGUID
    ^InstanceProperties classificationProperties]
   (.superClassifyEntityParameterValidation this userId entityGUID classificationName classificationProperties "classifyEntity")
   (let [{:keys [context
                 xtdb-node
                 metadata-collection-id
                 metadata-collection-name]} @(.state this)]
     (binding [om/*context* context]
       (let [entity-0        (is-entity-known-else-throw this entityGUID)
             type-def        (om/find-type-def-by-name classificationName)
             replicated-by   (when externalSourceGUID
                               metadata-collection-id)
             collection-id   (if externalSourceGUID
                               externalSourceGUID
                               metadata-collection-id)
             collection-name (if externalSourceGUID
                               externalSourceName
                               metadata-collection-name)
             provenance      (if externalSourceGUID
                               (.name InstanceProvenanceType/EXTERNAL_SOURCE)
                               (.name InstanceProvenanceType/LOCAL_COHORT))
             classfn-1       (-> (om/skeleton-classification
                                   {:metadata-collection-id   collection-id
                                    :metadata-collection-name collection-name
                                    :instance-provenance-type provenance
                                    :user-name                userId
                                    :type-name                classificationName
                                    :replicated-by            replicated-by})
                               (merge (om-datafy/InstanceProperties->map type-def classificationProperties)))
             classfn-1       (when classificationOrigin
                               (-> classfn-1
                                 (assoc :openmetadata.Classification/classificationOrigin (.name classificationOrigin))
                                 (assoc :openmetadata.Classification/classificationOriginGUID classificationOriginGUID)))
             classifications (->> (:openmetadata.Entity/classifications entity-0)
                               (filter #(not= classificationName (:openmetadata.Classification/name %))))
             classifications (cons classfn-1 classifications)
             entity-1        (-> entity-0
                               (update-in [:openmetadata.Entity/version] inc)
                               (assoc :openmetadata.Entity/classifications classifications)
                               (assoc :openmetadata.Entity/updatedBy userId)
                               (assoc :openmetadata.Entity/updateTime (Date.)))]
         (when entity-1
           (store/persist-entity xtdb-node entity-1)
           (om-datafy/map->EntityDetail entity-1)))))))

(defn ^EntityDetail -declassifyEntity
  [this
   ^String userId
   ^String entityGUID
   ^String classificationName]
  (.superDeclassifyEntityParameterValidation this userId entityGUID classificationName "declassifyEntity")
  (let [{:keys [context
                xtdb-node]} @(.state this)]
    (binding [om/*context* context]
      (let [entity-0          (is-entity-known-else-throw this entityGUID)
            classifications-0 (:openmetadata.Entity/classifications entity-0)
            classifications-1 (->> classifications-0
                                (filter #(not= classificationName (:openmetadata.Classification/name %))))
            entity-1          (-> entity-0
                                (update-in [:openmetadata.Entity/version] inc)
                                (assoc :openmetadata.Entity/classifications classifications-1)
                                (assoc :openmetadata.Entity/updatedBy userId)
                                (assoc :openmetadata.Entity/updateTime (Date.)))]
        (when entity-1
          (store/persist-entity xtdb-node entity-1)
          (om-datafy/map->EntityDetail entity-1))))))

(defn -updateEntityClassification
  [this
   ^String userId
   ^String entityGUID
   ^String classificationName
   ^InstanceProperties properties]
  (let [{:keys [context
                xtdb-node]} @(.state this)]
    (binding [om/*context* context]
      (let [entity-0          (is-entity-known-else-throw this entityGUID)
            type-def          (om/find-type-def-by-name classificationName)
            classifications-0 (:openmetadata.Entity/classifications entity-0)
            classfn-0         (->> classifications-0
                                (filter #(= classificationName (:openmetadata.Classification/name %)))
                                (first))
            classfn-1         (-> classfn-0
                                (merge (om-datafy/InstanceProperties->map type-def properties))
                                (assoc :openmetadata.Classification/updatedBy userId)
                                (assoc :openmetadata.Classification/updateTime (Date.))
                                (update-in [:openmetadata.Classification/version] inc))
            classifications-1 (->> classifications-0
                                (filter #(not= classificationName (:openmetadata.Classification/name %)))
                                (cons classfn-1))
            entity-1          (-> entity-0
                                (update-in [:openmetadata.Entity/version] inc)
                                (assoc :openmetadata.Entity/classifications classifications-1)
                                (assoc :openmetadata.Entity/updatedBy userId)
                                (assoc :openmetadata.Entity/updateTime (Date.)))]
        (when entity-1
          (store/persist-entity xtdb-node entity-1)
          (om-datafy/map->EntityDetail entity-1))))))

(defn ^Relationship -addRelationship
  [this
   ^String userId
   ^String relationshipTypeGUID
   ^InstanceProperties initialProperties
   ^String entityOneGUID
   ^String entityTwoGUID
   ^InstanceStatus initialStatus]
  (.superAddRelationshipParameterValidation this userId relationshipTypeGUID initialProperties entityOneGUID
    entityTwoGUID initialStatus "addRelationship")
  (let [{:keys [context]} @(.state this)]
    (binding [om/*context* context]
      (let [type-def            (om/find-type-def-by-guid relationshipTypeGUID)
            instance-properties (om-datafy/InstanceProperties->map type-def initialProperties)
            status              (.name initialStatus)
            relationship        (add-relationship this userId type-def entityOneGUID entityTwoGUID instance-properties status)]
        (om-datafy/map->Relationship relationship)))))

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
