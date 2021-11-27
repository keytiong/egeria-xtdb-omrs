(ns io.kosong.egeria.omrs.xtdb-metadata-store
  (:require [io.kosong.egeria.omrs :as omrs]
            [xtdb.api :as xt]
            [clojure.data]
            [clojure.set :as set])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances EntitySummary EntityDetail EntityProxy InstanceProperties Relationship InstanceGraph)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDef TypeDefCategory)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties MatchCriteria)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.search SearchProperties)
           (java.util List UUID Map Collections)
           (xtdb.api IXtdb)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector OMRSRepositoryHelper)
           (io.kosong.egeria.omrs GraphOMRSErrorCode)
           (org.odpi.openmetadata.repositoryservices.ffdc.exception EntityProxyOnlyException)))

(gen-class
  :name io.kosong.egeria.omrs.XtdbOMRSMetadataStore
  :implements [io.kosong.egeria.omrs.GraphOMRSMetadataStore]
  :init init
  :state state
  :constructors {[java.lang.String
                  java.lang.String
                  org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper
                  xtdb.api.IXtdb]
                 []})

(defn- find-instance [db key-attribute value]
  (let [?e (gensym "?e_")
        q  `{:find  [(eql/project ~?e [~'*])]
             :where [[~?e ~key-attribute ~value]]}]
    (first (first (xt/q db q)))))

(defn- find-entity-instance [db entity-guid]
  (find-instance db :openmetadata.Entity/guid entity-guid))

(defn- find-relationship-instance [db relationship-guid]
  (find-instance db :openmetadata.Relationship/guid relationship-guid))

(defn- find-classification-instances [db entity-guid]
  (let [q '{:find  [(eql/project ?c [*])]
            :in    [?entity-guid]
            :where [[?e :openmetadata.Entity/guid ?entity-guid]
                    [?e :openmetadata.Entity/classifications ?c]]}]
    (map first (xt/q db q entity-guid))))


(defn- ensure-xtdb-db-id [document]
  (if (:xt/id document)
    document
    (assoc document :xt/id (str (UUID/randomUUID)))))

(defn- save-single-document-tx-ops [document]
  (let [document (-> (ensure-xtdb-db-id document)
                   (into (sorted-map-by compare)))]
    [[::xt/put document]]))

(defn- save-entity-tx-ops [entity-detail]
  (let [cls-docs   (->> (:openmetadata.Entity/classifications entity-detail)
                     (map ensure-xtdb-db-id))
        cls-ids    (mapv :xt/id cls-docs)
        entity-doc (assoc entity-detail :openmetadata.Entity/classifications cls-ids)]
    (->> (map save-single-document-tx-ops cls-docs)
      (reduce concat (save-single-document-tx-ops entity-doc))
      (vec))))


(defn- save-relationship-tx-ops [relationship]
  (save-single-document-tx-ops relationship))


(defn- save-type-def [this ^TypeDef typeDef]
  (let [state         (.state this)
        type-def-doc  (omrs/->map typeDef)
        type-def-guid (:openmetadata.TypeDef/guid type-def-doc)
        type-def-name (:openmetadata.TypeDef/name type-def-doc)
        xtdb-node     (:xtdb-node @state)
        db            (xt/db xtdb-node)
        existing      (or
                        (find-instance db :openmetadata.TypeDef/guid type-def-guid)
                        (find-instance db :openmetadata.TypeDef/name type-def-name))]
    (when-not existing
      (let [tx-ops (save-single-document-tx-ops type-def-doc)]
        (xt/submit-tx xtdb-node tx-ops)))))

(defn- unique-type-def-attributes [repo-helper type-def]
  (->> (omrs/list-type-def-attributes repo-helper type-def)
    (filter :openmetadata.TypeDefAttribute/isUnique)))

(defn- match-single-query [[a v]]
  {:find  '[?e]
   :where [['?e a v]]})

(defn- match-any-query [avs]
  (let [triples (map (fn [[a v]] ['?e a v]) avs)]
    {:find  '[?e]
     :where `[(~'or ~@triples)]}))


(defn- match-all-query [avs]
  (let [triples (map (fn [[a v]] ['?e a v]) avs)]
    {:find  '[?e]
     :where `[~@triples]}))


(defn- entity-exist? [xtdb-node repo-helper entity-doc]
  (let [type-def-guid    (:openmetadata.Entity/typeDefGUID entity-doc)
        type-def         (omrs/find-type-def-by-guid repo-helper type-def-guid)
        unique-attrs     (unique-type-def-attributes repo-helper type-def)
        unique-prop-keys (conj (map omrs/qualify-property-key unique-attrs)
                           :openmetadata.Entity/guid)
        criteria         (select-keys entity-doc unique-prop-keys)
        query            (match-any-query criteria)
        db               (xt/db xtdb-node)]
    (not-empty (xt/q db query))))


(defn- entity-proxy? [local-metadata-collection-id entity]
  (and (:openmetadata.Entity/isProxy entity)
    (not= (:openmetadata.Entity/metadataCollectionId entity) local-metadata-collection-id)))


(defn- verify-unique-constraints [xtdb-node repo-helper new-entity]
  (let [entity-guid      (:openmetadata.Entity/guid new-entity)
        type-def-guid    (:openmetadata.Entity/typeDefGUID new-entity)
        type-def         (omrs/find-type-def-by-guid repo-helper type-def-guid)
        unique-props     (unique-type-def-attributes repo-helper type-def)
        unique-prop-keys (map omrs/qualify-property-key unique-props)
        criteria         (select-keys new-entity unique-prop-keys)
        query            (let [triples (map (fn [[a v]] ['?e a v]) criteria)]
                           `{:find  [~'?guid]
                             :where [[~'?e :openmetadata.Entity/guid ~'?guid]
                                     (~'or ~@triples)]})
        db               (xt/db xtdb-node)
        guids            (->> (xt/q db query)
                           (map first)
                           (into #{}))
        guids            (set/difference guids #{entity-guid})]
    (when-not (empty? guids)
      (throw (ex-info "Unique constraint violation" {:entity new-entity})))))


(defn- fetch-instance [db [a v]]
  (let [query {:find  '[(eql/project ?e [*])]
               :where `[[~'?e ~a ~v]]}]
    (map first (xt/q db query))))


(defn- verify-entity-proxy-one [xtdb-node repo-helper new-relationship]
  (let [entity-guid (:openmetadata.Relationship/entityOne new-relationship)
        db          (xt/db xtdb-node)
        entity      (fetch-instance db [:openmetadata.Entity/guid entity-guid])]
    (if-not entity
      (throw (ex-info "Entity one not found." {:relationship new-relationship})))))


(defn- verify-entity-proxy-two [xtdb-node repo-helper new-relationship]
  (let [entity-guid (:openmetadata.Relationship/entityOne new-relationship)
        db          (xt/db xtdb-node)
        entity      (fetch-instance db [:openmetadata.Entity/guid entity-guid])]
    (if-not entity
      (throw (ex-info "Entity two not found." {:relationship new-relationship})))))


(defn -init [^String repository-name ^String metadata-collection-id ^OMRSRepositoryHelper helper ^IXtdb xtdb-node]
  (let [state (atom {:repository-name        repository-name
                     :metadata-collection-id metadata-collection-id
                     :repository-helper      helper
                     :xtdb-node              (:node xtdb-node)})]
    [[] state]))


(defn -createEntityIndexes [this ^TypeDef typeDef]
  (save-type-def this typeDef))


(defn -createRelationshipIndexes [this ^TypeDef typeDef]
  (save-type-def this typeDef))


(defn -createClassificationIndexes [this ^TypeDef typeDef]
  (save-type-def this typeDef))


(defn -createEntityInStore [this ^EntityDetail newEntityDetail]
  (let [state                  @(.state this)
        xtdb-node              (:xtdb-node state)
        helper                 (:repository-helper state)
        metadata-collection-id (:metadata-collection-id state)
        db                     (xt/db xtdb-node)
        new-entity             (omrs/EntityDetail->map helper newEntityDetail)
        entity-guid            (:openmetadata.Entity/guid new-entity)
        stored-entity          (first (fetch-instance db [:openmetadata.Entity/guid entity-guid]))]
    ;;(verify-unique-constraints xtdb-node helper new-entity)
    (when (and stored-entity (not (entity-proxy? metadata-collection-id stored-entity)))
      (throw (ex-info "Entity already exist." {:entity new-entity})))
    (xt/submit-tx xtdb-node (save-entity-tx-ops new-entity))
    newEntityDetail))


(defn -createEntityProxyInStore [this ^EntityProxy newEntityProxy]
  (let [state             @(.state this)
        xtdb-node         (:xtdb-node state)
        helper            (:repository-helper state)
        db                (xt/db xtdb-node)
        new-entity-proxy  (omrs/EntityProxy->map helper newEntityProxy)
        entity-proxy-guid (:openmetadata.Entity/guid new-entity-proxy)
        stored-entity     (first (fetch-instance db [:openmetadata.Entity/guid entity-proxy-guid]))]
    ;;(verify-unique-constraints xtdb-node helper new-entity-proxy)
    (when stored-entity
      (throw (ex-info "Entity already exist." {:entity-proxy new-entity-proxy})))
    (xt/submit-tx xtdb-node (save-entity-tx-ops new-entity-proxy))
    newEntityProxy))


(defn -createRelationshipInStore [this ^Relationship relationship]
  (let [state               @(.state this)
        xtdb-node           (:xtdb-node state)
        helper              (:repository-helper state)
        db                  (xt/db xtdb-node)
        new-relationship    (omrs/Relationship->map helper relationship)
        relationship-guid   (:openmetadata.Relationship/guid new-relationship)
        stored-relationship (find-relationship-instance db relationship-guid)]
    ;;(verify-unique-constraints xtdb-node helper new-relationship)
    (when stored-relationship
      (throw (ex-info "Relationship already exist." {:relationship new-relationship})))
    (verify-entity-proxy-one xtdb-node helper new-relationship)
    (verify-entity-proxy-two xtdb-node helper new-relationship)
    (xt/submit-tx xtdb-node (save-relationship-tx-ops new-relationship))
    relationship))


(defn ^EntityDetail -getEntityDetailFromStore [this ^String guid]
  ;throws EntityProxyOnlyException, EntityNotKnownException, RepositoryErrorException
  (let [state           @(.state this)
        xtdb-node       (:xtdb-node state)
        helper          (:repository-helper state)
        repository-name (:repository-name state)
        db              (xt/db xtdb-node)
        entity          (find-entity-instance db guid)
        class-name      "xtdb-metadata-store"
        method-name     "getEntityDetailFromStore"]
    (when-not entity
      (let [msg-args (into-array String [guid class-name method-name repository-name])
            msg-defn (.getMessageDefinition (GraphOMRSErrorCode/ENTITY_NOT_FOUND) msg-args)]
        (throw (EntityProxyOnlyException. msg-defn class-name method-name))))
    (when (:openmetadata.Entity/isProxy entity)
      (let [msg-args (into-array String [guid class-name method-name repository-name])
            msg-defn (.getMessageDefinition (GraphOMRSErrorCode/ENTITY_PROXY_ONLY) msg-args)]
        (throw (EntityProxyOnlyException. msg-defn class-name method-name))))
    (omrs/map->EntityDetail helper entity)))


(defn ^Relationship -getRelationshipFromStore [this ^String guid]
  (let [state            @(.state this)
        xtdb-node        (:xtdb-node state)
        helper           (:repository-helper state)
        db               (xt/db xtdb-node)
        relationship-doc (find-relationship-instance db guid)
        entity-one-guid  (when relationship-doc
                           (:openmetadata.Relationship/entityOneGUID relationship-doc))
        entity-two-guid  (when relationship-doc
                           (:openmetadata.Relationship/entityTwoGUID relationship-doc))
        entity-one-doc   (when entity-one-guid
                           (find-entity-instance db entity-one-guid))
        entity-two-doc   (when entity-two-guid
                           (find-entity-instance db entity-two-guid))]
    (when relationship-doc
      (omrs/map->Relationship helper relationship-doc entity-one-doc entity-two-doc))))


(defn ^EntitySummary -getEntitySummaryFromStore [this ^String guid]
  (let [state      @(.state this)
        xtdb-node  (:xtdb-node state)
        helper     (:repository-helper state)
        db         (xt/db xtdb-node)
        entity-doc (find-entity-instance db guid)]
    (when entity-doc
      (omrs/map->EntitySummary helper entity-doc))))


(defn ^EntityProxy -getEntityProxyFromStore [this ^String entityProxyGUID]
  (let [state      @(.state this)
        xtdb-node  (:xtdb-node state)
        helper     (:repository-helper state)
        db         (xt/db xtdb-node)
        entity-doc (find-entity-instance db entityProxyGUID)]
    (when entity-doc
      (omrs/map->EntityProxy helper entity-doc))))


(defn ^List -getRelationshipsForEntity [this ^String entityGUID]
  (let [state     @(.state this)
        xtdb-node (:xtdb-node state)
        helper    (:repository-helper state)
        db        (xt/db xtdb-node)
        q         '{:find  [(eql/project ?r [*])]
                    :in    [guid]
                    :where [(or
                              [?r :openmetadata.Relationship/entityOneGUID guid]
                              [?r :openmetadata.Relationship/entityTwoGUID guid])]}]
    (->> (xt/q db q entityGUID)
      (map first)
      (map (fn [r]
             (let [e1-guid (:openmetadata.Relationship/entityOneGUID r)
                   e2-guid (:openmetadata.Relationship/entityTwoGUID r)
                   e1      (find-entity-instance db e1-guid)
                   e2      (find-entity-instance db e2-guid)]
               [r e1 e2])))
      (map (fn [[r e1 e2]]
             (omrs/map->Relationship helper r e1 e2))))))


(defn fetch-classifications [node entity-guid]
  (let [db (xt/db node)
        q  '{:find  [(eql/project ?c [*])]
             :in    [guid]
             :where [[?e :openmetadata.Entity/guid guid]
                     [?e :openmetadata.Entity/classifications ?c]]}]
    (->> (xt/q db q entity-guid)
      (map first))))


(defn -updateEntityInStore [this ^EntityDetail entityDetail]
  (let [state                              @(.state this)
        xtdb-node                          (:xtdb-node state)
        helper                             (:repository-helper state)
        db                                 (xt/db xtdb-node)
        new-entity                         (omrs/EntityDetail->map helper entityDetail)
        entity-guid                        (:openmetadata.Entity/guid new-entity)
        stored-entity                      (first (fetch-instance db [:openmetadata.Entity/guid entity-guid]))
        stored-classifications             (find-classification-instances db entity-guid)
        stored-classifications-name-id-map (zipmap
                                             (map :openmetadata.Classification/name stored-classifications)
                                             (map :xt/id stored-classifications))
        ensure-classification-xtdb-id      (fn [c]
                                             (let [classification-name      (:openmetadata.Classification/name c)
                                                   classification-id        (:xt/id c)
                                                   stored-classification-id (get stored-classifications-name-id-map classification-name)]
                                               (cond
                                                 classification-id c
                                                 stored-classification-id (assoc c :xt/id stored-classification-id)
                                                 :default (assoc c :xt/id (UUID/randomUUID)))))
        new-classifications                (->> (:openmetadata.Entity/classifications new-entity)
                                             (map ensure-classification-xtdb-id))
        stored-classification-xtdb-ids     (map :xt/id stored-classifications)
        new-classification-xtdb-ids        (map :xt/id new-classifications)
        deleted-classification-xtdb-ids    (-> (clojure.data/diff stored-classification-xtdb-ids new-classification-xtdb-ids)
                                             (first))
        delete-classification-tx-ops       (reduce conj [] (map (fn [x] [::xt/delete x]) deleted-classification-xtdb-ids))
        put-classification-tx-ops          (reduce conj [] (map (fn [x] [::xt/put x]) new-classifications))
        new-entity                         (-> new-entity
                                             (assoc :xt/id (:xt/id stored-entity))
                                             (assoc :openmetadata.Entity/classifications new-classification-xtdb-ids))
        put-entity-tx-op                   [::xt/put new-entity]
        tx-ops                             (conj
                                             (reduce conj (vec delete-classification-tx-ops) put-classification-tx-ops)
                                             put-entity-tx-op)]
    (xt/submit-tx xtdb-node tx-ops)
    entityDetail))


(defn -updateRelationshipInStore [this ^Relationship updatedRelationship]
  (let [state                  @(.state this)
        xtdb-node              (:xtdb-node state)
        helper                 (:repository-helper state)
        db                     (xt/db xtdb-node)
        updated-relationship   (omrs/Relationship->map helper updatedRelationship)
        relationship-guid      (:openmetadata.Entity/guid updated-relationship)
        stored-relationship    (find-relationship-instance db relationship-guid)
        new-relationship       (assoc updated-relationship :xt/id (:xt/id stored-relationship))
        put-relationship-tx-op [::xt/put new-relationship]]
    (xt/submit-tx xtdb-node [put-relationship-tx-op])
    updatedRelationship))


(defn -removeRelationshipFromStore [this ^String guid]
  (let [state                     @(.state this)
        xtdb-node                 (:xtdb-node state)
        db                        (xt/db xtdb-node)
        stored-relationship       (find-relationship-instance db guid)
        delete-relationship-tx-op [::xt/delete (:xt/id stored-relationship)]]
    (xt/submit-tx xtdb-node [delete-relationship-tx-op])))


(defn -removeEntityFromStore [this ^String guid]
  (let [state                        @(.state this)
        xtdb-node                    (:xtdb-node state)
        db                           (xt/db xtdb-node)
        stored-entity                (first (fetch-instance db [:openmetadata.Entity/guid guid]))
        classification-xtdb-ids      (:openmetadata.Entity/classifications stored-entity)
        delete-classification-tx-ops (reduce conj [] (map (fn [x] [::xt/delete x]) classification-xtdb-ids))
        delete-entity-tx-op          [::xt/delete (:xt/id stored-entity)]
        tx-ops                       (conj delete-classification-tx-ops delete-entity-tx-op)]
    (xt/submit-tx xtdb-node tx-ops)))


(defn ^List -findEntitiesForType [this
                                  ^String typeName,
                                  ^SearchProperties searchProperties,
                                  ^Boolean fullMatch]
  (let [state     @(.state this)
        xtdb-node (:xtdb-node state)
        db        (xt/db xtdb-node)
        ])
  )

(defn ^List -findEntitiesForTypes [this
                                   ^List validTypeNames,
                                   ^String filterTypeName,
                                   ^Map qualifiedPropertyNameToTypeDefinedAttribute,
                                   ^Map shortPropertyNameToQualifiedPropertyNames,
                                   ^SearchProperties matchProperties]
  ;;TODO
  (Collections/emptyList))

(defn ^List -findEntitiesByPropertyForType [this
                                            ^String typeName
                                            ^InstanceProperties searchProperties
                                            ^MatchCriteria matchCriteria
                                            ^Boolean fullMatch]
  ;;TODO
  (Collections/emptyList))

(defn ^List -findEntitiesByPropertyForTypes [this
                                             ^List validTypeNames,
                                             ^String filterTypeName,
                                             ^Map qualifiedPropertyNameToTypeDefinedAttribute,
                                             ^Map shortPropertyNameToQualifiedPropertyNames,
                                             ^InstanceProperties matchProperties,
                                             ^MatchCriteria matchCriteria]
  ;;TODO
  (Collections/emptyList))

(defn ^List -findEntitiesByPropertyValueForTypes [this
                                                  ^List validTypeNames,
                                                  ^String filterTypeName,
                                                  ^Map qualifiedPropertyNameToTypeDefinedAttribute,
                                                  ^Map shortPropertyNameToQualifiedPropertyNames,
                                                  ^InstanceProperties matchProperties,
                                                  ^MatchCriteria matchCriteria]
  ;;TODO
  (Collections/emptyList))


(defn ^List -findEntitiesByClassification [this
                                           ^String classificationName
                                           ^InstanceProperties matchClassificationProperties
                                           ^MatchCriteria matchCriteria
                                           ^Boolean performTypeFiltering
                                           ^List validTypeNames]
  ;;TODO
  (Collections/emptyList)
  )

(defn ^List -findRelationshipsForType [this
                                       ^String typeName
                                       ^SearchProperties searchProperties
                                       ^Boolean fullMatch]
  ;;TODO
  (Collections/emptyList))

(defn ^List -findRelationshipsForTypes [this
                                        ^List validTypeNames,
                                        ^String filterTypeName,
                                        ^Map qualifiedPropertyNameToTypeDefinedAttribute,
                                        ^Map shortPropertyNameToQualifiedPropertyNames,
                                        ^SearchProperties matchProperties]
  ;; TODO
  (Collections/emptyList))

(defn ^List -findRelationshipsByPropertyForType [this
                                                 ^String typeName,
                                                 ^InstanceProperties matchProperties,
                                                 ^MatchCriteria matchCriteria,
                                                 ^Boolean fullMatch]
  ;; TODO
  (Collections/emptyList))

(defn ^List -findRelationshipsByPropertyForTypes [this
                                                  ^List validTypeNames,
                                                  ^String filterTypeName,
                                                  ^Map qualifiedPropertyNameToTypeDefinedAttribute,
                                                  ^Map shortPropertyNameToQualifiedPropertyNames,
                                                  ^InstanceProperties matchProperties,
                                                  ^MatchCriteria matchCriteria]
  ;; TODO
  (Collections/emptyList))

(defn ^List -findRelationshipsByPropertyValueForTypes [this
                                                       ^List validTypeNames,
                                                       ^String filterTypeName,
                                                       ^Map qualifiedPropertyNameToTypeDefinedAttribute,
                                                       ^Map shortPropertyNameToQualifiedPropertyNames,
                                                       ^InstanceProperties matchProperties,
                                                       ^MatchCriteria matchCriteria]
  ;; TODO
  (Collections/emptyList))

(defn ^InstanceProperties -constructMatchPropertiesForSearchCriteriaForTypes [this
                                                                              ^TypeDefCategory typeDefCategory,
                                                                              ^String searchCriteria,
                                                                              ^String filterTypeName,
                                                                              ^List validTypeNames]
  ;; TODO
  (InstanceProperties.))

(defn -saveEntityReferenceCopyToStore [this ^EntityDetail entity]
  (-updateEntityInStore this entity))


(defn -saveRelationshipReferenceCopyToStore [this ^Relationship relationship]
  (-updateRelationshipInStore this relationship))


(defn ^InstanceGraph -getSubGraph [this
                                   ^String entityGUID
                                   ^List entityTypeGUIDs
                                   ^List relationshipTypeGUIDs
                                   ^List limitResultsByStatus
                                   ^List limitResultsByClassification
                                   level]
  ;; TODO
  (InstanceGraph.))


(defn ^InstanceGraph -getPaths [this
                                ^String startEntityGUID
                                ^String endEntityGUID
                                ^List limitResultsByStatus
                                maxPaths
                                maxDepth]
  ;; TODO
  (InstanceGraph.))

