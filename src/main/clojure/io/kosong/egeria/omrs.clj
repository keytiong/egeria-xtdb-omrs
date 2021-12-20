(ns io.kosong.egeria.omrs
  (:require [clojure.string :as str]
            [clojure.core.protocols :as p]
            [clojure.datafy :refer [datafy]]
            [clojure.tools.logging :as log])
  (:import (java.util Collections LinkedList UUID)
           (org.odpi.openmetadata.repositoryservices.localrepository.repositorycontentmanager OMRSRepositoryContentHelper OMRSRepositoryContentManager OMRSRepositoryContentValidator)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceProvenanceType InstanceProperties PrimitivePropertyValue MapPropertyValue EnumPropertyValue ArrayPropertyValue EntityDetail InstanceType InstanceStatus EntityProxy Classification ClassificationOrigin Relationship EntitySummary)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs PrimitiveDef PrimitiveDefCategory CollectionDefCategory CollectionDef EnumElementDef ClassificationDef AttributeTypeDef TypeDef TypeDefAttribute EntityDef RelationshipDef EnumDef ExternalStandardMapping TypeDefAttribute EnumElementDef TypeDefPatch TypeDefCategory AttributeTypeDefCategory TypeDefLink TypeDefStatus AttributeCardinality TypeDefAttributeStatus RelationshipEndDef RelationshipEndCardinality ClassificationPropagationRule)
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
     (p/datafy type-def))))

(defn find-type-def-by-name
  ([type-def-name]
   (find-type-def-by-name *repo-helper* type-def-name))
  ([^OMRSRepositoryContentHelper repo-helper type-def-name]
   (if-let [type-def (.getTypeDefByName repo-helper "omrs" type-def-name)]
     (datafy type-def))))

(defn- maybe-guid [guid-str]
  (try
    (UUID/fromString guid-str)
    (catch IllegalArgumentException _)))

(defn find-type-def
  ([guid-or-name]
   (find-type-def *repo-helper* guid-or-name))
  ([^OMRSRepositoryContentHelper repo-helper guid-or-name]
   (cond
     (maybe-guid guid-or-name) (find-type-def-by-guid repo-helper guid-or-name)

     :else (find-type-def-by-name repo-helper guid-or-name))))

(defn find-attribute-type-def-by-guid
  ([attribute-type-guid]
   (find-attribute-type-def-by-guid *repo-helper* attribute-type-guid))
  ([^OMRSRepositoryContentHelper repo-helper attribute-type-guid]
   (datafy (.getAttributeTypeDef repo-helper
             "local"
             attribute-type-guid
             "find-attribute-type-def-by-guid"))))

;; TODO
;; Refactor into a property-key -> attribute type def map on load
(defn find-type-def-attribute-by-property-key
  ([property-key]
   (find-type-def-attribute-by-property-key *repo-helper* property-key))
  ([^OMRSRepositoryContentHelper repo-helper property-key]
   (let [type-def-name  (namespace property-key)
         type-def-name  (if (str/starts-with? type-def-name "openmetadata.")
                          (.substring type-def-name (.length "openmetadata."))
                          type-def-name)
         attribute-name (name property-key)
         type-def       (find-type-def-by-name repo-helper type-def-name)]
     (->> (:openmetadata.TypeDef/propertiesDefinition type-def)
       (filter #(= (:openmetadata.TypeDefAttribute/attributeName %) attribute-name))
       (first)))))


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

(defn ->EntityDetail
  ([source-name metadata-collection-id user-name type-name]
   (->EntityDetail source-name metadata-collection-id user-name type-name
     (InstanceProperties.) Collections/EMPTY_LIST))
  ([source-name metadata-collection-id user-name type-name instance-props classifications]
   (let [entity-detail (.getNewEntity *repo-helper*
                         source-name
                         metadata-collection-id
                         InstanceProvenanceType/LOCAL_COHORT
                         user-name
                         type-name
                         instance-props
                         classifications)]
     entity-detail)))

(defmulti ->InstancePropertyValue (fn [attr-type-def value]
                                    (:openmetadata.AttributeTypeDef/category attr-type-def)))

(defmethod ->InstancePropertyValue "PRIMITIVE" [attr-type-def value]
  (let [primitive-category (-> (:openmetadata.PrimitiveDef/category attr-type-def)
                             (PrimitiveDefCategory/valueOf))]
    (doto (PrimitivePropertyValue.)
      (.setPrimitiveValue value)
      (.setPrimitiveDefCategory primitive-category)
      (.setTypeName (.getName primitive-category))
      (.setTypeGUID (.getGUID primitive-category)))))

(defn ->ArrayPropertyValue [coll-type-def array-values]
  (let [size                  (count array-values)
        obj                   (doto (ArrayPropertyValue.)
                                (.setArrayCount size))
        element-type-def-guid (-> (:openmetadata.CollectionDef/argumentTypes coll-type-def)
                                (first))
        element-type-def      (find-attribute-type-def-by-guid element-type-def-guid)]
    (when array-values
      (do
        (doseq [[i v] (map vector (range size) array-values)]
          (.setArrayValue i (->InstancePropertyValue element-type-def v)))
        obj))))

(defn ->MapPropertyValue [coll-type-def map-value]
  (let [obj                   (MapPropertyValue.)
        element-type-def-guid (-> (:openmetadata.CollectionDef/argumentTypes coll-type-def)
                                (second))
        value-type-def        (find-attribute-type-def-by-guid element-type-def-guid)]
    (doseq [[k v] map-value]
      (.setMapValue obj (str k) (->InstancePropertyValue value-type-def v)))
    obj))

(defmethod ->InstancePropertyValue "COLLECTION" [attr-type-def value]
  (let [category (some-> (:openmetadata.CollectionDef/category attr-type-def))]
    (condp = category
      "OM_COLLECTION_ARRAY" (->ArrayPropertyValue attr-type-def value)
      "OM_COLLECTION_MAP" (->MapPropertyValue attr-type-def value))))

(def find-enum-element-def
  (memoize
    (fn [enum-type-def enum-name]
      (some->> (:openmetadata.EnumDef/elementDefs enum-type-def)
        (filter (fn [x] (= (:openmetadata.EnumElementDef/value x) enum-name)))
        (first)))))

(defmethod ->InstancePropertyValue "ENUM_DEF" [attr-type-def value]
  (let [value            (or value (:openmetadata.EnumDef/defaultValue attr-type-def))
        type-def-guid    (:openmetadata.AttributeTypeDef/guid attr-type-def)
        enum-type-def    (find-attribute-type-def-by-guid type-def-guid)
        enum-element-def (find-enum-element-def enum-type-def value)]
    (when enum-element-def
      (let [{:openmetadata.EnumElementDef/keys [ordinal description value]} enum-element-def]
        (doto (EnumPropertyValue.)
          (.setOrdinal ^int ordinal)
          (.setDescription description)
          (.setSymbolicName value))))))

(defmethod ->InstancePropertyValue :default [attr-type-def value]
  (doto (MapPropertyValue.)))


(defn ->InstanceProperties
  ([type-def-name properties]
   (let [type-def  (find-type-def-by-name type-def-name)
         prop-keys (list-type-def-property-keys type-def)
         obj       (InstanceProperties.)]
     (doseq [k prop-keys]
       (let [type-def-attribute (find-type-def-attribute-by-property-key k)
             value              (or (when (qualified-keyword? k) (k properties))
                                  (get properties (keyword (name k))))
             attribute-type-def (find-attribute-type-def-by-guid (:openmetadata.TypeDefAttribute/attributeType type-def-attribute))
             property-value     (->InstancePropertyValue attribute-type-def value)]
         (.setProperty obj (name k) property-value)))
     obj)))

(extend-type PrimitivePropertyValue
  p/Datafiable
  (datafy [^PrimitivePropertyValue x]
    (p/datafy (.valueAsObject x))))

(extend-type ArrayPropertyValue
  p/Datafiable
  (datafy [^ArrayPropertyValue x]
    (when (> (.getArrayCount x) 0)
      (some->> (.getArrayValues x)
        (.getInstanceProperties)
        (sort (fn [[k1 _] [k2 _]]
                (< (Integer/valueOf k1) (Integer/valueOf k2))))
        (mapv second)
        datafy
        (into [])))))

(extend-type MapPropertyValue
  p/Datafiable
  (datafy [^MapPropertyValue x]
    (when-let [instance-props (some-> (.getMapValues x) (.getInstanceProperties))]
      (reduce
        (fn [m [k pv]]
          (assoc m k (datafy pv)))
        {}
        instance-props))))

(extend-type EnumPropertyValue
  p/Datafiable
  (datafy [^EnumPropertyValue x]
    (when x
      (.getSymbolicName x))))

(defn- collect-instance-map [instance-props m qualified-key]
  (let [k  (name qualified-key)
        pv (.getPropertyValue instance-props k)
        v  (datafy pv)]
    (assoc m qualified-key v)))

(defn InstanceProperties->map
  [instance-type-guid instance-props]
  (let [inst-type-def (find-type-def-by-guid instance-type-guid)
        property-keys (list-type-def-property-keys inst-type-def)]
    (reduce (partial collect-instance-map instance-props) {} property-keys)))

(extend-type Classification
  p/Datafiable
  (datafy [^Classification classification]
    (let [instance-props (or (.getProperties classification) (InstanceProperties.))
          type-guid      (some-> (.getType classification) (.getTypeDefGUID))]
      (merge
        #:openmetadata.Classification
            {:type                 (some-> (.getType classification) (.getTypeDefGUID))
             :name                 (.getName classification)
             :origin               (some-> (.getClassificationOrigin classification) (.name))
             :originGUID           (.getClassificationOriginGUID classification)
             :metadataCollectionId (.getMetadataCollectionId classification)
             :createdBy            (.getCreatedBy classification)
             :updatedBy            (.getUpdatedBy classification)
             :createTime           (.getCreateTime classification)
             :updateTime           (.getUpdateTime classification)
             :version              (.getVersion classification)
             :status               (some-> (.getStatus classification) (.name))
             :statusOnDelete       (some-> (.getStatusOnDelete classification) (.name))}
        (InstanceProperties->map type-guid instance-props)))))

(extend-type EntityDetail
  p/Datafiable
  (datafy [^EntityDetail entity-detail]
    (let [instance-props (or (.getProperties entity-detail) (InstanceProperties.))
          type-guid      (some-> (.getType entity-detail) (.getTypeDefGUID))]
      (merge
        #:openmetadata.Entity
            {:guid                 (.getGUID entity-detail)
             :type                 (some-> (.getType entity-detail) (.getTypeDefGUID))
             :instanceURL          (.getInstanceURL entity-detail)
             :classifications      (some->> (.getClassifications entity-detail) (mapv datafy))
             :metadataCollectionId (.getMetadataCollectionId entity-detail)
             :createdBy            (.getCreatedBy entity-detail)
             :updatedBy            (.getUpdatedBy entity-detail)
             :createTime           (.getCreateTime entity-detail)
             :updateTime           (.getUpdateTime entity-detail)
             :version              (.getVersion entity-detail)
             :status               (some-> (.getStatus entity-detail) (.name))
             :statusOnDelete       (some-> (.getStatusOnDelete entity-detail) (.name))}
        (InstanceProperties->map type-guid instance-props)))))

(extend-type EntityProxy
  p/Datafiable
  (datafy [^EntityProxy entity-proxy]
    (let [instance-props (or (.getUniqueProperties entity-proxy) (InstanceProperties.))
          type-guid      (some-> (.getType entity-proxy) (.getTypeDefGUID))]
      (merge
        #:openmetadata.Entity
            {:guid                 (.getGUID entity-proxy)
             :isProxy              true
             :type                 (some-> (.getType entity-proxy) (.getTypeDefGUID))
             :instanceURL          (.getInstanceURL entity-proxy)
             :classifications      (some->> (.getClassifications entity-proxy) (mapv datafy))
             :metadataCollectionId (.getMetadataCollectionId entity-proxy)
             :createdBy            (.getCreatedBy entity-proxy)
             :updatedBy            (.getUpdatedBy entity-proxy)
             :createTime           (.getCreateTime entity-proxy)
             :updateTime           (.getUpdateTime entity-proxy)
             :version              (.getVersion entity-proxy)
             :status               (some-> (.getStatus entity-proxy) (.name))
             :statusOnDelete       (some-> (.getStatusOnDelete entity-proxy) (.name))}
        (InstanceProperties->map type-guid instance-props)))))

(extend-type Relationship
  p/Datafiable
  (datafy [^Relationship relationship]
    (let [instance-props (or (.getProperties relationship) (InstanceProperties.))
          type-guid      (some-> (.getType relationship) (.getTypeDefGUID))]
      (merge
        #:openmetadata.Relationship
            {:guid                 (.getGUID relationship)
             :type                 (some-> (.getType relationship) (.getTypeDefGUID))
             :entityOne            (some-> (.getEntityOneProxy relationship) (.getGUID))
             :entityTwo            (some-> (.getEntityTwoProxy relationship) (.getGUID))
             :metadataCollectionId (.getMetadataCollectionId relationship)
             :createdBy            (.getCreatedBy relationship)
             :updatedBy            (.getUpdatedBy relationship)
             :createTime           (.getCreateTime relationship)
             :updateTime           (.getUpdateTime relationship)
             :version              (.getVersion relationship)
             :status               (some-> (.getStatus relationship) (.name))
             :statusOnDelete       (some-> (.getStatusOnDelete relationship) (.name))}
        (InstanceProperties->map type-guid instance-props)))))

(defn TypeDef->map
  [^TypeDef obj]
  #:openmetadata.TypeDef
      {:guid                     (.getGUID obj)
       :name                     (.getName obj)
       :status                   (some-> (.getStatus obj) (.name))
       :version                  (.getVersion obj)
       :versionName              (.getVersionName obj)
       :category                 (some-> (.getCategory obj) (.name))
       :superType                (some-> (.getSuperType obj) (.getGUID))
       :description              (.getDescription obj)
       :descriptionGUID          (.getDescriptionGUID obj)
       :origin                   (.getOrigin obj)
       :createdBy                (.getCreatedBy obj)
       :updatedBy                (.getUpdatedBy obj)
       :createTime               (.getCreateTime obj)
       :updateTime               (.getUpdateTime obj)
       :options                  (some-> (.getOptions obj))
       :externalStandardMappings (some->> (.getExternalStandardMappings obj)
                                   (mapv datafy))
       :validInstanceStatusList  (some->> (.getValidInstanceStatusList obj)
                                   (mapv #(.name %)))
       :initialStatus            (some-> (.getInitialStatus obj) (.name))
       :propertiesDefinition     (some->> (.getPropertiesDefinition obj)
                                   (mapv datafy))})

(defn AttributeTypeDef->map
  [^AttributeTypeDef obj]
  #:openmetadata.AttributeTypeDef
      {:version         (.getVersion obj)
       :versionName     (.getVersionName obj)
       :category        (some-> (.getCategory obj) (.name))
       :guid            (.getGUID obj)
       :name            (.getName obj)
       :description     (.getDescription obj)
       :descriptionGUID (.getDescriptionGUID obj)})

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

(extend-type TypeDef
  p/Datafiable
  (datafy [^TypeDef obj]
    (TypeDef->map obj)))

(extend-type EntityDef
  p/Datafiable
  (datafy [^EntityDef obj]
    (TypeDef->map obj)))

(extend-type RelationshipDef
  p/Datafiable
  (datafy [^RelationshipDef obj]
    (merge (TypeDef->map obj)
      #:openmetadata.RelationshipDef
          {:propagationRule                 (some-> (.getPropagationRule obj) (.name))

           :endDef1EntityType               (some-> (.getEndDef1 obj) (.getEntityType) (.getGUID))
           :endDef1AttributeName            (some-> (.getEndDef1 obj) (.getAttributeName))
           :endDef1AttributeDescription     (some-> (.getEndDef1 obj) (.getAttributeDescription))
           :endDef1AttributeDescriptionGUID (some-> (.getEndDef1 obj) (.getAttributeDescriptionGUID))
           :endDef1AttributeCardinality     (some-> (.getEndDef1 obj) (.getAttributeCardinality) (.name))

           :endDef2EntityType               (some-> (.getEndDef2 obj) (.getEntityType) (.getGUID))
           :endDef2AttributeName            (some-> (.getEndDef2 obj) (.getAttributeName))
           :endDef2AttributeDescription     (some-> (.getEndDef2 obj) (.getAttributeDescription))
           :endDef2AttributeDescriptionGUID (some-> (.getEndDef2 obj) (.getAttributeDescriptionGUID))
           :endDef2AttributeCardinality     (some-> (.getEndDef2 obj) (.getAttributeCardinality) (.name))})))

(extend-type ClassificationDef
  p/Datafiable
  (datafy [^ClassificationDef obj]
    (merge (TypeDef->map obj)
      #:openmetadata.ClassificationDef
          {:validEntityDefs (some->> (.getValidEntityDefs obj)
                              (mapv #(.getGUID %)))
           :propagatable    (.isPropagatable obj)})))

(extend-type PrimitiveDefCategory
  p/Datafiable
  (datafy [^PrimitiveDefCategory obj]
    #:openmetadata.PrimitiveDefCategory
        {:guid          (.getGUID obj)
         :name          (.getName obj)
         :ordinal       (.getOrdinal obj)
         :javaClassName (.getJavaClassName obj)}))

(extend-type PrimitiveDef
  p/Datafiable
  (datafy [^PrimitiveDef obj]
    (merge (AttributeTypeDef->map obj)
      #:openmetadata.PrimitiveDef
          {:primitiveDefCategory (some-> (.getPrimitiveDefCategory obj) (.name))})))

(extend-type CollectionDefCategory
  p/Datafiable
  (datafy [^CollectionDefCategory obj]
    #:openmetadata.CollectionDefCategory
        {:ordinal       (.getOrdinal obj)
         :name          (.getName obj)
         :argumentCount (.getArgumentCount obj)
         :javaClassName (.getJavaClassName obj)}))

(extend-type CollectionDef
  p/Datafiable
  (datafy [^CollectionDef obj]
    (merge (AttributeTypeDef->map obj)
      #:openmetadata.CollectionDef
          {:collectionDefCategory (some-> (.getCollectionDefCategory obj) (.name))
           :argumentCount         (.getArgumentCount obj)
           :argumentTypes         (some->> (.getArgumentTypes obj)
                                    (mapv #(.name %)))})))

(extend-type EnumElementDef
  p/Datafiable
  (datafy [^EnumElementDef obj]
    #:openmetadata.EnumElementDef
        {:ordinal         (.getOrdinal obj)
         :value           (.getValue obj)
         :description     (.getDescription obj)
         :descriptionGUID (.getDescriptionGUID obj)}))

(extend-type EnumDef
  p/Datafiable
  (datafy [^EnumDef obj]
    (merge (AttributeTypeDef->map ^AttributeTypeDef obj)
      #:openmetadata.EnumDef
          {:defaultValue (some-> (.getDefaultValue obj) (.getValue))
           :elementDefs  (some->> (.getElementDefs obj) (mapv datafy))})))

(extend-type TypeDefAttribute
  p/Datafiable
  (datafy [^TypeDefAttribute obj]
    #:openmetadata.TypeDefAttribute
        {:attributeName            (.getAttributeName obj)
         :attributeType            (some-> (.getAttributeType obj) (.getGUID))
         :attributeStatus          (some-> (.getAttributeStatus obj) (.name))
         :replacedByAttribute      (.getReplacedByAttribute obj)
         :attributeDescription     (.getAttributeDescription obj)
         :attributeDescriptionGUID (.getAttributeDescriptionGUID obj)
         :cardinality              (some-> (.getAttributeCardinality obj) (.name))
         :valuesMinCount           (.getValuesMinCount obj)
         :valuesMaxCount           (.getValuesMaxCount obj)
         :isIndexable              (.isIndexable obj)
         :isUnique                 (.isUnique obj)
         :defaultValue             (.getDefaultValue obj)
         :externalStandardMappings (some->> (.getExternalStandardMappings obj)
                                     (mapv datafy))}))

(extend-type ExternalStandardMapping
  p/Datafiable
  (datafy [^ExternalStandardMapping obj]
    #:openmetadata.ExternalStandardMapping
        {:standardName         (.getStandardName obj)
         :standardOrganization (.getStandardOrganization obj)
         :standardTypeName     (.getStandardTypeName obj)}))

(defn ->TypeDefLink [guid]
  (let [repo-helper  *repo-helper*
        type-def-obj (.getTypeDef repo-helper
                       "unknown"
                       "guid"
                       guid,
                       "->TypeDefLink")]
    (doto (TypeDefLink.)
      (.setName (.getName type-def-obj))
      (.setGUID (.getGUID type-def-obj))
      (.setStatus (.getStatus type-def-obj))
      (.setReplacedByTypeGUID (.getReplacedByTypeGUID type-def-obj))
      (.setReplacedByTypeName (.getReplacedByTypeName type-def-obj)))))

(defn map->ExternalStandardMapping [m]
  (let [{:openmetadata.ExternalStandardMapping/keys [standardName
                                                     standardOrganization
                                                     standardTypeName]} m]
    (doto (ExternalStandardMapping.)
      (.setStandardName standardName)
      (.setStandardOrganization standardOrganization)
      (.setStandardTypeName standardTypeName))))

(defn set-attribute-type-def-fields [^AttributeTypeDef obj m]
  (let [{:openmetadata.AttributeTypeDef/keys [version
                                              versionName
                                              category
                                              guid
                                              name
                                              description
                                              descriptionGUID]} m
        category (some-> category (AttributeTypeDefCategory/valueOf))]
    (doto obj
      (.setName name)
      (.setVersion version)
      (.setVersionName versionName)
      (.setCategory category)
      (.setGUID guid)
      (.setDescription description)
      (.setDescriptionGUID descriptionGUID))))

(defn map->PrimitiveDef [m]
  (let [primitiveDefCategory (:openmetadata.PrimitiveDef/primitiveDefCategory m)
        primitiveDefCategory (PrimitiveDefCategory/valueOf primitiveDefCategory)
        obj                  (PrimitiveDef.)]
    (set-attribute-type-def-fields obj m)
    (doto obj
      (.setPrimitiveDefCategory primitiveDefCategory))))

(defn map->CollectionDef [m]
  (let [{:openmetadata.CollectionDef/keys [collectionDefCategory
                                           argumentCount
                                           argumentTypes]} m
        collectionDefCategory (some-> collectionDefCategory (CollectionDefCategory/valueOf))
        argumentTypes         (->> argumentTypes
                                (map #(PrimitiveDefCategory/valueOf %))
                                (into [])
                                (doall))
        obj                   (CollectionDef.)]
    (set-attribute-type-def-fields obj m)
    (doto obj
      (.setCollectionDefCategory collectionDefCategory)
      (.setArgumentCount argumentCount)
      (.setArgumentTypes argumentTypes))))

(defn map->EnumElementDef [m]
  (let [{:openmetadata.EnumElementDef/keys [ordinal
                                            value
                                            description
                                            descriptionGUID]} m]
    (doto (EnumElementDef.)
      (.setOrdinal ordinal)
      (.setValue value)
      (.setDescription description)
      (.setDescriptionGUID descriptionGUID))))

(defn map->EnumDef [m]
  (let [{:openmetadata.EnumDef/keys [defaultValue
                                     elementDefs]} m
        defaultValue (when defaultValue
                       (->> elementDefs
                         (filter #(= (:openmetadata.EnumElementDef/value %) defaultValue))
                         (first)
                         map->EnumElementDef))
        elementDefs  (doall (map map->EnumElementDef elementDefs))
        obj          (EnumDef.)]
    (set-attribute-type-def-fields obj m)
    (doto obj
      (.setDefaultValue defaultValue)
      (.setElementDefs elementDefs))))

(defn map->AttributeTypeDef [m]
  (let [category (:openmetadata.AttributeTypeDef/category m)]
    (case category
      "PRIMITIVE" (map->PrimitiveDef m)
      "COLLECTION" (map->CollectionDef m)
      "ENUM_DEF" (map->EnumDef m))))

(defn map->TypeDefAttribute [m]
  (let [{:openmetadata.TypeDefAttribute/keys [attributeName
                                              attributeType
                                              attributeStatus
                                              replacedByAttribute
                                              attributeDescription
                                              attributeDescriptionGUID
                                              cardinality
                                              valuesMinCount
                                              valuesMaxCount
                                              isIndexable
                                              isUnique
                                              defaultValue
                                              externalStandardMappings]} m
        attributeType            (some-> attributeType
                                   find-attribute-type-def-by-guid
                                   map->AttributeTypeDef)
        attributeStatus          (some-> attributeStatus (TypeDefAttributeStatus/valueOf))
        cardinality              (some-> cardinality (AttributeCardinality/valueOf))
        externalStandardMappings (some->> externalStandardMappings (map map->ExternalStandardMapping))
        ]
    (doto (TypeDefAttribute.)
      (.setAttributeName attributeName)
      (.setAttributeType attributeType)
      (.setAttributeStatus attributeStatus)
      (.setReplacedByAttribute replacedByAttribute)
      (.setAttributeDescription attributeDescription)
      (.setAttributeDescriptionGUID attributeDescriptionGUID)
      (.setAttributeCardinality cardinality)
      (.setValuesMinCount valuesMinCount)
      (.setValuesMaxCount valuesMaxCount)
      (.setIndexable isIndexable)
      (.setUnique isUnique)
      (.setDefaultValue defaultValue)
      (.setExternalStandardMappings externalStandardMappings))))

(defn set-type-def-fields [obj m]
  (let [{:openmetadata.TypeDef/keys [guid
                                     name
                                     status
                                     version
                                     versionName
                                     replacedByTypeGUID
                                     replacedByTypeName
                                     category
                                     superType
                                     description
                                     descriptionGUID
                                     origin
                                     createdBy
                                     updatedBy
                                     createTime
                                     updateTime
                                     options
                                     externalStandardMappings
                                     validInstanceStatusList
                                     initialStatus
                                     propertiesDefinition]} m
        status                   (some-> status (TypeDefStatus/valueOf))
        category                 (some-> category (TypeDefCategory/valueOf))
        superType                (when superType
                                   (->TypeDefLink superType))
        externalStandardMappings (some->> externalStandardMappings (map map->ExternalStandardMapping))
        validInstanceStatusList  (some->> validInstanceStatusList (map #(InstanceStatus/valueOf %)))
        initialStatus            (some-> initialStatus (InstanceStatus/valueOf))
        propertiesDefinition     (some->> propertiesDefinition (map map->TypeDefAttribute))]
    (doto obj
      (.setGUID guid)
      (.setName name)
      (.setStatus status)
      (.setVersion version)
      (.setVersionName versionName)
      (.setReplacedByTypeGUID replacedByTypeGUID)
      (.setReplacedByTypeName replacedByTypeName)
      (.setCategory category)
      (.setSuperType superType)
      (.setDescription description)
      (.setDescriptionGUID descriptionGUID)
      (.setOrigin origin)
      (.setCreatedBy createdBy)
      (.setCreateTime createTime)
      (.setUpdatedBy updatedBy)
      (.setUpdateTime updateTime)
      (.setOptions options)
      (.setExternalStandardMappings externalStandardMappings)
      (.setValidInstanceStatusList validInstanceStatusList)
      (.setInitialStatus initialStatus)
      (.setPropertiesDefinition propertiesDefinition))))

(defn map->EntityDef [m]
  (let [obj (EntityDef.)]
    (set-type-def-fields obj m)))

(defn map->RelationshipDef [m]
  (let [{:openmetadata.RelationshipDef/keys [propagationRule
                                             endDef1EntityType
                                             endDef1AttributeName
                                             endDef1AttributeDescription
                                             endDef1AttributeDescriptionGUID
                                             endDef1AttributeCardinality
                                             endDef2EntityType
                                             endDef2AttributeName
                                             endDef2AttributeDescription
                                             endDef2AttributeDescriptionGUID
                                             endDef2AttributeCardinality]} m
        endDef1AttributeCardinality (some-> endDef1AttributeCardinality (RelationshipEndCardinality/valueOf))
        endDef1TypeDefLink          (->TypeDefLink endDef1EntityType)
        endDef2AttributeCardinality (some-> endDef2AttributeCardinality (RelationshipEndCardinality/valueOf))
        endDef2TypeDefLink          (->TypeDefLink endDef2EntityType)
        endDef1                     (doto (RelationshipEndDef.)
                                      (.setEntityType endDef1TypeDefLink)
                                      (.setAttributeName endDef1AttributeName)
                                      (.setAttributeDescription endDef1AttributeDescription)
                                      (.setAttributeDescriptionGUID endDef1AttributeDescriptionGUID)
                                      (.setAttributeCardinality endDef1AttributeCardinality))
        endDef2                     (doto (RelationshipEndDef.)
                                      (.setEntityType endDef2TypeDefLink)
                                      (.setAttributeName endDef2AttributeName)
                                      (.setAttributeDescription endDef2AttributeDescription)
                                      (.setAttributeDescriptionGUID endDef2AttributeDescriptionGUID)
                                      (.setAttributeCardinality endDef2AttributeCardinality))
        propagationRule             (some-> propagationRule (ClassificationPropagationRule/valueOf))
        obj                         (RelationshipDef.)]
    (set-type-def-fields obj m)
    (doto obj
      (.setEndDef1 endDef1)
      (.setEndDef2 endDef2)
      (.setPropagationRule propagationRule))))

(defn map->ClassificationDef [m]
  (let [{:openmetadata.ClassificationDef/keys [validEntityDefs
                                               propagatable]} m
        validEntityDefs (->> validEntityDefs
                          (map ->TypeDefLink))
        obj             (ClassificationDef.)]
    (set-type-def-fields obj m)
    (doto obj
      (.setValidEntityDefs validEntityDefs)
      (.setPropagatable propagatable))))

(defn map->TypeDef [m]
  (let [category (:openmetadata.TypeDef/category m)]
    (case category
      "ENTITY_DEF" (map->EntityDef m)
      "RELATIONSHIP_DEF" (map->RelationshipDef m)
      "CLASSIFICATION_DEF" (map->ClassificationDef m))))

(defn map->InstanceType [type-def]
  (doto (InstanceType.)
    (.setTypeDefGUID (:openmetadata.TypeDef/guid type-def))
    (.setTypeDefName (:openmetadata.TypeDef/name type-def))
    (.setTypeDefCategory (some-> (:openmetadata.TypeDef/category type-def)
                           (TypeDefCategory/valueOf)))
    (.setTypeDefDescription (:openmetadata.TypeDef/description type-def))
    (.setTypeDefDescriptionGUID (:openmetadata.TypeDef/descriptionGUID type-def))
    (.setTypeDefVersion (:openmetadata.TypeDef/version type-def))))

(defn map->InstanceProperties [type-def m]
  (let [prop-keys (list-type-def-property-keys type-def)
        obj       (InstanceProperties.)]
    (doseq [k prop-keys]
      (let [type-def-attribute (find-type-def-attribute-by-property-key k)
            value              (or (when (qualified-keyword? k) (k m))
                                 (get m (keyword (name k))))
            attribute-type-def (find-attribute-type-def-by-guid (:openmetadata.TypeDefAttribute/attributeType type-def-attribute))
            property-value     (->InstancePropertyValue attribute-type-def value)]
        (.setProperty obj (name k) property-value)))
    obj))

(defn map->Classification [m]
  (let [{:openmetadata.Classification/keys
         [name
          origin
          originGUID
          type
          instanceProvenanceType
          metadataCollectionName
          metadataCollectionId
          version
          maintainedBy
          replicatedBy
          instanceLicense
          createdBy
          updatedBy
          createTime
          updateTime
          status
          statusOnDelete
          mappingProperties]} m
        type-def       (when type
                         (find-type-def-by-guid type))
        inst-type      (map->InstanceType type-def)
        inst-prov-type (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                         InstanceProvenanceType/UNKNOWN)
        inst-props     (map->InstanceProperties type-def m)]
    (doto (Classification.)
      (.setName name)
      (.setType inst-type)
      (.setInstanceProvenanceType inst-prov-type)
      (.setClassificationOrigin (some-> origin (ClassificationOrigin/valueOf)))
      (.setClassificationOriginGUID originGUID)
      (.setProperties inst-props)
      (.setMetadataCollectionId metadataCollectionId)
      (.setMetadataCollectionName metadataCollectionName)
      (.setVersion (or version 1))
      (.setMaintainedBy (some-> maintainedBy (LinkedList.)))
      (.setReplicatedBy replicatedBy)
      (.setInstanceLicense instanceLicense)
      (.setCreatedBy createdBy)
      (.setUpdatedBy updatedBy)
      (.setCreateTime createTime)
      (.setUpdateTime updateTime)
      (.setStatus (some-> status (InstanceStatus/valueOf)))
      (.setStatusOnDelete (some-> statusOnDelete (InstanceStatus/valueOf)))
      (.setMappingProperties mappingProperties))))


(defn map->EntityDetail [m]
  (let [{:openmetadata.Entity/keys
         [guid
          type
          instanceProvenanceType
          instanceURL
          metadataCollectionName
          metadataCollectionId
          version
          maintainedBy
          replicatedBy
          instanceLicense
          createdBy
          updatedBy
          createTime
          updateTime
          status
          statusOnDelete
          mappingProperties
          classifications]} m
        type-def               (when type
                                 (find-type-def-by-guid type))
        inst-type              (map->InstanceType type-def)
        instanceProvenanceType (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                                 InstanceProvenanceType/UNKNOWN)
        status                 (some-> status (InstanceStatus/valueOf))
        statusOnDelete         (some-> statusOnDelete (InstanceStatus/valueOf))
        guid                   (or guid (str (UUID/randomUUID)))
        instance-properties    (map->InstanceProperties type-def m)
        classifications        (map #(map->Classification %) classifications)]
    (doto (EntityDetail.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType instanceProvenanceType)
      (.setInstanceURL instanceURL)
      (.setProperties instance-properties)
      (.setMetadataCollectionId metadataCollectionId)
      (.setMetadataCollectionName metadataCollectionName)
      (.setVersion version)
      (.setMaintainedBy maintainedBy)
      (.setReplicatedBy replicatedBy)
      (.setInstanceLicense instanceLicense)
      (.setCreatedBy createdBy)
      (.setUpdatedBy updatedBy)
      (.setCreateTime createTime)
      (.setUpdateTime updateTime)
      (.setStatus status)
      (.setStatusOnDelete statusOnDelete)
      (.setMappingProperties mappingProperties)
      (.setClassifications classifications))))


(defn map->EntitySummary [m]
  (let [{:openmetadata.Entity/keys
         [guid
          type
          instanceProvenanceType
          instanceURL
          metadataCollectionName
          metadataCollectionId
          version
          maintainedBy
          replicatedBy
          instanceLicense
          createdBy
          updatedBy
          createTime
          updateTime
          status
          statusOnDelete
          mappingProperties
          classifications]} m
        type-def               (when type
                                 (find-type-def-by-guid type))
        instance-type          (map->InstanceType type-def)
        status                 (some-> status (InstanceStatus/valueOf))
        statusOnDelete         (some-> statusOnDelete (InstanceStatus/valueOf))
        instanceProvenanceType (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))

        classifications        (map #(map->Classification %) classifications)]
    (doto (EntitySummary.)
      (.setGUID guid)
      (.setType instance-type)
      (.setInstanceProvenanceType instanceProvenanceType)
      (.setInstanceURL instanceURL)
      (.setMetadataCollectionId metadataCollectionId)
      (.setMetadataCollectionName metadataCollectionName)
      (.setVersion version)
      (.setMaintainedBy maintainedBy)
      (.setReplicatedBy replicatedBy)
      (.setInstanceLicense instanceLicense)
      (.setCreatedBy createdBy)
      (.setUpdatedBy updatedBy)
      (.setCreateTime createTime)
      (.setUpdateTime updateTime)
      (.setStatus status)
      (.setStatusOnDelete statusOnDelete)
      (.setMappingProperties mappingProperties)
      (.setClassifications (LinkedList. classifications)))))


(defn map->EntityProxy [m]
  (let [{:openmetadata.Entity/keys
         [guid
          type
          instanceProvenanceType
          instanceURL
          metadataCollectionName
          metadataCollectionId
          version
          maintainedBy
          replicatedBy
          instanceLicense
          createdBy
          updatedBy
          createTime
          updateTime
          status
          statusOnDelete
          mappingProperties
          classifications]} m
        type-def               (when type
                                 (find-type-def-by-guid type))
        inst-type              (map->InstanceType type-def)
        instanceProvenanceType (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
        inst-props             (map->InstanceProperties type-def m)
        status                 (some-> status (InstanceStatus/valueOf))
        statusOnDelete         (some-> statusOnDelete (InstanceStatus/valueOf))
        classifications        (map #(map->Classification %) classifications)]
    (doto (EntityProxy.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType instanceProvenanceType)
      (.setInstanceURL instanceURL)
      (.setUniqueProperties inst-props)
      (.setMetadataCollectionId metadataCollectionId)
      (.setMetadataCollectionName metadataCollectionName)
      (.setVersion version)
      (.setMaintainedBy maintainedBy)
      (.setReplicatedBy replicatedBy)
      (.setInstanceLicense instanceLicense)
      (.setCreatedBy createdBy)
      (.setUpdatedBy updatedBy)
      (.setCreateTime createTime)
      (.setUpdateTime updateTime)
      (.setStatus status)
      (.setStatusOnDelete statusOnDelete)
      (.setMappingProperties mappingProperties)
      (.setClassifications classifications))))


(defn map->Relationship [r e1 e2]
  (let [{:openmetadata.Relationship/keys
         [guid
          type
          instanceProvenanceType
          instanceURL
          metadataCollectionName
          metadataCollectionId
          version
          maintainedBy
          replicatedBy
          instanceLicense
          createdBy
          updatedBy
          createTime
          updateTime
          status
          statusOnDelete
          mappingProperties
          entityOne
          entityTwo]} r
        type-def               (when type
                                 (find-type-def-by-guid type))
        inst-type              (map->InstanceType type-def)
        instanceProvenanceType (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
        instance-properties    (map->InstanceProperties type-def r)
        status                 (some-> status (InstanceStatus/valueOf))
        statusOnDelete         (some-> statusOnDelete (InstanceStatus/valueOf))
        entity-proxy-one       (map->EntityProxy e1)
        entity-proxy-two       (map->EntityProxy e2)]
    (doto (Relationship.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType instanceProvenanceType)
      (.setInstanceURL instanceURL)
      (.setProperties instance-properties)
      (.setMetadataCollectionId metadataCollectionId)
      (.setMetadataCollectionName metadataCollectionName)
      (.setVersion version)
      (.setMaintainedBy maintainedBy)
      (.setReplicatedBy replicatedBy)
      (.setInstanceLicense instanceLicense)
      (.setCreatedBy createdBy)
      (.setUpdatedBy updatedBy)
      (.setCreateTime createTime)
      (.setUpdateTime updateTime)
      (.setStatus status)
      (.setStatusOnDelete statusOnDelete)
      (.setMappingProperties mappingProperties)
      (.setEntityOneProxy entity-proxy-one)
      (.setEntityTwoProxy entity-proxy-two))))
