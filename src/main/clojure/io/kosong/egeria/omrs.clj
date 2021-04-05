(ns io.kosong.egeria.omrs
  (:require [clojure.string :as str])
  (:import
    (java.util Collections LinkedList UUID)
    (org.odpi.openmetadata.repositoryservices.localrepository.repositorycontentmanager OMRSRepositoryContentHelper OMRSRepositoryContentManager OMRSRepositoryContentValidator)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceProvenanceType InstanceProperties PrimitivePropertyValue MapPropertyValue EnumPropertyValue ArrayPropertyValue EntityDetail InstanceType InstanceStatus EntityProxy Classification ClassificationOrigin Relationship EntitySummary)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs PrimitiveDef PrimitiveDefCategory CollectionDefCategory CollectionDef EnumElementDef ClassificationDef AttributeTypeDef TypeDef TypeDefAttribute EntityDef RelationshipDef EnumDef ExternalStandardMapping TypeDefAttribute EnumElementDef TypeDefPatch TypeDefCategory)
    (org.odpi.openmetadata.opentypes OpenMetadataTypesArchive)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.auditlogstore OMRSAuditLogRecord OMRSAuditLogStore)
    (org.odpi.openmetadata.repositoryservices.auditlog OMRSAuditLogDestination OMRSAuditLog)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.archivestore.properties OpenMetadataArchiveTypeStore OpenMetadataArchive)
    (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.utilities OMRSRepositoryPropertiesUtilities)))

(defprotocol Mappable
  (->map [obj]))


(def find-type-def-by-guid
  (fn [^OMRSRepositoryContentHelper repo-helper type-def-guid]
    (if-let [type-def (.getTypeDef repo-helper "omrs" "guid" type-def-guid "find-type-def-by-guid")]
      (->map type-def))))


(def find-type-def-by-name
  (fn [^OMRSRepositoryContentHelper repo-helper type-def-name]
    (if-let [type-def (.getTypeDefByName repo-helper "omrs" type-def-name)]
      (->map type-def))))


(def find-type-def
  (fn [^OMRSRepositoryContentHelper repo-helper guid-or-name]
    (let [type-def-guid (try
                          (UUID/fromString guid-or-name)
                          (catch IllegalArgumentException _))
          type-def-name (when-not type-def-guid
                          guid-or-name)]
      (if type-def-guid
        (find-type-def-by-guid repo-helper type-def-guid)
        (find-type-def-by-name repo-helper type-def-name)))))


(defn find-attribute-type-def-by-guid [^OMRSRepositoryContentHelper repo-helper
                                       attribute-type-guid]
  (->map (.getAttributeTypeDef repo-helper
           "omrs-source-name"
           attribute-type-guid
           "find-attribute-type-def-by-guid")))

(defn find-type-def-attribute-by-property-key
  [^OMRSRepositoryContentHelper repo-helper property-key]
  (let [type-def-name  (namespace property-key)
        type-def-name  (if (str/starts-with? type-def-name "openmetadata.")
                         (.substring type-def-name (.length "openmetadata."))
                         type-def-name)
        attribute-name (name property-key)
        type-def       (find-type-def-by-name repo-helper type-def-name)]
    (->> (:openmetadata.TypeDef/propertiesDefinition type-def)
      (filter #(= (:openmetadata.TypeDefAttribute/attributeName %) attribute-name))
      (first))))


(defn list-type-def-attributes
  "Resolves properties of the given type-def and its super type. Returns a sequence of type def attributes."
  [^OMRSRepositoryContentHelper repo-helper type-def]
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
      attrs)))

(defn qualify-property-key [attr]
  (let [ns   (str "openmetadata." (:openmetadata.TypeDef/name attr))
        name (:openmetadata.TypeDefAttribute/attributeName attr)]
    (keyword ns name)))

(defn list-type-def-property-keys
  [^OMRSRepositoryContentHelper repo-helper type-def]
  (let [attrs (list-type-def-attributes repo-helper type-def)]
    (map qualify-property-key attrs)))

(defn list-type-defs
  [^OMRSRepositoryContentHelper repo-helper]
  (map ->map (.getKnownTypeDefs repo-helper)))

(defn ->EntityDetail
  ([^OMRSRepositoryContentHelper repo-helper source-name metadata-collection-id user-name type-name]
   (->EntityDetail repo-helper source-name metadata-collection-id user-name type-name
     (InstanceProperties.) Collections/EMPTY_LIST))
  ([^OMRSRepositoryContentHelper repo-helper source-name metadata-collection-id user-name type-name instance-props classifications]
   (let [entity-detail (.getNewEntity repo-helper
                         source-name
                         metadata-collection-id
                         InstanceProvenanceType/LOCAL_COHORT
                         user-name
                         type-name
                         instance-props
                         classifications)]
     entity-detail)))

(defmulti ->InstancePropertyValue (fn [repo_helper attr-type-def value]
                                    (:openmetadata.AttributeTypeDef/category attr-type-def)))

(defmethod ->InstancePropertyValue "PRIMITIVE" [repo_helper attr-type-def value]
  (let [primitive-category (-> (:openmetadata.PrimitiveDef/category attr-type-def)
                             (PrimitiveDefCategory/valueOf))]
    (doto (PrimitivePropertyValue.)
      (.setPrimitiveValue value)
      (.setPrimitiveDefCategory primitive-category)
      (.setTypeName (.getName primitive-category))
      (.setTypeGUID (.getGUID primitive-category)))))

(defn ->ArrayPropertyValue [repo-helper coll-type-def array-values]
  (let [size                  (count array-values)
        obj                   (doto (ArrayPropertyValue.)
                                (.setArrayCount size))
        element-type-def-guid (-> (:openmetadata.CollectionDef/argumentTypes coll-type-def)
                                (first))
        element-type-def      (find-attribute-type-def-by-guid repo-helper element-type-def-guid)]
    (when array-values
      (do
        (doseq [[i v] (map vector (range size) array-values)]
          (.setArrayValue i (->InstancePropertyValue repo-helper element-type-def v)))
        obj))))

(defn ->MapPropertyValue [repo-helper coll-type-def map-value]
  (let [obj                   (MapPropertyValue.)
        element-type-def-guid (-> (:openmetadata.CollectionDef/argumentTypes coll-type-def)
                                (second))
        value-type-def        (find-attribute-type-def-by-guid repo-helper element-type-def-guid)]
    (doseq [[k v] map-value]
      (.setMapValue obj (str k) (->InstancePropertyValue repo-helper value-type-def v)))
    obj))

(defmethod ->InstancePropertyValue "COLLECTION" [repo-helper attr-type-def value]
  (let [category (some-> (:openmetadata.CollectionDef/category attr-type-def))]
    (condp = category
      "OM_COLLECTION_ARRAY" (->ArrayPropertyValue repo-helper attr-type-def value)
      "OM_COLLECTION_MAP" (->MapPropertyValue repo-helper attr-type-def value))))

(def find-enum-element-def
  (memoize
    (fn [enum-type-def enum-name]
      (some->> (:openmetadata.EnumDef/elementDefs enum-type-def)
        (filter (fn [x] (= (:openmetadata.EnumElementDef/value x) enum-name)))
        (first)))))

(defmethod ->InstancePropertyValue "ENUM_DEF" [repo-helper attr-type-def value]
  (let [value            (or value (:openmetadata.EnumDef/defaultValue attr-type-def))
        type-def-guid    (:openmetadata.AttributeTypeDef/guid attr-type-def)
        enum-type-def    (find-attribute-type-def-by-guid repo-helper type-def-guid)
        enum-element-def (find-enum-element-def enum-type-def value)]
    (when enum-element-def
      (let [{:openmetadata.EnumElementDef/keys [ordinal description value]} enum-element-def]
        (doto (EnumPropertyValue.)
          (.setOrdinal ^int ordinal)
          (.setDescription description)
          (.setSymbolicName value))))))

(defmethod ->InstancePropertyValue :default [repo-helper attr-type-def value]
  (println attr-type-def)
  (doto (MapPropertyValue.)))


(defn ->InstanceProperties
  ([^OMRSRepositoryContentHelper repo-helper type-def-name properties]
   (let [type-def  (find-type-def-by-name repo-helper type-def-name)
         prop-keys (list-type-def-property-keys repo-helper type-def)
         obj       (InstanceProperties.)]
     (doseq [k prop-keys]
       (let [type-def-attribute (find-type-def-attribute-by-property-key repo-helper k)
             value              (or (when (qualified-keyword? k) (k properties))
                                  (get properties (keyword (name k))))
             attribute-type-def (find-attribute-type-def-by-guid repo-helper (:openmetadata.TypeDefAttribute/attributeType type-def-attribute))
             property-value     (->InstancePropertyValue repo-helper attribute-type-def value)]
         (.setProperty obj (name k) property-value)))
     obj)))

(defmulti InstancePropertyValue->val (fn [repo-helper property-value]
                                       (type property-value)))

(defmethod InstancePropertyValue->val nil [repo-helper property-value]
  nil)

(defmethod InstancePropertyValue->val PrimitivePropertyValue [repo-helper property-value]
  (.valueAsObject property-value))

(defmethod InstancePropertyValue->val ArrayPropertyValue [repo-helper property-value]
  (when (> (.getArrayCount property-value) 0)
    (some->> (.getArrayValues property-value)
      (.getInstanceProperties)
      (sort (fn [[k1 _] [k2 _]]
              (< (Integer/valueOf k1) (Integer/valueOf k2))))
      (mapv second))))

(defmethod InstancePropertyValue->val MapPropertyValue [repo-helper property-value]
  (when-let [instance-props (some-> (.getMapValues property-value) (.getInstanceProperties))]
    (println instance-props)
    (reduce
      (fn [m [k pv]]
        (assoc m k (InstancePropertyValue->val repo-helper pv)))
      {}
      instance-props)))

(defmethod InstancePropertyValue->val EnumPropertyValue [repo-helper property-value]
  (when property-value
    (.getSymbolicName property-value)))

(defn- collect-instance-map [repo-helper instance-props m qualified-key]
  (let [k  (name qualified-key)
        pv (.getPropertyValue instance-props k)
        v  (InstancePropertyValue->val repo-helper pv)]
    (assoc m qualified-key v)))

(defn InstanceProperties->map
  [^OMRSRepositoryContentHelper repo-helper instance-type-guid instance-props]
  (let [inst-type-def (find-type-def-by-guid repo-helper instance-type-guid)
        property-keys (list-type-def-property-keys repo-helper inst-type-def)]
    (reduce (partial collect-instance-map repo-helper instance-props) {} property-keys)))

(defn Classification->map
  [^OMRSRepositoryContentHelper repo-helper ^Classification classification]
  (let [instance-props (or (.getProperties classification) (InstanceProperties.))
        type-guid      (some-> (.getType classification) (.getTypeDefGUID))]
    (merge
      #:openmetadata.Classification
          {:typeDefGUID          (some-> (.getType classification) (.getTypeDefGUID))
           :typeDefName          (some-> (.getType classification) (.getTypeDefName))
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
      (InstanceProperties->map repo-helper type-guid instance-props))))

(defn EntityDetail->map
  [^OMRSRepositoryContentHelper repo-helper entity-detail]
  (let [instance-props (or (.getProperties entity-detail) (InstanceProperties.))
        type-guid      (some-> (.getType entity-detail) (.getTypeDefGUID))]
    (merge
      #:openmetadata.Entity
          {:guid                 (.getGUID entity-detail)
           :typeDefGUID          (some-> (.getType entity-detail) (.getTypeDefGUID))
           :typeDefName          (some-> (.getType entity-detail) (.getTypeDefName))
           :instanceURL          (.getInstanceURL entity-detail)
           :classifications      (some->> (.getClassifications entity-detail) (mapv (partial Classification->map repo-helper)))
           :metadataCollectionId (.getMetadataCollectionId entity-detail)
           :createdBy            (.getCreatedBy entity-detail)
           :updatedBy            (.getUpdatedBy entity-detail)
           :createTime           (.getCreateTime entity-detail)
           :updateTime           (.getUpdateTime entity-detail)
           :version              (.getVersion entity-detail)
           :status               (some-> (.getStatus entity-detail) (.name))
           :statusOnDelete       (some-> (.getStatusOnDelete entity-detail) (.name))}
      (InstanceProperties->map repo-helper type-guid instance-props))))

(defn EntityProxy->map
  [^OMRSRepositoryContentHelper repo-helper ^EntityProxy entity-proxy]
  (let [instance-props (or (.getUniqueProperties entity-proxy) (InstanceProperties.))
        type-guid      (some-> (.getType entity-proxy) (.getTypeDefGUID))]
    (merge
      #:openmetadata.Entity
          {:guid                 (.getGUID entity-proxy)
           :isProxy              true
           :typeDefGUID          (some-> (.getType entity-proxy) (.getTypeDefGUID))
           :typeDefName          (some-> (.getType entity-proxy) (.getTypeDefName))
           :instanceURL          (.getInstanceURL entity-proxy)
           :classifications      (some->> (.getClassifications entity-proxy) (mapv Classification->map))
           :metadataCollectionId (.getMetadataCollectionId entity-proxy)
           :createdBy            (.getCreatedBy entity-proxy)
           :updatedBy            (.getUpdatedBy entity-proxy)
           :createTime           (.getCreateTime entity-proxy)
           :updateTime           (.getUpdateTime entity-proxy)
           :version              (.getVersion entity-proxy)
           :status               (some-> (.getStatus entity-proxy) (.name))
           :statusOnDelete       (some-> (.getStatusOnDelete entity-proxy) (.name))}
      (InstanceProperties->map repo-helper type-guid instance-props))))

(defn Relationship->map
  [^OMRSRepositoryContentHelper repo-helper relationship]
  (let [instance-props (or (.getProperties relationship) (InstanceProperties.))
        type-guid      (some-> (.getType relationship) (.getTypeDefGUID))]
    (merge
      #:openmetadata.Relationship
          {:guid                 (.getGUID relationship)
           :typeDefGUID          (some-> (.getType relationship) (.getTypeDefGUID))
           :typeDefName          (some-> (.getType relationship) (.getTypeDefName))
           :entityOneGUID        (some-> (.getEntityOneProxy relationship) (.getGUID))
           :entityTwoGUID        (some-> (.getEntityTwoProxy relationship) (.getGUID))
           :metadataCollectionId (.getMetadataCollectionId relationship)
           :createdBy            (.getCreatedBy relationship)
           :updatedBy            (.getUpdatedBy relationship)
           :createTime           (.getCreateTime relationship)
           :updateTime           (.getUpdateTime relationship)
           :version              (.getVersion relationship)
           :status               (some-> (.getStatus relationship) (.name))
           :statusOnDelete       (some-> (.getStatusOnDelete relationship) (.name))}
      (InstanceProperties->map repo-helper type-guid instance-props))))

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
                                   (mapv ->map))
       :validInstanceStatusList  (some->> (.getValidInstanceStatusList obj)
                                   (mapv #(.name %)))
       :initialStatus            (some-> (.getInitialStatus obj) (.name))
       :propertiesDefinition     (some->> (.getPropertiesDefinition obj)
                                   (mapv ->map))})

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

(defn ^OpenMetadataArchive ->openmetadata-archive
  []
  (-> (OpenMetadataTypesArchive.)
    (.getOpenMetadataArchive)))

(defn ->console-audit-log-store
  []
  (proxy [OMRSAuditLogStore] []
    (storeLogRecord [^OMRSAuditLogRecord record]
      #_(println (str record)))))

(defn ->audit-log-store
  []
  (->console-audit-log-store))

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
        type-def-map       (atom {})]
    (.setOpenMetadataTypesOriginGUID repo-content-manager archive-guid)
    (doseq [type-def (.getNewTypeDefs archive-type-store)]
      (swap! type-def-map assoc (.getName type-def) type-def)
      (.addTypeDef repo-content-manager user-id type-def))
    (doseq [attribute-type-def (.getAttributeTypeDefs archive-type-store)]
      (.addAttributeTypeDef repo-content-manager user-id attribute-type-def))
    (doseq [^TypeDefPatch patch (.getTypeDefPatches archive-type-store)]
      (let [type-def-name     (.getTypeDefName patch)
            original-type-def (get @type-def-map type-def-name)]
        (if original-type-def
          (let [updated-type-def (.applyPatch repo-util "" original-type-def patch "")]
            (swap! type-def-map assoc type-def-name updated-type-def)
            (.updateTypeDef repo-content-manager "" updated-type-def))
          (println type-def-name))))))

(defn ^OMRSRepositoryContentManager ->repository-content-manager
  [{:keys [user-id
           audit-log
           archive]}]
  (doto (OMRSRepositoryContentManager. user-id audit-log)
    (init-repo-content-manager archive user-id)))

(extend-type TypeDef
  Mappable
  (->map [^TypeDef obj]
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
                                     (mapv ->map))
         :validInstanceStatusList  (some->> (.getValidInstanceStatusList obj)
                                     (mapv #(.name %)))
         :initialStatus            (.getInitialStatus obj)
         :propertiesDefinition     (some->> (.getPropertiesDefinition obj)
                                     (mapv ->map))}))

(extend-type EntityDef
  Mappable
  (->map [^EntityDef obj]
    (TypeDef->map ^TypeDef obj)))

(extend-type RelationshipDef
  Mappable
  (->map [^RelationshipDef obj]
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
  Mappable
  (->map [^ClassificationDef obj]
    (merge (TypeDef->map obj)
      #:openmetadata.ClassificationDef
          {:validEntityDefs (some->> (.getValidEntityDefs obj)
                              (mapv #(.getGUID %)))
           :propagatable    (.isPropagatable obj)})))

(extend-type PrimitiveDefCategory
  Mappable
  (->map [^PrimitiveDefCategory obj]
    #:openmetadata.PrimitiveDefCategory
        {:guid          (.getGUID obj)
         :name          (.getName obj)
         :ordinal       (.getOrdinal obj)
         :javaClassName (.getJavaClassName obj)}))

(extend-type PrimitiveDef
  Mappable
  (->map [^PrimitiveDef obj]
    (merge (AttributeTypeDef->map obj)
      #:openmetadata.PrimitiveDef
          {:category (some-> (.getPrimitiveDefCategory obj) (.name))})))

(extend-type CollectionDefCategory
  Mappable
  (->map [^CollectionDefCategory obj]
    #:openmetadata.CollectionDefCategory
        {:ordinal       (.getOrdinal obj)
         :name          (.getName obj)
         :argumentCount (.getArgumentCount obj)
         :javaClassName (.getJavaClassName obj)}))

(extend-type CollectionDef
  Mappable
  (->map [^CollectionDef obj]
    (merge (AttributeTypeDef->map obj)
      #:openmetadata.CollectionDef
          {:category      (some-> (.getCollectionDefCategory obj) (.name))
           :argumentCount (.getArgumentCount obj)
           :argumentTypes (some->> (.getArgumentTypes obj)
                            (mapv #(.getGUID %)))})))

(extend-type EnumElementDef
  Mappable
  (->map [^EnumElementDef obj]
    #:openmetadata.EnumElementDef
        {:ordinal         (.getOrdinal obj)
         :value           (.getValue obj)
         :description     (.getDescription obj)
         :descriptionGUID (.getDescriptionGUID obj)}))

(extend-type EnumDef
  Mappable
  (->map [^EnumDef obj]
    (merge (AttributeTypeDef->map ^AttributeTypeDef obj)
      #:openmetadata.EnumDef
          {:defaultValue (some-> (.getDefaultValue obj) (.getValue))
           :elementDefs  (some->> (.getElementDefs obj) (mapv #(->map %)))})))

(extend-type TypeDefAttribute
  Mappable
  (->map [^TypeDefAttribute obj]
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
                                     (mapv ->map))}))

(extend-type ExternalStandardMapping
  Mappable
  (->map [^ExternalStandardMapping obj]
    #:openmetadata.ExternalStandardMapping
        {:standardName         (.getStandardName obj)
         :standardOrganization (.getStandardOrganization obj)
         :standardTypeName     (.getStandardTypeName obj)}))

(defn map->InstanceType [repo-helper type-def]
  (doto (InstanceType.)
    (.setTypeDefGUID (:openmetadata.TypeDef/guid type-def))
    (.setTypeDefName (:openmetadata.TypeDef/name type-def))
    (.setTypeDefCategory (some-> (:openmetadata.TypeDef/category type-def)
                           (TypeDefCategory/valueOf)))
    (.setTypeDefDescription (:openmetadata.TypeDef/description type-def))
    (.setTypeDefDescriptionGUID (:openmetadata.TypeDef/descriptionGUID type-def))
    (.setTypeDefVersion (:openmetadata.TypeDef/version type-def))))

(defn map->InstanceProperties [repo-helper type-def m]
  (let [prop-keys (list-type-def-property-keys repo-helper type-def)
        obj       (InstanceProperties.)]
    (doseq [k prop-keys]
      (let [type-def-attribute (find-type-def-attribute-by-property-key repo-helper k)
            value              (or (when (qualified-keyword? k) (k m))
                                 (get m (keyword (name k))))
            attribute-type-def (find-attribute-type-def-by-guid repo-helper (:openmetadata.TypeDefAttribute/attributeType type-def-attribute))
            property-value     (->InstancePropertyValue repo-helper attribute-type-def value)]
        (.setProperty obj (name k) property-value)))
    obj))

(defn map->Classification [repo-helper m]
  (let [{:openmetadata.Classification/keys
         [name
          origin
          originGUID
          typeDefGUID
          typeDefName
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
        type-def       (if typeDefGUID
                         (find-type-def-by-guid repo-helper typeDefGUID)
                         (find-type-def-by-name repo-helper typeDefName))
        inst-type      (map->InstanceType repo-helper type-def)
        inst-prov-type (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                         InstanceProvenanceType/UNKNOWN)
        inst-props     (map->InstanceProperties repo-helper type-def m)]
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


(defn map->EntityDetail [repo-helper m]
  (let [{:openmetadata.Entity/keys
         [guid
          typeDefGUID
          typeDefName
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
        type-def       (if typeDefGUID
                         (find-type-def-by-guid repo-helper typeDefGUID)
                         (find-type-def-by-name repo-helper typeDefName))
        inst-type      (map->InstanceType repo-helper type-def)
        inst-prov-type (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                         InstanceProvenanceType/UNKNOWN)
        guid           (or guid (str (UUID/randomUUID)))
        inst-props     (map->InstanceProperties repo-helper type-def m)
        cls            (->> (or classifications [])
                         (map #(map->Classification repo-helper %)))]
    (doto (EntityDetail.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType inst-prov-type)
      (.setInstanceURL instanceURL)
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
      (.setMappingProperties mappingProperties)
      (.setClassifications (LinkedList. cls)))))


(defn map->EntitySummary [repo-helper m]
  (let [{:openmetadata.Entity/keys
         [guid
          typeDefGUID
          typeDefName
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
        type-def       (if typeDefGUID
                         (find-type-def-by-guid repo-helper typeDefGUID)
                         (find-type-def-by-name repo-helper typeDefName))
        inst-type      (map->InstanceType repo-helper type-def)
        inst-prov-type (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                         InstanceProvenanceType/UNKNOWN)
        guid           (or guid (str (UUID/randomUUID)))
        cls            (->> (or classifications [])
                         (map #(map->Classification repo-helper %)))]
    (doto (EntitySummary.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType inst-prov-type)
      (.setInstanceURL instanceURL)
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
      (.setMappingProperties mappingProperties)
      (.setClassifications (LinkedList. cls)))))


(defn map->EntityProxy [repo-helper m]
  (let [{:openmetadata.Entity/keys
         [guid
          typeDefGUID
          typeDefName
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
        type-def       (if typeDefGUID
                         (find-type-def-by-guid repo-helper typeDefGUID)
                         (find-type-def-by-name repo-helper typeDefName))
        inst-type      (map->InstanceType repo-helper type-def)
        inst-prov-type (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                         InstanceProvenanceType/UNKNOWN)
        guid           (or guid (str (UUID/randomUUID)))
        inst-props     (map->InstanceProperties repo-helper type-def m)
        cls            (->> (or classifications [])
                         (map #(map->Classification repo-helper %)))]
    (doto (EntityProxy.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType inst-prov-type)
      (.setInstanceURL instanceURL)
      (.setUniqueProperties inst-props)
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
      (.setMappingProperties mappingProperties)
      (.setClassifications (LinkedList. cls)))))


(defn map->Relationship [repo-helper r e1 e2]
  (let [{:openmetadata.Relationship/keys
         [guid
          typeDefGUID
          typeDefName
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
          entityOneGUID
          entityTwoGUID]} r
        type-def         (if typeDefGUID
                           (find-type-def-by-guid repo-helper typeDefGUID)
                           (find-type-def-by-name repo-helper typeDefName))
        inst-type        (map->InstanceType repo-helper type-def)
        inst-prov-type   (or (some-> instanceProvenanceType (InstanceProvenanceType/valueOf))
                           InstanceProvenanceType/UNKNOWN)
        guid             (or guid (str (UUID/randomUUID)))
        inst-props       (map->InstanceProperties repo-helper type-def r)
        entity-proxy-one (map->EntityProxy repo-helper e1)
        entity-proxy-two (map->EntityProxy repo-helper e2)]
    (doto (Relationship.)
      (.setGUID guid)
      (.setType inst-type)
      (.setInstanceProvenanceType inst-prov-type)
      (.setInstanceURL instanceURL)
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
      (.setMappingProperties mappingProperties)
      (.setEntityOneProxy entity-proxy-one)
      (.setEntityTwoProxy entity-proxy-two))))
