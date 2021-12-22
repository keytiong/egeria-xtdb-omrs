(ns io.kosong.egeria.omrs.datafy
  (:require [clojure.core.protocols :as p]
            [clojure.datafy :refer [datafy]]
            [io.kosong.egeria.omrs :as omrs])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances PrimitivePropertyValue ArrayPropertyValue MapPropertyValue EnumPropertyValue InstanceStatus)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs TypeDef EntityDef RelationshipDef ClassificationDef PrimitiveDef CollectionDef EnumElementDef EnumDef AttributeTypeDef PrimitiveDefCategory CollectionDefCategory TypeDefAttribute ExternalStandardMapping AttributeTypeDefCategory AttributeCardinality TypeDefAttributeStatus TypeDefStatus TypeDefCategory TypeDefLink ClassificationPropagationRule RelationshipEndDef RelationshipEndCardinality)))

;;
;; Egeria->Data mappings
;;

(def ^:private external-standard-mapping->map
  #:openmetadata.ExternalStandardMapping
      {:standardName         (fn [^ExternalStandardMapping o] (.getStandardName o))
       :standardOrganization (fn [^ExternalStandardMapping o] (.getStandardOrganization o))
       :standardTypeName     (fn [^ExternalStandardMapping o] (.getStandardTypeName o))})

(def ^:private enum-element-def->map
  #:openmetadata.EnumElementDef
      {:ordinal         (fn [^EnumElementDef o] (.getOrdinal o))
       :value           (fn [^EnumElementDef o] (.getValue o))
       :description     (fn [^EnumElementDef o] (.getDescription o))
       :descriptionGUID (fn [^EnumElementDef o] (.getDescriptionGUID o))})

(def ^:private attribute-type-def->map
  #:openmetadata.AttributeTypeDef
      {:version         (fn [^AttributeTypeDef o] (.getVersion o))
       :versionName     (fn [^AttributeTypeDef o] (.getVersionName o))
       :category        (fn [^AttributeTypeDef o] (some-> o .getCategory .name))
       :guid            (fn [^AttributeTypeDef o] (.getGUID o))
       :name            (fn [^AttributeTypeDef o] (.getName o))
       :description     (fn [^AttributeTypeDef o] (.getDescription o))
       :descriptionGUID (fn [^AttributeTypeDef o] (.getDescriptionGUID o))})

(def ^:private primitive-def->map
  (merge attribute-type-def->map
    #:openmetadata.PrimitiveDef
        {:primitiveDefCategory (fn [^PrimitiveDef o]
                                 (some-> o .getPrimitiveDefCategory .name))}))

(def ^:private collection-def->map
  (merge attribute-type-def->map
    #:openmetadata.CollectionDef
        {:collectionDefCategory (fn [^CollectionDef o] (some-> o .getCollectionDefCategory .name))
         :argumentCount         (fn [^CollectionDef o] (.getArgumentCount o))
         :argumentTypes         (fn [^CollectionDef o]
                                  (some->> (.getArgumentTypes o)
                                    (mapv #(.name %))))}))

(def ^:private enum-def->map
  (merge attribute-type-def->map
    #:openmetadata.EnumDef
        {:defaultValue (fn [^EnumDef o] (some-> o .getDefaultValue datafy))
         :elementDefs  (fn [^EnumDef o]
                         (some->> (.getElementDefs o)
                           (mapv datafy)))}))

(def ^:private type-def-attribute->map
  #:openmetadata.TypeDefAttribute
      {:attributeName            (fn [^TypeDefAttribute o] (.getAttributeName o))
       :attributeType            (fn [^TypeDefAttribute o] (some-> o .getAttributeType .getGUID))
       :attributeStatus          (fn [^TypeDefAttribute o] (some-> o .getAttributeStatus .name))
       :replacedByAttribute      (fn [^TypeDefAttribute o] (.getReplacedByAttribute o))
       :attributeDescription     (fn [^TypeDefAttribute o] (.getAttributeDescription o))
       :attributeDescriptionGUID (fn [^TypeDefAttribute o] (.getAttributeDescriptionGUID o))
       :cardinality              (fn [^TypeDefAttribute o] (some-> o .getAttributeCardinality .name))
       :valuesMinCount           (fn [^TypeDefAttribute o] (.getValuesMinCount o))
       :valuesMaxCount           (fn [^TypeDefAttribute o] (.getValuesMaxCount o))
       :isIndexable              (fn [^TypeDefAttribute o] (.isIndexable o))
       :isUnique                 (fn [^TypeDefAttribute o] (.isUnique o))
       :defaultValue             (fn [^TypeDefAttribute o] (.getDefaultValue o))
       :externalStandardMappings (fn [^TypeDefAttribute o] (some->> o .getExternalStandardMappings (mapv datafy)))})

(def ^:private type-def-link->map
  #:openmetadata.TypeDef
      {:guid               (fn [^TypeDefLink o] (.getGUID o))
       :name               (fn [^TypeDefLink o] (.getName o))
       :status             (fn [^TypeDefLink o] (some-> (.getStatus o) .name))
       :replacedByTypeGUID (fn [^TypeDefLink o] (.getReplacedByTypeGUID o))
       :replacedByTypeName (fn [^TypeDefLink o] (.getReplacedByTypeName o))})

(def ^:private type-def->map
  (merge type-def-link->map
    #:openmetadata.TypeDef
        {:version                  (fn [^TypeDef o] (.getVersion o))
         :versionName              (fn [^TypeDef o] (.getVersionName o))
         :category                 (fn [^TypeDef o] (some-> o .getCategory .name))
         :superType                (fn [^TypeDef o] (some-> o .getSuperType .getGUID))
         :description              (fn [^TypeDef o] (.getDescription o))
         :descriptionGUID          (fn [^TypeDef o] (.getDescriptionGUID o))
         :origin                   (fn [^TypeDef o] (.getOrigin o))
         :createdBy                (fn [^TypeDef o] (.getCreatedBy o))
         :updatedBy                (fn [^TypeDef o] (.getUpdatedBy o))
         :createTime               (fn [^TypeDef o] (.getCreateTime o))
         :updateTime               (fn [^TypeDef o] (.getUpdateTime o))
         :options                  (fn [^TypeDef o] (some-> (.getOptions o)))
         :externalStandardMappings (fn [^TypeDef o] (some->> o .getExternalStandardMappings (mapv datafy)))
         :validInstanceStatusList  (fn [^TypeDef o] (some->> o .getValidInstanceStatusList (mapv #(.name %))))
         :initialStatus            (fn [^TypeDef o] (some-> o .getInitialStatus .name))
         :propertiesDefinition     (fn [^TypeDef o] (some->> o .getPropertiesDefinition (mapv datafy)))}))

(def ^:private entity-def->map type-def->map)

(def ^:private relationship-end-def->map
  #:openmetadata.RelationshipEndDef
      {:entityType               (fn [^RelationshipEndDef o] (some-> o .getEntityType .getGUID))
       :attributeName            (fn [^RelationshipEndDef o] (.getAttributeName o))
       :attributeDescription     (fn [^RelationshipEndDef o] (.getAttributeDescription o))
       :attributeDescriptionGUID (fn [^RelationshipEndDef o] (.getAttributeDescriptionGUID o))
       :attributeCardinality     (fn [^RelationshipEndDef o] (some-> o .getAttributeCardinality .name))})

(def ^:private relationship-def->map
  (merge type-def->map
    #:openmetadata.RelationshipDef
        {:propagationRule (fn [^RelationshipDef o] (some-> o .getPropagationRule .name))
         :endDef1         (fn [^RelationshipDef o] (some-> o .getEndDef1 datafy))
         :endDef2         (fn [^RelationshipDef o] (some-> o .getEndDef2 datafy))}))

(def ^:private classification-def->map
  (merge type-def->map
    #:openmetadata.ClassificationDef
        {:validEntityDefs (fn [^ClassificationDef o] (some->> o (.getValidEntityDefs) (mapv #(.getGUID %))))
         :propagatable    (fn [^ClassificationDef o] (.isPropagatable o))}))

;;
;; Data->Egeria mappings
;;

(declare map->ExternalStandardMappings)
(declare map->EnumElementDef)
(declare map->EntityDef)
(declare map->TypeDefAttribute)
(declare map->TypeDefLink)
(declare map->RelationshipEndDef)
(declare map->AttributeTypeDef)


(def ^:private map->external-standard-mapping
  #:openmetadata.ExternalStandardMapping
      {:standardName         (fn [^ExternalStandardMapping o v] (doto o (.setStandardName v)))
       :standardOrganization (fn [^ExternalStandardMapping o v] (doto o (.setStandardOrganization v)))
       :standardTypeName     (fn [^ExternalStandardMapping o v] (doto o (.setStandardTypeName v)))})

(def ^:private map->enum-element-def
  #:openmetadata.EnumElementDef
      {:ordinal         (fn [^EnumElementDef o v] (doto o (.setOrdinal v)))
       :value           (fn [^EnumElementDef o v] (doto o (.setValue v)))
       :description     (fn [^EnumElementDef o v] (doto o (.setDescription v)))
       :descriptionGUID (fn [^EnumElementDef o v] (doto o (.setDescriptionGUID v)))})

(def ^:private map->attribute-type-def
  #:openmetadata.AttributeTypeDef
      {:version         (fn [^AttributeTypeDef o v] (doto o (.setVersion v)))
       :versionName     (fn [^AttributeTypeDef o v] (doto o (.setVersionName v)))
       :category        (fn [^AttributeTypeDef o v]
                          (let [c (some-> v (AttributeTypeDefCategory/valueOf))]
                            (doto o (.setCategory c))))
       :guid            (fn [^AttributeTypeDef o v] (doto o (.setGUID v)))
       :name            (fn [^AttributeTypeDef o v] (doto o (.setName v)))
       :description     (fn [^AttributeTypeDef o v] (doto o (.setDescription v)))
       :descriptionGUID (fn [^AttributeTypeDef o v] (doto o (.setDescriptionGUID v)))})

(def ^:private map->primitive-def
  (merge map->attribute-type-def
    #:openmetadata.PrimitiveDef
        {:primitiveDefCategory (fn [^PrimitiveDef o v]
                                 (let [c (some-> v (PrimitiveDefCategory/valueOf))]
                                   (doto o (.setPrimitiveDefCategory c))))}))

(def ^:private map->collection-def
  (merge map->attribute-type-def
    #:openmetadata.CollectionDef
        {:collectionDefCategory (fn [^CollectionDef o v]
                                  (let [c (some-> v (CollectionDefCategory/valueOf))]
                                    (doto o (.setCollectionDefCategory c))))
         :argumentCount         (fn [^CollectionDef o v] (doto o (.setArgumentCount v)))
         :argumentTypes         (fn [^CollectionDef o v]
                                  (let [types (some->> v (mapv #(PrimitiveDefCategory/valueOf %)))]
                                    (doto o
                                      (.setArgumentTypes types))))}))

(def ^:private map->enum-def
  (merge map->attribute-type-def
    #:openmetadata.EnumDef
        {:defaultValue (fn [^EnumDef o v]
                         (let [e (some-> v (map->EnumElementDef))]
                           (doto o (.setDefaultValue e))))
         :elementDefs  (fn [^EnumDef o v]
                         (let [e (some->> v (mapv map->EnumElementDef))]
                           (doto o (.setElementDefs e))))}))

(def ^:private map->type-def-attribute
  #:openmetadata.TypeDefAttribute
      {:attributeName            (fn [^TypeDefAttribute o v] (doto o (.setAttributeName v)))
       :attributeType            (fn [^TypeDefAttribute o v]
                                   (let [type (some-> v map->AttributeTypeDef)]
                                     (doto o (.setAttributeType type))))
       :attributeStatus          (fn [^TypeDefAttribute o v]
                                   (let [x (some-> v (TypeDefAttributeStatus/valueOf))]
                                     (doto o (.setAttributeStatus x))))
       :replacedByAttribute      (fn [^TypeDefAttribute o v] (doto o (.setReplacedByAttribute v)))
       :attributeDescription     (fn [^TypeDefAttribute o v] (doto o (.setAttributeDescription v)))
       :attributeDescriptionGUID (fn [^TypeDefAttribute o v] (doto o (.setAttributeDescriptionGUID v)))
       :cardinality              (fn [^TypeDefAttribute o v]
                                   (let [x (some-> v (AttributeCardinality/valueOf))]
                                     (doto o (.setAttributeCardinality x))))
       :valuesMinCount           (fn [^TypeDefAttribute o v] (doto o (.setValuesMinCount v)))
       :valuesMaxCount           (fn [^TypeDefAttribute o v] (doto o (.setValuesMaxCount v)))
       :isIndexable              (fn [^TypeDefAttribute o v] (doto o (.setIndexable v)))
       :isUnique                 (fn [^TypeDefAttribute o v] (doto o (.setUnique v)))
       :defaultValue             (fn [^TypeDefAttribute o v] (doto o (.setDefaultValue v)))
       :externalStandardMappings (fn [^TypeDefAttribute o v]
                                   (let [xs (some->> v (mapv map->ExternalStandardMappings))]
                                     (doto o (.setExternalStandardMappings xs))))})

(def ^:private map->type-def-link
  #:openmetadata.TypeDef
      {:guid               (fn [^TypeDefLink o v] (doto o (.setGUID v)))
       :name               (fn [^TypeDefLink o v] (doto o (.setName v)))
       :status             (fn [^TypeDefLink o v]
                             (let [s (some-> v (TypeDefStatus/valueOf))]
                               (doto o (.setStatus s))))
       :replacedByTypeGUID (fn [^TypeDefLink o v] (doto o (.setReplacedByTypeGUID v)))
       :replacedByTypeName (fn [^TypeDefLink o v] (doto o (.setReplacedByTypeName v)))})

(def ^:private map->type-def
  (merge map->type-def-link
    #:openmetadata.TypeDef
        {:version                  (fn [^TypeDef o v] (doto o (.setVersion v)))
         :versionName              (fn [^TypeDef o v] (doto o (.setVersionName v)))
         :category                 (fn [^TypeDef o v]
                                     (let [x (some-> v (TypeDefCategory/valueOf))]
                                       (doto o (.setCategory x))))
         :superType                (fn [^TypeDef o v]
                                     (let [x (some-> v (map->TypeDefLink))]
                                       (doto o (.setSuperType x))))
         :description              (fn [^TypeDef o v] (doto o (.setDescription v)))
         :descriptionGUID          (fn [^TypeDef o v] (doto o (.setDescriptionGUID v)))
         :origin                   (fn [^TypeDef o v] (doto o (.setOrigin v)))
         :createdBy                (fn [^TypeDef o v] (doto o (.setCreatedBy v)))
         :updatedBy                (fn [^TypeDef o v] (doto o (.setUpdatedBy v)))
         :createTime               (fn [^TypeDef o v] (doto o (.setCreateTime v)))
         :updateTime               (fn [^TypeDef o v] (doto o (.setUpdateTime v)))
         :options                  (fn [^TypeDef o v] (doto o (.setOptions v)))
         :externalStandardMappings (fn [^TypeDef o v]
                                     (let [xs (some->> v (mapv map->ExternalStandardMappings))]
                                       (doto o (.setExternalStandardMappings xs))))
         :validInstanceStatusList  (fn [^TypeDef o v]
                                     (let [xs (some->> v (mapv #(InstanceStatus/valueOf %)))]
                                       (doto o (.setValidInstanceStatusList xs))))
         :initialStatus            (fn [^TypeDef o v] (doto o (.setInitialStatus (some-> v (InstanceStatus/valueOf)))))
         :propertiesDefinition     (fn [^TypeDef o v]
                                     (let [xs (some->> v (mapv map->TypeDefAttribute))]
                                       (doto o (.setPropertiesDefinition xs))))}))

(def ^:private map->entity-def map->type-def)

(def ^:private map->relationship-end-def
  #:openmetadata.RelationshipEndDef
      {:entityType               (fn [^RelationshipEndDef o v] (doto o (.setEntityType (map->TypeDefLink v))))
       :attributeName            (fn [^RelationshipEndDef o v] (doto o (.setAttributeName v)))
       :attributeDescription     (fn [^RelationshipEndDef o v] (doto o (.setAttributeDescription v)))
       :attributeDescriptionGUID (fn [^RelationshipEndDef o v] (doto o (.setAttributeDescriptionGUID v)))
       :attributeCardinality     (fn [^RelationshipEndDef o v]
                                   (let [x (some-> v (RelationshipEndCardinality/valueOf))]
                                     (doto o (.setAttributeCardinality x))))})

(def ^:private map->relationship-def
  (merge map->type-def
    #:openmetadata.RelationshipDef
        {:propagationRule (fn [^RelationshipDef o v]
                            (let [x (some-> v (ClassificationPropagationRule/valueOf))]
                              (doto o (.setPropagationRule x))))
         :endDef1         (fn [^RelationshipDef o v]
                            (let [x (some-> v (map->RelationshipEndDef))]
                              (doto o (.setEndDef1 x))))
         :endDef2         (fn [^RelationshipDef o v]
                            (let [x (some-> v (map->RelationshipEndDef))]
                              (doto o (.setEndDef2 x))))}))

(def ^:private map->classification-def
  (merge map->type-def
    #:openmetadata.ClassificationDef
        {:validEntityDefs (fn [^ClassificationDef o v]
                            (let [xs (some->> v (mapv map->TypeDefLink))]
                              (doto o (.setValidEntityDefs xs))))
         :propagatable    (fn [^ClassificationDef o v] (doto o (.setPropagatable v)))}))

;;
;; Data navigation functions
;;

(defmulti navigate (fn [_ _ k _] k))

(defn- nav-attribute-type-def [repository-helper guid]
  (if repository-helper
    (binding [omrs/*repo-helper* repository-helper]
      (omrs/find-attribute-type-def-by-guid guid))
    guid))

(defn- nav-type-def [repository-helper guid]
  (if repository-helper
    (binding [omrs/*repo-helper* repository-helper]
      (when guid
        (omrs/find-type-def-by-guid guid)))
    guid))

(defmethod navigate :openmetadata.TypeDefAttribute/attributeType
  [{:keys [repository-helper]} coll k v]
  (nav-attribute-type-def repository-helper v))

(defmethod navigate :openmetadata.RelationshipEndDef/entityType
  [{:keys [repository-helper]} coll k v]
  (nav-type-def repository-helper v))

(defmethod navigate :openmetadata.TypeDef/superType
  [{:keys [repository-helper]} coll k v]
  (nav-type-def repository-helper v))

(defmethod navigate :openmetadata.ClassificationDef/validEntityDefs
  [{:keys [repository-helper]} coll k v]
  (->> v
    (mapv #(nav-type-def repository-helper %))))

(defmethod navigate :default [_ coll _ v] v)

(defn- omrs-navigator []
  (let [omrs {:repository-helper omrs/*repo-helper*}]
    (fn [coll k v]
      (navigate omrs coll k v))))

;;
;; Egeria->Data
;;

(defn- datafy-egeria [kfs o]
  (let [v (reduce-kv (fn [m k f] (assoc m k (f o))) {} kfs)]
    (vary-meta v assoc `p/nav (omrs-navigator))))

(extend-type PrimitivePropertyValue
  p/Datafiable
  (datafy [^PrimitivePropertyValue o]
    (p/datafy (.valueAsObject o))))

(extend-type EntityDef
  p/Datafiable
  (datafy [^EntityDef o]
    (datafy-egeria entity-def->map o)))

(extend-type RelationshipEndDef
  p/Datafiable
  (datafy [^RelationshipEndDef o]
    (datafy-egeria relationship-end-def->map o)))

(extend-type RelationshipDef
  p/Datafiable
  (datafy [^RelationshipDef o]
    (datafy-egeria relationship-def->map o)))

(extend-type ClassificationDef
  p/Datafiable
  (datafy [^ClassificationDef o]
    (datafy-egeria classification-def->map o)))

(extend-type PrimitiveDef
  p/Datafiable
  (datafy [^PrimitiveDef o]
    (datafy-egeria primitive-def->map o)))

(extend-type CollectionDef
  p/Datafiable
  (datafy [^CollectionDef o]
    (datafy-egeria collection-def->map o)))

(extend-type EnumElementDef
  p/Datafiable
  (datafy [^EnumElementDef o]
    (datafy-egeria enum-element-def->map o)))

(extend-type EnumDef
  p/Datafiable
  (datafy [^EnumDef o]
    (datafy-egeria enum-def->map o)))

(extend-type TypeDefAttribute
  p/Datafiable
  (datafy [^TypeDefAttribute o]
    (datafy-egeria type-def-attribute->map o)))

(extend-type ExternalStandardMapping
  p/Datafiable
  (datafy [^ExternalStandardMapping o]
    (datafy-egeria external-standard-mapping->map o)))

(extend-type ArrayPropertyValue
  p/Datafiable
  (datafy [^ArrayPropertyValue o]
    (some->> (.getArrayValues o)
      .getInstanceProperties
      (sort (fn [[^String k1 _] [^String k2 _]] (< (Integer/valueOf k1) (Integer/valueOf k2))))
      (map second)
      (mapv datafy))))

(extend-type MapPropertyValue
  p/Datafiable
  (datafy [^MapPropertyValue o]
    (when-let [instance-props (some-> o .getMapValues .getInstanceProperties)]
      (reduce-kv (fn [m k pv] (assoc m k (datafy pv))) {} instance-props))))

(extend-type EnumPropertyValue
  p/Datafiable
  (datafy [^EnumPropertyValue o]
    (.getSymbolicName o)))

;;
;; Data->Egeria
;;

(defn- map->egeria [kfs o m]
  (reduce-kv (fn [o k f]
               (let [v (p/nav m k (k m))]
                 (f o v)))
    o
    kfs))

(defn map->TypeDefLink [m]
  (map->egeria map->type-def-link (TypeDefLink.) m))

(defn map->EntityDef [m]
  (map->egeria map->entity-def (EntityDef.) m))

(defn map->RelationshipEndDef [m]
  (map->egeria map->relationship-end-def (RelationshipEndDef.) m))

(defn map->RelationshipDef [m]
  (map->egeria map->relationship-def (RelationshipDef.) m))

(defn map->ClassificationDef [m]
  (map->egeria map->classification-def (ClassificationDef.) m))

(defn map->TypeDef [m]
  (condp = (TypeDefCategory/valueOf (:openmetadata.TypeDef/category m))
    TypeDefCategory/ENTITY_DEF (map->EntityDef m)
    TypeDefCategory/RELATIONSHIP_DEF (map->RelationshipDef m)
    TypeDefCategory/CLASSIFICATION_DEF (map->ClassificationDef m)))

(defn map->PrimitiveDef [m]
  (map->egeria map->primitive-def (PrimitiveDef.) m))

(defn map->CollectionDef [m]
  (map->egeria map->collection-def (CollectionDef.) m))

(defn map->EnumElementDef [m]
  (map->egeria map->enum-element-def (EnumElementDef.) m))

(defn map->EnumDef [m]
  (map->egeria map->enum-def (EnumDef.) m))

(defn map->AttributeTypeDef [m]
  (let [category (some-> (:openmetadata.AttributeTypeDef/category m)
                   (AttributeTypeDefCategory/valueOf))]
    (condp = category
      AttributeTypeDefCategory/PRIMITIVE (map->PrimitiveDef m)
      AttributeTypeDefCategory/COLLECTION (map->CollectionDef m)
      AttributeTypeDefCategory/ENUM_DEF (map->EnumDef m))))

(defn map->TypeDefAttribute [m]
  (map->egeria map->type-def-attribute (TypeDefAttribute.) m))

(defn map->ExternalStandardMappings [m]
  (map->egeria map->external-standard-mapping (ExternalStandardMapping.) m))
