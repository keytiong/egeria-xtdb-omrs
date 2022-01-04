(ns io.kosong.egeria.omrs.datafy
  (:require [clojure.core.protocols :as p]
            [clojure.datafy :refer [datafy]]
            [io.kosong.egeria.omrs :as omrs])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances PrimitivePropertyValue ArrayPropertyValue MapPropertyValue EnumPropertyValue InstanceStatus InstanceProvenanceType Classification EntityDetail EntitySummary InstanceType InstanceProperties StructPropertyValue InstancePropertyCategory EntityProxy Relationship ClassificationOrigin)
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
      {:headerVersion      (fn [^TypeDefLink o] (.getHeaderVersion o))
       :guid               (fn [^TypeDefLink o] (.getGUID o))
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


(def ^:private entity-instance->map
  #:openmetadata.Entity
      {:guid                 nil
       :instanceURL          nil
       :reIdentifiedFromGUID nil})

(def ^:private classification->map
  #:openmetadata.Classification
      {:headerVersion            (fn [^Classification o] (.getHeaderVersion o))
       :type                     (fn [^Classification o] (some-> o .getType .getTypeDefGUID))
       :instanceProvenanceType   (fn [^Classification o] (some-> o .getInstanceProvenanceType .name))
       :metadataCollectionId     (fn [^Classification o] (.getMetadataCollectionId o))
       :metadataCollectionName   (fn [^Classification o] (.getMetadataCollectionName o))
       :replicatedBy             (fn [^Classification o] (.getReplicatedBy o))
       :instanceLicense          (fn [^Classification o] (.getInstanceLicense o))
       :createdBy                (fn [^Classification o] (.getCreatedBy o))
       :updatedBy                (fn [^Classification o] (.getUpdatedBy o))
       :maintainedBy             (fn [^Classification o] (.getMaintainedBy o))
       :createTime               (fn [^Classification o] (.getCreateTime o))
       :updateTime               (fn [^Classification o] (.getUpdateTime o))
       :version                  (fn [^Classification o] (.getVersion o))
       :status                   (fn [^Classification o] (some-> o .getStatus .name))
       :statusOnDelete           (fn [^Classification o] (some-> o .getStatusOnDelete .name))
       :mappingProperties        (fn [^Classification o] (.getMappingProperties o))
       :name                     (fn [^Classification o] (.getName o))
       :classificationOrigin     (fn [^Classification o] (some-> o .getClassificationOrigin .name))
       :classificationOriginGUID (fn [^Classification o] (.getClassificationOriginGUID o))})

(def ^:private entity-summary->map
  #:openmetadata.Entity
      {:headerVersion          (fn [^EntitySummary o] (.getHeaderVersion o))
       :type                   (fn [^EntitySummary o] (some-> o .getType .getTypeDefGUID))
       :instanceProvenanceType (fn [^EntitySummary o] (some-> o .getInstanceProvenanceType .name))
       :metadataCollectionId   (fn [^EntitySummary o] (.getMetadataCollectionId o))
       :metadataCollectionName (fn [^EntitySummary o] (.getMetadataCollectionName o))
       :replicatedBy           (fn [^EntitySummary o] (.getReplicatedBy o))
       :instanceLicense        (fn [^EntitySummary o] (.getInstanceLicense o))
       :createdBy              (fn [^EntitySummary o] (.getCreatedBy o))
       :updatedBy              (fn [^EntitySummary o] (.getUpdatedBy o))
       :maintainedBy           (fn [^EntitySummary o] (.getMaintainedBy o))
       :createTime             (fn [^EntitySummary o] (.getCreateTime o))
       :updateTime             (fn [^EntitySummary o] (.getUpdateTime o))
       :version                (fn [^EntitySummary o] (.getVersion o))
       :status                 (fn [^EntitySummary o] (some-> o .getStatus .name))
       :statusOnDelete         (fn [^EntitySummary o] (some-> o .getStatusOnDelete .name))
       :mappingProperties      (fn [^EntitySummary o] (.getMappingProperties o))
       :guid                   (fn [^EntitySummary o] (.getGUID o))
       :instanceURL            (fn [^EntitySummary o] (.getInstanceURL o))
       :reIdentifiedFromGUID   (fn [^EntitySummary o] (.getReIdentifiedFromGUID o))
       :classifications        (fn [^EntitySummary o] (some->> o .getClassifications (mapv datafy)))})

(def ^:private entity-detail->map
  (merge entity-summary->map
    {}))

(def ^:private entity-proxy->map
  (merge entity-summary->map
    {:openmetadata.Entity/isProxy (fn [^EntityProxy o] true)}))

(def ^:private relationship->map
  #:openmetadata.Relationship
      {:headerVersion          (fn [^Relationship o] (.getHeaderVersion o))
       :type                   (fn [^Relationship o] (some-> o .getType .getTypeDefGUID))
       :instanceProvenanceType (fn [^Relationship o] (some-> o .getInstanceProvenanceType .name))
       :metadataCollectionId   (fn [^Relationship o] (.getMetadataCollectionId o))
       :metadataCollectionName (fn [^Relationship o] (.getMetadataCollectionName o))
       :replicatedBy           (fn [^Relationship o] (.getReplicatedBy o))
       :instanceLicense        (fn [^Relationship o] (.getInstanceLicense o))
       :createdBy              (fn [^Relationship o] (.getCreatedBy o))
       :updatedBy              (fn [^Relationship o] (.getUpdatedBy o))
       :maintainedBy           (fn [^Relationship o] (.getMaintainedBy o))
       :createTime             (fn [^Relationship o] (.getCreateTime o))
       :updateTime             (fn [^Relationship o] (.getUpdateTime o))
       :version                (fn [^Relationship o] (.getVersion o))
       :status                 (fn [^Relationship o] (some-> o .getStatus .name))
       :statusOnDelete         (fn [^Relationship o] (some-> o .getStatusOnDelete .name))
       :mappingProperties      (fn [^Relationship o] (.getMappingProperties o))
       :guid                   (fn [^Relationship o] (.getGUID o))
       :instanceURL            (fn [^Relationship o] (.getInstanceURL o))
       :reIdentifiedFromGUID   (fn [^Relationship o] (.getReIdentifiedFromGUID o))
       :entityOne              (fn [^Relationship o] (some-> o .getEntityOneProxy .getGUID))
       :entityTwo              (fn [^Relationship o] (some-> o .getEntityTwoProxy .getGUID))})
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
(declare map->InstanceType)
(declare map->Classification)
(declare map->EntityProxy)

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
                                    (doto o (.setArgumentTypes types))))}))

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
      {:headerVersion      (fn [^TypeDefLink o v] (doto o (.setHeaderVersion v)))
       :guid               (fn [^TypeDefLink o v] (doto o (.setGUID v)))
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

(def ^:private map->classification
  #:openmetadata.Classification
      {:headerVersion            (fn [^Classification o v] (doto o (.setHeaderVersion v)))
       :type                     (fn [^Classification o v]
                                   (let [x (some-> v map->InstanceType)]
                                     (doto o (.setType x))))
       :instanceProvenanceType   (fn [^Classification o v]
                                   (let [x (some-> v InstanceProvenanceType/valueOf)]
                                     (doto o (.setInstanceProvenanceType x))))
       :metadataCollectionId     (fn [^Classification o v] (doto o (.setMetadataCollectionId v)))
       :metadataCollectionName   (fn [^Classification o v] (doto o (.setMetadataCollectionName v)))
       :replicatedBy             (fn [^Classification o v] (doto o (.setReplicatedBy v)))
       :instanceLicense          (fn [^Classification o v] (doto o (.setInstanceLicense v)))
       :createdBy                (fn [^Classification o v] (doto o (.setCreatedBy v)))
       :updatedBy                (fn [^Classification o v] (doto o (.setUpdatedBy v)))
       :maintainedBy             (fn [^Classification o v] (doto o (.setMaintainedBy v)))
       :createTime               (fn [^Classification o v] (doto o (.setCreateTime v)))
       :updateTime               (fn [^Classification o v] (doto o (.setUpdateTime v)))
       :version                  (fn [^Classification o v] (doto o (.setVersion v)))
       :status                   (fn [^Classification o v]
                                   (let [x (some-> v InstanceStatus/valueOf)]
                                     (doto o (.setStatus x))))
       :statusOnDelete           (fn [^Classification o v]
                                   (let [x (some-> v InstanceStatus/valueOf)]
                                     (doto o (.setStatusOnDelete x))))
       :mappingProperties        (fn [^Classification o v] (doto o (.setMappingProperties v)))
       :name                     (fn [^Classification o v] (doto o (.setName v)))
       :classificationOrigin     (fn [^Classification o v] (doto o (.setClassificationOrigin (ClassificationOrigin/valueOf v))))
       :classificationOriginGUID (fn [^Classification o v] (doto o (.setClassificationOriginGUID v)))})

(def ^:private map->entity-summary
  #:openmetadata.Entity
      {:headerVersion          (fn [^EntitySummary o v] (doto o (.setHeaderVersion v)))
       :type                   (fn [^EntitySummary o v]
                                 (let [x (some-> v map->InstanceType)]
                                   (doto o (.setType x))))
       :instanceProvenanceType (fn [^EntitySummary o v]
                                 (let [x (some-> v InstanceProvenanceType/valueOf)]
                                   (doto o (.setInstanceProvenanceType x))))
       :metadataCollectionId   (fn [^EntitySummary o v] (doto o (.setMetadataCollectionId v)))
       :metadataCollectionName (fn [^EntitySummary o v] (doto o (.setMetadataCollectionName v)))
       :replicatedBy           (fn [^EntitySummary o v] (doto o (.setReplicatedBy v)))
       :instanceLicense        (fn [^EntitySummary o v] (doto o (.setInstanceLicense v)))
       :createdBy              (fn [^EntitySummary o v] (doto o (.setCreatedBy v)))
       :updatedBy              (fn [^EntitySummary o v] (doto o (.setUpdatedBy v)))
       :maintainedBy           (fn [^EntitySummary o v] (doto o (.setMaintainedBy v)))
       :createTime             (fn [^EntitySummary o v] (doto o (.setCreateTime v)))
       :updateTime             (fn [^EntitySummary o v] (doto o (.setUpdateTime v)))
       :version                (fn [^EntitySummary o v] (doto o (.setVersion v)))
       :status                 (fn [^EntitySummary o v]
                                 (let [x (some-> v InstanceStatus/valueOf)]
                                   (doto o (.setStatus x))))
       :statusOnDelete         (fn [^EntitySummary o v]
                                 (let [x (some-> v InstanceStatus/valueOf)]
                                   (doto o (.setStatusOnDelete x))))
       :mappingProperties      (fn [^EntitySummary o v] (doto o (.setMappingProperties v)))
       :guid                   (fn [^EntitySummary o v] (doto o (.setGUID v)))
       :instanceURL            (fn [^EntitySummary o v] (doto o (.setInstanceURL v)))
       :reIdentifiedFromGUID   (fn [^EntitySummary o v] (doto o (.setReIdentifiedFromGUID v)))
       :classifications        (fn [^EntitySummary o v]
                                 (let [xs (some->> v (mapv map->Classification))]
                                   (doto o (.setClassifications xs))))})

(def ^:private map->entity-detail
  (merge map->entity-summary
    {}))

(def ^:private map->entity-proxy
  (merge map->entity-summary
    {}))

(def ^:private map->instance-type
  #:openmetadata.TypeDef
      {:guid                    (fn [^InstanceType o v] (doto o (.setTypeDefGUID v)))
       :name                    (fn [^InstanceType o v] (doto o (.setTypeDefName v)))
       :version                 (fn [^InstanceType o v] (doto o (.setTypeDefVersion v)))
       :category                (fn [^InstanceType o v]
                                  (let [x (some-> v TypeDefCategory/valueOf)]
                                    (doto o (.setTypeDefCategory x))))
       :ancestorTypes           (fn [^InstanceType o v]
                                  (let [xs (mapv map->TypeDefLink v)]
                                    (doto o (.setTypeDefSuperTypes xs))))
       :description             (fn [^InstanceType o v] (doto o (.setTypeDefDescription v)))
       :descriptionGUID         (fn [^InstanceType o v] (doto o (.setTypeDefDescriptionGUID v)))
       :validInstanceStatusList (fn [^InstanceType o v]
                                  o
                                  ;; A bug in Egeria Repository Content Manager to always return null for validStatusList
                                  #_(let [xs (some->> v (mapv #(InstanceStatus/valueOf %)))]
                                      (doto o (.setValidStatusList xs))))
       :validInstanceProperties (fn [^InstanceType o v]
                                  (let [xs (mapv :openmetadata.TypeDefAttribute/attributeName v)]
                                    (doto o (.setValidInstanceProperties xs))))})

(def ^:private map->relationship
  #:openmetadata.Relationship
      {:headerVersion          (fn [^Relationship o v] (doto o (.setHeaderVersion v)))
       :type                   (fn [^Relationship o v]
                                 (let [x (some-> v map->InstanceType)]
                                   (doto o (.setType x))))
       :instanceProvenanceType (fn [^Classification o v]
                                 (let [x (some-> v InstanceProvenanceType/valueOf)]
                                   (doto o (.setInstanceProvenanceType x))))
       :metadataCollectionId   (fn [^Relationship o v] (doto o (.setMetadataCollectionId v)))
       :metadataCollectionName (fn [^Relationship o v] (doto o (.setMetadataCollectionName v)))
       :replicatedBy           (fn [^Relationship o v] (doto o (.setReplicatedBy v)))
       :instanceLicense        (fn [^Relationship o v] (doto o (.setInstanceLicense v)))
       :createdBy              (fn [^Relationship o v] (doto o (.setCreatedBy v)))
       :updatedBy              (fn [^Relationship o v] (doto o (.setUpdatedBy v)))
       :maintainedBy           (fn [^Relationship o v] (doto o (.setMaintainedBy v)))
       :createTime             (fn [^Relationship o v] (doto o (.setCreateTime v)))
       :updateTime             (fn [^Relationship o v] (doto o (.setUpdateTime v)))
       :version                (fn [^Relationship o v] (doto o (.setVersion v)))
       :status                 (fn [^Relationship o v]
                                 (let [x (some-> v InstanceStatus/valueOf)]
                                   (doto o (.setStatus x))))
       :statusOnDelete         (fn [^Relationship o v]
                                 (let [x (some-> v InstanceStatus/valueOf)]
                                   (doto o (.setStatusOnDelete x))))
       :mappingProperties      (fn [^Relationship o v] (doto o (.setMappingProperties v)))
       :guid                   (fn [^Relationship o v] (doto o (.setGUID v)))
       :instanceURL            (fn [^Relationship o v] (doto o (.setInstanceURL v)))
       :reIdentifiedFromGUID   (fn [^Relationship o v] (doto o (.setReIdentifiedFromGUID v)))
       :entityOne              (fn [^Relationship o v] (doto o (.setEntityOneProxy (some-> v map->EntityProxy))))
       :entityTwo              (fn [^Relationship o v] (doto o (.setEntityTwoProxy (some-> v map->EntityProxy))))})
;;
;; Data navigation functions
;;

(defmulti navigate (fn [_ k _] k))

(defn- nav-attribute-type-def [guid]
  (when guid
    (omrs/find-attribute-type-def-by-guid guid)))

(defn- nav-type-def [guid]
  (when guid
    (omrs/find-type-def-by-guid guid)))

(defn- nav-entity [guid]
  (when guid
    (omrs/find-entity-by-guid guid)))

(defn- nav-classification [guid])

(defmethod navigate :openmetadata.TypeDefAttribute/attributeType
  [coll k v]
  (nav-attribute-type-def v))

(defmethod navigate :openmetadata.RelationshipEndDef/entityType
  [coll k v]
  (nav-type-def v))

(defmethod navigate :openmetadata.TypeDef/superType
  [coll k v]
  (nav-type-def v))

(defmethod navigate :openmetadata.ClassificationDef/validEntityDefs
  [coll k v]
  (->> v
    (mapv #(nav-type-def %))))

(defmethod navigate :openmetadata.Classification/type
  [coll k v]
  (nav-type-def v))

(defmethod navigate :openmetadata.Entity/type
  [coll k v]
  (nav-type-def v))

(defmethod navigate :openmetadata.Relationship/type
  [coll k v]
  (nav-type-def v))

(defmethod navigate :openmetadata.Relationship/entityOne
  [coll k v]
  (nav-entity v))

(defmethod navigate :openmetadata.Relationship/entityTwo
  [coll k v]
  (nav-entity v))

(defmethod navigate :default [coll k v] v)

;;
;; Egeria->Data
;;

(defn- datafy-egeria [kfs o]
  (let [v (reduce-kv (fn [m k f] (assoc m k (f o))) {} kfs)]
    v))

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

(extend-type StructPropertyValue
  p/Datafiable
  (datafy [^StructPropertyValue o]
    (when-let [instance-props (some-> o .getAttributes .getInstanceProperties)]
      (reduce-kv (fn [m k pv] (assoc m k (datafy pv))) {} instance-props))))

(extend-type EnumPropertyValue
  p/Datafiable
  (datafy [^EnumPropertyValue o]
    (.getSymbolicName o)))

(defn InstanceProperties->map [type-def ^InstanceProperties instance-props]
  (let [attribute-keys (omrs/attribute-keys type-def)
        props          (some-> instance-props .getInstanceProperties)]
    (reduce (fn [m attribute-key]
              (let [short-name (name attribute-key)
                    v          (get props short-name)]
                (assoc m attribute-key (datafy v))))
      {}
      attribute-keys)))

(extend-type Classification
  p/Datafiable
  (datafy [^Classification o]
    (let [props    (.getProperties o)
          guid     (-> o .getType .getTypeDefGUID)
          type-def (omrs/find-type-def-by-guid guid)]
      (merge
        (datafy-egeria classification->map o)
        (InstanceProperties->map type-def props)))))

(extend-type EntityDetail
  p/Datafiable
  (datafy [^EntityDetail o]
    (let [props    (.getProperties o)
          guid     (some-> o .getType .getTypeDefGUID)
          type-def (omrs/find-type-def-by-guid guid)]
      (merge
        (datafy-egeria entity-summary->map o)
        (InstanceProperties->map type-def props)))))

(extend-type EntityProxy
  p/Datafiable
  (datafy [^EntityProxy o]
    (let [props    (.getUniqueProperties o)
          guid     (some-> o .getType .getTypeDefGUID)
          type-def (omrs/find-type-def-by-guid guid)]
      (merge
        (datafy-egeria entity-proxy->map o)
        (InstanceProperties->map type-def props)))))

(extend-type EntitySummary
  p/Datafiable
  (datafy [^EntitySummary o]
    (let [guid     (some-> o .getType .getTypeDefGUID)
          type-def (omrs/find-type-def-by-guid guid)]
      (merge
        (datafy-egeria entity-summary->map o)))))

(extend-type Relationship
  p/Datafiable
  (datafy [^Relationship o]
    (let [props    (.getProperties o)
          guid     (some-> o .getType .getTypeDefGUID)
          type-def (omrs/find-type-def-by-guid guid)]
      (merge
        (datafy-egeria relationship->map o)
        (InstanceProperties->map type-def props)))))
;;
;; Data->Egeria
;;

(defn- map->egeria [kfs o m]
  (reduce-kv (fn [o k f]
               (let [v (navigate m k (k m))]
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

(defmulti ->PrimitiveValue (fn [category value] category))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_BYTE
  [category v]
  (byte v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_SHORT
  [category v]
  (short v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_CHAR
  [category v]
  (char v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_INT
  [category v]
  (int v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_LONG
  [category v]
  (long v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_FLOAT
  [category v]
  (float v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_DOUBLE
  [category v]
  (double v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_BIGINTEGER
  [categroy v]
  (BigInteger/valueOf v))

(defmethod ->PrimitiveValue PrimitiveDefCategory/OM_PRIMITIVE_TYPE_BIGDECIMAL
  [category v]
  (BigDecimal/valueOf v))

(defmethod ->PrimitiveValue :default
  [category v]
  v)

(defmulti ->InstancePropertyValue
  (fn [attr-type-def v]
    (or
      (some-> attr-type-def :openmetadata.CollectionDef/collectionDefCategory
        CollectionDefCategory/valueOf)
      (some-> attr-type-def :openmetadata.AttributeTypeDef/category
        AttributeTypeDefCategory/valueOf))))

(defmethod ->InstancePropertyValue AttributeTypeDefCategory/PRIMITIVE
  [attr-type-def v]
  (when v
    (let [primitive-def-category (some-> attr-type-def
                                   :openmetadata.PrimitiveDef/primitiveDefCategory
                                   PrimitiveDefCategory/valueOf)
          primitive-value        (->PrimitiveValue primitive-def-category v)]
      (doto (PrimitivePropertyValue.)
        (.setTypeGUID (:openmetadata.AttributeTypeDef/guid attr-type-def))
        (.setTypeName (:openmetadata.AttributeTypeDef/name attr-type-def))
        (.setPrimitiveDefCategory primitive-def-category)
        (.setInstancePropertyCategory InstancePropertyCategory/PRIMITIVE)
        (.setPrimitiveValue primitive-value)))))

(defmethod ->InstancePropertyValue AttributeTypeDefCategory/ENUM_DEF
  [attr-type-def v]
  (when v
    (let [enum-element-def (->> (:openmetadata.EnumDef/elementDefs attr-type-def)
                             (filter #(= v (:openmetadata.EnumElementDef/value %)))
                             (first))]
      (doto (EnumPropertyValue.)
        (.setTypeGUID (:openmetadata.AttributeTypeDef/guid attr-type-def))
        (.setTypeName (:openmetadata.AttributeTypeDef/name attr-type-def))
        (.setDescription (:openmetadata.EnumElementDef/description enum-element-def))
        (.setSymbolicName (:openmetadata.EnumElementDef/value enum-element-def))
        (.setOrdinal (:openmetadata.EnumElementDef/ordinal enum-element-def))))))

(defmethod ->InstancePropertyValue CollectionDefCategory/OM_COLLECTION_ARRAY
  [attr-type-def v]
  (when v
    (let [elem-type (some->> attr-type-def
                      :openmetadata.CollectionDef/argumentTypes
                      (map (fn [x] {:openmetadata.AttributeTypeDef/category         (.name AttributeTypeDefCategory/PRIMITIVE)
                                    :openmetadata.PrimitiveDef/primitiveDefCategory x}))
                      first)
          pv        (doto (ArrayPropertyValue.)
                      (.setTypeGUID (:openmetadata.AttributeTypeDef/guid attr-type-def))
                      (.setTypeName (:openmetadata.AttributeTypeDef/name attr-type-def))
                      (.setArrayCount (count v)))]
      (doseq [[idx x] (map-indexed #(vector %1 %2) v)]
        (.setArrayValue pv idx (->InstancePropertyValue elem-type x)))
      pv)))

(defmethod ->InstancePropertyValue CollectionDefCategory/OM_COLLECTION_MAP
  [attr-type-def v]
  (when v
    (let [elem-types (some->> attr-type-def
                       :openmetadata.CollectionDef/argumentTypes
                       (map (fn [x] {:openmetadata.AttributeTypeDef/category         (.name AttributeTypeDefCategory/PRIMITIVE)
                                     :openmetadata.PrimitiveDef/primitiveDefCategory x})))
          pv         (doto (MapPropertyValue.)
                       (.setTypeGUID (:openmetadata.AttributeTypeDef/guid attr-type-def))
                       (.setTypeName (:openmetadata.AttributeTypeDef/name attr-type-def)))]
      (doseq [[k x] v]
        (.setMapValue pv (str k) (->InstancePropertyValue (second elem-types) x)))
      pv)))

(defn map->InstanceProperties [type-def m]
  (->> (omrs/type-def-attribute-key->attribute type-def)
    (map (fn [[attr-key type-def-attr]]
           [attr-key
            (-> (:openmetadata.TypeDefAttribute/attributeType type-def-attr)
              omrs/find-attribute-type-def-by-guid)]))
    (reduce (fn [^InstanceProperties o [attr-key attr-type-def]]
              (let [property-name  (name attr-key)
                    property-value (some->> (attr-key m) (->InstancePropertyValue attr-type-def))]
                (if property-value
                  (doto o (.setProperty property-name property-value))
                  o)))
      (InstanceProperties.))))

(defn map->Classification [m]
  (let [^Classification o (map->egeria map->classification (Classification.) m)
        type-def          (omrs/find-type-def-by-guid (:openmetadata.Classification/type m))
        instance-props    (map->InstanceProperties type-def m)]
    (doto o (.setProperties instance-props))))

(defn map->EntitySummary [m]
  (map->egeria map->entity-summary (EntitySummary.) m))

(defn map->EntityDetail [m]
  (let [^EntityDetail o (map->egeria map->entity-detail (EntityDetail.) m)
        type-def        (omrs/find-type-def-by-guid (:openmetadata.Entity/type m))
        instance-props  (map->InstanceProperties type-def m)]
    (doto o (.setProperties instance-props))))

(defn map->EntityProxy [m]
  (let [^EntityProxy o (map->egeria map->entity-proxy (EntityProxy.) m)
        type-def       (omrs/find-type-def-by-guid (:openmetadata.Entity/type m))
        instance-props (map->InstanceProperties type-def m)]
    (doto o (.setUniqueProperties instance-props))))

(defn map->InstanceType [m]
  (let [ancestors       (omrs/find-type-def-ancestors m)
        type-attr-pairs (omrs/type-def-attribute-key->attribute m)
        attrs           (mapv second type-attr-pairs)
        m               (-> m
                          (assoc :openmetadata.TypeDef/ancestorTypes ancestors)
                          (assoc :openmetadata.TypeDef/validInstanceProperties attrs))]
    (map->egeria map->instance-type (InstanceType.) m)))

(defn map->Relationship [m]
  (tap> "before")
  (map->egeria map->relationship (Relationship.) m)
  (let [^Relationship o (map->egeria map->relationship (Relationship.) m)
        type-def        (omrs/find-type-def-by-guid (:openmetadata.Relationship/type m))
        instance-props  (map->InstanceProperties type-def m)]
    (doto o (.setProperties instance-props))))
