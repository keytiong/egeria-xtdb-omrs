(ns io.kosong.egeria.omrs-test
  (:use [midje.sweet])
  (:require [io.kosong.egeria.omrs :as om]
            [io.kosong.egeria.omrs.datafy :as omrs-datafy]
            [clojure.datafy :refer [datafy]]
            [dev]
            [integrant.core :as ig])
  (:import (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceProvenanceType InstanceProperties ClassificationOrigin)))


(def system
  (ig/init dev/dev-config))

(def server-name (-> system :dev/repository-config :server-name))

(def user-id "garygeeke")

(def metadata-collection-id
  (-> system :dev/repository-config :metadata-collection-id))

(def metadata-collection-name
  (-> system :dev/repository-config :metadata-collection-name))

(dev/load-openmetadata-types system)

(om/set-context! (dev/context system))

(def repository-content-helper (:dev/repository-content-helper system))

(defn- attribute-type-def-by-name [name]
  (.getAttributeTypeDefByName repository-content-helper server-name name))

(defn- type-def-by-name [name]
  (.getTypeDefByName repository-content-helper server-name name))


(let [last-attachment-type-def      (type-def-by-name "LastAttachment")
      type-def-status               (.getStatus last-attachment-type-def)
      last-attachment-link-type-def (type-def-by-name "LastAttachmentLink")
      end-def-2                     (.getEndDef2 last-attachment-link-type-def)
      entity-type                   (.getEntityType end-def-2)]
  (.setStatus entity-type type-def-status)
  (.setEntityType end-def-2 entity-type)
  (.setEndDef2 last-attachment-link-type-def end-def-2))

;;
;; Attribute Type Defs
;;
(def primitive-string-def
  (attribute-type-def-by-name "string"))

(def string-string-map-def
  (attribute-type-def-by-name "map<string,string>"))

(def string-long-map-def
  (attribute-type-def-by-name "map<string,long>"))

(def order-by-enum-def
  (attribute-type-def-by-name "OrderBy"))

;;
;; Entity Def
;;

(def referenceable-type-def
  (type-def-by-name "Referenceable"))

(def asset-type-def
  (type-def-by-name "Asset"))

;;
;; Relationship Def
;;

(def foreign-key-type-def
  (type-def-by-name "ForeignKey"))

;;
;; Classification Def
;;

(def anchors-type-def
  (type-def-by-name "Anchors"))

(facts "Datafy PrimitiveDef"
  (let [datafied (datafy primitive-string-def)]
    (fact
      (:openmetadata.AttributeTypeDef/name datafied)
      =>
      "string")
    (fact
      (:openmetadata.AttributeTypeDef/category datafied)
      =>
      "PRIMITIVE")
    (fact
      (:openmetadata.AttributeTypeDef/guid datafied)
      =>
      "b34a64b9-554a-42b1-8f8a-7d5c2339f9c4")
    (fact
      (:openmetadata.AttributeTypeDef/version datafied)
      => 1)
    (fact
      (:openmetadata.AttributeTypeDef/versionName datafied)
      =>
      "1.0")
    (fact "Primitive Def Category"
      (:openmetadata.PrimitiveDef/primitiveDefCategory datafied)
      =>
      "OM_PRIMITIVE_TYPE_STRING")))

(doseq [type-def (.getKnownTypeDefs (dev/repository-content-helper system))]
  (fact
    (-> type-def
      datafy
      omrs-datafy/map->TypeDef)
    =>
    type-def))

(doseq [attr-type-def (.getKnownAttributeTypeDefs (dev/repository-content-helper system))]
  (fact
    (-> attr-type-def
      datafy
      omrs-datafy/map->AttributeTypeDef)
    =>
    attr-type-def))

(def anchor-classification (.getNewClassification (dev/repository-content-helper system)
                             "omrs-test"                                        ; sourceName
                             metadata-collection-id
                             metadata-collection-name
                             InstanceProvenanceType/LOCAL_COHORT
                             user-id
                             "Anchors"                                           ; Classification Type Name
                             "Asset"                                            ; Entity Type Name
                             ClassificationOrigin/ASSIGNED
                             nil ; Classification Origin GUID
                             (InstanceProperties.)))

(def asset-entity
  (let [props (let [p (InstanceProperties.)]
                (.addStringPropertyToInstance repository-content-helper
                  server-name
                  p
                  "name"
                  "foo"
                  "test-1")
                p)]
    (.getNewEntity repository-content-helper
      server-name
      metadata-collection-id
      metadata-collection-name
      InstanceProvenanceType/LOCAL_COHORT
      user-id,
      "Asset"
      props
      [anchor-classification])))

(fact
  (-> anchor-classification
    datafy
    omrs-datafy/map->Classification)
  =>
  anchor-classification)

(fact
  (-> asset-entity
    datafy
    omrs-datafy/map->EntityDetail)
  => asset-entity)

(def foreign-key-relationship (.getNewRelationship repository-content-helper
                                server-name
                                metadata-collection-id
                                metadata-collection-name
                                InstanceProvenanceType/LOCAL_COHORT
                                user-id,
                                "ForeignKey"
                                (InstanceProperties.)))

(fact
  (-> foreign-key-relationship
    datafy
    omrs-datafy/map->Relationship)
  => foreign-key-relationship)

(ig/halt! system)


