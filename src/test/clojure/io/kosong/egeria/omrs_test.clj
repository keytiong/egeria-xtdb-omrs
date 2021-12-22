(ns io.kosong.egeria.omrs-test
  (:use [midje.sweet])
  (:require [io.kosong.egeria.omrs :as omrs]
            [io.kosong.egeria.omrs.datafy :as omrs-datafy]
            [clojure.datafy :refer [datafy]]))

(def server-name "omrs-test-server")
(def server-type "Open Metadata Repository Service")
(def organization "kosong.io")
(def user-id "garygeeke")
(def component-id 1001)
(def component-name "Egeria XTDB Open Metadata Repository Service")
(def component-description "Egeria XTDB Open Metadata Repository Service")
(def component-wiki-url "https://github.com/keytiong")

(def audit-log-store
  (omrs/->null-audit-log-store))

(def audit-log-destination
  (omrs/->audit-log-destination {:audit-log-stores [audit-log-store]
                                 :server-name      server-name
                                 :server-type      server-type
                                 :organization     organization}))

(def audit-log
  (omrs/->audit-log {:audit-log-destination audit-log-destination
                     :component-id          component-id
                     :component-name        component-name
                     :component-description component-description
                     :component-wiki-url    component-wiki-url}))

(def openmetadata-types-archive
  (omrs/->openmetadata-types-archive))

(def repo-content-manager
  (omrs/->repository-content-manager {:user-id   user-id
                                      :audit-log audit-log}))

(def repo-helper
  (omrs/->repository-helper {:content-manager repo-content-manager}))

(omrs/set-repo-helper! repo-helper)

(omrs/init-repo-content-manager repo-content-manager openmetadata-types-archive user-id)

(defn- attribute-type-def-by-name [name]
  (.getAttributeTypeDefByName repo-helper "omrs-test" name))

(defn- type-def-by-name [name]
  (.getTypeDefByName repo-helper "omrs-test" name))


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

(doseq [type-def (.getKnownTypeDefs repo-helper)]
  (fact
    (-> type-def
      datafy
      omrs-datafy/map->TypeDef)
    =>
    type-def))

(doseq [attr-type-def (.getKnownAttributeTypeDefs repo-helper)]
  (fact
    (-> attr-type-def
      datafy
      omrs-datafy/map->AttributeTypeDef)
    =>
    attr-type-def))

