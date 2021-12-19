(ns io.kosong.egeria.omrs-test
  (:use [midje.sweet])
  (:require [io.kosong.egeria.omrs :as omrs]
            [clojure.datafy :refer [datafy]]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str]))

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


(defn list-attribute-type-defs [openmetadata-types-archive]
  (let [archive-store (.getArchiveTypeStore openmetadata-types-archive)]
    (.getAttributeTypeDefs archive-store)))

(defn attribute-type-def-by-name [openmetadata-types-archive name]
  (->> (list-attribute-type-defs openmetadata-types-archive)
    (filter #(= (.getName %) name))
    (first)))

(defn list-type-defs [openmetadata-types-archive]
  (let [archive-store (.getArchiveTypeStore openmetadata-types-archive)]
    (.getNewTypeDefs archive-store)))

(defn type-def-by-name [openmetadata-types-archive name]
  (->> (list-type-defs openmetadata-types-archive)
    (filter #(= (.getName %) name))
    (first)))

;;
;; Attribute Type Defs
;;
(def primitive-string-def
  (attribute-type-def-by-name openmetadata-types-archive "string"))

(def string-string-map-def
  (attribute-type-def-by-name openmetadata-types-archive "map<string,string>"))

(def string-long-map-def
  (attribute-type-def-by-name openmetadata-types-archive "map<string,long>"))

(def order-by-enum-def
  (attribute-type-def-by-name openmetadata-types-archive "OrderBy"))

;;
;; Entity Def
;;

(def referenceable-type-def
  (type-def-by-name openmetadata-types-archive "Referenceable"))

(def asset-type-def
  (type-def-by-name openmetadata-types-archive "Asset"))

;;
;; Relationship Def
;;

(def foreign-key-type-def
  (type-def-by-name openmetadata-types-archive "ForeignKey"))

;;
;; Classification Def
;;

(def anchors-type-def
  (type-def-by-name openmetadata-types-archive "Anchors"))


(facts "Datafy PrimitiveDef"
  (let [datafied (clojure.datafy/datafy primitive-string-def)]
    (fact "attribute type def name"
      (:openmetadata.AttributeTypeDef/name datafied) => "string")
    (fact "attribute type def category"
      (:openmetadata.AttributeTypeDef/category datafied)
      => "PRIMITIVE")
    (fact "attribute type def guid"
      (:openmetadata.AttributeTypeDef/guid datafied)
      => "b34a64b9-554a-42b1-8f8a-7d5c2339f9c4")
    (fact "attribute type def version"
      (:openmetadata.AttributeTypeDef/version datafied)
      => 1)
    (fact "attribute type def version name"
      (:openmetadata.AttributeTypeDef/versionName datafied)
      => "1.0")
    (fact "Primitive Def Category"
      (:openmetadata.PrimitiveDef/primitiveDefCategory datafied)
      => "OM_PRIMITIVE_TYPE_STRING")))

(facts "Round trip AttributeTypeDef -> map -> AttributeTypeDef"
  (fact "primitive string"
    (-> primitive-string-def clojure.datafy/datafy omrs/map->AttributeTypeDef)
    => primitive-string-def)

  (fact "map<string,string> data"
    (-> string-string-map-def clojure.datafy/datafy omrs/map->AttributeTypeDef)
    => string-string-map-def)

  (fact "map<string,map>"
    (-> string-long-map-def clojure.datafy/datafy omrs/map->AttributeTypeDef)
    => string-long-map-def)

  (fact "enum"
    (-> order-by-enum-def clojure.datafy/datafy omrs/map->AttributeTypeDef)
    => order-by-enum-def))

(facts "Round trip TypeDef -> map -> TypeDef"
  (fact "top level entity def"
    (-> referenceable-type-def clojure.datafy/datafy omrs/map->TypeDef)
    => referenceable-type-def)
  (fact "subtype entity def"
    (-> asset-type-def clojure.datafy/datafy omrs/map->TypeDef)
    => asset-type-def))

(facts "Round trip TypeDef -> map -> TypeDef"
  (fact "relationship def"
    (-> foreign-key-type-def clojure.datafy/datafy omrs/map->TypeDef)
    => foreign-key-type-def))

(facts "Round trip TypeDef -> map -> TypeDef"
  (fact "classification def"
    (-> anchors-type-def clojure.datafy/datafy omrs/map->TypeDef)
    => anchors-type-def))

