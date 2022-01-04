(ns io.kosong.egeria.omrs.xtdb.xtdb-metadata-collection-test
  (:use [midje.sweet])
  (:require [io.kosong.egeria.omrs :as om]
            [io.kosong.egeria.omrs.datafy :as om-datafy]
            [integrant.core :as ig]
            [dev]
            [xtdb.api :as xt]
            [clojure.datafy :refer [datafy]]
            [io.kosong.egeria.omrs.xtdb.metadata-store :as store])
  (:import (java.util UUID Date)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances InstanceStatus EntityDetail PrimitivePropertyValue Classification)))

(def system
  (ig/init dev/dev-config))

(om/set-context! (dev/context system))



(def user-alice "alice")
(def user-bob "bob")

(def metadata-collection-id
  (-> system :dev/repository-config :metadata-collection-id))

(def metadata-collection-name
  (-> system :dev/repository-config :metadata-collection-name))


;; Easy reference to the metadata collection test subject
(def metadata-collection
  (dev/metadata-collection system))


;; We will test metadata collection entity lifecycle operations with Glossary

;; EntityDef
(def glossary-type (om/find-type-def-by-name "Glossary"))
(def glossary-term-type (om/find-type-def-by-name "ControlledGlossaryTerm"))

;; Glossary -> Glossary Term RelationshipDef
(def term-anchor-type (om/find-type-def-by-name "TermAnchor"))

;; ClassificationDef
(def anchors-type-def (om/find-type-def-by-name "Anchors"))
(def confidence-type-def (om/find-type-def-by-name "Confidence"))

(def glossary-type-guid (:openmetadata.TypeDef/guid glossary-type))
(def glossary-term-type-guid (:openmetadata.TypeDef/guid glossary-term-type))
(def term-anchor-type-guid (:openmetadata.TypeDef/guid term-anchor-type))

(defn- InstanceProperties-for [type-def m]
  (om-datafy/map->InstanceProperties type-def m))

;; Invoke addEntity to create a new entity
(def glossary-001-v1
  (.addEntity metadata-collection
    user-alice
    glossary-type-guid
    (InstanceProperties-for glossary-type
      {:openmetadata.Referenceable/qualifiedName "OMRS:Glossary-001"
       :openmetadata.Glossary/displayName "Glossary 001"
       :openmetadata.Glossary/description "Glossary 001 description"})
    [] ;; Classification list
    InstanceStatus/ACTIVE))

(def glossary-001-guid (.getGUID glossary-001-v1))

(facts "addEntity()"
  (fact
    (type glossary-001-v1) => EntityDetail)

  (fact
    (.getVersion glossary-001-v1) => 1)

  (fact
    (.getCreatedBy glossary-001-v1) => user-alice)

  (fact
    (type (.getCreateTime glossary-001-v1)) => Date)

  (fact
    (.getMetadataCollectionId glossary-001-v1) => metadata-collection-id)

  (fact
    (.getMetadataCollectionName glossary-001-v1) => metadata-collection-name)

  (fact
    glossary-001-guid =test=> (fn [x] (UUID/fromString x))))

(fact "isEntityKnown()"
  (.isEntityKnown metadata-collection
    user-alice
    glossary-001-guid) => glossary-001-v1)

(def glossary-term-001-v1
  (.addEntity metadata-collection
    user-alice
    glossary-term-type-guid
    (InstanceProperties-for glossary-type
      {:openmetadata.Referenceable/qualifiedName "GlossaryTerm-001"
       :openmetadata.GlossaryTerm/displayName "Glossary Term 001"
       :openmetadata.GlossaryTerm/description "Glossary Term 001 description"})
    [] ;; Classification list
    InstanceStatus/DRAFT))

(def glossary-term-001-guid (.getGUID glossary-term-001-v1))

(def glossary-term-001-v2
  (.updateEntityProperties metadata-collection
    user-bob
    glossary-term-001-guid
    (InstanceProperties-for glossary-term-type
      {:openmetadata.Referenceable/qualifiedName "GlossaryTerm-001"
       :openmetadata.GlossaryTerm/displayName "Glossary Term 001"
       :openmetadata.GlossaryTerm/description "Glossary Term 001 description v2"})))

(facts "updateEntityProperties()"

  (fact
    (.getVersion glossary-term-001-v2) => 2)

  (fact
    (.getUpdatedBy glossary-term-001-v2) => user-bob)

  (fact
    (.getUpdateTime glossary-term-001-v2) =not=> nil)

  (fact
    (some-> glossary-term-001-v2
      .getProperties
      .getInstanceProperties
      (.get "description")
      .valueAsString)
    => "Glossary Term 001 description v2"))


(def glossary-term-001-v3
  (.updateEntityStatus metadata-collection
    user-alice
    glossary-term-001-guid
    InstanceStatus/PROPOSED))

(facts "updateEntityStatus()"
  (fact
    (.getStatus glossary-term-001-v3) => InstanceStatus/PROPOSED)

  (fact
    (.getVersion glossary-term-001-v3) => 3))


(def glossary-term-001-v4
  (.classifyEntity metadata-collection
    user-alice
    glossary-term-001-guid
    "Anchors"
    (InstanceProperties-for anchors-type-def
      {:openmetadata.Anchors/anchorGUID glossary-001-guid})))

(xt/sync (dev/xtdb-node system))

(def glossary-term-001-v5
  (.classifyEntity metadata-collection
    user-alice
    glossary-term-001-guid
    "Confidence"
    (InstanceProperties-for confidence-type-def
      {:openmetadata.Confidence/confidence 51})))

(facts "classifyEntity()"
  (fact
    (.getVersion glossary-term-001-v4) => 4)

  (fact
    (-> (datafy glossary-term-001-v4)
      :openmetadata.Entity/classifications
      count)
    => 1)

  (fact
    (.getVersion glossary-term-001-v5) => 5)

  (fact
    (-> (datafy glossary-term-001-v5)
      :openmetadata.Entity/classifications
      count)
    => 2))

(def glossary-term-001-v6
  (.updateEntityClassification metadata-collection
    user-alice
    glossary-term-001-guid
    "Confidence"
    (InstanceProperties-for confidence-type-def
      {:openmetadata.Confidence/confidence 95})))

(facts "updateEntityClassification()"
  (fact
    (.getVersion glossary-term-001-v6) => 6)

  (fact
    (->> (datafy glossary-term-001-v6)
      :openmetadata.Entity/classifications
      (filter #(= "Confidence" (:openmetadata.Classification/name %)))
      (first)
      :openmetadata.Classification/version) => 2)

  (fact
    (->> (datafy glossary-term-001-v6)
      :openmetadata.Entity/classifications
      (filter #(= "Confidence" (:openmetadata.Classification/name %)))
      (first)
      :openmetadata.Confidence/confidence) => 95))

(def glossary-term-001-v7
  (.declassifyEntity metadata-collection
    user-alice
    glossary-term-001-guid
    "Anchors"))

(facts "declassifyEntity()"

  (fact
    (.getVersion glossary-term-001-v7) => 7)

  (fact
    (->> (datafy glossary-term-001-v7)
      :openmetadata.Entity/classifications
      (filter #(= "Anchors" (:openmetadata.Classification/name %)))
      empty?) => truthy))

(def relationship-001
  (.addRelationship metadata-collection
    user-alice
    term-anchor-type-guid
    nil
    glossary-001-guid
    glossary-term-001-guid
    InstanceStatus/ACTIVE))

(def relationship-001-guid (.getGUID relationship-001))

(facts "addRelationship()"
  (fact
    (-> relationship-001 .getEntityOneProxy .getGUID) => glossary-001-guid)
  (fact
    (-> relationship-001 .getEntityTwoProxy .getGUID) => glossary-term-001-guid)

  (fact
    (-> relationship-001 .getEntityOneProxy .getType .getTypeDefGUID) => glossary-type-guid)

  (fact
    (-> relationship-001 .getEntityTwoProxy .getType .getTypeDefGUID) => glossary-term-type-guid))



;;
;; clean up
;;

(om/set-context! nil)

(ig/halt! system)