(ns io.kosong.egeria.omrs
  (:require [clojure.string :as str]
            [clojure.core.protocols :as p]
            [clojure.datafy :refer [datafy]]
            [clojure.tools.logging :as log]
            [io.kosong.egeria.omrs.protocols :as om-p]
            [io.kosong.egeria.omrs.xtdb.metadata-store]
            [io.kosong.egeria.omrs.xtdb.metadata-store :as store])
  (:import (java.util Date)))

(defonce ^:dynamic *context* nil)

(defn ->context [{:keys [type-store instance-store]}]
  {:type-store     type-store
   :instance-store instance-store})

(defn set-context! [context]
  (alter-var-root #'*context* (constantly context))
  nil)

(defn find-type-def-by-guid
  [guid]
  (if-let [type-store (:type-store *context*)]
    (om-p/fetch-type-def-by-guid type-store guid)))

(defn find-type-def-by-name
  [type-def-name]
  (if-let [type-store (:type-store *context*)]
    (om-p/fetch-type-def-by-name type-store type-def-name)))

(defn find-attribute-type-def-by-guid
  [guid]
  (let [type-store (:type-store *context*)]
    (om-p/fetch-attribute-type-def-by-guid type-store guid)))

(defn find-type-def-ancestors
  [type-def]
  (loop [ts [] t type-def]
    (let [parent (some-> t
                   :openmetadata.TypeDef/superType
                   find-type-def-by-guid)]
      (if parent
        (recur (conj ts parent) parent)
        ts))))

(defn list-type-defs
  []
  (if-let [type-store (:type-store *context*)]
    (om-p/list-type-defs type-store)))

(defn list-attribute-type-defs
  []
  (if-let [type-store (:type-store *context*)]
    (om-p/list-attribute-type-defs type-store)))

(defn find-type-def-descendants
  [type-def]
  (let [type-def-guid (:openmetadata.TypeDef/guid type-def)
        descendants?  (fn [t]
                        (let [ids (->> (find-type-def-ancestors t)
                                    (map :openmetadata.TypeDef/guid)
                                    set)]
                          (ids type-def-guid)))]
    (->> (list-type-defs)
      (filter descendants?))))

(defn find-type-def-by-property-name
  [property-name]
  (let [f (fn [t]
            (->> (:openmetadata.TypeDef/propertiesDefinition t)
              (filter #(= (:openmetadata.TypeDefAttribute/attributeName %) property-name))
              (not-empty)))]
    (->> (list-type-defs)
      (filter f))))

(defn find-entity-by-guid [guid]
  [guid]
  (if-let [instance-store (:instance-store *context*)]
    (om-p/fetch-entity-by-guid instance-store guid)))

(defn namespace-for-type-def [type-def]
  (if-let [type-name (:openmetadata.TypeDef/name type-def)]
    (str "openmetadata." type-name)))

(defn- qualify-attribute-name-with-type [type-def attr-def]
  (let [type-def-ns   (namespace-for-type-def type-def)
        attr-name (:openmetadata.TypeDefAttribute/attributeName attr-def)]
    (keyword type-def-ns attr-name)))

(defn- deduplicate-overridden-attribute-names [type-def-attribute-pairs]
  (->> type-def-attribute-pairs
    (reduce (fn [m [type-def attr-type-def :as pair]]
              (let [k (:openmetadata.TypeDefAttribute/attributeName attr-type-def)]
                (assoc m k pair)))
      {})
    (vals)))

(defn- all-type-def-attributes [type-def]
  (->> (find-type-def-ancestors type-def)
    (cons type-def)
    (reverse)
    (mapcat (fn [type-def]
              (map (fn [attr-type-def]
                     [type-def attr-type-def])
                (:openmetadata.TypeDef/propertiesDefinition type-def))))
    deduplicate-overridden-attribute-names))

(defn type-def-attribute-key->attribute [type-def]
  (reduce (fn [m [type-def type-def-attr]]
            (let [k (qualify-attribute-name-with-type type-def type-def-attr)]
              (assoc m k type-def-attr)))
    {}
    (all-type-def-attributes type-def)))

(defn attribute-keys [type-def]
  (->> (type-def-attribute-key->attribute type-def)
    (keys)))

(defn list-attribute-keys []
  (->> (list-type-defs)
    (mapcat attribute-keys)
    set))

(defn find-attribute-type-def-by-name [name]
  (->> (list-attribute-type-defs)
    (filter #(= (:openmetadata.AttributeTypeDef/name %) name))
    first))

(defn skeleton-entity
  [{:keys [guid metadata-collection-id metadata-collection-name instance-provenance-type
           user-name type-name]}]
  (let [type-def       (find-type-def-by-name type-name)
        type-def-guid  (:openmetadata.TypeDef/guid type-def)
        initial-status (:openmetadata.TypeDef/initialStatus type-def)]
    (when type-def
      #:openmetadata.Entity
          {:headerVersion          1
           :instanceProvenanceType instance-provenance-type
           :metadataCollectionId   metadata-collection-id
           :metadataCollectionName metadata-collection-name
           :guid                   guid
           :createTime             (Date.)
           :version                1
           :type                   type-def-guid
           :status                 initial-status
           :createdBy              user-name})))

(defn skeleton-classification
  [{:keys [metadata-collection-id metadata-collection-name instance-provenance-type
           replicated-by user-name type-name]}]
  (let [type-def       (find-type-def-by-name type-name)
        type-def-guid  (:openmetadata.TypeDef/guid type-def)
        initial-status (:openmetadata.TypeDef/initialStatus type-def)]
    (when type-def
      #:openmetadata.Classification
          {:headerVersion          1
           :instanceProvenanceType instance-provenance-type
           :metadataCollectionId   metadata-collection-id
           :metadataCollectionName metadata-collection-name
           :createTime             (Date.)
           :version                1
           :name                   (:openmetadata.TypeDef/name type-def)
           :type                   type-def-guid
           :status                 initial-status
           :createdBy              user-name
           :replicatedBy           replicated-by})))
