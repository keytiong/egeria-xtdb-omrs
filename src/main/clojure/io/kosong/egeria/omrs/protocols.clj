(ns io.kosong.egeria.omrs.protocols
  (:require [clojure.core.protocols :as p])
  (:import (org.odpi.openmetadata.repositoryservices.rest.properties InstanceHistoricalFindRequest)
           (org.odpi.openmetadata.repositoryservices.localrepository.repositorycontentmanager OMRSRepositoryContentHelper)
           (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore OMRSMetadataCollection)))

(defprotocol TypeStore
  (list-type-defs [this])
  (list-attribute-type-defs [this])
  (fetch-type-def-by-guid [this guid])
  (fetch-type-def-by-name [this name])
  (fetch-attribute-type-def-by-guid [this guid]))

(defprotocol InstanceStore
  (fetch-entity-by-guid [this guid])
  (fetch-relationship-by-guid [this guid]))

(extend-type OMRSRepositoryContentHelper
  TypeStore
  (list-type-defs [this]
    (map p/datafy (.getKnownTypeDefs this)))
  (list-attribute-type-defs [this]
    (map p/datafy (.getKnownAttributeTypeDefs this)))
  (fetch-type-def-by-guid [this guid]
    (.getTypeDef this "TypeStore" "guid" guid "fetch-type-def-by-guid"))
  (fetch-type-def-by-name [this name]
    (.getTypeDefByName this "TypeStore" name))
  (fetch-attribute-type-def-by-guid [this guid]
    (.getAttributeTypeDef this "TypeStore" guid "fetch-attribute-type-def-by-guid")))

(extend-type OMRSMetadataCollection
  InstanceStore
  (fetch-entity-by-guid [^OMRSMetadataCollection this guid]
    (p/datafy (.getEntityDetail this "system" guid))))