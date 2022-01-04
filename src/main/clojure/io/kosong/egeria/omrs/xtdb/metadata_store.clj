(ns io.kosong.egeria.omrs.xtdb.metadata-store
  (:require [xtdb.api :as xt]
            [clojure.set :as set])
  (:import (java.util UUID Date)))

(compile 'clojure.tools.reader.reader-types)

(defn- random-uuid-str []
  (str (UUID/randomUUID)))

(defn- ensure-xt-id
  [guid-key e]
  (if (:xt/id e)
    e
    (let [guid (guid-key e)]
      (assoc e :xt/id guid))))

(def ensure-entity-xt-id
  (partial ensure-xt-id :openmetadata.Entity/guid))

(def ensure-type-def-xt-id
  (partial ensure-xt-id :openmetadata.TypeDef/guid))

(def ensure-attribute-type-def-xt-id
  (partial ensure-xt-id :openmetadata.AttributeTypeDef/guid))

(def ensure-relationship-xt-id
  (partial ensure-xt-id :openmetadata.Relationship/guid))

(defn- entity-exist? [node guid]
  (let [db (xt/db node)
        q  `{:find  [?e]
             :in    [?guid]
             :where [[?e :openmetadata.Entity/guid ?guid]]}
        rs (xt/q db q guid)]
    (not-empty rs)))

(defn fetch-entity-by-guid
  ([node guid]
   (fetch-entity-by-guid node guid (Date.)))
  ([node guid valid-time]
   (let [db  (xt/db node valid-time)
         q   `{:find  [?e]
               :in    [?guid]
               :where [[?e :openmetadata.Entity/guid ?guid]]}
         rs  (xt/q db q guid)
         eid (ffirst rs)]
     (when eid
       (-> (xt/entity db eid))))))

(defn- fetch-classifications-by-entity-guid
  ([node guid]
   (fetch-classifications-by-entity-guid node guid (Date.)))
  ([node guid valid-time]
   (let [db   (xt/db node valid-time)
         q    `{:find  [?c]
                :in    [?guid]
                :where [[?e :openmetadata.Entity/guid ?guid]
                        [?e :openmetadata.Entity/classifications ?c]]}
         rs   (xt/q db q guid)
         eids (map first rs)]
     (mapv #(xt/entity db %) eids))))

(defn fetch-classification-by-entity-guid-and-name
  ([node guid name]
   (fetch-classification-by-entity-guid-and-name node guid name (Date.)))
  ([node guid name valid-time]
   (let [db  (xt/db node valid-time)
         q   `{:find  [?c]
               :in    [?guid ?name]
               :where [[?e :openmetadata.Entity/guid ?guid]
                       [?e :openmetadata.Entity/classifications ?c]
                       [?c :openmetadata.Classification/name ?name]]}
         rs  (xt/q db q guid name)
         eid (ffirst rs)]
     (when eid
       (xt/entity db eid)))))

(defn fetch-entity-by-guid
  ([node guid]
   (fetch-entity-by-guid node guid (Date.)))
  ([node guid valid-time]
   (let [db              (xt/db node valid-time)
         q               `{:find  [?e]
                           :in    [?guid]
                           :where [[?e :openmetadata.Entity/guid ?guid]]}
         classifications (fetch-classifications-by-entity-guid node guid valid-time)
         rs              (xt/q db q guid)
         eid             (ffirst rs)]
     (when eid
       (-> (xt/entity db eid)
         (assoc :openmetadata.Entity/classifications classifications))))))

(defn- different? [left right]
  (not= (dissoc left :xt/id) (dissoc right :xt/id)))

(defn- fetch-document-by-attribute-value [node attr v]
  (let [db  (xt/db node)
        q   `{:find  [e]
              :where [[e ~attr ~v]]}
        rs  (xt/q db q)
        eid (ffirst rs)]
    (when eid
      (xt/entity db eid))))

(defn fetch-type-def-by-guid [node guid]
  (fetch-document-by-attribute-value node
    :openmetadata.TypeDef/guid guid))

(defn fetch-type-def-by-name [node name]
  (fetch-document-by-attribute-value node
    :openmetadata.TypeDef/name name))

(defn fetch-attribute-type-def-by-guid [node guid]
  (fetch-document-by-attribute-value node
    :openmetadata.AttributeTypeDef/guid guid))

(defn fetch-attribute-type-def-by-name [node name]
  (fetch-document-by-attribute-value node
    :openmetadata.AttributeTypeDef/name name))

(defn fetch-relationship-by-guid [node guid]
  (fetch-document-by-attribute-value node
    :openmetadata.Relationship/guid guid))

(defn- fetch-documents-by-attribute [node attribute]
  (let [db  (xt/db node)
        q   `{:find  [e]
              :where [[e ~attribute]]}
        rs  (xt/q db q)
        ids (map first rs)]
    (mapv #(xt/entity db %) ids)))

(defn fetch-type-defs [node]
  (fetch-documents-by-attribute node :openmetadata.TypeDef/guid))

(defn fetch-attribute-type-defs [node]
  (fetch-documents-by-attribute node :openmetadata.AttributeTypeDef/guid))

(defn type-def-version-later? [stored incoming]
  (let [{stored-guid    :openmetadata.TypeDef/guid
         stored-version :openmetadata.TypeDef/version} stored
        {incoming-guid    :openmetadata.TypeDef/guid
         incoming-version :openmetadata.TypeDef/version} incoming]
    (if-not stored
      true
      (and (= stored-guid incoming-guid) (< stored-version incoming-version)))))

(defn- type-def-valid-time [type-def]
  (or (:openmetadata.TypeDef/updateTime type-def)
    (:openmetadata.TypeDef/createTime type-def)
    (Date.)))

(defn- entity-valid-time [entity]
  (or (:openmetadata.Entity/updateTime entity)
    (:openmetadata.Entity/createTime entity)
    (Date.)))

(defn- relationship-valid-time [relationship]
  (or (:openmetadata.Relationship/updateTime relationship)
    (:openmetadata.Relationship/createTime relationship)
    (Date.)))


(defn- classification-valid-time [classification]
  (or (:openmetadata.Classification/updateTime classification)
    (:openmetadata.Classification/createTime classification)
    (Date.)))

(defn persist-type-def [node type-def]
  (let [guid       (:openmetadata.TypeDef/guid type-def)
        stored     (fetch-type-def-by-guid node guid)
        type-def   (ensure-type-def-xt-id type-def)
        valid-time (type-def-valid-time type-def)]
    (when (type-def-version-later? stored type-def)
      (xt/submit-tx node [[::xt/put type-def valid-time]]))))

(defn persist-attribute-type-def [node attribute-type-def]
  (let [guid               (:openmetadata.AttributeTypeDef/guid attribute-type-def)
        stored             (fetch-attribute-type-def-by-guid node guid)
        attribute-type-def (ensure-attribute-type-def-xt-id attribute-type-def)]
    (if (different? stored attribute-type-def)
      (xt/submit-tx node [[::xt/put attribute-type-def]]))))

(defn name->classification [classifications]
  (reduce (fn [m c]
            (let [k (:openmetadata.Classification/name c)]
              (assoc m k c)))
    {}
    classifications))

(defn ensure-classification-xt-id [name->classification-0 classification-1]
  (if (:xt/id classification-1)
    classification-1
    (let [cname            (:openmetadata.Classification/name classification-1)
          classification-0 (name->classification-0 cname)
          xt-id            (or (some-> classification-0 :xt/id)
                             (random-uuid-str))]
      (assoc classification-1 :xt/id xt-id))))

(defn persist-entity [node entity]
  (let [guid                   (:openmetadata.Entity/guid entity)
        entity-0               (fetch-entity-by-guid node guid)
        entity-1               (ensure-entity-xt-id entity)
        classifications-0      (:openmetadata.Entity/classifications entity-0)
        name->classification-0 (name->classification classifications-0)
        classifications-1      (->> (:openmetadata.Entity/classifications entity)
                                 (map #(ensure-classification-xt-id name->classification-0 %)))
        name->classification-1 (name->classification classifications-1)
        names-0                (set (keys name->classification-0))
        names-1                (set (keys name->classification-1))
        deleted                (set/difference names-0 names-1)
        added                  (set/difference names-1 names-0)
        changed                (reduce (fn [s cname]
                                         (let [c0 (name->classification-0 cname)
                                               c1 (name->classification-1 cname)]
                                           (if (different? c0 c1)
                                             (conj s cname)
                                             s)))
                                 #{}
                                 (set/intersection names-0 names-1))
        tx-ops                 (transient [])]
    (doseq [cname deleted]
      (let [eid (:xt/id (name->classification-0 cname))]
        (conj! tx-ops [::xt/delete eid])))
    (doseq [cname changed]
      (let [c (name->classification-1 cname)]
        (conj! tx-ops [::xt/put c (classification-valid-time c)])))
    (doseq [cname added]
      (let [c (name->classification-1 cname)]
        (conj! tx-ops [::xt/put c (classification-valid-time c)])))
    (let [entity-0 (assoc entity-0 :openmetadata.Entity/classifications
                                   (mapv :xt/id classifications-0))
          entity-1 (assoc entity-1 :openmetadata.Entity/classifications
                                   (mapv :xt/id classifications-1))]
      (when (different? entity-0 entity-1)
        (conj! tx-ops [::xt/put entity-1 (entity-valid-time entity-1)])))

    (let [tx-ops (persistent! tx-ops)]
      (when (not-empty tx-ops)
        (xt/submit-tx node tx-ops)))))

(defn persist-relationship [node relationship]
  (let [guid           (:openmetadata.Relationship/guid relationship)
        relationship-0 (fetch-relationship-by-guid node guid)
        relationship-1 (ensure-relationship-xt-id relationship)]
    (when (different? relationship-0 relationship-1)
      (xt/submit-tx node [[::xt/put relationship-1 (relationship-valid-time relationship-1)]]))))

