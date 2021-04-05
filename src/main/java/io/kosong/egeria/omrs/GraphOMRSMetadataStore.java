package io.kosong.egeria.omrs;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.MatchCriteria;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.search.SearchProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefCategory;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.EntityNotKnownException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.EntityProxyOnlyException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;

import java.util.List;
import java.util.Map;

public interface GraphOMRSMetadataStore {

    void createEntityIndexes(TypeDef typeDef);

    void createRelationshipIndexes(TypeDef typeDef);

    void createClassificationIndexes(TypeDef typeDef);

    EntityDetail createEntityInStore(EntityDetail newEntity);

    void createEntityProxyInStore(EntityProxy entityProxy);

    void createRelationshipInStore(Relationship relationship);

    EntityDetail getEntityDetailFromStore(String guid) throws EntityProxyOnlyException, EntityNotKnownException, RepositoryErrorException;

    Relationship getRelationshipFromStore(String guid);

    EntitySummary getEntitySummaryFromStore(String guid);

    EntityProxy getEntityProxyFromStore(String entityOneGUID);

    List<Relationship> getRelationshipsForEntity(String entityGUID);

    void updateEntityInStore(EntityDetail updatedEntity);

    void updateRelationshipInStore(Relationship updatedRelationship);

    void removeRelationshipFromStore(String guid);

    void removeEntityFromStore(String guid);

    List<EntityDetail> findEntitiesForType(
            String typeName,
            SearchProperties searchProperties,
            Boolean fullMatch);

    List<EntityDetail> findEntitiesForTypes(
            List<String> validTypeNames,
            String filterTypeName,
            Map<String, TypeDefAttribute> qualifiedPropertyNameToTypeDefinedAttribute,
            Map<String, List<String>> shortPropertyNameToQualifiedPropertyNames,
            SearchProperties matchProperties);

    List<EntityDetail> findEntitiesByPropertyForType(String typeName, InstanceProperties searchProperties, MatchCriteria matchCriteria, Boolean fullMatch);

    List<EntityDetail> findEntitiesByPropertyForTypes(
            List<String> validTypeNames,
            String filterTypeName,
            Map<String, TypeDefAttribute> qualifiedPropertyNameToTypeDefinedAttribute,
            Map<String, List<String>> shortPropertyNameToQualifiedPropertyNames,
            InstanceProperties matchProperties,
            MatchCriteria matchCriteria);

    List<EntityDetail> findEntitiesByPropertyValueForTypes(
            List<String> validTypeNames,
            String filterTypeName,
            Map<String, TypeDefAttribute> qualifiedPropertyNameToTypeDefinedAttribute,
            Map<String, List<String>> shortPropertyNameToQualifiedPropertyNames,
            InstanceProperties matchProperties,
            MatchCriteria matchCriteria);

    List<EntityDetail> findEntitiesByClassification(String classificationName, InstanceProperties matchClassificationProperties, MatchCriteria matchCriteria, Boolean performTypeFiltering, List<String> validTypeNames);

    List<Relationship> findRelationshipsForType(
            String typeName,
            SearchProperties searchProperties,
            Boolean fullMatch);

    List<Relationship> findRelationshipsForTypes(
            List<String> validTypeNames,
            String filterTypeName,
            Map<String, TypeDefAttribute> qualifiedPropertyNameToTypeDefinedAttribute,
            Map<String, List<String>> shortPropertyNameToQualifiedPropertyNames,
            SearchProperties matchProperties);

    List<Relationship> findRelationshipsByPropertyForType(
            String typeName,
            InstanceProperties matchProperties,
            MatchCriteria matchCriteria,
            Boolean fullMatch);

    List<Relationship> findRelationshipsByPropertyForTypes(
            List<String> validTypeNames,
            String filterTypeName,
            Map<String, TypeDefAttribute> qualifiedPropertyNameToTypeDefinedAttribute,
            Map<String, List<String>> shortPropertyNameToQualifiedPropertyNames,
            InstanceProperties matchProperties,
            MatchCriteria matchCriteria);

    List<Relationship> findRelationshipsByPropertyValueForTypes(
            List<String> validTypeNames,
            String filterTypeName,
            Map<String, TypeDefAttribute> qualifiedPropertyNameToTypeDefinedAttribute,
            Map<String, List<String>> shortPropertyNameToQualifiedPropertyNames,
            InstanceProperties matchProperties,
            MatchCriteria matchCriteria);

    InstanceProperties constructMatchPropertiesForSearchCriteriaForTypes(
            TypeDefCategory typeDefCategory,
            String searchCriteria,
            String filterTypeName,
            List<String> validTypeNames);

    void saveEntityReferenceCopyToStore(EntityDetail entity);

    void saveRelationshipReferenceCopyToStore(Relationship relationship);

    InstanceGraph getSubGraph(String entityGUID, List<String> entityTypeGUIDs, List<String> relationshipTypeGUIDs, List<InstanceStatus> limitResultsByStatus, List<String> limitResultsByClassification, int level);

    InstanceGraph getPaths(
            String startEntityGUID,
            String endEntityGUID,
            List<InstanceStatus> limitResultsByStatus,
            int maxPaths,
            int maxDepth);
}
