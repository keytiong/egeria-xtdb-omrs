package io.kosong.egeria.omrs.xtdb;



import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditingComponent;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnectorProviderBase;

/**
 * In the Open Connector Framework (OCF), a ConnectorProvider is a factory for a specific type of connector.
 * The XtdbOMRSRepositoryConnectorProvider is the connector provider for the InMemoryOMRSRepositoryConnector.
 * It extends OMRSRepositoryConnectorProviderBase which in turn extends the OCF ConnectorProviderBase.
 * ConnectorProviderBase supports the creation of connector instances.
 *
 * The XtdbOMRSRepositoryConnectorProvider must initialize ConnectorProviderBase with the Java class
 * name of the OMRS Connector implementation (by calling super.setConnectorClassName(className)).
 * Then the connector provider will work.
 */
public class XtdbOMRSRepositoryConnectorProvider extends OMRSRepositoryConnectorProviderBase
{
    static final String  connectorTypeGUID = "8c1e2220-a0c7-4344-9e60-f071c40a23ea";
    static final String  connectorTypeName = "OMRS Xtdb Repository Connector";
    static final String  connectorTypeDescription = "OMRS Repository Connector that uses a Xtdb bitemporal data store.";


    /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * OMRS Connector implementation.
     */
    public XtdbOMRSRepositoryConnectorProvider()
    {
        Class<?>    connectorClass = XtdbOMRSRepositoryConnector.class;

        super.setConnectorClassName(connectorClass.getName());

        ConnectorType connectorType = new ConnectorType();
        connectorType.setType(ConnectorType.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(this.getClass().getName());

        super.connectorTypeBean = connectorType;
        super.setConnectorComponentDescription(OMRSAuditingComponent.REPOSITORY_CONNECTOR);
    }
}