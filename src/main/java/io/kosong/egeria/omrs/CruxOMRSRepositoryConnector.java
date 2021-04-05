package io.kosong.egeria.omrs;

import crux.api.Crux;
import crux.api.ICruxAPI;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSMetadataCollection;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.OMRSLogicErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;

import java.io.File;
import java.io.IOException;

/**
 * The CruxOMRSRepositoryConnector is a connector to a local open metadata repository that uses a Crux bitemporal data
 * store for its persistence.
 */
public class CruxOMRSRepositoryConnector extends OMRSRepositoryConnector
{

    private ICruxAPI cruxNode;

    /**
     * Default constructor used by the OCF Connector Provider.
     */
    public CruxOMRSRepositoryConnector()
    {
        /*
         * Nothing to do (yet !)
         */
    }

    @Override
    public OMRSMetadataCollection getMetadataCollection() throws RepositoryErrorException{

        String methodName = "getMetadataCollection";

        if (metadataCollection == null) {

            if (metadataCollectionId != null) {
                try {
                    cruxNode = Crux.startNode(new File("data/crux-node.edn"));
                    metadataCollection = new CruxOMRSMetadataCollection(this,
                            repositoryName, repositoryHelper, repositoryValidator, metadataCollectionId, auditLog, cruxNode);
                } catch (Throwable t) {
                    ExceptionMessageDefinition messageDefinition = OMRSErrorCode.UNEXPECTED_EXCEPTION.getMessageDefinition();
                    throw new RepositoryErrorException(messageDefinition, this.getClass().getName(), methodName, t);
                }
            } else {
                throw new OMRSLogicErrorException(OMRSErrorCode.NULL_METADATA_COLLECTION.getMessageDefinition(repositoryName),
                        this.getClass().getName(),
                        methodName);
            }
        }
        return metadataCollection;
    }

    @Override
    public void start() throws ConnectorCheckedException {
        super.start();
    }

    @Override
    public void disconnect() throws ConnectorCheckedException {
        String methodName = "disconnect";
        if (cruxNode != null) {
            try {
                cruxNode.close();
                metadataCollection = null;
                super.disconnect();
            } catch (IOException e) {
                ExceptionMessageDefinition messageDefinition = OMRSErrorCode.UNEXPECTED_EXCEPTION.getMessageDefinition();
                throw new ConnectorCheckedException(messageDefinition, this.getClass().getName(), methodName, e);
            }

        }
    }

    public ICruxAPI getCruxNode() {
        return cruxNode;
    }
}