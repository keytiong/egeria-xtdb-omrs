package io.kosong.egeria.omrs.xtdb;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator;
import xtdb.api.IXtdb;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSMetadataCollection;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.OMRSLogicErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * The XtdbOMRSRepositoryConnector is a connector to a local open metadata repository that uses a Xtdb bitemporal data
 * store for its persistence.
 */
public class XtdbOMRSRepositoryConnector extends OMRSRepositoryConnector
{

    private IXtdb xtdbNode;

    /**
     * Default constructor used by the OCF Connector Provider.
     */
    public XtdbOMRSRepositoryConnector()
    {
        /*
         * Nothing to do (yet !)
         */
    }

    @Override
    public OMRSMetadataCollection getMetadataCollection() throws RepositoryErrorException{

        String methodName = "getMetadataCollection";

        if (metadataCollection == null) {
            throw new OMRSLogicErrorException(OMRSErrorCode.NULL_METADATA_COLLECTION.getMessageDefinition(repositoryName),
                    this.getClass().getName(),
                    methodName);
        }

        return metadataCollection;
    }

    @Override
    public void start() throws ConnectorCheckedException {
        String methodName = "start";
        super.start();
        if (metadataCollectionId != null) {
            try {
                xtdbNode = IXtdb.startNode(new File("data/xtdb-node.edn"));
                Class clazz = Class.forName("io.kosong.egeria.omrs.xtdb.XtdbOMRSMetadataCollection");
                Constructor constructor = clazz.getConstructor(new Class[] {
                        XtdbOMRSRepositoryConnector.class,
                        String.class,
                        OMRSRepositoryHelper.class,
                        OMRSRepositoryValidator.class,
                        String.class
                });
                metadataCollection = (OMRSMetadataCollection) constructor.newInstance(this,
                        repositoryName, repositoryHelper, repositoryValidator, metadataCollectionId);
                metadataCollection.setAuditLog(auditLog);
            } catch (Throwable t) {
                ExceptionMessageDefinition messageDefinition = OMRSErrorCode.UNEXPECTED_EXCEPTION.getMessageDefinition();
                throw new ConnectorCheckedException(messageDefinition, this.getClass().getName(), methodName, t);
            }
        } else {
            throw new OMRSLogicErrorException(OMRSErrorCode.NULL_METADATA_COLLECTION.getMessageDefinition(repositoryName),
                    this.getClass().getName(),
                    methodName);
        }
    }

    @Override
    public void disconnect() throws ConnectorCheckedException {
        String methodName = "disconnect";
        if (xtdbNode != null) {
            try {
                xtdbNode.close();
                metadataCollection = null;
                super.disconnect();
            } catch (IOException e) {
                ExceptionMessageDefinition messageDefinition = OMRSErrorCode.UNEXPECTED_EXCEPTION.getMessageDefinition();
                throw new ConnectorCheckedException(messageDefinition, this.getClass().getName(), methodName, e);
            }

        }
    }

    public IXtdb getXtdbNode() {
        return xtdbNode;
    }
}