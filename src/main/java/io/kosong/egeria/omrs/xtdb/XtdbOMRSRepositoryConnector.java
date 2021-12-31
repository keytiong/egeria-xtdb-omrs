package io.kosong.egeria.omrs.xtdb;

import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;
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
import java.time.Duration;
import java.util.Map;

/**
 * The XtdbOMRSRepositoryConnector is a connector to a local open metadata repository that uses a Xtdb bitemporal data
 * store for its persistence.
 */
public class XtdbOMRSRepositoryConnector extends OMRSRepositoryConnector
{

    private static final String XTDB_METADATA_COLLECTION_CLASS_NAME = "io.kosong.egeria.omrs.xtdb.XtdbOMRSMetadataCollection";

    private static final String XTDB_CONFIG_FILE_PATH = "xtdbConfigPath";

    private static final Duration DEFAULT_XTDB_STARTUP_SYNC_DURATION = Duration.ofSeconds(60);

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

        if (!isActive()) {
            try {
                this.start();
            } catch (ConnectorCheckedException e) {
                throw new RepositoryErrorException(e);
            }
        }

        if (metadataCollection == null) {
            throw new OMRSLogicErrorException(OMRSErrorCode.NULL_METADATA_COLLECTION.getMessageDefinition(repositoryName),
                    this.getClass().getName(),
                    methodName);
        }

        return metadataCollection;
    }

    @Override
    public synchronized void start() throws ConnectorCheckedException {
        if (!isActive()) {
            startXtdbNode();
            initMetadataCollection();
        }
        super.start();
    }

    private void initMetadataCollection() throws ConnectorCheckedException {
        String methodName = "initMetadataCollection";
        if (metadataCollectionId != null) {
            try {
                Class<? extends OMRSMetadataCollection> clazz = Class.forName(XTDB_METADATA_COLLECTION_CLASS_NAME).asSubclass(OMRSMetadataCollection.class);

                Constructor<? extends OMRSMetadataCollection> constructor = clazz.getConstructor(
                        XtdbOMRSRepositoryConnector.class,
                        String.class,
                        OMRSRepositoryHelper.class,
                        OMRSRepositoryValidator.class,
                        String.class,
                        AuditLog.class);

                metadataCollection = constructor.newInstance(this, repositoryName, repositoryHelper,
                        repositoryValidator, metadataCollectionId, auditLog);
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

    private void startXtdbNode() throws ConnectorCheckedException {

        String methodName = "startXtdbNode";

        ConnectionProperties connectionProperties = getConnection();

        Map<String,Object> configProps = connectionProperties.getConfigurationProperties();

        String xtdbConfigPath = (String) configProps.get(XTDB_CONFIG_FILE_PATH);

        File configFile = null;

        if (xtdbConfigPath != null) {
            new File(xtdbConfigPath);
        }

        if (xtdbNode == null) {
            try {
                if (configFile != null) {
                    xtdbNode = IXtdb.startNode(configFile);
                } else {
                    xtdbNode = IXtdb.startNode();
                }
                xtdbNode.sync(DEFAULT_XTDB_STARTUP_SYNC_DURATION);
            } catch (Throwable t) {
                ExceptionMessageDefinition messageDefinition = OMRSErrorCode.UNEXPECTED_EXCEPTION.getMessageDefinition();
                throw new ConnectorCheckedException(messageDefinition, this.getClass().getName(), methodName, t);
            }
        }
    }

    @Override
    public void disconnect() throws ConnectorCheckedException {
        String methodName = "disconnect";
        stopXtdbNode();
        super.disconnect();
    }

    private void stopXtdbNode() throws ConnectorCheckedException {
        String methodName = "stopXtdbNode";
        if (xtdbNode != null) {
            try {
                xtdbNode.close();
                xtdbNode = null;
                metadataCollection = null;
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