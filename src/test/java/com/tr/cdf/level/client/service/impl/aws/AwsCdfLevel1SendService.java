/*
 *
 */
package com.tr.cdf.level.client.service.impl.aws;

import com.tr.cdf.Envelope;
import com.tr.cdf.aws.common.logging.Logger;
import com.tr.cdf.aws.common.logging.LoggerFactory;
import com.tr.cdf.aws.common.logging.model.DefaultLogEvent;
import com.tr.cdf.level.client.service.impl.aws.components.ComponentsProvider;
import com.tr.cdf.level1.client.publish.CdfLevel1ClientRuntimeException;
import com.tr.cdf.level1.client.publish.EnvelopeSerialization;
import com.tr.cdf.level1.client.publish.SystemProperties;
import com.tr.cdf.level1.client.publish.api.CdfLevel1Service;
import com.tr.cdf.level1.client.publish.api.RetryableActionRunner;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;
import org.kohsuke.MetaInfServices;

/**
 *
 * @author Bronisław Truszkowski {@literal <}bronislaw.truszkowski@thomsonreuters.com{@literal >}
 */
@MetaInfServices
public class AwsCdfLevel1SendService implements CdfLevel1Service {

    private static final Logger LOG = LoggerFactory.getLogger(AwsCdfLevel1SendService.class);

    private final EnvelopeSerialization serialization = new EnvelopeSerialization();

    @Override
    public Response sendSerializedEnvelope(byte[] serializedData) {
        Objects.requireNonNull(serializedData, "Serialized envelope data cannot be null");
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("sendSerializedEnvelope called with %d bytes", serializedData.length));
        }
        final Envelope envelope = serialization.deserialize(serializedData);

        return sendEnvelope(envelope, serializedData);
    }

    /**
     * Sends an envelope that is not serialized. It's faster to use this method than the one with byte array on input.
     *
     * @param envelope
     * @return
     */
    public Response sendEnvelope(Envelope envelope) {
        return sendEnvelope(envelope, null);
    }

    /**
     * Sends an envelope that is not serialized. It's faster to use this method than the one with byte array on input.
     *
     * @param envelope
     * @return
     */
    private Response sendEnvelope(Envelope envelope, byte[] serializedEnvelope) {
        if (LOG.isTraceEnabled()) {
            final DefaultLogEvent logEvent = new DefaultLogEvent().setMessage("Aws configuration properties values");
            for (AwsServiceSystemProperties property : AwsServiceSystemProperties.values()) {
                logEvent.set(property.getPropertyName(), property.get());
            }
            LOG.trace(logEvent);
        }

        fillHeader(envelope);

        final SendEnvelopeAction action = new SendEnvelopeAction(
                envelope,
                ComponentsProvider.getEnvelopePersister(),
                AwsServiceSystemProperties.getNotificationServiceType()
        );
        action.setSerializedEnvelope(serializedEnvelope);

        final Response result;
        if (AwsServiceSystemProperties.NO_RETRIES_ON_SERVICE_LEVEL.getBoolean()) {
            result = action.run();
        } else {
            final RetryableActionRunner<CdfLevel1Service.Response> actionRunner
                    = new RetryableActionRunner<>(SystemProperties.RETRY_UNLIMITED_RETRIES.getBoolean(),
                            SystemProperties.RETRY_MAX_RETRIES.getInt(), SystemProperties.RETRY_EXPONENTIAL_MAX_SECONDS.getInt()
                    );
            try {
            result = actionRunner.runWithRetries(action);
            } catch(Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
        LOG.debug(result);

        return result;
    }

    protected void fillHeader(Envelope envelope) {
        if (envelope != null) {
            if (envelope.getHeader() == null) {
                envelope.setHeader(new HashMap<String, String>(1));
            }
            envelope.getHeader().put(VERSION_PROPERTY_NAME, HeaderValueHolder.INSTANCE.getVersion());
        }
    }

    public static String VERSION_PROPERTY_NAME = "cdf.plugins.version";

    private static enum HeaderValueHolder {
        INSTANCE;

        private HeaderValueHolder() {
            final Properties properties = new Properties();
            try (final InputStream stream = getClass().getClassLoader().getResourceAsStream("plugins.build.information.properties")) {
                properties.load(stream);
                version = properties.getProperty(VERSION_PROPERTY_NAME);

            } catch (IOException ex) {
                throw new CdfLevel1ClientRuntimeException("Error while loading build properties file", ex);
            }
        }

        private final String version;

        public String getVersion() {
            return version;
        }

    }

}
