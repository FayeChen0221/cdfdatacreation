package com.refinitiv.edp.cnm.orca.cdf.request;

import com.tr.cdf.datamodel.level2.core.ecp.registry.EcpRegistry;
import org.kohsuke.MetaInfServices;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Default implementation of {@link EcpRegistry}. Uses hardcoded values.
 *
 * @author Binglin Copied the default version from Bronisław - We configure the
 * default implementation because we make the conversion between ecpId and
 * readable names instead of depending on {@link DefaultEcpRegistry}
 *
 * @author Bronisław Truszkowski <bronislaw.truszkowski@thomsonreuters.com>
 */
@MetaInfServices
public class DefaultEcpRegistry implements EcpRegistry {

    /**
     * Default constructor.
     */
    public DefaultEcpRegistry() {
    }

    private final Map<String, String> descriptiveToSerialized = new HashMap<>();
    private final Map<String, String> serializedToDescriptive = new HashMap<>();

    @Override
    public String toSerializedValue(String descriptiveValue) {
        final String result;
        if (descriptiveToSerialized.containsKey(descriptiveValue)) {
            result = descriptiveToSerialized.get(descriptiveValue);
        } else {
            result = descriptiveValue;
        }
        return descriptiveValue;
    }

    @Override
    public String toDescriptiveValue(String serializedValue) {
        final String result;
        if (serializedToDescriptive.containsKey(serializedValue)) {
            result = serializedToDescriptive.get(serializedValue);
        } else {
            result = serializedValue;
        }
        return serializedValue;
    }
}
