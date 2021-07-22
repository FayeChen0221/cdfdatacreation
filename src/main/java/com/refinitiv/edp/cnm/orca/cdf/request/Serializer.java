/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import com.tr.cdf.CDFObjectRequest;
import com.tr.cdf.CDFObjectResponse;
import com.tr.cdf.CDFRelationshipRequest;
import com.tr.cdf.CDFRelationshipResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author from EDPAutomation/matterhorn
 */
public class Serializer {
    
    private static final Logger LOGGER = LogManager.getLogger(Serializer.class);

    public byte[] serializeObjectRequest(CDFObjectRequest request) {
        final AvroSerializer<CDFObjectRequest> objectRequestSerializer = new AvroSerializer<CDFObjectRequest>(CDFObjectRequest.class);
        try {
            return objectRequestSerializer.serialize(request);
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error(e);
            }
            throw new RuntimeException("Object request serialization failed due to: ", e.getCause());
        }
    }

    public CDFObjectResponse deserializeObjectResponse(byte[] response) {
        final AvroSerializer<CDFObjectResponse> objectResponseSerializer = new AvroSerializer<>(CDFObjectResponse.class);
        try {
            return objectResponseSerializer.deserialize(response);
        } catch (Exception e) {
            throw new RuntimeException("Object response deserialization failed due to: ", e.getCause());
        }
    }

    public byte[] serializeRelationshipRequest(CDFRelationshipRequest request) {
        final AvroSerializer<CDFRelationshipRequest> relationshipRequestSerializer = new AvroSerializer<>(CDFRelationshipRequest.class);
        try {
            return relationshipRequestSerializer.serialize(request);
        } catch (Exception e) {
            throw new RuntimeException("Relationship request serialization failed due to: ", e.getCause());
        }
    }

    public CDFRelationshipResponse deserializeRelationshipResponse(byte[] response) {
        final AvroSerializer<CDFRelationshipResponse> relationshipResponseSerializer = new AvroSerializer<>(CDFRelationshipResponse.class);
        try {
            return relationshipResponseSerializer.deserialize(response);
        } catch (Exception e) {
            throw new RuntimeException("Relationship response deserialization failed due to: ", e.getCause());
        }
    }
}
