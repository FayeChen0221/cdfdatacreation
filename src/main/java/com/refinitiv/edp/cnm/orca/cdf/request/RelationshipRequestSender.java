/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import com.google.gson.Gson;
import com.tr.cdf.CDFRelationshipResponse;
import com.tr.cdf.EcpId;
import com.tr.cdf.ResponseType;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;

/**
 *
 * @author zhaoyan.shi
 */
public class RelationshipRequestSender {
    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger(RelationshipRequestSender.class);

    private Gson gson = new Gson();
    
    private String token;
    
    public RelationshipRequestSender(String token) {
        this.token = token;
    }

    protected CDFRelationshipResponse sendCdfRelationshipRequest(
            String retrievalAPIURL, 
            EcpId version, 
            EcpId relationshipTypeId,
            List<EcpId> relationshipIds, 
            EcpId requestor, 
            ResponseType responseType) throws IOException {
        RelationshipRequestCreator requestCreator = new RelationshipRequestCreator();
        final byte[] serializedRequest = requestCreator.createRequest(
                version, 
                relationshipTypeId, 
                relationshipIds, 
                requestor, 
                responseType);
        HttpRequest httpRequest = new HttpRequest(token);
        final byte[] byteResponse = httpRequest.sendPost(retrievalAPIURL, serializedRequest);
        Serializer serializer = new Serializer();
        return serializer.deserializeRelationshipResponse(byteResponse);
    }

    /**
     * 
     */
    public JSONObject getCDFRetrievalResponse(
            String retrievalAPIURL,
            EcpId version, 
            EcpId relationshipTypeId, 
            List<EcpId> relationshipIds,
            EcpId requestor, 
            ResponseType responseType) {

        try {
            CDFRelationshipResponse response;
            response = sendCdfRelationshipRequest(
                    retrievalAPIURL, 
                    version, 
                    relationshipTypeId, 
                    relationshipIds, 
                    requestor,
                    responseType);

            if (response != null) {
                String responseString = gson.toJson(response);

                return new JSONObject(responseString);
            } else {

                return new JSONObject();
            }

        } catch (IOException ex) {
            LOGGER.error("Failed: {}", ex);
            throw new RuntimeException(ex);
        }

    }
}
