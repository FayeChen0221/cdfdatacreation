/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import com.google.gson.Gson;
import com.tr.cdf.CDFObjectResponse;
import com.tr.cdf.EcpId;
import com.tr.cdf.ResponseType;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;

/**
 *
 * @author from EDPAutomation/matterhorn
 */
public class ObjectRequestSender {
    
    private String token;
    public ObjectRequestSender(String token) {
        this.token = token;
    }

    private Gson gson = new Gson();
    
    private Serializer serializer = new Serializer();
    
    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger(ObjectRequestSender.class);
    
    public CDFObjectResponse sendCdfObjectRequest(
            String retrievalAPIURL, 
            EcpId version, 
            EcpId objectTypeID, 
            List<EcpId> objectIDList,
            List<EcpId> ciTypeIds, 
            EcpId requester,
            ResponseType responseType) throws IOException {
        
        final byte[] serializedRequest = new ObjectRequestCreator().createRequest(version, objectTypeID, objectIDList, ciTypeIds, requester, responseType);
        final byte[] byteResponse = new HttpRequest(token).sendPost(retrievalAPIURL, serializedRequest);
        
        return new Serializer().deserializeObjectResponse(byteResponse);
    }

    public JSONObject getCDFRetrievalResponse(
            String retrievalAPIURL,
            EcpId version,
            EcpId objectTypeID, 
            List<EcpId> objectIDList,
            List<EcpId> ciTypeIds, 
            EcpId requestor, 
            ResponseType responseType) {

        try {
            CDFObjectResponse response;
            response = sendCdfObjectRequest(retrievalAPIURL, version, objectTypeID, objectIDList, ciTypeIds, requestor, responseType);

            if (response != null) {
                String responseString = gson.toJson(response);

                return new JSONObject(responseString);
            } else {
                return new JSONObject();
            }
        } catch (IOException ex) {
            LOGGER.error(ex);
            throw new RuntimeException(ex);
        }

    }

}
