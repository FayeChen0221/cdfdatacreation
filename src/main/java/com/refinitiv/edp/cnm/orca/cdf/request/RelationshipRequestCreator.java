/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import com.tr.cdf.BaseRequest;
import com.tr.cdf.CDFRelationshipRequest;
import com.tr.cdf.EcpFQId;
import com.tr.cdf.EcpId;
import com.tr.cdf.FormsToInclude;
import com.tr.cdf.RequestCurrentFromTo;
import com.tr.cdf.RequestEffectiveFromTo;
import com.tr.cdf.ResponseType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author from EDPAutomation/matterhorn
 */
public class RelationshipRequestCreator {
    
    public byte[] createRequest(
            EcpId version, 
            EcpId relationshipTypeId,
            List<EcpId> relationshipEcpIds, 
            EcpId requestor, 
            ResponseType responseType) {
        List<EcpFQId> ids = new ArrayList<>(relationshipEcpIds.size());
        
        for(EcpId relationshipEcpId: relationshipEcpIds) {
            EcpFQId ecpFQId = EcpFQId.newBuilder().setObjectTypeId(relationshipTypeId)
                    .setObjectId(relationshipEcpId)
                    .build();
            ids.add(ecpFQId);
        }
        CDFRelationshipRequest request = new CDFRelationshipRequest();
        RequestCurrentFromTo requestCurrentFromTo = new RequestCurrentFromTo();
        requestCurrentFromTo.setCurrentFrom(0L);
        requestCurrentFromTo.setCurrentTo(null);
        request.setCurrentFromTo(requestCurrentFromTo);

        RequestEffectiveFromTo requestEffectiveFromTo = new RequestEffectiveFromTo();
        requestEffectiveFromTo.setEffectiveTo(null);
        request.setEffectiveFromTo(requestEffectiveFromTo);
        request.setNormalizeTemporalState(false);

        BaseRequest baseRequest = new BaseRequest();
        baseRequest.setRequestId(UUID.randomUUID().toString());
        baseRequest.setRequestor(requestor);
        baseRequest.setEntitlements(new ArrayList<>());
        baseRequest.setFormsToInclude(
                Arrays.asList(
                        FormsToInclude.Delta, 
                        FormsToInclude.FullState, 
                        FormsToInclude.Index));
        baseRequest.setVersion(version);
        baseRequest.setTimestamp(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC));
        //baseRequest.setResponseType(ResponseType.ChangeHistory);
        baseRequest.setResponseType(responseType);
        baseRequest.setReturnedStatuses(new ArrayList<>());
        baseRequest.setIds(ids);

        request.setBaseRequest(baseRequest);
        Serializer serializer = new Serializer();
        return serializer.serializeRelationshipRequest(request);
    }
}
