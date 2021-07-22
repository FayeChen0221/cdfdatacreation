/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import com.tr.cdf.BaseRequest;
import com.tr.cdf.CDFObjectRequest;
import com.tr.cdf.ContentItemSpecifier;
import com.tr.cdf.EcpFQId;
import com.tr.cdf.EcpId;
import com.tr.cdf.FormsToInclude;
import com.tr.cdf.ResponseType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author from EDPAutomation/matterhorn
 */
public class ObjectRequestCreator {

    public byte[] createRequest(EcpId version, EcpId objectTypeID, List<EcpId> objectIDList, List<EcpId> ciTypeIds, EcpId requester, ResponseType responseType) {
        final CDFObjectRequest request = new CDFObjectRequest();

        final List<Integer> entitlements = new ArrayList<>();

        //response will contain FullStates and Deltas
        final List<FormsToInclude> formsToInclude = new ArrayList<>();
        formsToInclude.add(FormsToInclude.FullState);
        formsToInclude.add(FormsToInclude.Delta);
        formsToInclude.add(FormsToInclude.Index);
        //objects to return
        List<EcpFQId> ecpFQIdList = new ArrayList<>();
        for (EcpId id : objectIDList) {
            ecpFQIdList.add(new EcpFQId(objectTypeID, id));
        }

        //ids.add(new EcpFQId(OBJECT_TYPE_ID, OBJECT_ID_2));
        BaseRequest baseRequest = new BaseRequest();
        baseRequest.setRequestId(UUID.randomUUID().toString());
        baseRequest.setRequestor(requester);
        baseRequest.setEntitlements(entitlements);
        baseRequest.setFormsToInclude(formsToInclude);
        baseRequest.setVersion(version);
        baseRequest.setTimestamp(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC));
        

        // ResponseType accepts three values: StateHistory, ChangeHistory and AggregatedState
        //1. StateHistory: retrieval returns all states of objects and its CIs; state and index are in the response, but delta is null
        //2. ChangeHistory: retrieval returns all states of objects and its CIs; types of payload elements are defined in FormsToInclude
        //3. AggregatedState: retrieval returns state of objects and its ContentItems as per currentAt (last transactions); state and index are in the response, but delta is null
        baseRequest.setResponseType(responseType);
        baseRequest.setReturnedStatuses(new ArrayList<>());
        baseRequest.setIds(ecpFQIdList);

        request.setBaseRequest(baseRequest);

        request.setCurrentFrom(0L);
        request.setCurrentTo(null);

        // ContentItemSpecifier takes three values: ALL, Include, Exclude
        //1. All - retrieval returns all content items
        //2. Include - retrieval returns only content items that are provided in ReturnedContentItemTypeIds element
        //2. Exclude - retrieval returns all content items
        if (ciTypeIds.isEmpty()) {
            request.setReturnedContentItems(ContentItemSpecifier.All);
            request.setReturnedContentItemTypeIds(new ArrayList<>());
        } else {
            request.setReturnedContentItems(ContentItemSpecifier.Include);
            request.setReturnedContentItemTypeIds(ciTypeIds);
        }

        // ReturnedContentItems is set to All, unnecessary to provide content items to return by retrieval
        // final List<EcpId> contentItems = new ArrayList<>();
        // contentItems.add(new EcpId("ecp:9-01004712-11b9-42c5-af29-d4dd0e02ab85"));
        // contentItems.add(new EcpId("ecp:9-3b5f5929-4f0b-4059-aad0-769367d5fd2e"));
        // request.setReturnedContentItemTypeIds(new ArrayList<>(contentItems));
//        request.setReturnedContentItemTypeIds(new ArrayList<>());
        Serializer serializer = new Serializer();
        return serializer.serializeObjectRequest(request);
    }
}
