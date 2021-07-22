/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.services;

import com.refinitiv.edp.cnm.orca.cdf.request.HttpRequest;
import java.util.Base64;
import junit.framework.TestCase;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

/**
 *
 * @author Binglin Yu
 */
public class Test_IM_CDF_2048 {

    String IM_USER = "cdf.identity.minting";
    String IM_PWD = "Id3NaLunc2@";

    String getObjectIdUrl = "https://kifildx677.execute-api.us-east-1.amazonaws.com/QA/getObjectId";
    String searchObjectIdUrl = "https://kifildx677.execute-api.us-east-1.amazonaws.com/QA/searchObjectId";
    String removeObjectIdKeysUrl = "https://kifildx677.execute-api.us-east-1.amazonaws.com/QA/removeObjectIdKeys";
    //Get
    String getObjectIdKeysUrl = "https://kifildx677.execute-api.us-east-1.amazonaws.com/QA/getObjectIdKeys";

    String token = Base64.getEncoder().encodeToString(String.format("%s:%s", IM_USER, IM_PWD).getBytes());

    protected JSONObject objectRequest(String url, String postJsonData) {
        try {

            HttpRequest httpRequest = new HttpRequest(token);

            System.out.println("\n***** input: " + postJsonData);

            String responseString;
            if (postJsonData != null) {
                responseString = new String(httpRequest.sendPost(url, postJsonData.getBytes()));
            } else {
                responseString = new String(httpRequest.sendGet(url));
            }

            System.out.println("***** response : " + responseString);

            //printing result from response
            JSONObject responseJson = new JSONObject(responseString);

            return responseJson;
        } finally {

        }

    }

    /**
     * IM service must return null for the same (keyTypeId,keyValue) but
     * detached effective duration
     */
    @Test
    public void Test_searchObjectId_CDF_2048() {
        Long suffix = System.nanoTime();

        // first round of creation
        String postJsonData1 = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda.1007398472803135." + suffix + "\",\n"
                + "            \"effectiveFrom\" : 1007398472803135,\n"
                + "            \"effectiveTo\" : 1007400620286782\n"
                + "        }, {\n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb.1007398472803135." + suffix + "\",\n"
                + "            \"effectiveFrom\" : 1007398472803135,\n"
                + "            \"effectiveTo\" : 1007400620286782\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        String postJsonData2 = "{\n"
                + "\"objectTypeId\":\"govcorp\",\n"
                + "\"indexInfo\":[\n"
                + "        {\n"
                + "            \"keyTypeId\":\"ORGADDRESS\",\n"
                + "            \"keyValue\":\"zda.1007398472803135." + suffix + "\",\n"
                + "            \"effectiveFrom\":1007396325319488,\n"
                + "            \"effectiveTo\":1007398472803135\n"
                + "        },\n"
                + "        {\n"
                + "            \"keyTypeId\":\"CEO\",\n"
                + "            \"keyValue\":\"zdb.1007398472803135." + suffix + "\",\n"
                + "            \"effectiveFrom\":1007396325319488,\n"
                + "            \"effectiveTo\":1007398472803135\n"
                + "        }\n"
                + "    ]\n"
                + "}";

        // create the first group
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData1);

        System.out.println("\n***** response: " + response);

        JSONArray body = new JSONArray(response.getString("body"));

        System.out.println("\n***** body: " + body);

        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));

        String objectId = body.getJSONObject(0).getString("objectId");

        // search objectId with old (keyTypeId, keyValue) but detached effective duration
        response = objectRequest(searchObjectIdUrl, postJsonData2);

        System.out.println("\n***** response: " + response);

        body = new JSONArray(response.getString("body"));

        System.out.println("\n***** body: " + body);

        
        // expect to have no object id returned, because detached effective durations are provided
        TestCase.assertTrue(body.getJSONObject(0).isNull("objectId"));

    }

}
