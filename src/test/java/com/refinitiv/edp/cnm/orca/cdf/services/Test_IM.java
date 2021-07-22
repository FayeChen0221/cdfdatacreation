/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.services;

import com.refinitiv.edp.cnm.orca.cdf.request.HttpRequest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import junit.framework.TestCase;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

/**
 *
 * @author Binglin Yu
 */
public class Test_IM {
    
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
     * <ul>
     * <li>1. IM service must return valid object for a new group of
     * (keyTypeId,keyValue) </li>
     * <li>2. IM service must return the same object for same
     * (keyTypeId,keyValue) but overlapped effective durations </li>
     * <li>3. IM service must link the new input (keyTypeId,keyValue) to the
     * existing (keyTypeId,keyValue) </li>
     * </ul>
     */
    @Test
    public void Test_getObjectId_new_same_group() {
        Long suffix = System.nanoTime();
        Long effectiveFrom = suffix;
        Long effectiveTo = suffix + Integer.MAX_VALUE;
        
        Long effectiveFrom1 = effectiveFrom - Integer.MAX_VALUE;
        
        Long effectiveTo1 = effectiveTo + Integer.MAX_VALUE;

        // first round of creation
        String postJsonData = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId = body.getJSONObject(0).getString("objectId");

        // call it again to ensure it return the same objectId
        response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        TestCase.assertEquals(objectId, body.getJSONObject(0).getString("objectId"));

        // update the effective duration, which covers the old
        {
            JSONObject tmp = new JSONObject(postJsonData);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom", effectiveFrom1);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo", effectiveTo1);
            
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveFrom", effectiveFrom1);
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveTo", effectiveTo1);
            postJsonData = tmp.toString();
            
        }

        // call it again to ensure it return the same objectId
        response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        TestCase.assertEquals(objectId, body.getJSONObject(0).getString("objectId"));
        
        TestCase.assertEquals(effectiveFrom,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(0).getLong("effectiveFrom")));
        
        TestCase.assertEquals(effectiveTo,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(0).getLong("effectiveTo")));
        
        TestCase.assertEquals(effectiveFrom,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(1).getLong("effectiveFrom")));
        
        TestCase.assertEquals(effectiveTo,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(1).getLong("effectiveTo")));
        
        {
            
            JSONObject tmp = new JSONObject(postJsonData);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom", tmp.getJSONArray("indexInfo").getJSONObject(0).getLong("effectiveFrom") - Integer.MAX_VALUE);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo", tmp.getJSONArray("indexInfo").getJSONObject(0).getLong("effectiveTo") + Integer.MAX_VALUE);
            
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveFrom", tmp.getJSONArray("indexInfo").getJSONObject(1).getLong("effectiveFrom") - Integer.MAX_VALUE);
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveTo", tmp.getJSONArray("indexInfo").getJSONObject(1).getLong("effectiveTo") + Integer.MAX_VALUE);
            
            long ts = System.nanoTime();
            JSONObject newKeyInfo = new JSONObject(
                    "{            \"keyTypeId\" : \"dummy_type." + ts + "\",\n"
                    + "            \"keyValue\" : \"dummy_value." + ts + "\",\n"
                    + "            \"effectiveFrom\" : " + effectiveFrom1 + ",\n"
                    + "            \"effectiveTo\" : " + effectiveTo1 + "\n"
                    + "}");
            tmp.getJSONArray("indexInfo").put(2, newKeyInfo);
            
            ts = System.nanoTime();
            newKeyInfo = new JSONObject(
                    "{            \"keyTypeId\" : \"dummy_type." + ts + "\",\n"
                    + "            \"keyValue\" : \"dummy_value." + ts + "\",\n"
                    + "            \"effectiveFrom\" : " + effectiveFrom1 + ",\n"
                    + "            \"effectiveTo\" : " + effectiveTo1 + "\n"
                    + "}");
            tmp.getJSONArray("indexInfo").put(3, newKeyInfo);
            
            postJsonData = tmp.toString();
        }

        // call it again to ensure it return the same objectId
        response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        TestCase.assertEquals(objectId, body.getJSONObject(0).getString("objectId"));
        
        TestCase.assertEquals(effectiveFrom,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(0).getLong("effectiveFrom")));
        
        TestCase.assertEquals(effectiveTo,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(0).getLong("effectiveTo")));
        
        TestCase.assertEquals(effectiveFrom,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(1).getLong("effectiveFrom")));
        
        TestCase.assertEquals(effectiveTo,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(1).getLong("effectiveTo")));
        
        TestCase.assertEquals(effectiveFrom1,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(2).getLong("effectiveFrom")));
        
        TestCase.assertEquals(effectiveTo1,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(2).getLong("effectiveTo")));
        
        TestCase.assertEquals(effectiveFrom1,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(3).getLong("effectiveFrom")));
        
        TestCase.assertEquals(effectiveTo1,
                Long.valueOf(body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(3).getLong("effectiveTo")));
        
    }

    /**
     * <ul>
     * <li>1. IM service must return valid objectId for a new group of
     * (keyTypeId,keyValue)</li>
     * <li>2. IM service must return a different objectId for a same
     * (keyTypeId,keyValue) but detached effective durations </li>
     * <li>3. IM service must return valid objectId for another new group of
     * (keyTypeId,keyValue)</li>
     * <li>4. IM service must return different objectIds for existing mapping
     * {objectId+(keyTypeId,keyValue)+(effectiveFrom,effectiveTo)}</li>
     * <li>
     * </ul>
     */
    @Test
    public void Test_getObjectId_new_different_group() {
        Long suffix = System.nanoTime();
        Long effectiveFrom = suffix;
        Long effectiveTo = suffix + Integer.MAX_VALUE;
        
        Long effectiveFrom1 = effectiveFrom - Integer.MAX_VALUE;
        
        Long effectiveTo1 = effectiveFrom;
        
        Long effectiveFrom2 = effectiveFrom - 2 * Integer.MAX_VALUE;
        
        Long effectiveTo2 = effectiveTo + 2 * Integer.MAX_VALUE;

        // first round of creation
        String postJsonData = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        String postJsonData1 = postJsonData;
        String postJsonData2 = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda.1." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom2 + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo2 + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb.1." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom2 + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo2 + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";

        // create the first group
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId = body.getJSONObject(0).getString("objectId");

        // create the second group
        {
            JSONObject tmp = new JSONObject(postJsonData1);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom", effectiveFrom1);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo", effectiveTo1);
            
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveFrom", effectiveFrom1);
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveTo", effectiveTo1);
            postJsonData1 = tmp.toString();
        }
        response = objectRequest(getObjectIdUrl, postJsonData1);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId1 = body.getJSONObject(0).getString("objectId");
        
        TestCase.assertFalse(objectId.equals(objectId1));

        // create the third group
        response = objectRequest(getObjectIdUrl, postJsonData2);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId2 = body.getJSONObject(0).getString("objectId");
        
        TestCase.assertFalse(objectId.equals(objectId2));
        TestCase.assertFalse(objectId1.equals(objectId2));

        // create a fouth group with mixed key info
        String postJsonData_mixed = null;
        Long effectiveFrom3 = Collections.min(Arrays.asList(effectiveFrom, effectiveFrom1, effectiveFrom2));
        Long effectiveTo3 = Collections.max(Arrays.asList(effectiveTo, effectiveTo1, effectiveTo2));
        {
            
            JSONObject tmp = new JSONObject(postJsonData);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom", effectiveFrom3);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo", effectiveTo3);
            
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveFrom", effectiveFrom3);
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveTo", effectiveTo3);
            
            JSONObject tmp2 = new JSONObject(postJsonData2);
            tmp2.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom", effectiveFrom3);
            tmp2.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo", effectiveTo3);
            
            tmp2.getJSONArray("indexInfo").getJSONObject(1).put("effectiveFrom", effectiveFrom3);
            tmp2.getJSONArray("indexInfo").getJSONObject(1).put("effectiveTo", effectiveTo3);
            
            JSONObject tmp3 = new JSONObject("{\n"
                    + "    \"objectTypeId\":\"govcorp\",\n"
                    + "    \"indexInfo\":[\n"
                    + "\n"
                    + "    ]\n"
                    + "}");
            
            tmp3.getJSONArray("indexInfo").put(tmp.getJSONArray("indexInfo").get(0));
            tmp3.getJSONArray("indexInfo").put(tmp2.getJSONArray("indexInfo").get(0));
            
            postJsonData_mixed = tmp3.toString();
        }

        // call it again to ensure it return three groups of mapping
        response = objectRequest(getObjectIdUrl, postJsonData_mixed);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray responseJSONArray = new JSONArray(response.getString("body"));
        TestCase.assertEquals(3, responseJSONArray.length());
        
        System.out.println("\n***** mixed existing: " + responseJSONArray);
        
        int i = 0, j = 1, k = 2;
        for (int t = 0; t < responseJSONArray.length(); t++) {
            if (effectiveFrom.equals(responseJSONArray.getJSONObject(t).getJSONArray("keyIdValuePairs")
                    .getJSONObject(0)
                    .getLong("effectiveFrom"))) {
                i = t;
            }
            if (effectiveFrom1.equals(responseJSONArray.getJSONObject(t).getJSONArray("keyIdValuePairs")
                    .getJSONObject(0)
                    .getLong("effectiveFrom"))) {
                j = t;
            }
            if (effectiveFrom2.equals(responseJSONArray.getJSONObject(t).getJSONArray("keyIdValuePairs")
                    .getJSONObject(0)
                    .getLong("effectiveFrom"))) {
                k = t;
            }
        }
        
        TestCase.assertEquals(objectId, responseJSONArray.getJSONObject(i).getString("objectId"));
        TestCase.assertEquals(objectId1, responseJSONArray.getJSONObject(j).getString("objectId"));
        TestCase.assertEquals(objectId2, responseJSONArray.getJSONObject(k).getString("objectId"));
    }

    /**
     * <ul>
     * <li>1. IM service must return valid objectId for a new group of
     * (keyTypeId,keyValue), while calling getObjectId</li>
     * <li>2. IM service must return the same objectId for the same
     * (keyTypeId,keyValue), while calling searchObjectId </li>
     * <li>3. IM service must return null for the same (keyTypeId,keyValue) but
     * detached effective duration</li>
     * <li>4. IM service must return null for different (keyTypeId,keyValue),
     * while calling searchObjectId </li>
     * </ul>
     */
    @Test
    public void Test_searchObjectId() {
        Long suffix = System.nanoTime();
        Long effectiveFrom = suffix;
        Long effectiveTo = suffix + Integer.MAX_VALUE;
        
        Long effectiveFrom1 = effectiveFrom - Integer.MAX_VALUE;
        Long effectiveTo1 = effectiveTo + Integer.MAX_VALUE;

        // first round of creation
        String postJsonData = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        
        String postJsonData1 = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda.1." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom1 + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo1 + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb.1." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom1 + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo1 + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";

        // create the first group
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId = body.getJSONObject(0).getString("objectId");

        // search objectId with old (keyTypeId, keyValue)
        response = objectRequest(searchObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertEquals(objectId, body.getJSONObject(0).getString("objectId"));

        // search objectId with new (keyTypeId, keyValue)
        response = objectRequest(searchObjectIdUrl, postJsonData1);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).isNull("objectId"));

        // search by mixed (keyTypeId, keyValue)
        String postJsonData_mixed;
        {
            JSONObject tmp = new JSONObject(postJsonData);
            JSONObject tmp1 = new JSONObject(postJsonData1);
            tmp.getJSONArray("indexInfo").put(tmp1.getJSONArray("indexInfo").getJSONObject(0));
            tmp.getJSONArray("indexInfo").put(tmp1.getJSONArray("indexInfo").getJSONObject(1));
            postJsonData_mixed = tmp.toString();
        }
        response = objectRequest(searchObjectIdUrl, postJsonData_mixed);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertEquals(2, body.length());
        
        int i = 0;
        int j = 1;
        if (!effectiveFrom.equals(
                body.getJSONObject(0).getJSONArray("keyIdValuePairs").getJSONObject(0).getLong("effectiveFrom"))) {
            i = 1;
            j = 0;
        }
        
        TestCase.assertEquals(objectId, body.getJSONObject(i).getString("objectId"));
        TestCase.assertTrue(body.getJSONObject(j).isNull("objectId"));

        // search objectId with old (keyTypeId, keyValue) but different effective duration
        String postJsonData2;
        {
            JSONObject tmp = new JSONObject(postJsonData);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom",
                    effectiveFrom - Integer.MAX_VALUE);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo",
                    effectiveFrom);
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveFrom",
                    effectiveFrom - Integer.MAX_VALUE);
            tmp.getJSONArray("indexInfo").getJSONObject(1).put("effectiveTo",
                    effectiveFrom);
            
            postJsonData2 = tmp.toString();
        }
        response = objectRequest(searchObjectIdUrl, postJsonData2);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).isNull("objectId"));
        
    }

    /**
     * <ul>
     * <li>1. IM service must return valid object for a new group of
     * (keyTypeId,keyValue) </li>
     * <li>2. IM service must return the same objectId, while calling
     * getObjectId with both old (keyTypeId,keyValue) and new
     * (keyTypeId,keyValue) </li>
     * <li>3. IM service must return two sets of (keyTypeId,keyValue) while
 calling getObjectIdKeysUrl</li>
     * </ul>
     */
    @Test
    public void Test_getObjectIdKeys() {
        Long suffix = System.nanoTime();
        Long effectiveFrom = suffix;
        Long effectiveTo = suffix + Integer.MAX_VALUE;

        // first round of creation
        String postJsonData = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        
        String postJsonData1 = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId = body.getJSONObject(0).getString("objectId");

        // call it again to ensure it return the same objectId
        response = objectRequest(getObjectIdUrl, postJsonData1);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        TestCase.assertEquals(objectId, body.getJSONObject(0).getString("objectId"));

        // getKeys
        response = objectRequest(//                "https://kifildx677.execute-api.us-east-1.amazonaws.com/QA/getObjectIdKeys/govcorp/1441615718",
                this.getObjectIdKeysUrl + "/govcorp/" + objectId,
                null);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        JSONObject expected = new JSONObject(postJsonData1);
        int i = 0;
        int j = 1;
        if (!expected.getJSONArray("indexInfo").getJSONObject(0).getString("keyTypeId")
                .equals(body.getJSONObject(0).getString("keyTypeId"))) {
            i = 1;
            j = 0;
        }
        
        TestCase.assertEquals(expected.getJSONArray("indexInfo").getJSONObject(0).getString("keyValue"),
                body.getJSONObject(i).getString("keyValue"));
        TestCase.assertEquals(expected.getJSONArray("indexInfo").getJSONObject(0).getLong("effectiveFrom"),
                body.getJSONObject(i).getLong("effectiveFrom"));
        TestCase.assertEquals(expected.getJSONArray("indexInfo").getJSONObject(0).getLong("effectiveTo"),
                body.getJSONObject(i).getLong("effectiveTo"));
        
        TestCase.assertEquals(expected.getJSONArray("indexInfo").getJSONObject(1).getString("keyValue"),
                body.getJSONObject(j).getString("keyValue"));
        TestCase.assertEquals(expected.getJSONArray("indexInfo").getJSONObject(1).getLong("effectiveFrom"),
                body.getJSONObject(j).getLong("effectiveFrom"));
        TestCase.assertEquals(expected.getJSONArray("indexInfo").getJSONObject(1).getLong("effectiveTo"),
                body.getJSONObject(j).getLong("effectiveTo"));
    }

    /**
     * <ul>
     * <li>1. IM service must return valid object for a new group of
     * (keyTypeId,keyValue) </li>
     * <li>2. IM service must return the same objectId, while calling
     * getObjectId with both old (keyTypeId,keyValue) and new
     * (keyTypeId,keyValue) </li>
     * <li>3. IM service must return two sets of (keyTypeId,keyValue) while
 calling getObjectIdKeysUrl</li>
     * </ul>
     */
    @Test
    public void Test_getObjectId_multiple_effective_durations() {
        Long suffix = System.nanoTime();
        Long effectiveFrom = suffix;
        Long effectiveTo = suffix + Integer.MAX_VALUE;
        
        Long effectiveFrom1 = effectiveFrom - Integer.MAX_VALUE;
        Long effectiveTo1 = effectiveFrom - 1;

        // first round of creation
        String postJsonData = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"dummy-keyType\",\n"
                + "            \"keyValue\" : \"dummy-keyValue." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"dummy-keyType\",\n"
                + "            \"keyValue\" : \"dummy-keyValue." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom1 + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo1 + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId = body.getJSONObject(0).getString("objectId");
        
        TestCase.assertNotNull(objectId);
        
    }

    /**
     * <ul>
     * <li>1. IM service must return valid object for a new group of
     * (keyTypeId,keyValue) </li>
     * <li>2. IM service must return the same objectId, while calling
     * getObjectId with both old (keyTypeId,keyValue) and new
     * (keyTypeId,keyValue) </li>
     * <li>3. IM service must return two sets of (keyTypeId,keyValue) while
 calling getObjectIdKeysUrl</li>
     * </ul>
     */
    @Test
    public void Test_removeObjectId() {
        Long suffix = System.nanoTime();
        Long effectiveFrom = suffix;
        Long effectiveTo = suffix + Integer.MAX_VALUE;

        // first round of creation
        String postJsonData = "{\n"
                + "   \"objectTypeId\" : \"govcorp\",\n"
                + "   \"indexInfo\" : [{\n"
                + "            \"keyTypeId\" : \"ORGADDRESS\",\n"
                + "            \"keyValue\" : \"zda." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"dummy-keyType\",\n"
                + "            \"keyValue\" : \"dummy-keyValue." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }, { \n"
                + "            \"keyTypeId\" : \"CEO\",\n"
                + "            \"keyValue\" : \"zdb." + suffix + "\",\n"
                + "            \"effectiveFrom\" : " + effectiveFrom + ",\n"
                + "            \"effectiveTo\" : " + effectiveTo + "\n"
                + "        }\n"
                + "    ]\n"
                + "}";
        
        JSONObject response = objectRequest(getObjectIdUrl, postJsonData);
        
        System.out.println("\n***** response: " + response);
        
        JSONArray body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertTrue(body.getJSONObject(0).has("objectId"));
        
        String objectId = body.getJSONObject(0).getString("objectId");

        // calling remove with unmatched effective duration
        String postJsonData1;
        {
            JSONObject tmp = new JSONObject(postJsonData);
            tmp.getJSONArray("indexInfo").remove(2);
            tmp.getJSONArray("indexInfo").remove(0);
            tmp.put("objectId", objectId);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveFrom", effectiveFrom - 1);
            tmp.getJSONArray("indexInfo").getJSONObject(0).put("effectiveTo", effectiveTo + 1);
            postJsonData1 = tmp.toString();
        }
        try {
            response = objectRequest(this.removeObjectIdKeysUrl, postJsonData1);
        } catch (Exception e) {
            System.out.println("\n***** get exception as expected: " + e.getMessage());
            JSONObject tmp = new JSONObject(e.getMessage());
            
            TestCase.assertTrue(tmp.getInt("status") != 200);
//            TestCase.assertTrue(e.toString().contains("No records to remove for given key-value pairs"));
        }
        
        // getKeys: expect to have no records to remove
        response = objectRequest(this.getObjectIdKeysUrl + "/govcorp/" + objectId,
                null);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertEquals(3, body.length());
        
        
        // try to remove again with matched effective duration
        {
            JSONObject tmp = new JSONObject(postJsonData);
            tmp.getJSONArray("indexInfo").remove(2);
            tmp.getJSONArray("indexInfo").remove(0);
            tmp.put("objectId", objectId);
            postJsonData1 = tmp.toString();
        }
        
        response = objectRequest(this.removeObjectIdKeysUrl, postJsonData1);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);

        // getKeys
        response = objectRequest(this.getObjectIdKeysUrl + "/govcorp/" + objectId,
                null);
        
        System.out.println("\n***** response: " + response);
        
        body = new JSONArray(response.getString("body"));
        
        System.out.println("\n***** body: " + body);
        
        TestCase.assertEquals(2, body.length());
        
    }
    
}
