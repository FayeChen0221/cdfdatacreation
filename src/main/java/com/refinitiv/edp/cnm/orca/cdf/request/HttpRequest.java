/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.request;

import cdf.shaded.org.apache.commons.io.IOUtils;

import java.io.IOException;
import javax.ws.rs.core.Response.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * @author zhaoyan.shi from EDPAutomation/matterhorn
 */
public class HttpRequest {

    private static final Logger LOGGER = LogManager.getLogger(HttpRequest.class);
    private static final int RETRY_REQUEST = 3;

    private String token;

//    private CloseableHttpClient httpclient;
    public HttpRequest(String token) {
//        if (type.equals(RequestType.CDFContent)) {
//            token = DatatypeConverter.printBase64Binary(
//                    String.format("%s:%s", System.getProperty("COGNITO_USER"), System.getProperty("COGNITO_PASSWORD"))
//                            .getBytes(StandardCharsets.UTF_8));
//        } else if (type.equals(RequestType.CDFIM)) {
//            String s = String.format("%s:%s", IM_USER, IM_PWD);
//    } else {
//            throw new UnsupportedOperationException("Not supported yet");
//        }

        this.token = token;
    }

    public byte[] sendPost(String urlString, byte[] message) {
        int retry = RETRY_REQUEST;
        String response = null;

        HttpPost httpPost;

        CloseableHttpClient httpclient = HttpClientBuilder.create()
                .setConnectionTimeToLive(1000l, TimeUnit.MILLISECONDS)
                //                .setConnectionManager(cm)
                //                .setKeepAliveStrategy(myStrategy)
                .build();
        httpPost = new HttpPost(urlString);
        httpPost.setHeader("Authorization", String.format("Basic %s", token));
        httpPost.setHeader("Content-Type", "application/octet-stream");
        httpPost.setHeader("Accept", "application/octet-stream");

        httpPost.setEntity(new ByteArrayEntity(message));

        CloseableHttpResponse closeableHttpResponse = null;

        try {
            while (retry > 0) {
                LOGGER.debug("call-" + (RETRY_REQUEST - retry) + " : " + httpPost.getParams());
                retry--;
                InputStream inputStream = null;

                try {
                    closeableHttpResponse = httpclient.execute(httpPost);
                    HttpEntity entity = closeableHttpResponse.getEntity();
                    inputStream = entity.getContent();
                    if (closeableHttpResponse.getStatusLine().getStatusCode() != Status.OK.getStatusCode()) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                        JSONObject tmp = new JSONObject();
                        String errorMessage = new String(IOUtils.toByteArray(inputStream));
                        tmp.put("status", closeableHttpResponse.getStatusLine().getStatusCode());
                        tmp.put("message", closeableHttpResponse.getStatusLine().getReasonPhrase());
                        tmp.put("error", errorMessage);
                        response = tmp.toString();
                    } else {
                        return IOUtils.toByteArray(inputStream);
                    }
                } catch (IOException ex) {
                    // skip
                } finally {
                    if (closeableHttpResponse != null) {
                        try {
                            closeableHttpResponse.close();
                        } catch (IOException ex) {
                            // skip
                        }
                    }
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException ex) {
                            // skip 
                        }
                    }
                }
            }
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ex) {
                    // skip the exception
                }
            }
//            cm.closeIdleConnections(100l, TimeUnit.MILLISECONDS);
//            cm.closeExpiredConnections();
        }
        throw new RuntimeException(response);
    }
    
    public byte[] sendGet(String urlString) {
        String msg = String.format("setGet:%n", urlString);
        LOGGER.info(msg);
        
        CloseableHttpClient httpclient = HttpClientBuilder.create()
                .setConnectionTimeToLive(1000l, TimeUnit.MILLISECONDS)
                //                .setConnectionManager(cm)
                //                .setKeepAliveStrategy(myStrategy)
                .build();
        HttpGet httpGet = new HttpGet(urlString);
        httpGet.setHeader("Authorization", String.format("Basic %s", token));
        httpGet.setHeader("Content-Type", "application/octet-stream");
        httpGet.setHeader("Accept", "application/octet-stream");

        CloseableHttpResponse closeableHttpResponse = null;

        int retry = RETRY_REQUEST;
        String response = null;
        try {
            while (retry > 0) {
                LOGGER.debug("call-" + (RETRY_REQUEST - retry) + " : " + httpGet.getParams());
                retry--;
                InputStream inputStream = null;

                try {
                    closeableHttpResponse = httpclient.execute(httpGet);
                    HttpEntity entity = closeableHttpResponse.getEntity();
                    inputStream = entity.getContent();
                    if (closeableHttpResponse.getStatusLine().getStatusCode() != Status.OK.getStatusCode()) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                        
                        JSONObject tmp = new JSONObject();
                        String errorMessage = new String(IOUtils.toByteArray(inputStream));
                        tmp.put("status", closeableHttpResponse.getStatusLine().getStatusCode());
                        tmp.put("message", closeableHttpResponse.getStatusLine().getReasonPhrase());
                        tmp.put("error", errorMessage);
                        response = tmp.toString();
                    } else {
                        return IOUtils.toByteArray(inputStream);
                    }
                } catch (IOException ex) {
                    // skip
                } finally {
                    if (closeableHttpResponse != null) {
                        try {
                            closeableHttpResponse.close();
                        } catch (IOException ex) {
                            // skip
                        }
                    }
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException ex) {
                            // skip 
                        }
                    }
                }
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ex) {
                    // skip the exception
                }
            }
//            cm.closeIdleConnections(100l, TimeUnit.MILLISECONDS);
//            cm.closeExpiredConnections();
        }
        throw new RuntimeException(response);

    }

}
