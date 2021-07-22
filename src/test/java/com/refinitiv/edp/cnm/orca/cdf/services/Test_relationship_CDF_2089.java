/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.services;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.google.common.io.Files;
import com.refinitiv.edp.cnm.orca.cdf.request.RelationshipRequestSender;
import com.tr.cdf.EcpId;
import com.tr.cdf.Envelope;
import com.tr.cdf.OperationType;
import com.tr.cdf.ResponseType;
import com.tr.cdf.aws.common.aws.AmazonS3ClientProvider;
import com.tr.cdf.aws.common.aws.EnvVariables;
import com.tr.cdf.aws.common.lang.ExternalVariableSuppliers;
import com.tr.cdf.datamodel.level2.api.DeltaFactory;
import com.tr.cdf.level.client.service.impl.aws.AwsCdfLevel1SendService;
import com.tr.cdf.level.client.service.impl.aws.AwsServiceSystemProperties;
import com.tr.cdf.level.client.service.impl.aws.components.EnvelopePersisterS3Impl;
import com.tr.cdf.level1.client.envelope.fluent.builders.EnvelopeFluentBuilder;
import com.tr.cdf.level1.client.publish.DeltaSerialization;
import com.tr.cdf.level1.client.publish.EnvelopeBuilders;
import com.tr.cdf.level1.client.publish.SystemProperties;
import com.tr.cdf.level1.client.publish.api.CdfLevel1Service;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.function.Supplier;
import javax.ws.rs.core.Response;
import junit.framework.TestCase;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * Different setting to {@link com.tr.cdf.CDFRelationshipRequest#setNormalizeTemporalState(java.lang.Boolean)
 * } will trigger different behaviors. What are the expectations?
 *
 * @author Binglin Yu
 */
public class Test_relationship_CDF_2089 {

    String token;
    
    String distributionPairId = "ecp:9-020a4ab9-088b-4719-8af8-dab03bab4106";
    String partitionId = "ecp:9-8a060dcc-31ac-4c71-a491-57c7641b8110";
    String versionId = "ecp:9-3f93a310-8dd4-4b1c-b114-997305735f53";
    String senderId = "ecp:9-e32e7619-c32d-4518-b257-7dc0a5172f9f";

    String relationshipTypeId = "ecp:9-fff486bc-24e7-487c-9d17-418bd0592c26";
    String relatedObjectTypeId = "ecp:9-2fe153d2-f511-469d-8575-1aa9b7f571d9";

    String relationObjectTypeId = "ecp:9-b3c92a84-c7ec-4dd9-b37c-8bb3e618f388";

    String retrievalUrl = "https://qyot6vzb26.execute-api.us-east-1.amazonaws.com/PreProduction/retrieve/relationship";

    @Before
    public void before() throws IOException {
        String str = distributionPairId + "," + partitionId
                + "," + versionId
                + ",a205065-a204892-ppe-us-east-1"
                + ",a205065-a204892-OrcaCdfTestRelationship-Stream"
                + ",OrcaCdfTestRelationship";

        File registryFile = new java.io.File("/tmp/cdf.tmp.csv");
        Files.write(str.getBytes(), registryFile);

        System.setProperty("sp-environmentLabel", "ppe");

        System.setProperty("cdf.level.1.aws.ecpregistry.cache.expiry.minutes", "3600");

        System.setProperty("com.amazonaws.sdk.disableCbor", "true");

        System.setProperty("COGNITO_USER", "orca-plus-framework");
        System.setProperty("COGNITO_PASSWORD", "0rca-Plus-Framework!");
        
        this.token = Base64.getEncoder()
                        .encodeToString(String.format("%s:%s", "orca-plus-framework", "0rca-Plus-Framework!").getBytes());

        AwsServiceSystemProperties.NOTIFICATION_SERVICE_TYPE.set("REST");

        SystemProperties.REGISTER_SERVICE_URL_PROPERTY.
                set("https://62m72qetcf.execute-api.us-east-1.amazonaws.com/PreProduction/cdfinput");

        EnvVariables.setExternalVariableSupplier(ExternalVariableSuppliers.SYSTEM_PROPERTIES);

        SystemProperties.RETRY_MAX_RETRIES.set("3");

        SystemProperties.RETRY_EXPONENTIAL_MAX_SECONDS.set("10");

        AmazonS3ClientProvider.setSupplier(new Supplier<AmazonS3>() {
            private AmazonS3 instance = AmazonS3ClientBuilder.standard()
                    .withClientConfiguration(new ClientConfiguration()
                            .withMaxConnections(Runtime.getRuntime().availableProcessors() * 4)
                            .withConnectionTimeout(10_000)
                            .withMaxErrorRetry(10)
                            .withThrottledRetries(false)
                            .withRequestTimeout(-1))
                    .withRegion("us-east-1").build();

            @Override
            public AmazonS3 get() {
                return instance;
            }
        }
        );

        AwsServiceSystemProperties.NO_RETRIES_ON_SERVICE_LEVEL.set("false");

        AwsServiceSystemProperties.ECP_REGISTRY_CSV_MOCK_ACTIVE.set("TRUE");

        //ACL code as Rafal suggestion
        EnvelopePersisterS3Impl.setNewPutObjectRequestConsumer(request -> request.setCannedAcl(CannedAccessControlList.BucketOwnerFullControl));

        AwsServiceSystemProperties.ECP_REGISTRY_CSV_MOCK_INPUT_FILENAME.set(registryFile.getAbsolutePath());

    }

    public JSONObject save_withoutL2(
            String relationshipId, String relatedObjectId, String relationObjectId,
            Date effectiveFrom, Date effectiveTo) {
        EnvelopeFluentBuilder envBuilder = EnvelopeBuilders.defaultBuilder();

        final Envelope envelope = envBuilder
                .setDistributionPairId(distributionPairId)
                .setSender(senderId)
                .relationship()
                .setOperationType(OperationType.Upsert)
                .setAdminStatus("Published")
                .setRelationshipTypeId(relationshipTypeId)
                .setRelationshipId(relationshipId)
                .setVersion(versionId)
                .setRelatedObjectTypeId(relatedObjectTypeId)
                .setRelatedObjectId(relatedObjectId)
                .setRelationObjectTypeId(relationObjectTypeId)
                .setRelationObjectId(relationObjectId)
                .setPartitionId(partitionId)
                .setCurrentAt(new java.util.Date())
                .setEffectiveFrom(effectiveFrom.getTime())
                .setEffectiveTo(effectiveTo.getTime())
                .setDeltaEmbedded(new DeltaSerialization()
                        .serialize(DeltaFactory.createDelta(Collections.EMPTY_LIST)))
                .build();

        CdfLevel1Service.Response response = new AwsCdfLevel1SendService().sendEnvelope(envelope);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("response", response);
        jsonObject.put("id", relationshipId);
        return jsonObject;
    }

    @Test
    public void test_save_get() throws IOException {

        Long systemNanoTime = System.nanoTime();
        String relatedObjectId = "integration.test.related." + systemNanoTime;
        String relationObjectId = "integration.test.relation." + systemNanoTime;
        String relationshipId = "integration.test." + systemNanoTime;

        Date effectiveFrom = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        JSONObject jsonObject = this.save_withoutL2(relationshipId, relatedObjectId, relationObjectId,
                effectiveFrom, effectiveTo);
        CdfLevel1Service.Response response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        JSONObject responseGet = new RelationshipRequestSender(token)
                .getCDFRetrievalResponse(
                        retrievalUrl,
                        EcpId.newBuilder().setUri(versionId).build(),
                        EcpId.newBuilder().setUri(relationshipTypeId).build(),
                        Arrays.asList(EcpId.newBuilder().setUri(relationshipId).build()),
                        EcpId.newBuilder().setUri(senderId).build(),
                        ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        JSONObject temporalState = responseGet.getJSONArray("response").getJSONObject(0)
                .getJSONObject("change").getJSONObject("temporalState");
        Date effectiveFrom1 = temporalState.has("effectiveFrom") ? new Date(temporalState.getLong("effectiveFrom")) : null;
        Date effectiveTo1 = temporalState.has("effectiveTo") ? new Date(temporalState.getJSONObject("effectiveTo").getLong("iMillis")) : null;

        TestCase.assertEquals(effectiveFrom, effectiveFrom1);
        TestCase.assertEquals(effectiveTo, effectiveTo1);

        System.out.println("\n***** [effectiveFrom, effectiveTo): " + "["
                + (effectiveFrom == null ? null : effectiveFrom.toInstant().toString())
                + ", "
                + (effectiveTo == null ? null : effectiveTo.toInstant().toString())
                + ")");

    }

    @Test
    public void test_multiple_writes() {
        RelationshipRequestSender sender = new RelationshipRequestSender(token);

        Long systemNanoTime = System.nanoTime();
        String relatedObjectId = "integration.test.related." + systemNanoTime;
        String relationObjectId = "integration.test.relation." + systemNanoTime;
        String relationshipId = "integration.test." + systemNanoTime;

        Date effectiveFromInput1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveToInput1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        JSONObject jsonObject = this.save_withoutL2(relationshipId, relatedObjectId, relationObjectId,
                effectiveFromInput1, effectiveToInput1);
        CdfLevel1Service.Response response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        JSONObject responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(relationshipTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(relationshipId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        JSONObject temporalState = responseGet.getJSONArray("response").getJSONObject(0)
                .getJSONObject("change").getJSONObject("temporalState");
        Date effectiveFromOutput1 = temporalState.has("effectiveFrom") ? new Date(temporalState.getLong("effectiveFrom")) : null;
        Date effectiveToOutput1 = temporalState.has("effectiveTo") ? new Date(temporalState.getJSONObject("effectiveTo").getLong("iMillis")) : null;

        System.out.println("\n***** output [effectiveFrom, effectiveTo): " + "["
                + (effectiveFromOutput1 == null ? null : effectiveFromOutput1.toInstant().toString())
                + ", "
                + (effectiveToOutput1 == null ? null : effectiveToOutput1.toInstant().toString())
                + ")");

        TestCase.assertEquals(effectiveFromInput1, effectiveFromOutput1);
        TestCase.assertEquals(effectiveToInput1, effectiveToOutput1);

        // second round of writing
        Date effectiveFromInput2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveToInput2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        jsonObject = this.save_withoutL2(relationshipId, relatedObjectId, relationObjectId,
                effectiveFromInput2, effectiveToInput2);
        response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(relationshipTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(relationshipId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        temporalState = responseGet.getJSONArray("response").getJSONObject(0)
                .getJSONObject("change").getJSONObject("temporalState");
        Date effectiveFromOutput2 = temporalState.has("effectiveFrom") ? new Date(temporalState.getLong("effectiveFrom")) : null;
        Date effectiveToOutput2 = temporalState.has("effectiveTo") ? new Date(temporalState.getJSONObject("effectiveTo").getLong("iMillis")) : null;

        System.out.println("\n***** output [effectiveFrom, effectiveTo): " + "["
                + (effectiveFromOutput2 == null ? null : effectiveFromOutput2.toInstant().toString())
                + ", "
                + (effectiveToOutput2 == null ? null : effectiveToOutput2.toInstant().toString())
                + ")");

        TestCase.assertEquals(effectiveFromInput2, effectiveFromOutput2);
        TestCase.assertEquals(effectiveToInput2, effectiveToOutput2);

        // third round of writing
        Date effectiveFromInput3 = Date.from(Instant.parse("2016-01-01T00:00:00.000Z"));
        Date effectiveToInput3 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        jsonObject = this.save_withoutL2(relationshipId, relatedObjectId, relationObjectId,
                effectiveFromInput3, effectiveToInput3);
        response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(relationshipTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(relationshipId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        temporalState = responseGet.getJSONArray("response").getJSONObject(0)
                .getJSONObject("change").getJSONObject("temporalState");
        Date effectiveFromOutput3 = temporalState.has("effectiveFrom") ? new Date(temporalState.getLong("effectiveFrom")) : null;
        Date effectiveToOutput3 = temporalState.has("effectiveTo") ? new Date(temporalState.getJSONObject("effectiveTo").getLong("iMillis")) : null;

        System.out.println("\n***** output [effectiveFrom, effectiveTo): " + "["
                + (effectiveFromOutput3 == null ? null : effectiveFromOutput3.toInstant().toString())
                + ", "
                + (effectiveToOutput3 == null ? null : effectiveToOutput3.toInstant().toString())
                + ")");

        TestCase.assertEquals(effectiveFromInput3, effectiveFromOutput3);
        TestCase.assertEquals(effectiveToInput3, effectiveToOutput3);

    }
}
