/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.services;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.google.common.io.Files;
import com.refinitiv.edp.cnm.orca.cdf.request.ObjectRequestSender;
import com.tr.cdf.EcpId;
import com.tr.cdf.Envelope;
import com.tr.cdf.OperationType;
import com.tr.cdf.ResponseType;
import com.tr.cdf.datamodel.level2.core.TemporalState;
import com.tr.cdf.aws.common.aws.AmazonS3ClientProvider;
import com.tr.cdf.aws.common.aws.EnvVariables;
import com.tr.cdf.aws.common.aws.facades.S3FacadeForCdf;
import com.tr.cdf.aws.common.lang.ExternalVariableSuppliers;
import com.tr.cdf.aws.registry.EcpRegistry;
import com.tr.cdf.aws.registry.EcpRegistryProvider;
import com.tr.cdf.aws.registry.ResourceNotFoundException;
import com.tr.cdf.aws.registry.configuration.EcpRegistryConfiguration;
import com.tr.cdf.aws.registry.models.ContentItem;
import com.tr.cdf.aws.registry.models.DistributionPair;
import com.tr.cdf.aws.registry.models.DistributionPairPartitionVersionStoreMap;
import com.tr.cdf.aws.registry.models.Store;
import com.tr.cdf.datamodel.level2.api.DeltaFactory;
import com.tr.cdf.datamodel.level2.api.Level2Builders;
import com.tr.cdf.datamodel.level2.api.TemporalStateFactory;
import com.tr.cdf.datamodel.level2.api.change.operations.DataItemEntityChangeFactory;
import com.tr.cdf.datamodel.level2.core.BiTemporalDataItemEntityHistory;
import com.tr.cdf.datamodel.level2.core.DataItemEntityChange;
import com.tr.cdf.level.client.service.impl.aws.AwsCdfLevel1SendService;
import com.tr.cdf.level.client.service.impl.aws.AwsServiceSystemProperties;
import com.tr.cdf.level.client.service.impl.aws.components.EnvelopePersisterS3Impl;
import com.tr.cdf.level1.client.envelope.fluent.builders.EnvelopeFluentBuilder;
import com.tr.cdf.level1.client.publish.DeltaSerialization;
import com.tr.cdf.level1.client.publish.EnvelopeBuilders;
import com.tr.cdf.level1.client.publish.FullHistorySerialization;
import com.tr.cdf.level1.client.publish.SystemProperties;
import com.tr.cdf.level1.client.publish.api.CdfLevel1Service;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import junit.framework.TestCase;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * Different "enrichmentEnabled" of distributionPair, will trigger different
 * behaviors. What are the expectations?
 *
 * @author Binglin Yu
 */
public class Test_object_CDF_1583 {

    String token;
//        = 
//                Base64.getEncoder()
//            .encodeToString(String.format("%s:%s", "orca-plus-framework", "0rca-Plus-Framework!").getBytes());

    // for setting info for registry
    @BeforeClass
    public static void beforeClass() {
        String pwd = "Ph^DD(Q2&bA#K4hQ";
        String usr = "ECATCurationandMastering";
        String apiKey = "8piwMXwbp58WpPLsit7nh4XezDvX4Reu6DnVzH46";
        String authURL = "https://ecp-registry-qa-int.thomsonreuters.com/auth";
        String queryURL = "https://ecp-registry-qa-int.thomsonreuters.com/metadata/sparql/list";
        String thingURL = "https://ecp-registry-qa-int.thomsonreuters.com/metadata/thing";

        // declare system properties for CDF
        System.setProperty(EcpRegistryConfiguration.PASSWORD.getPropertyName(), pwd);
        System.setProperty(EcpRegistryConfiguration.USERNAME.getPropertyName(), usr);
        System.setProperty(EcpRegistryConfiguration.X_API_GATEWAY_KEY.getPropertyName(), apiKey);
        System.setProperty(EcpRegistryConfiguration.AUTH_URL.getPropertyName(), authURL);

        System.setProperty(EcpRegistryConfiguration.REGISTRY_QUERY_URL.getPropertyName(), queryURL);

        System.setProperty(EcpRegistryConfiguration.REGISTRY_THING_URL.getPropertyName(), thingURL);

    }

//    String ORCA_DistributionPairPartitionVersionStoreMap_EcpId = "ecp:9-09483403-8ff0-4dd9-baaa-e8e6281e7307";
    String distributionPairId;
    String partitionId;
    String versionId;
//    String senderId;

    String objectTypeId;
    String contentItemTypeId;

    String s3BucketName;
    String inboundStream;
    String s3BucketPrefix;

//    String distributionPairId = "ecp:9-020a4ab9-088b-4719-8af8-dab03bab4106";
//    String partitionId = "ecp:9-8a060dcc-31ac-4c71-a491-57c7641b8110";
//    String versionId = "ecp:9-3f93a310-8dd4-4b1c-b114-997305735f53";
    String senderId = "ecp:9-e32e7619-c32d-4518-b257-7dc0a5172f9f";
//
//    String relationshipTypeId = "ecp:9-fff486bc-24e7-487c-9d17-418bd0592c26";
//    String relatedObjectTypeId = "ecp:9-2fe153d2-f511-469d-8575-1aa9b7f571d9";
//
//    String relationObjectTypeId = "ecp:9-b3c92a84-c7ec-4dd9-b37c-8bb3e618f388";

    String retrievalUrl = "https://qyot6vzb26.execute-api.us-east-1.amazonaws.com/PreProduction/retrieve/object";

    protected void switchRegistry(String DistributionPairPartitionVersionStoreMap_EcpId,
            Boolean expectedAsyncEnrichment,
            Boolean expectedEnrichmentEnabled) {
        try {
            EcpRegistry ecpRegistry = EcpRegistryProvider.INSTANCE.getEcpRegistry();
            DistributionPairPartitionVersionStoreMap map = ecpRegistry.getById(
                    DistributionPairPartitionVersionStoreMap_EcpId,
                    DistributionPairPartitionVersionStoreMap.class);

            System.out.println("distributionPair: " + map.getDistributionPairId());
            System.out.println("partition: " + map.getPartitionId());
            System.out.println("version: " + map.getVersionId());

            this.distributionPairId = map.getDistributionPairId();
            this.partitionId = map.getPartitionId();
            this.versionId = map.getVersionId();

            System.out.println("store: " + map.getStoreId());
            Store store = ecpRegistry.getById(map.getStoreId(), Store.class);

            System.out.println("s3BucketName: " + store.getS3BucketName());
            System.out.println("inboundStream: " + store.getInboundStream());
            System.out.println("prefix: " + store.getS3BucketPrefix());

            this.s3BucketName = store.getS3BucketName();
            this.inboundStream = store.getInboundStream();
            this.s3BucketPrefix = store.getS3BucketPrefix();

            DistributionPair distributionPair = ecpRegistry.getById(map.getDistributionPairId(), DistributionPair.class);
            System.out.println("objectType: " + distributionPair.getSupportedObjectTypeId());
            System.out.println("asyncEnrichments: " + distributionPair.getAsyncEnrichments());
            System.out.println("enrichmentEnableds: " + distributionPair.getEnrichmentEnableds());
            this.objectTypeId = distributionPair.getSupportedObjectTypeId();

            if (expectedAsyncEnrichment != null) {
                TestCase.assertEquals("Expect to have expected expectedAsyncEnrichment(" + expectedAsyncEnrichment
                        + ") configured in MR",
                        expectedAsyncEnrichment.booleanValue(),
                        Boolean.parseBoolean(distributionPair.getAsyncEnrichments().get(0).getValue()));
            }

            if (expectedEnrichmentEnabled != null) {
                TestCase.assertEquals("Expect to have expected expectedEnrichmentEnabled(" + expectedEnrichmentEnabled
                        + ") configured in MR",
                        expectedEnrichmentEnabled.booleanValue(),
                        Boolean.parseBoolean(distributionPair.getEnrichmentEnableds().get(0).getValue()));
            }

            List<ContentItem> contentItems = ecpRegistry.getContentItems(distributionPairId, versionId, System.currentTimeMillis());

            if (contentItems.isEmpty()) {
                throw new RuntimeException("Expect to have content items!");
            }

            this.contentItemTypeId = contentItems.get(0).getId();

            registryCDFCfg();
        } catch (IOException | ResourceNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    // registry info for cdf saving/retrieval
    protected void registryCDFCfg() throws IOException {

        String str = distributionPairId + "," + partitionId
                + "," + versionId
                + "," + this.s3BucketName
                + "," + this.inboundStream
                + "," + this.s3BucketPrefix;

        File registryFile = new java.io.File("C:/Faye/cdf-contract-test/src/tmp/cdf.tmp/" + System.nanoTime() + ".csv");
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
        EnvelopePersisterS3Impl.setNewPutObjectRequestConsumer(request -> request.setCannedAcl(CannedAccessControlList.PublicReadWrite));

        AwsServiceSystemProperties.ECP_REGISTRY_CSV_MOCK_INPUT_FILENAME.set(registryFile.getAbsolutePath());

    }

    public JSONObject save_withL2(
            String objectId,
            Date effectiveFrom,
            Date effectiveTo,
            BiTemporalDataItemEntityHistory originalHistory) {
        EnvelopeFluentBuilder envBuilder = EnvelopeBuilders.defaultBuilder();

        Date currentAt = new Date();

        DataItemEntityChange change = null;

        String path = "pro-1";
        String value1 = "value1." + System.nanoTime();

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, effectiveTo, currentAt, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        if (originalHistory == null) {
            originalHistory = Level2Builders.history().add(change).build();
        } else {
            originalHistory.add(change);
        }

//        this.distributionPairId = "ecp:9-f8c27180-e435-4217-b61c-aab1dd0e7856";
//        this.partitionId = "ecp:9-f91b048c-a57e-4b5d-b304-615eb6b40993";
//        this.versionId = "ecp:9-cfb2b7e8-669c-4690-86cf-931471d0caae";
//        this.contentItemTypeId = "ecp:9-f6aa048a-b4b6-4c0b-bb4b-68e7e2e0272b";
//        this.objectTypeId = "ecp:9-a77e3be6-a17f-431a-8c96-aac939dae3c2";

        final Envelope envelope = envBuilder
                .setDistributionPairId(distributionPairId)
                .setSender(senderId)
                .object()
                .setPartitionId(partitionId)
                .setObjectTypeEcpId(objectTypeId)
                .setContentObjectEcpId(objectId)
                .setAdminStatus("Published")
                .setCurrentAt(currentAt.getTime())
                .setVersion(versionId)
                .setCurrentAt(currentAt)
                .addContentItem()
                .setContentItemTypeId(contentItemTypeId)
                .setOperationType(OperationType.Upsert)
                .setDeltaEmbedded(new DeltaSerialization()
                        .serialize(DeltaFactory.createDelta(Arrays.asList(change))))
                .setHistoryNewStateEmbedded(new FullHistorySerialization().serialize(originalHistory))
                .build();

        CdfLevel1Service.Response response = new AwsCdfLevel1SendService().sendEnvelope(envelope);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("response", response);
        jsonObject.put("id", objectId);
        return jsonObject;
    }

    @Test
    public void test_save_get() throws IOException {

       switchRegistry("ecp:9-7df617ca-5d20-444d-ab1f-8c1ae82f1f40", true, false);

        Long systemNanoTime = System.nanoTime();
        String objectId = "integration.test." + systemNanoTime;

        Date effectiveFrom = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        JSONObject jsonObject = this.save_withL2(
                objectId,
                effectiveFrom, effectiveTo,
                null);
        CdfLevel1Service.Response response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        JSONObject responseGet = new ObjectRequestSender(token)
                .getCDFRetrievalResponse(
                        retrievalUrl,
                        EcpId.newBuilder().setUri(versionId).build(),
                        EcpId.newBuilder().setUri(objectTypeId).build(),
                        Arrays.asList(EcpId.newBuilder().setUri(objectId).build()),
                        Arrays.asList(EcpId.newBuilder().setUri(contentItemTypeId).build()),
                        EcpId.newBuilder().setUri(senderId).build(),
                        ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        BiTemporalDataItemEntityHistory history = getHistory(responseGet);

        TestCase.assertEquals(1, history.getChanges().size());

        System.out.println("\n***** changes: " + history.getChanges());

    }

    protected BiTemporalDataItemEntityHistory getHistory(JSONObject retrievalResponse) {

        JSONObject jsonObject = retrievalResponse.getJSONArray("response").getJSONObject(0)
                .getJSONObject("change").getJSONArray("changes")
                .getJSONObject(0).getJSONObject("newState");

        if (!jsonObject.has("compressionMethod") || !jsonObject.get("compressionMethod").equals("ZLIB")) {
            throw new RuntimeException("Only support compressionMethod=ZLIB");
        }

        if (!jsonObject.has("payloadType") || !jsonObject.get("payloadType").equals("Reference")) {
            throw new RuntimeException("Only support payloadType=Reference");
        }

        if (!jsonObject.has("referenceId") || !(jsonObject.get("referenceId") instanceof String)) {
            throw new RuntimeException("Only support String type of referenceId");
        }

        String referenceId = jsonObject.getString("referenceId");

        int index = referenceId.indexOf("/");
        byte[] decompressedData = null;
        String bucketName = referenceId.substring(0, index);
        String key = referenceId.substring(index + 1);

        try {
            byte[] data = S3FacadeForCdf.readBytes(bucketName, key);

            Inflater decompresser = new Inflater();
            decompresser.setInput(data);

            if (data.length == 0) {
                throw new RuntimeException("Unexpect to have empty data in s3: " + referenceId);
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[1000];

            while (!decompresser.finished()) {
                int resultLength = decompresser.inflate(buf);
                out.write(buf, 0, resultLength);
            }

            decompressedData = out.toByteArray();

            out.close();
            decompresser.end();
        } catch (AmazonS3Exception e) {
            String msg = String.format("bucket: %s, key: %s. message: %s", bucketName, key, e.getMessage());
            throw new RuntimeException(msg);
        } catch (IOException | DataFormatException ex) {
            throw new RuntimeException(ex);
        }

        BiTemporalDataItemEntityHistory biTemporalDataItemEntityHistory = new FullHistorySerialization().deserialize(decompressedData);
        return biTemporalDataItemEntityHistory;
    }

    /**
     * *
     * test that, CDF keeps all the delta in full-histiry for relationship with
     * (enrichmentEnabled=false)
     */
    @Test
    public void test_multiple_writes_enrichmentEnabled_false() {
        switchRegistry("ecp:9-7df617ca-5d20-444d-ab1f-8c1ae82f1f40", true, false);

        ObjectRequestSender sender = new ObjectRequestSender(token);

        Long systemNanoTime = System.nanoTime();

        String objectId = "integration.test." + systemNanoTime;

        Date effectiveFromInput1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveToInput1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        JSONObject jsonObject = this.save_withL2(
                objectId,
                effectiveFromInput1, effectiveToInput1,
                null);
        CdfLevel1Service.Response response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        JSONObject responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(objectTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(objectId).build()),
                Arrays.asList(EcpId.newBuilder().setUri(contentItemTypeId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        BiTemporalDataItemEntityHistory history = getHistory(responseGet);

        TestCase.assertEquals(1, history.getChanges().size());

        // second round of writing
        Date effectiveFromInput2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveToInput2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        jsonObject = this.save_withL2(
                objectId,
                effectiveFromInput2, effectiveToInput2,
                history);

        response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(objectTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(objectId).build()),
                Arrays.asList(EcpId.newBuilder().setUri(contentItemTypeId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        history = getHistory(responseGet);

        TestCase.assertEquals(2, history.getChanges().size());

        // third round of writing
        Date effectiveFromInput3 = Date.from(Instant.parse("2016-01-01T00:00:00.000Z"));
        Date effectiveToInput3 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        jsonObject = this.save_withL2(
                objectId,
                effectiveFromInput3, effectiveToInput3,
                history);
        response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(objectTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(objectId).build()),
                Arrays.asList(EcpId.newBuilder().setUri(contentItemTypeId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        history = getHistory(responseGet);

        TestCase.assertEquals(3, history.getChanges().size());

        System.out.println("\n***** changes: " + history.getChanges());

        List<TemporalState> temporalStates = new ArrayList<>(3);
        for (DataItemEntityChange change : history.getChanges()) {
            temporalStates.add(change.getTemporalState());
        }
        TestCase.assertEquals(effectiveFromInput1, temporalStates.get(0).getEffectiveFrom().get());
        TestCase.assertEquals(effectiveFromInput2, temporalStates.get(1).getEffectiveFrom().get());
        TestCase.assertEquals(effectiveFromInput3, temporalStates.get(2).getEffectiveFrom().get());

        TestCase.assertEquals(effectiveToInput1, temporalStates.get(0).getEffectiveTo().get());
        TestCase.assertEquals(effectiveToInput2, temporalStates.get(1).getEffectiveTo().get());
        TestCase.assertEquals(effectiveToInput3, temporalStates.get(2).getEffectiveTo().get());

    }

    /**
     * *
     * test that, CDF CANNOT keep the all delta in full-history for relationship
     * with (enrichmentEnabled=true)
     */
    @Test
    public void test_multiple_writes_enrichmentEnabled_true() {
        switchRegistry("ecp:9-ad3f5da5-639a-44e8-a1c7-0bdefb4e7e12", true, true);

        ObjectRequestSender sender = new ObjectRequestSender(token);

        Long systemNanoTime = System.nanoTime();
        String objectId = "integration.test." + systemNanoTime;

        Date effectiveFromInput1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveToInput1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        JSONObject jsonObject = this.save_withL2(
                objectId,
                effectiveFromInput1, effectiveToInput1,
                null);
        CdfLevel1Service.Response response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        JSONObject responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(objectTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(objectId).build()),
                Arrays.asList(EcpId.newBuilder().setUri(contentItemTypeId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        BiTemporalDataItemEntityHistory history = getHistory(responseGet);

        TestCase.assertEquals(1, history.getChanges().size());

        TestCase.assertEquals(effectiveFromInput1, history.getChanges().get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveToInput1, history.getChanges().get(0).getTemporalState().getEffectiveTo().get());

        // second round of writing
        Date effectiveFromInput2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveToInput2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        jsonObject = this.save_withL2(
                objectId,
                effectiveFromInput2, effectiveToInput2,
                history);

        response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        // third round of writing
        Date effectiveFromInput3 = Date.from(Instant.parse("2016-01-01T00:00:00.000Z"));
        Date effectiveToInput3 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        jsonObject = this.save_withL2(
                objectId,
                effectiveFromInput3, effectiveToInput3,
                history);
        response = (CdfLevel1Service.Response) jsonObject.get("response");

        System.out.println("\n***** response: " + response);

        TestCase.assertEquals(Response.Status.OK.getStatusCode(), response.getCode());

        responseGet = sender.getCDFRetrievalResponse(
                retrievalUrl,
                EcpId.newBuilder().setUri(versionId).build(),
                EcpId.newBuilder().setUri(objectTypeId).build(),
                Arrays.asList(EcpId.newBuilder().setUri(objectId).build()),
                Arrays.asList(EcpId.newBuilder().setUri(contentItemTypeId).build()),
                EcpId.newBuilder().setUri(senderId).build(),
                ResponseType.AggregatedState);

        System.out.println("\n***** cdf retrieval response: " + responseGet);

        history = getHistory(responseGet);

        System.out.println("\n***** changes: " + history.getChanges());

        TestCase.assertTrue(history.getChanges().size() == 3);

    }
}
