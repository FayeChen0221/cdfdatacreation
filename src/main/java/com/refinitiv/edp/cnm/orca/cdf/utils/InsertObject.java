package com.refinitiv.edp.cnm.orca.cdf.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;
import com.refinitiv.edp.cnm.orca.cdf.object.ExtendMetadata;
import com.refinitiv.edp.cnm.orca.cdf.object.Metadata;
import com.tr.cdf.Envelope;
import com.tr.cdf.OperationType;
import com.tr.cdf.aws.common.aws.AmazonS3ClientProvider;
import com.tr.cdf.aws.common.aws.EnvVariables;
import com.tr.cdf.aws.common.lang.ExternalVariableSuppliers;
import com.tr.cdf.datamodel.level2.api.DeltaFactory;
import com.tr.cdf.datamodel.level2.api.Level2Builders;
import com.tr.cdf.datamodel.level2.api.TemporalStateFactory;
import com.tr.cdf.datamodel.level2.api.change.operations.DataItemEntityChangeFactory;
import com.tr.cdf.datamodel.level2.api.data.items.DataItemBuilder;
import com.tr.cdf.datamodel.level2.core.*;
import com.tr.cdf.level.client.service.impl.aws.AwsCdfLevel1SendService;
import com.tr.cdf.level.client.service.impl.aws.AwsServiceSystemProperties;
import com.tr.cdf.level.client.service.impl.aws.components.EnvelopePersisterS3Impl;
import com.tr.cdf.level1.client.envelope.fluent.builders.EnvelopeFluentBuilder;
import com.tr.cdf.level1.client.publish.DeltaSerialization;
import com.tr.cdf.level1.client.publish.EnvelopeBuilders;
import com.tr.cdf.level1.client.publish.FullHistorySerialization;
import com.tr.cdf.level1.client.publish.SystemProperties;
import com.tr.cdf.level1.client.publish.api.CdfLevel1Service;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.function.Supplier;

public class InsertObject {

    private String partitionId;
    private String distributionPairId;
    private String s3BucketName;
    private String versionId;
    private String s3BucketPrefix;
    private String inboundStream;
    private String contentItemTypeId;
    private String objectTypeId;
    private String senderId;

    public InsertObject setPartitionId(String partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public InsertObject setDistributionPairId(String distributionPairId) {
        this.distributionPairId = distributionPairId;
        return this;
    }

    public InsertObject setS3BucketName(String s3BucketName) {
        this.s3BucketName = s3BucketName;
        return this;
    }

    public InsertObject setVersionId(String versionId) {
        this.versionId = versionId;
        return this;
    }

    public InsertObject setObjectTypeId(String objectTypeId) {
        this.objectTypeId = objectTypeId;
        return this;
    }

    public InsertObject setContentItemTypeId(String contentItemTypeId) {
        this.contentItemTypeId = contentItemTypeId;
        return this;
    }

    public InsertObject setInboundStream(String inboundStream) {
        this.inboundStream = inboundStream;
        return this;
    }

    public InsertObject setSenderId(String senderId) {
        this.senderId = senderId;
        return this;
    }

    public InsertObject setS3BucketPrefix(String s3BucketPrefix) {
        this.s3BucketPrefix = s3BucketPrefix;
        return this;
    }


    public void registryCDFCfg() throws IOException {
        String str = distributionPairId + "," + partitionId
                + "," + versionId
                + "," + s3BucketName
                + "," + inboundStream
                + "," + s3BucketPrefix;

        File registryFile = new java.io.File("C:/Faye/cdf-contract-test/src/tmp/cdf.tmp/"
                + System.nanoTime() + ".csv");
        Files.write(str.getBytes(), registryFile);

        AwsServiceSystemProperties.NOTIFICATION_SERVICE_TYPE.set("REST");
        SystemProperties.REGISTER_SERVICE_URL_PROPERTY.
                set("https://input-sync.ppe.cdf.refinitiv.com/cdfinput");
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
                                                       //.withCredentials(getCredentials(
                                                               //"arn:aws:iam::608014515287:role/205065-cdf-devops-service-PreProduction-RoleFor250266"))
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

    private AWSCredentialsProvider getCredentials(String roleArn) {
        AWSSecurityTokenService ssmClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName("default");
        Credentials credentials = ssmClient.assumeRole(assumeRoleRequest).getCredentials();
        return new AWSStaticCredentialsProvider(
                new BasicSessionCredentials(credentials.getAccessKeyId(),
                        credentials.getSecretAccessKey(),
                        credentials.getSessionToken()));
    }

    public JSONObject saveWithMetaDataL2(
            Metadata metadata,
            String objectId,
            Date effectiveFrom,
            Date effectiveTo,
            BiTemporalDataItemEntityHistory originalHistory) throws IOException {
        Iterator<Map.Entry<String, JsonNode>> fields = metadata.createMetadata();
        DataItemBuilder dataItemBuilder = Level2Builders.dataItem();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            dataItemBuilder.appendChild(Level2Builders.dataItem()
                    .setDataItemTypeIdentifier(fieldName)
                    .setValue(fieldValue.asText()));
        }

        JSONObject response;
        response = getResponse(
                objectId,
                effectiveFrom,
                effectiveTo,
                dataItemBuilder,
                originalHistory
        );
        return response;
    }

    public JSONObject saveWithExtendedMetaDataL2(
            ExtendMetadata extendMetadata,
            String objectId,
            Date effectiveFrom,
            Date effectiveTo,
            BiTemporalDataItemEntityHistory originalHistory) throws IOException {
        Iterator<Map.Entry<String, JsonNode>> fields = extendMetadata.createExtendedMetadata();
        DataItemBuilder dataItemBuilder = Level2Builders.dataItem();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            dataItemBuilder.appendChild(Level2Builders.dataItem()
                    .setDataItemTypeIdentifier(fieldName)
                    .setValue(fieldValue.asText()));
        }

        JSONObject response;
        response = getResponse(
                objectId,
                effectiveFrom,
                effectiveTo,
                dataItemBuilder,
                originalHistory
        );
        return response;
    }

    public JSONObject saveWithSemiStructuredL2(String objectId) throws IOException {
        Date currentAt = new Date();
        String filePath = "C:\\Faye\\cdf-contract-test\\src\\main\\java\\com\\refinitiv\\edp\\cnm\\orca\\cdf\\blob\\689ae1a5-84cc-40f7-90cb-2318afa783c7_semistructured.json";
        File file = new File(filePath);
        byte[] blob = java.nio.file.Files.readAllBytes(file.toPath());

        EnvelopeFluentBuilder envBuilder = EnvelopeBuilders.defaultBuilder();
        final Envelope envelope = envBuilder
                .setDistributionPairId(distributionPairId)
                .setSender(senderId)
                .stratum()
                .setPartitionId(partitionId)
                .setObjectTypeEcpId(objectTypeId)
                .setContentObjectEcpId(objectId)
                .setAdminStatus("Published")
                .setCurrentAt(currentAt.getTime())
                .setVersion(versionId)
                .setCurrentAt(currentAt)
                .setContentItemTypeId(contentItemTypeId)
                .setOperation(OperationType.Upsert)
                .setFile(blob)
                .build();

        CdfLevel1Service.Response response = new AwsCdfLevel1SendService().sendEnvelope(envelope);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("response", response);
        jsonObject.put("id", objectId);
        return jsonObject;
    }

    private JSONObject getResponse(
            String objectId,
            Date effectiveFrom,
            Date effectiveTo,
            DataItemBuilder dataItemBuilder,
            BiTemporalDataItemEntityHistory originalHistory) throws IOException {
        Date currentAt = new Date();
        DataItemEntityChange change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, effectiveTo, currentAt, null));
        change.getChangeOperations().addAll(dataItemBuilder.buildValueSettingOperations());
        if (originalHistory == null) {
            originalHistory = Level2Builders.history().add(change).build();
        } else {
            originalHistory.add(change);
        }

        EnvelopeFluentBuilder envBuilder = EnvelopeBuilders.defaultBuilder();
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
                        .serialize(DeltaFactory.createDelta(Collections.singletonList(change))))
                .setHistoryNewStateEmbedded(new FullHistorySerialization().serialize(originalHistory))
                .build();

        CdfLevel1Service.Response response = new AwsCdfLevel1SendService().sendEnvelope(envelope);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("response", response);
        jsonObject.put("id", objectId);
        return jsonObject;
    }
}