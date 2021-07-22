package com.applayer;

import com.refinitiv.edp.cnm.orca.cdf.object.ExtendMetadata;
import com.refinitiv.edp.cnm.orca.cdf.object.Metadata;
import com.tr.cdf.level1.client.publish.api.CdfLevel1Service;
import org.json.JSONObject;
import org.junit.Test;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import com.refinitiv.edp.cnm.orca.cdf.utils.InsertObject;


public class Test_data {

//    final private  String partitionId = "ecp:9-f91b048c-a57e-4b5d-b304-615eb6b40993";;
//    final private String distributionPairId = "ecp:9-f8c27180-e435-4217-b61c-aab1dd0e7856";
//    final private String versionId = "ecp:9-cfb2b7e8-669c-4690-86cf-931471d0caae";
//    final private String metadataContentItemTypeId = "ecp:9-f6aa048a-b4b6-4c0b-bb4b-68e7e2e0272b";
//    final private String extendedMetadataContentItemTypeId = "ecp:9-a69cb3f0-ce4e-46bf-b9a6-191628f490e7";
//    final private String semiStructuredContentItemTypeId = "ecp:9-872ef936-3452-4a08-9a5e-3727bddb7474";
//    final private String objectTypeId = "ecp:9-a77e3be6-a17f-431a-8c96-aac939dae3c2";
//    final private String senderId = "ecp:9-e32e7619-c32d-4518-b257-7dc0a5172f9f";
//    final private String s3BucketName = "a205065-a206166-preproduction-us-east-1";
//    final private String inboundStream = "a205065-a206166-DocumentDEV-Stream";
//    final private String s3BucketPrefix = "DocumentDEV";

    final private String partitionId = "ecp:9-dc995847-0209-4d03-b636-5134132040db";;
    final private String distributionPairId = "ecp:9-4cce028e-d0b5-4c3c-a1f2-59558b929540";
    final private String versionId = "ecp:9-a9e8f7be-c003-45f6-92e5-168f7cd773d9";
    final private String metadataContentItemTypeId = "ecp:9-400d4224-bd70-459f-b763-eed454bf2e2d";
    final private String extendedMetadataContentItemTypeId = "ecp:9-e583d94c-9c31-4d86-822e-e6b3da0b7a73";
    final private String semiStructuredContentItemTypeId = "ecp:9-c7102cb5-ce77-4a40-b932-ba3cffb52748";
    final private String objectTypeId = "ecp:9-da9122d8-db3f-4c8e-83d9-e83569ef7653";
    final private String senderId = "ecp:9-e32e7619-c32d-4518-b257-7dc0a5172f9f";
    final private String s3BucketName = "a205065-a206166-preproduction-us-east-1";
    final private String inboundStream = "a205065-a206166-FilingsMasterDocument-Stream";
    final private String s3BucketPrefix = "FilingsMasterDocument";



    private String documentDate = "2021-01-01T00:00:00.000Z";
    private String releaseDate = "2021-01-01T00:00:00.000Z";
    private Date effectiveFrom = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
    private Date effectiveTo = Date.from(Instant.parse("2022-01-01T00:00:00.000Z"));

    private void insert_data(
            String caseId,
            String objectId,
            String metadataStatus,
            String extendedMetadataStatus,
            String pdfReference,
            String type) throws IOException {

        String accessionNumber = caseId;
        String documentTitle = caseId;
        String sedarSubmissionNumber = String.format("s%s", caseId);
        String sedarProjectNumber = String.format("p%s", caseId);
        String sedarFilerTypeCode = String.format("f%s", caseId);
        String sedarFilingTypecode = String.format("ft%s", caseId);
        String sedarSubmissionTypeCode = String.format("st%s", caseId);
        String sedarDocumentTypeCode = String.format("sd%s", caseId);

        Metadata metadata = new Metadata()
                .setStatus(metadataStatus)
                .setAccessionNumber(accessionNumber)
                .setDocumentTitle(documentTitle)
                .setSedarSubmissionNumber(sedarSubmissionNumber)
                .setSedarProjectNumber(sedarProjectNumber)
                .setSedarFilerTypeCode(sedarFilerTypeCode)
                .setSedarFilingTypecode(sedarFilingTypecode)
                .setSedarSubmissionTypeCode(sedarSubmissionTypeCode)
                .setSedarDocumentTypeCode(sedarDocumentTypeCode)
                .setPdfReference(pdfReference)
                .setDocumentDate(documentDate)
                .setReleaseDate(releaseDate);

        ExtendMetadata extendedMetadata = new ExtendMetadata()
                .setStatus(extendedMetadataStatus)
                .setAccessionNumber(accessionNumber)
                .setDocumentTitle(documentTitle)
                .setSedarSubmissionNumber(sedarSubmissionNumber)
                .setSedarProjectNumber(sedarProjectNumber)
                .setSedarFilerTypeCode(sedarFilerTypeCode)
                .setSedarFilingTypecode(sedarFilingTypecode)
                .setSedarSubmissionTypeCode(sedarSubmissionTypeCode)
                .setSedarDocumentTypeCode(sedarDocumentTypeCode)
                .setPdfReference(pdfReference)
                .setDocumentDate(documentDate)
                .setReleaseDate(releaseDate);

        InsertObject action = new InsertObject().setPartitionId(partitionId)
                .setDistributionPairId(distributionPairId)
                .setVersionId(versionId)
                .setObjectTypeId(objectTypeId)
                .setSenderId(senderId)
                .setS3BucketName(s3BucketName)
                .setInboundStream(inboundStream)
                .setS3BucketPrefix(s3BucketPrefix);

        action.registryCDFCfg();

        JSONObject metadataJsonObject;
        JSONObject extendedMetadataJsonObject;
        JSONObject semiStructuredJsonObject;
        CdfLevel1Service.Response metadataResponse;
        CdfLevel1Service.Response extendedMetadataResponse;
        CdfLevel1Service.Response semiStructuredResponse;


        switch (type){
            case "Order_1":
                // MetaData-> Semi structured-> ExtendedMetadata
                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom,
                        effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);

                action.setContentItemTypeId(semiStructuredContentItemTypeId);
                semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                        objectId);
                semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                        .get("response");
                System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                        "\n***** semiStructured object id: " + objectId);

                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);
                break;

            case "Order_2":
                // ExtendedMetadata-> MetaData-> Semi structured
                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);

                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);

                action.setContentItemTypeId(semiStructuredContentItemTypeId);
                semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                        objectId);
                semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                        .get("response");
                System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                        "\n***** semiStructured object id: " + objectId);
                break;

            case "Order_3":
                // ExtendedMetadata-> Semi structured-> MetaData
                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);

                action.setContentItemTypeId(semiStructuredContentItemTypeId);
                semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                        objectId);
                semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                        .get("response");
                System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                        "\n***** semiStructured object id: " + objectId);

                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);
                break;

            case "Order_4":
                // Semi structured-> MetaData-> ExtendedMetadata
                action.setContentItemTypeId(semiStructuredContentItemTypeId);
                semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                        objectId);
                semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                        .get("response");
                System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                        "\n***** semiStructured object id: " + objectId);

                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);

                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);
                break;

            case "Order_5":
                // Semi structured-> ExtendedMetadata-> MetaData
                action.setContentItemTypeId(semiStructuredContentItemTypeId);
                semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                        objectId);
                semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                        .get("response");
                System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                        "\n***** semiStructured object id: " + objectId);

                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);

                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);
                break;

            case "NoSemi":
                // Semi structured-> ExtendedMetadata-> MetaData
                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);

                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);
                break;

            default:
                //MetaData-> ExtendedMetadata-> Semi structured
                action.setContentItemTypeId(metadataContentItemTypeId);
                metadataJsonObject = action.saveWithMetaDataL2(
                        metadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                metadataResponse = (CdfLevel1Service.Response) metadataJsonObject.get("response");
                System.out.println("\n***** metadata response: " + metadataResponse +
                        "\n***** metadata object id: " + objectId);

                action.setContentItemTypeId(extendedMetadataContentItemTypeId);
                extendedMetadataJsonObject = action.saveWithExtendedMetaDataL2(
                        extendedMetadata,
                        objectId,
                        effectiveFrom, effectiveTo,
                        null);
                extendedMetadataResponse = (CdfLevel1Service.Response) extendedMetadataJsonObject
                        .get("response");
                System.out.println("\n***** extended metadata response: " + extendedMetadataResponse +
                        "\n***** extended metadata object id: " + objectId);

                action.setContentItemTypeId(semiStructuredContentItemTypeId);
                semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                        objectId);
                semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                        .get("response");
                System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                        "\n***** semiStructured object id: " + objectId);
        }
    }

    @Test
    public void test_semi_single() throws IOException{
        String caseId = "C32795505";
        String objectId = String.format("QA_orchestration_test_%s_", caseId) + System.nanoTime();

//        String objectId = "689ae1a5-84cc-40f7-90cb-2318afa783c7";

        InsertObject action = new InsertObject().setPartitionId(partitionId)
                .setDistributionPairId(distributionPairId)
                .setVersionId(versionId)
                .setObjectTypeId(objectTypeId)
                .setSenderId(senderId)
                .setS3BucketName(s3BucketName)
                .setInboundStream(inboundStream)
                .setS3BucketPrefix(s3BucketPrefix)
                .setContentItemTypeId(semiStructuredContentItemTypeId);

        action.registryCDFCfg();

        action.setContentItemTypeId(semiStructuredContentItemTypeId);
        JSONObject semiStructuredJsonObject = action.saveWithSemiStructuredL2(
                objectId);
        CdfLevel1Service.Response semiStructuredResponse = (CdfLevel1Service.Response) semiStructuredJsonObject
                .get("response");
        System.out.println("\n***** semiStructured response: " + semiStructuredResponse +
                "\n***** semiStructured object id: " + objectId);
    }
    @Test
    public void test_C32653184() throws IOException {
        String caseId = "C32653184";
        String objectId = String.format("QA_orchestration_test_%s_", caseId) + System.nanoTime();
        String metadataStatus = "Index_Complete";
        String extendedMetadataStatus = "Index_Complete_OAID";
        String pdfReference = "a205065-a206166-preproduction-us-east-1/DocumentDEV" +
                "/QAE0001493152-20-013179_E2E_2020-12-0318572600000966_2" +
                "/FCS-SQS-22a39cf1-fbf5-44b4-b3b2-81c34b05a831/20201203191139998.html_20201203191145239.pdf";
        insert_data(caseId, objectId, metadataStatus, extendedMetadataStatus, pdfReference, "Order_1");
    }

    @Test
    public void test_add_size_data() throws IOException {
        String caseId = "QA_BoomiSWTest_test_1";
        String objectId = "QA_BoomiSWTest_test_1";
        String metadataStatus = "Index_Complete";
        String extendedMetadataStatus = "Index_Complete";
        String pdfReference = "QA_BoomiSWTest_test_1.pdf";
        insert_data(caseId, objectId, metadataStatus, extendedMetadataStatus, pdfReference, "");
    }

    @Test
    public void test_add_data() throws IOException {
        for (int i = 50; i < 500; i++) {
            String caseId = String.format("QA_BoomiSWTest_perf_%s", i +1);
            String objectId = String.format("QA_BoomiSWTest_perf_%s", i +1);
            String metadataStatus = "Index_Complete";
            String extendedMetadataStatus = "Index_Complete";
            String pdfReference = "QA_BoomiSWTest_perf.pdf";
            insert_data(caseId, objectId, metadataStatus, extendedMetadataStatus, pdfReference, "");
        }
    }
}
