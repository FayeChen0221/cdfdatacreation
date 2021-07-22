package com.refinitiv.edp.cnm.orca.cdf.object;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Metadata {
    @JsonProperty("Status")
    private String status;

    @JsonProperty("AccessionNumber")
    private String accessionNumber;

    @JsonProperty("DocumentTitle")
    private String documentTitle;

    @JsonProperty("DocumentDate")
    private String documentDate;

    @JsonProperty("ReleaseDate")
    private String releaseDate;

    @JsonProperty("SedarSubmissionNumber")
    private String sedarSubmissionNumber;

    @JsonProperty("SedarProjectNumber")
    private String sedarProjectNumber;

    @JsonProperty("SedarFilerTypeCode")
    private String sedarFilerTypeCode;

    @JsonProperty("SedarFilingTypecode")
    private String sedarFilingTypecode;

    @JsonProperty("SedarSubmissionTypeCode")
    private String sedarSubmissionTypeCode;

    @JsonProperty("SedarDocumentTypeCode")
    private String sedarDocumentTypeCode;

    @JsonProperty("PDFReference")
    private String pdfReference;

    @JsonProperty("DCN")
    private String dcn;

    @JsonProperty("FormType")
    private String formType;

    @JsonProperty("IsInternal")
    private String isInternal;

    @JsonProperty("lastchangeDate")
    private String lastchangeDate;

    @JsonProperty("FeedId")
    private String feedId;

    @JsonProperty("FilingsCapturedDateTime")
    private String filingsCapturedDateTime;

    @JsonProperty("SourcingChannel")
    private String sourcingChannel;

    @JsonProperty("FeedName")
    private String feedName;

    @JsonProperty("AsReportedFileName")
    private String asReportedFileName;

    @JsonProperty("AsReportedFileFormat")
    private String asReportedFileFormat;

    @JsonProperty("AsReportedDocumentCount")
    private String asReportedDocumentCount;

    @JsonProperty("SourceName")
    private String sourceName;

    @JsonProperty("PDFPageCount")
    private String pdfPageCount;


    public String getStatus() {
        return status;
    }

    public Metadata setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getAccessionNumber() {
        return accessionNumber;
    }

    public Metadata setAccessionNumber(String accessionNumber) {
        this.accessionNumber = accessionNumber;
        return this;
    }

    public String getDocumentTitle() {
        return documentTitle;
    }

    public Metadata setDocumentTitle(String documentTitle) {
        this.documentTitle = documentTitle;
        return this;
    }

    public String getDocumentDate() {
        return documentDate;
    }

    public Metadata setDocumentDate(String documentDate) {
        this.documentDate = documentDate;
        return this;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public Metadata setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
        return this;
    }

    public String getSedarSubmissionNumber() {
        return sedarSubmissionNumber;
    }

    public Metadata setSedarSubmissionNumber(String sedarSubmissionNumber) {
        this.sedarSubmissionNumber = sedarSubmissionNumber;
        return this;
    }

    public String getSedarProjectNumber() {
        return sedarProjectNumber;
    }

    public Metadata setSedarProjectNumber(String sedarSubmissionNumber) {
        this.sedarProjectNumber = sedarSubmissionNumber;
        return this;
    }

    public String getSedarFilerTypeCode() {
        return sedarFilerTypeCode;
    }

    public Metadata setSedarFilerTypeCode(String sedarFilerTypeCode) {
        this.sedarFilerTypeCode = sedarFilerTypeCode;
        return this;
    }

    public String getSedarFilingTypecode() {
        return sedarFilingTypecode;
    }

    public Metadata setSedarFilingTypecode(String sedarFilingTypecode) {
        this.sedarFilingTypecode = sedarFilingTypecode;
        return this;
    }

    public String getSedarSubmissionTypeCode() {
        return sedarSubmissionTypeCode;
    }

    public Metadata setSedarSubmissionTypeCode(String sedarSubmissionTypeCode) {
        this.sedarSubmissionTypeCode = sedarSubmissionTypeCode;
        return this;
    }

    public String getSedarDocumentTypeCode() {
        return sedarDocumentTypeCode;
    }

    public Metadata setSedarDocumentTypeCode(String sedarDocumentTypeCode) {
        this.sedarDocumentTypeCode = sedarDocumentTypeCode;
        return this;
    }

    public String getPdfReference() {
        return pdfReference;
    }

    ;

    public Metadata setPdfReference(String reference) {
        this.pdfReference = reference;
        return this;
    }

    public String getDCN() {
        return dcn;
    }

    public Metadata setDCN(String dcn) {
        this.dcn = dcn;
        return this;
    }

    public String getFormType() {
        return formType;
    }

    public Metadata setFormType(String formType) {
        this.formType = formType;
        return this;
    }

    public String getIsInternal() {
        return isInternal;
    }

    public Metadata setIsInternal(String isInternal) {
        this.isInternal = isInternal;
        return this;
    }

    public String getLastChangeDate() {
        return lastchangeDate;
    }

    public Metadata setLastChangeDate(String lastChangeDate) {
        this.lastchangeDate = lastChangeDate;
        return this;
    }

    public String getFeedId() {
        return feedId;
    }

    public Metadata setFeedId(String feedId) {
        this.feedId = feedId;
        return this;
    }

    public String getFilingsCapturedDateTime() {
        return filingsCapturedDateTime;
    }

    public Metadata setFilingsCapturedDateTime(String filingsCapturedDateTime) {
        this.filingsCapturedDateTime = filingsCapturedDateTime;
        return this;
    }

    public String getSourcingChannel() {
        return sourcingChannel;
    }

    public Metadata setSourcingChannel(String sourcingChannel) {
        this.sourcingChannel = sourcingChannel;
        return this;
    }

    public String getFeedName() {
        return feedName;
    }

    public Metadata setFeedName(String feedName) {
        this.feedName = feedName;
        return this;
    }

    public String getAsReportedFileName() {
        return asReportedFileName;
    }

    public Metadata setAsReportedFileName(String asReportedFileName) {
        this.asReportedFileName = asReportedFileName;
        return this;
    }

    public String getAsReportedFileFormat() {
        return asReportedFileFormat;
    }

    public Metadata setAsReportedFileFormat(String asReportedFileFormat) {
        this.asReportedFileFormat = asReportedFileFormat;
        return this;
    }

    public String getAsReportedDocumentCount() {
        return asReportedDocumentCount;
    }

    public Metadata setAsReportedDocumentCount(String asReportedDocumentCount) {
        this.asReportedDocumentCount = asReportedDocumentCount;
        return this;
    }

    public String getSourceName() {
        return sourceName;
    }

    public Metadata setSourceName(String sourceName) {
        this.sourceName = sourceName;
        return this;
    }

    public String getPdfPageCount() {
        return pdfPageCount;
    }

    public Metadata setPdfPageCount(String pdfPageCount) {
        this.pdfPageCount = pdfPageCount;
        return this;
    }


    public Iterator<Map.Entry<String, JsonNode>> createMetadata() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        Metadata data = new Metadata()
                .setStatus(status)
                .setAccessionNumber(accessionNumber)
                .setDocumentTitle(documentTitle)
                .setDocumentDate(documentDate)
                .setReleaseDate(releaseDate)
                .setSedarSubmissionNumber(sedarSubmissionNumber)
                .setSedarProjectNumber(sedarProjectNumber)
                .setSedarFilerTypeCode(sedarFilerTypeCode)
                .setSedarFilingTypecode(sedarFilingTypecode)
                .setSedarSubmissionTypeCode(sedarSubmissionTypeCode)
                .setSedarDocumentTypeCode(sedarDocumentTypeCode)
                .setPdfReference(pdfReference)
                .setIsInternal(isInternal)
                .setLastChangeDate(lastchangeDate)
                .setFeedId(feedId)
                .setFilingsCapturedDateTime(filingsCapturedDateTime)
                .setSourcingChannel(sourcingChannel)
                .setFeedName(feedName)
                .setAsReportedFileName(asReportedFileName)
                .setAsReportedFileFormat(asReportedFileFormat)
                .setAsReportedDocumentCount(asReportedDocumentCount)
                .setSourceName(sourceName)
                .setPdfPageCount(pdfPageCount);

        String jsonString = mapper.writeValueAsString(data);
        JsonNode node = mapper.readTree(jsonString);
        return node.fields();
    }
}
