package com.refinitiv.edp.cnm.orca.cdf.object;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ExtendMetadata {
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

    @JsonProperty("IsHTMLFormatAvailable")
    private String isHTMLFormatAvailable;

    @JsonProperty("IsXBRLAvailable")
    private String isXBRLAvailable;

    @JsonProperty("DocumentSourcedUserid")
    private String documentSourcedUserid;

    @JsonProperty("IsAmendment")
    private String isAmendment;

    @JsonProperty("IsTXtFormatAvailable")
    private String isTXtFormatAvailable;

    @JsonProperty("FeedName")
    private String feedName;

    @JsonProperty("lastchangeDate")
    private String lastchangeDate;

    @JsonProperty("IsPDFFormatAvailable")
    private String isPDFFormatAvailable;

    public String getStatus() { return status; }

    public ExtendMetadata setStatus(String status)
    {
        this.status = status;
        return this;
    }

    public String getAccessionNumber() { return accessionNumber; }

    public ExtendMetadata setAccessionNumber(String accessionNumber)
    {
        this.accessionNumber = accessionNumber;
        return this;
    }

    public String getDocumentTitle() { return documentTitle; }

    public ExtendMetadata setDocumentTitle(String documentTitle)
    {
        this.documentTitle = documentTitle;
        return this;
    }

    public String getDocumentDate() { return documentDate; }

    public ExtendMetadata setDocumentDate(String documentDate)
    {
        this.documentDate = documentDate;
        return this;
    }

    public String getReleaseDate() { return releaseDate; }

    public ExtendMetadata setReleaseDate(String releaseDate)
    {
        this.releaseDate = releaseDate;
        return this;
    }

    public String getSedarSubmissionNumber() { return sedarSubmissionNumber; }

    public ExtendMetadata setSedarSubmissionNumber(String sedarSubmissionNumber)
    {
        this.sedarSubmissionNumber = sedarSubmissionNumber;
        return this;
    }

    public String getSedarProjectNumber() { return sedarProjectNumber; }

    public ExtendMetadata setSedarProjectNumber(String sedarSubmissionNumber)
    {
        this.sedarProjectNumber = sedarSubmissionNumber;
        return this;
    }

    public String getSedarFilerTypeCode() { return sedarFilerTypeCode; }

    public ExtendMetadata setSedarFilerTypeCode(String sedarFilerTypeCode)
    {
        this.sedarFilerTypeCode = sedarFilerTypeCode;
        return this;
    }

    public String getSedarFilingTypecode() { return sedarFilingTypecode; }

    public ExtendMetadata setSedarFilingTypecode(String sedarFilingTypecode) {
        this.sedarFilingTypecode = sedarFilingTypecode;
        return this;
    }

    public String getSedarSubmissionTypeCode() { return sedarSubmissionTypeCode; }

    public ExtendMetadata setSedarSubmissionTypeCode(String sedarSubmissionTypeCode) {
        this.sedarSubmissionTypeCode = sedarSubmissionTypeCode;
        return this;
    }

    public String getSedarDocumentTypeCode() { return sedarDocumentTypeCode; }

    public ExtendMetadata setSedarDocumentTypeCode(String sedarDocumentTypeCode)
    {
        this.sedarDocumentTypeCode = sedarDocumentTypeCode;
        return this;
    }

    public String getPdfReference() {return pdfReference;};

    public ExtendMetadata setPdfReference(String reference)
    {
        this.pdfReference = reference;
        return this;
    }

    public String getDCN() { return dcn; }

    public ExtendMetadata setDCN(String dcn)
    {
        this.dcn = dcn;
        return this;
    }

    public String FormType() { return formType; }

    public ExtendMetadata setFormType(String formType)
    {
        this.formType = formType;
        return this;
    }

    public String getIsHTMLFormatAvailable() { return isHTMLFormatAvailable; }

    public ExtendMetadata setIsHTMLFormatAvailable(String isHTMLFormatAvailable)
    {
        this.isHTMLFormatAvailable = isHTMLFormatAvailable;
        return this;
    }

    public String getIsXBRLAvailable() { return isXBRLAvailable; }

    public ExtendMetadata setIsXBRLAvailable(String isXBRLAvailable)
    {
        this.isXBRLAvailable = isXBRLAvailable;
        return this;
    }

    public String getDocumentSourcedUserid() { return documentSourcedUserid; }

    public ExtendMetadata setDocumentSourcedUserid(String documentSourcedUserid)
    {
        this.documentSourcedUserid = documentSourcedUserid;
        return this;
    }

    public String getIsAmendment() { return isAmendment; }

    public ExtendMetadata setIsAmendment(String isAmendment)
    {
        this.isAmendment = isAmendment;
        return this;
    }

    public String getIsTXtFormatAvailable() { return isTXtFormatAvailable; }

    public ExtendMetadata setIsTXtFormatAvailable(String isTXtFormatAvailable)
    {
        this.isTXtFormatAvailable = isTXtFormatAvailable;
        return this;
    }

    public String getFeedName() { return isTXtFormatAvailable; }

    public ExtendMetadata setFeedName(String isTXtFormatAvailable)
    {
        this.isTXtFormatAvailable = isTXtFormatAvailable;
        return this;
    }

    public String getLastChangeDate() { return lastchangeDate; }

    public ExtendMetadata setLastChangeDate(String lastchangeDate)
    {
        this.lastchangeDate = lastchangeDate;
        return this;
    }

    public String getIsPDFFormatAvailable() { return isPDFFormatAvailable; }

    public ExtendMetadata setIsPDFFormatAvailable(String isPDFFormatAvailable)
    {
        this.isPDFFormatAvailable = isPDFFormatAvailable;
        return this;
    }

    public Iterator<Map.Entry<String, JsonNode>> createExtendedMetadata() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        ExtendMetadata data = new ExtendMetadata()
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
                .setIsHTMLFormatAvailable(isHTMLFormatAvailable)
                .setIsXBRLAvailable(isXBRLAvailable)
                .setDocumentSourcedUserid(documentSourcedUserid)
                .setIsAmendment(isAmendment)
                .setIsTXtFormatAvailable(isTXtFormatAvailable)
                .setFeedName(feedName)
                .setLastChangeDate(lastchangeDate)
                .setIsPDFFormatAvailable(isPDFFormatAvailable);

        String jsonString = mapper.writeValueAsString(data);
        JsonNode node = mapper.readTree(jsonString);
        return node.fields();
    }
}
