/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.sdk;

import com.tr.cdf.datamodel.level2.api.Level2Builders;
import com.tr.cdf.datamodel.level2.api.TemporalStateFactory;
import com.tr.cdf.datamodel.level2.api.change.operations.DataItemEntityChangeFactory;
import com.tr.cdf.datamodel.level2.api.data.items.DataItemBuilder;
import com.tr.cdf.datamodel.level2.core.BiTemporalDataItemEntityHistory;
import com.tr.cdf.datamodel.level2.core.CDFProvenanceEntity;
import com.tr.cdf.datamodel.level2.core.DataItemEntity;
import com.tr.cdf.datamodel.level2.core.DataItemEntityChange;
import com.tr.cdf.datamodel.level2.core.EntitlementDataUtils;
import com.tr.cdf.datamodel.level2.core.EntitlementEntity;
import com.tr.cdf.datamodel.level2.core.RegisteredContainerType;
import com.tr.cdf.datamodel.level2.core.provenance.ActivityType;
import com.tr.cdf.datamodel.level2.core.provenance.Agent;
import com.tr.cdf.datamodel.level2.core.provenance.AgentIdProvider;
import com.tr.cdf.datamodel.level2.core.provenance.AgentType;
import com.tr.cdf.datamodel.level2.core.provenance.SourceBulkFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Date;
import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * Note that: two cases failed
 * 1) test_provenance: [Provenance support poorly now!!!!! Chase CDF to enhance !!!!!]
 *   when no data changes but only provenance, CDF side can well support save but fail with get
 * 2) test_provenance_plus_entitlement:
 *   question to Betty: will there be such a case? NO - entitlement will always go together with data
 * 
 * @author Binglin Yu
 */
public class Test_entitleMents {

    // send data only
    @Test
    public void test_data() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path = "pro-1";
        String value = "value-1";

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));

        DataItemBuilder dataItemBuilder1 = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value)
                );

        DataItemEntity dataItemEntity = dataItemBuilder1.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertFalse(dataItemEntity.isDraft());

        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
        dataItemBuilder1 = Level2Builders.dataItem().from(dataItemEntity);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, null, systemFrom, null));
        change.getChangeOperations().addAll(dataItemBuilder1.buildValueSettingOperations());

        history = Level2Builders.history().add(change).build();

        System.out.println("\n***** changes: " + change);

        dataItemEntity = history.getStateAt(null, null);

        dataItemEntity.moveAdditionalInfoFromSubtreesToDataItems();
        
        // output for checking
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);

        // expect to have dataItemEntity.isDraft() = true
        System.out.println("\n***** dataItemEntity.isDraft: " + dataItemEntity.isDraft());

        TestCase.assertFalse(dataItemEntity.isDraft());
        
        TestCase.assertEquals(value, dataItemEntity.getByPath(path).getSimpleValue().get());
        
        TestCase.assertEquals(1, dataItemEntity.getChildKeys().size());

    }
    
    /**
     * EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity) 
     * ! fail to work with provenance alone !
     * 
     */
    @Test
    public void test_provenance() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path = "pro-1";
//        String value = "value-1";

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        
        String provenanceComment = "test.entitlements." + System.nanoTime();

        DataItemBuilder dataItemBuilder = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue((String)null)
                        .setProvenanceEntity(new CDFProvenanceEntity()
                                .setActivityType(new ActivityType(new URI("test")))
                                .addAgent(new Agent(AgentType.HUMAN, "agent-test", AgentIdProvider.IRPM))
                                .setComment(provenanceComment)
                                .setProvenanceStartTime(systemFrom.getTime())
                                .setProvenanceEndTime(systemFrom.getTime() + 1)
                                .setRequestedByAgent(new Agent(AgentType.HUMAN, "requested-agent-test", AgentIdProvider.IRPM))
                                .addSourceAgent(new Agent(AgentType.HUMAN, "source-agent-test", AgentIdProvider.IRPM))
                                .addSourceEntity(new SourceBulkFile(new URI("ecp:9-cdf-reference-000"), "123")))
                );

        DataItemEntity dataItemEntity = dataItemBuilder.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertFalse(dataItemEntity.isDraft());

        System.out.println("\n***** initial dataItemEntity: " + dataItemEntity);

        dataItemEntity.moveAdditionalInfoFromDataItemsToSubtrees(); 
//        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
    
        dataItemBuilder = Level2Builders.dataItem().from(dataItemEntity);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, null, systemFrom, null));
        change.getChangeOperations().addAll(dataItemBuilder.buildValueSettingOperations());

        history = Level2Builders.history().add(change).build();

        System.out.println("\n***** changes: " + change);

        dataItemEntity = history.getStateAt(null, null);
        
        dataItemEntity.moveAdditionalInfoFromSubtreesToDataItems();
        
        // output for checking
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);

        // expect to have dataItemEntity.isDraft() = true
        System.out.println("\n***** dataItemEntity.isDraft: " + dataItemEntity.isDraft());

        TestCase.assertFalse(dataItemEntity.isDraft());
        
//        TestCase.assertEquals(value, dataItemEntity.getByPath(path).getSimpleValue().get());
        
        System.out.println("\n***** dataItemEntity.getChildKeys(): " + dataItemEntity.getChildKeys());
        
        TestCase.assertEquals(1, dataItemEntity.getChildKeys().size());
        
        System.out.println("\n***** history.getChanges().size(): " + history.getChanges().size());
        
        TestCase.assertEquals(1, history.getChanges().size());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getEntitlementEntity() == null || !dataItemEntity.getByPath(path).getEntitlementEntity().isDraft());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getProvenanceStatement().getValue().contains(provenanceComment));
        
        // TODO: check data in dataItemEntity.getByPath(path).getProvenanceEntity()
        
    }
    
    /**
     * EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity) 
     * ! fail to work with provenance alone !
     * 
     * TODO: Unfinished !!
     * 
     * Question: will there be such cases?
     */
    @Test
    public void test_provenance_plus_entitlement() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path = "pro-1";
//        String value = "value-1";

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        
        String provenanceComment = "test.entitlements." + System.nanoTime();

        DataItemBuilder dataItemBuilder = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue((String)null)
                        .setProvenanceEntity(new CDFProvenanceEntity()
                                .setActivityType(new ActivityType(new URI("test")))
                                .addAgent(new Agent(AgentType.HUMAN, "agent-test", AgentIdProvider.IRPM))
                                .setComment(provenanceComment)
                                .setProvenanceStartTime(systemFrom.getTime())
                                .setProvenanceEndTime(systemFrom.getTime() + 1)
                                .setRequestedByAgent(new Agent(AgentType.HUMAN, "requested-agent-test", AgentIdProvider.IRPM))
                                .addSourceAgent(new Agent(AgentType.HUMAN, "source-agent-test", AgentIdProvider.IRPM))
                                .addSourceEntity(new SourceBulkFile(new URI("ecp:9-cdf-reference-000"), "123")))
                        .setEntitlementEntity(new EntitlementEntity(true))
                );

        DataItemEntity dataItemEntity = dataItemBuilder.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertTrue(dataItemEntity.isDraft());

    }

    @Test
    public void test_data_plus_entitlement() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path = "pro-1";
        String value = "value-1";

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));

        DataItemBuilder dataItemBuilder1 = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value)
                        .setEntitlementEntity(new EntitlementEntity(true))
                );

        DataItemEntity dataItemEntity = dataItemBuilder1.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertTrue(dataItemEntity.isDraft());

        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
        dataItemBuilder1 = Level2Builders.dataItem().from(dataItemEntity);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, null, systemFrom, null));
        change.getChangeOperations().addAll(dataItemBuilder1.buildValueSettingOperations());

        history = Level2Builders.history().add(change).build();

        System.out.println("\n***** changes: " + change);

        dataItemEntity = history.getStateAt(null, null);
        
        dataItemEntity.moveAdditionalInfoFromSubtreesToDataItems();
        // output for checking
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);

        // expect to have dataItemEntity.isDraft() = true
        System.out.println("\n***** dataItemEntity.isDraft: " + dataItemEntity.isDraft());

        TestCase.assertTrue(dataItemEntity.isDraft());
        
        TestCase.assertEquals(value, dataItemEntity.getByPath(path).getSimpleValue().get());
        
        System.out.println("\n***** dataItemEntity.getChildKeys(): " + dataItemEntity.getChildKeys());
        
        TestCase.assertEquals(1, dataItemEntity.getChildKeys().size());
        
        System.out.println("\n***** history.getChanges().size(): " + history.getChanges().size());
        
        TestCase.assertEquals(1, history.getChanges().size());
        
        System.out.println("\n***** dataItemEntity.getByPath(path).getEntitlementEntity().isDraft(): "
                + history.getChanges().size());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getEntitlementEntity().isDraft());

    }

    // send data first, then send data with data plus entitlement
    @Test
    public void test_data_plus_entitlement_1() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path1 = "pro-1";
        String path2 = "pro-2";
        String value1 = "value-1";
        String value2 = "value-2";

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));

        DataItemBuilder dataItemBuilder1 = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path1)
                        .setValue(value1)
                );

        DataItemEntity dataItemEntity = dataItemBuilder1.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertFalse(dataItemEntity.isDraft());

        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
        dataItemBuilder1 = Level2Builders.dataItem().from(dataItemEntity);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, null, systemFrom, null));
        change.getChangeOperations().addAll(dataItemBuilder1.buildValueSettingOperations());

        DataItemBuilder dataItemBuilder2 = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path2)
                        .setValue(value2)
                        .setEntitlementEntity(new EntitlementEntity(true))
                );

        dataItemEntity = dataItemBuilder2.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertTrue(dataItemEntity.isDraft());

        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
        dataItemBuilder2 = Level2Builders.dataItem().from(dataItemEntity);

        change.getChangeOperations().addAll(dataItemBuilder2.buildValueSettingOperations());

        history = Level2Builders.history().add(change).build();

        System.out.println("\n***** changes: " + change);

        dataItemEntity = history.getStateAt(null, null);

        dataItemEntity.moveAdditionalInfoFromSubtreesToDataItems();
        
        // output for checking
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);

        // expect to have dataItemEntity.isDraft() = true
        System.out.println("\n***** dataItemEntity.isDraft: " + dataItemEntity.isDraft());

        TestCase.assertTrue(dataItemEntity.isDraft());
        
        TestCase.assertEquals(value1, dataItemEntity.getByPath(path1).getSimpleValue().get());
        
        TestCase.assertEquals(value2, dataItemEntity.getByPath(path2).getSimpleValue().get());
        
        System.out.println("\n***** dataItemEntity.getChildKeys(): " + dataItemEntity.getChildKeys());
        
        TestCase.assertEquals(2, dataItemEntity.getChildKeys().size());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path1).getEntitlementEntity() == null || !dataItemEntity.getByPath(path1).getEntitlementEntity().isDraft());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path2).getEntitlementEntity().isDraft());
    }
    
    
    /**
     * EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity) 
     * ! fail to work with provenance alone !
     */
    @Test
    public void test_data_plus_provenance() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path = "pro-1";
        String value = "value-1";

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        
        String provenanceComment = "test.entitlements." + System.nanoTime();

        DataItemBuilder dataItemBuilder = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value)
                        .setProvenanceEntity(new CDFProvenanceEntity()
                                .setActivityType(new ActivityType(new URI("test")))
                                .addAgent(new Agent(AgentType.HUMAN, "agent-test", AgentIdProvider.IRPM))
                                .setComment(provenanceComment)
                                .setProvenanceStartTime(systemFrom.getTime())
                                .setProvenanceEndTime(systemFrom.getTime() + 1)
                                .setRequestedByAgent(new Agent(AgentType.HUMAN, "requested-agent-test", AgentIdProvider.IRPM))
                                .addSourceAgent(new Agent(AgentType.HUMAN, "source-agent-test", AgentIdProvider.IRPM))
                                .addSourceEntity(new SourceBulkFile(new URI("ecp:9-cdf-reference-000"), "123")))
                );

        DataItemEntity dataItemEntity = dataItemBuilder.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertFalse(dataItemEntity.isDraft());

        System.out.println("\n***** initial dataItemEntity: " + dataItemEntity);

        dataItemEntity.moveAdditionalInfoFromDataItemsToSubtrees(); 
//        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
    
        dataItemBuilder = Level2Builders.dataItem().from(dataItemEntity);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, null, systemFrom, null));
        change.getChangeOperations().addAll(dataItemBuilder.buildValueSettingOperations());

        history = Level2Builders.history().add(change).build();

        System.out.println("\n***** changes: " + change);

        dataItemEntity = history.getStateAt(null, null);

        dataItemEntity.moveAdditionalInfoFromSubtreesToDataItems();
        
        // output for checking
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);

        // expect to have dataItemEntity.isDraft() = true
        System.out.println("\n***** dataItemEntity.isDraft: " + dataItemEntity.isDraft());

        TestCase.assertFalse(dataItemEntity.isDraft());
        
        TestCase.assertEquals(value, dataItemEntity.getByPath(path).getSimpleValue().get());
        
        System.out.println("\n***** dataItemEntity.getChildKeys(): " + dataItemEntity.getChildKeys());
        
        TestCase.assertEquals(1, dataItemEntity.getChildKeys().size());
        
        System.out.println("\n***** history.getChanges().size(): " + history.getChanges().size());
        
        TestCase.assertEquals(1, history.getChanges().size());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getEntitlementEntity() == null || !dataItemEntity.getByPath(path).getEntitlementEntity().isDraft());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getProvenanceStatement().getValue().contains(provenanceComment));
        
        // TODO: check data in dataItemEntity.getByPath(path).getProvenanceEntity()
        
    }

    // send L2 data, plus provenance data, plus entitlement flag
    /**
     * EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity) 
     * ! fail to work with provenance data !
     */
    @Test
    public void test_data_plus_provenance_plus_entitlement() throws URISyntaxException {

        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        String path = "pro-1";
        String value = "value-1";
        
        String provenanceComment = "test.entitlements." + System.nanoTime();

        Date effectiveFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));
        Date systemFrom = Date.from(Instant.parse("2001-01-01T00:00:00.000Z"));

        DataItemBuilder dataItemBuilder = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value)
                        .setProvenanceEntity(new CDFProvenanceEntity()
                                .setActivityType(new ActivityType(new URI("test")))
                                .addAgent(new Agent(AgentType.HUMAN, "agent-test", AgentIdProvider.IRPM))
                                .setComment(provenanceComment)
                                .setProvenanceStartTime(systemFrom.getTime())
                                .setProvenanceEndTime(systemFrom.getTime() + 1)
                                .setRequestedByAgent(new Agent(AgentType.HUMAN, "requested-agent-test", AgentIdProvider.IRPM))
                                .addSourceAgent(new Agent(AgentType.HUMAN, "source-agent-test", AgentIdProvider.IRPM))
                                .addSourceEntity(new SourceBulkFile(new URI("ecp:9-cdf-reference-000"), "123")))
                        .setEntitlementEntity(new EntitlementEntity(true))
                );

        DataItemEntity dataItemEntity = dataItemBuilder.buildEntity(RegisteredContainerType.METADATA);

        TestCase.assertTrue(dataItemEntity.isDraft());

        System.out.println("\n***** initial dataItemEntity: " + dataItemEntity);

        dataItemEntity.moveAdditionalInfoFromDataItemsToSubtrees();
        //        EntitlementDataUtils.fillEntitlementSubtreeFromDataItemsEntitlementInfo(dataItemEntity);
        
        dataItemBuilder = Level2Builders.dataItem().from(dataItemEntity);
        
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom, null, systemFrom, null));
        change.getChangeOperations().addAll(dataItemBuilder.buildValueSettingOperations());

        history = Level2Builders.history().add(change).build();

        System.out.println("\n***** changes: " + change);

        dataItemEntity = history.getStateAt(null, null);

        dataItemEntity.moveAdditionalInfoFromSubtreesToDataItems();
        
        // output for checking
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);

        // expect to have dataItemEntity.isDraft() = true
        System.out.println("\n***** dataItemEntity.isDraft: " + dataItemEntity.isDraft());

        TestCase.assertTrue(dataItemEntity.isDraft());

        TestCase.assertEquals(value, dataItemEntity.getByPath(path).getSimpleValue().get());
        
        System.out.println("\n***** dataItemEntity.getChildKeys(): " + dataItemEntity.getChildKeys());
        
        TestCase.assertEquals(1, dataItemEntity.getChildKeys().size());
        
        System.out.println("\n***** history.getChanges().size(): " + history.getChanges().size());
        
        TestCase.assertEquals(1, history.getChanges().size());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getEntitlementEntity().isDraft());
        
        TestCase.assertTrue(dataItemEntity.getByPath(path).getProvenanceStatement().getValue().contains(provenanceComment));
        
        // TODO: check data in dataItemEntity.getByPath(path).getProvenanceEntity()
        
    }

}
