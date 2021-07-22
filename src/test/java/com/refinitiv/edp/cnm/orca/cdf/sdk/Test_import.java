/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.sdk;

import com.tr.cdf.datamodel.level2.api.DataItemPathFactory;
import com.tr.cdf.datamodel.level2.api.HistoryUtils;
import com.tr.cdf.datamodel.level2.api.Level2Builders;
import com.tr.cdf.datamodel.level2.api.TemporalStateFactory;
import com.tr.cdf.datamodel.level2.api.change.operations.DataItemEntityChangeFactory;
import com.tr.cdf.datamodel.level2.api.tools.ImportExternalDataTool;
import com.tr.cdf.datamodel.level2.api.tools.ImportExternalDataToolFactory;
import com.tr.cdf.datamodel.level2.api.tools.SystemDatesStrategy;
import com.tr.cdf.datamodel.level2.core.BiTemporalDataItemEntityHistory;
import com.tr.cdf.datamodel.level2.core.DataItemEntity;
import com.tr.cdf.datamodel.level2.core.DataItemEntityChange;
import com.tr.cdf.datamodel.level2.core.RegisteredContainerType;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * @author Binglin Yu
 */
public class Test_import {
    String path = "pro-1";
    
    protected List<DataItemEntityChange> getInitHistory() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        
        Date effectiveFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        
        Date systemFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        
        String value1 = "value1";
        String value2 = "value2";

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom2, effectiveTo2, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value2)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        history = historyBuilder.build();

        List<DataItemEntity> dataItemEntityList = HistoryUtils.getAllSnapshotsForPath(history.getChanges(), DataItemPathFactory.create(path));

        // output for checking
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);

        }
        
        return history.getChanges();
    }
    
    // the overwriting can only happens when the speicifed paths match the existing (valid) property paths
    // and the new changes will have system date as systemFrom (not the speicifed systemFrom)!
    @Test
    public void test_workable() {
        

        BiTemporalDataItemEntityHistory history = Level2Builders.history().add(this.getInitHistory()).build();

        Date effectiveFrom3 = Date.from(Instant.parse("2020-01-01T00:00:00.000Z"));
        Date systemFrom3 = Date.from(Instant.parse("2020-02-01T00:00:00.000Z"));
        
        ImportExternalDataTool importTool = ImportExternalDataToolFactory.create();
        
        DataItemEntity dataItemEntity_killer = Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue((String)null)
                ).buildEntity(RegisteredContainerType.METADATA);
        
        dataItemEntity_killer.setTemporalState(TemporalStateFactory.create(effectiveFrom3, null, systemFrom3, null));
        
        System.out.println("\n***** dataItemEntity_killer: " + dataItemEntity_killer);

        importTool.setSystemDatesStrategy(SystemDatesStrategy.FROM_INPUT_DATA);
        
        List<DataItemEntityChange> changes = importTool.updateWithDeltaFromEntity(history, dataItemEntity_killer);
        
        System.out.println("\n***** history: " + history.getChanges());
        
        System.out.println("\n***** changes: " + changes);
        
        // !! CDF use the latest importing time, not the specified systemFrom !!
        TestCase.assertEquals(systemFrom3, changes.get(0).getTemporalState().getSystemFrom().get());
        
        DataItemEntity dataItemEntity = history.getStateAt(effectiveFrom3, systemFrom3);
        
        TestCase.assertNull(dataItemEntity);
        
//        TestCase.assertTrue(dataItemEntity.getChildKeys());
        
    }
    
    // the overwriting can only happens when the speicifed paths match the existing (valid) property paths
    // otherwise, no new actions by importing
    @Test
    public void test_fail() {
        BiTemporalDataItemEntityHistory history = Level2Builders.history().add(this.getInitHistory()).build();

        Date effectiveFrom3 = Date.from(Instant.parse("2020-01-01T00:00:00.000Z"));
        Date systemFrom3 = Date.from(Instant.parse("2020-02-01T00:00:00.000Z"));
        
        ImportExternalDataTool importTool = ImportExternalDataToolFactory.create();
        
        importTool.setSystemDatesStrategy(SystemDatesStrategy.FROM_INPUT_DATA);
        
        DataItemEntity dataItemEntity_killer = Level2Builders.dataItem()
                .buildEntity(RegisteredContainerType.METADATA);
        
        dataItemEntity_killer.setTemporalState(TemporalStateFactory.create(effectiveFrom3, null, systemFrom3, null));
        
        System.out.println("\n***** dataItemEntity_killer: " + dataItemEntity_killer);

        List<DataItemEntityChange> changes = importTool.updateWithDeltaFromEntity(history, dataItemEntity_killer);
        
        System.out.println("\n***** history: " + history.getChanges());
        
        System.out.println("\n***** changes: " + changes);
        
        // !! new changes is empty !!
        TestCase.assertTrue(changes.isEmpty());
        
        DataItemEntity dataItemEntity = history.getStateAt(effectiveFrom3, systemFrom3);
        
        System.out.println("\n***** dataItemEntity: " + dataItemEntity);
        
        TestCase.assertNotNull(dataItemEntity);
        
    }
}
