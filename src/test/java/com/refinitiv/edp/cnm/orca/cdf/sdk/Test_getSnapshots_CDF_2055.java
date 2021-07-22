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
import com.tr.cdf.datamodel.level2.core.BiTemporalDataItemEntityHistory;
import com.tr.cdf.datamodel.level2.core.DataItemEntity;
import com.tr.cdf.datamodel.level2.core.DataItemEntityChange;
import com.tr.cdf.datamodel.level2.core.tools.snapshots.index.Grid;
import com.tr.cdf.datamodel.level2.core.tools.snapshots.index.HistoryGridUtils;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * @author Binglin Yu
 */
public class Test_getSnapshots_CDF_2055 {

    /**
     * *
     * same property+value, and overlapping effective durations (effectiveTo =
     * null) effectiveFrom2 &lt effectiveFrom1
     *
     * expect to have the active version has [effectiveFrom2, null)
     *
     */
    @Test
    public void test_CDF_2055__default() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, null, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom2, null, systemFrom2, null));
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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertNull(dataItemEntity.getTemporalState().getEffectiveTo());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertNull(dataItemEntity.getTemporalState().getEffectiveTo());

    }

    /**
     * *
     * same property+value, and overlapping effective durations (effectiveTo =
     * null) effectiveFrom2 &lt effectiveFrom1
     *
     * expect to have two active versions 1. [effectiveFrom2, effectiveFrom1) 2.
     * [effectiveFrom1, null)
     */
    @Test
    public void test_CDF_2055__TO_WEST_THEN_TO_SOUTH() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        HistoryGridUtils.DEFAULT_NODES_LINKING_ORDER = Grid.NodesLinkingOrder.TO_WEST_THEN_TO_SOUTH;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, null, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom2, null, systemFrom2, null));
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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertNull(dataItemEntity.getTemporalState().getEffectiveTo());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }
}
