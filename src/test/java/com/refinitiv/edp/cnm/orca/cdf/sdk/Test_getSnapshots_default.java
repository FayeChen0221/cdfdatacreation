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
import java.time.Instant;
import java.util.Date;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * We focus on the case that: 1) same property and same value but different
 * system/effective duration 2) only verify the active versions
 *
 * Use 1 and 2 indicate two changes which come in sequence (i.e. systemFrom1 &lt
 * systemFrom2)
 *
 *
 *
 * <ul>
 * 
 * <li>ef1<et1<ef2<et2 (detached) </li> 
 * <li>ef1<et1=ef2<et2 (detached) </li>
 * <li>ef1<ef2<et1<et2 </li>
 * <li>ef1<ef2<et1=et2 </li>
 * <li>ef1<ef2<et2<et1 </li>
 *
 * 
 * <li>ef1=ef2<et1<et2 </li>
 * <li>ef1=ef2<et1=et2 </li>
 * <li>ef1=ef2<et2<et1 </li>
 *
 * <li>ef2<ef1<et1<et2 </li>
 * <li>ef2<ef1<et1=et2 </li>
 * <li>ef2<ef1<et2<et1 </li>
 * <li>ef2<et2=ef1<et1 (detached) </li>
 * <li>ef2<et2<ef1<et1 (detached) </li>
 * 
 * </ul>
 * 
 *</ul>
 * @author Binglin Yu
 */
public class Test_getSnapshots_default {

    /**
     * ef1=ef2<et1<et2 expect to have one active version has [effectiveFrom2,
     * effectiveTo2)
     */
    @Test
    public void test__ef1_eq_ef2_lt_et1_lt_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("3999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef1=ef2<et1=et2 expect to have one active version has [effectiveFrom1,
     * effectiveTo1)
     */
    @Test
    public void test__ef1_eq_ef2_lt_et1_eq_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef1=ef2<et2<et1 expect to have one active version has [effectiveFrom1,
     * effectiveTo1)
     */
    @Test
    public void test__ef1_eq_ef2_lt_et2_lt_et1() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("3999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveTo2, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }
    
    /**
     * ef1<et1<ef2<et2 (detached)
     *
     * * expect to have two active versions has [effectiveFrom1,
     * effectiveFrom2) and [effectiveFrom2, effectiveTo2)
     */
    @Test
    public void test__ef1_lt_et1_lt_ef2_lt_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }
    
    /**
     * ef1<et1=ef2<et2 (detached)
     *
     * expect to have two active versions has [effectiveFrom1,
     * effectiveFrom2) and [effectiveFrom2, effectiveTo2)
     */
    @Test
    public void test__ef1_lt_et1_eq_ef2_lt_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef1<ef2<et1<et2
     *
     * * expect to have two active versions has [effectiveFrom1,
     * effectiveFrom2) and [effectiveFrom2, effectiveTo2)
     */
    @Test
    public void test__ef1_lt_ef2_lt_et1_lt_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("3999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef1<ef2<et1=et2
     *
     * * expect to have one active version has [effectiveFrom1, effectiveTo1)
     */
    @Test
    public void test__ef1_lt_ef2_lt_et1_eq_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef1<ef2<et2<et1
     *
     * * expect to have one active version with [effectiveFrom1, effectiveTo1)
     */
    @Test
    public void test__ef1_lt_ef2_lt_et2_lt_et1() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        Date effectiveTo1 = Date.from(Instant.parse("3999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveTo2, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     *
     * ef2<ef1<et1<et2
     *
     * expect to have one active version has [ef2, et2)
     *
     */
    @Test
    public void test__ef2_lt_ef1_lt_et1_lt_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("3999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     *
     * ef2<ef1<et1=et2
     *
     * expect to have the active version with [effectiveFrom2, effectiveTo2)
     *
     */
    @Test
    public void test__ef2_lt_ef1_lt_et1_eq_et2() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef2<ef1<et2<et1
     *
     * expect to have two active versions has [ef2, et2) and [et2, et1)
     */
    @Test
    public void test__ef2_lt_ef1_lt_et2_lt_et1() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo1 = Date.from(Instant.parse("3999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));

        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveTo2, new Date());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }
    
    /**
     * ef2<et2=ef1<et1
     *
     * expect to have two active versions has [ef2, et2) and [ef1, et1)
     */
    @Test
    public void test__ef2_lt_et2_eq_ef1_lt_et1() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

    /**
     * ef2<et2<ef1<et1
     *
     * expect to have two active versions has [ef2, et2) and [ef1, et1)
     */
    @Test
    public void test__ef2_lt_et2_lt_ef1_lt_et1() {
        BiTemporalDataItemEntityHistory history = null;
        DataItemEntityChange change = null;

        Date effectiveFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date effectiveFrom2 = Date.from(Instant.parse("2017-01-01T00:00:00.000Z"));
        Date effectiveTo1 = Date.from(Instant.parse("2999-01-01T00:00:00.000Z"));
        Date effectiveTo2 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));

        Date systemFrom1 = Date.from(Instant.parse("2018-01-01T00:00:00.000Z"));
        Date systemFrom2 = Date.from(Instant.parse("2019-01-01T00:00:00.000Z"));
        String path = "pro-1";
        String value1 = "value1";
        String value2 = value1;

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

        DataItemEntity dataItemEntity = history.getStateAt(new Date(), new Date());

        System.out.println("\n***** dataItemEntity now: " + dataItemEntity);

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom2, new Date());

        TestCase.assertEquals(effectiveFrom2, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo2, dataItemEntity.getTemporalState().getEffectiveTo().get());

        dataItemEntity = history.getStateAt(effectiveFrom1, new Date());

        TestCase.assertEquals(effectiveFrom1, dataItemEntity.getTemporalState().getEffectiveFrom().get());

        TestCase.assertEquals(effectiveTo1, dataItemEntity.getTemporalState().getEffectiveTo().get());

    }

}
