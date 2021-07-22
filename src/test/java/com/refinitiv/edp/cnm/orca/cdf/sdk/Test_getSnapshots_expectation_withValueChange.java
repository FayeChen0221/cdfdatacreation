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
import java.util.ArrayList;
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
 * <li>ef1<et1<ef2<et2 (detached) </li> [done]
 * <li>ef1<et1=ef2<et2 (detached) </li> [done]
 * <li>ef1<ef2<et1<et2 </li> [done]
 * <li>ef1<ef2<et1=et2 </li> [done]
 * <li>ef1<ef2<et2<et1 </li> [done]
 *
 *
 * <li>ef1=ef2<et1<et2 </li> [done]
 * <li>ef1=ef2<et1=et2 </li> [done]
 * <li>ef1=ef2<et2<et1 </li> [done]
 *
 * <li>ef2<ef1<et1<et2 </li> [done]
 * <li>ef2<ef1<et1=et2 </li> [done]
 * <li>ef2<ef1<et2<et1 </li> [done]
 * <li>ef2<et2=ef1<et1 (detached) </li> [done]
 * <li>ef2<et2<ef1<et1 (detached) </li> [done]
 *
 * </ul>
 *
 * !! failed ones !!
 * <ul>
 * <li>test__ef1_eq_ef2_lt_et2_lt_et1</li>
 * <li>test__ef1_lt_ef2_lt_et1_eq_et2</li>
 * <li>test__ef1_lt_ef2_lt_et2_lt_et1</li>
 * </ul>
 *
 * @author Binglin Yu
 */
public class Test_getSnapshots_expectation_withValueChange {

    String path = "pro-1";
    String value1 = "value1";
    String value2 = value1 + "." + System.nanoTime();

    /**
     *
     * ef1=ef2<et1<et2
     *
     * Correct Operation: add new
     *
     * expect to have only one active version with [effectiveFrom2,
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change: add new
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     * ef1=ef2<et1=et2
     *
     * Correct Operation: add new only
     *
     * expect to have one active version has [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     *
     * ef1=ef2<et2<et1
     *
     * Correct Operation: remove the old, and add new
     *
     * expect to have one active version with [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change
        // 1. remove the old one
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveTo2, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // 2. add new one
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     * ef1<et1<ef2<et2 (detached)
     *
     * Correct Operation: remove the old and add the new ones
     *
     * expect to have one active version has [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build the old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build the correct operations
        // 1. remove old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);
        // 2. add new
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     * ef1<et1=ef2<et2 (detached)
     *
     * Correct Operation: remove old and add new
     *
     * expect to have only one active version with [effectiveFrom2,
     * effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build the old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build the new changes
        // 1. remove old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // 2. add new
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     * ef1<ef2<et1<et2
     *
     * Correct Operation: remove [ef1, ef2) and add new
     *
     * expect to have only active version with [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old version
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new version
        // 1. remove old 
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveFrom2, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());
    }

    /**
     * ef1<ef2<et1=et2
     *
     * Correct Operation: remove [ef1, ef2) and add new
     *
     * expect to have one active version with [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change
        // 1. remove old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveFrom2, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // add new 
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     * ef1<ef2<et2<et1
     *
     * Correct Operation: remove two parts of old, and add new
     *
     * expect to have one active version with [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new changes
        // 1. remove two pieces of old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveFrom2, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveTo2, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // 2. add new
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom2, effectiveTo2, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value2)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        historyBuilder.add(change);

        history = historyBuilder.build();

        List<DataItemEntity> dataItemEntityList = HistoryUtils.getAllSnapshotsForPath(
                history.getChanges(), DataItemPathFactory.create(path));

        // output for checking
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     *
     * ef2<ef1<et1<et2
     *
     * Correct Operation: add new only
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());
    }

    /**
     *
     * ef2<ef1<et1=et2
     *
     * Correct Operation: add new
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());
    }

    /**
     * ef2<ef1<et2<et1
     *
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();
        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change
        // 1. remove old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveTo2, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // 2. add new 
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());
    }

    /**
     * ef2<et2=ef1<et1
     *
     * Correct Operation: remove old and add new
     *
     * expect to have one active version with [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new changes
        // 1. remove old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // 2. add new
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

    /**
     * ef2<et2<ef1<et1
     *
     * Correct Operation: remove old and add new
     *
     * expect to have one active version with [effectiveFrom2, effectiveTo2)
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

        Level2Builders.HistoryBuilder historyBuilder = Level2Builders.history();

        // build old change
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom1, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                        .setValue(value1)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // build new change
        // 1. remove old
        change = DataItemEntityChangeFactory.create();
        change.setTemporalState(TemporalStateFactory.create(effectiveFrom1, effectiveTo1, systemFrom2, null));
        change.getChangeOperations().addAll(Level2Builders.dataItem()
                .appendChild(Level2Builders.dataItem()
                        .setDataItemTypeIdentifier(path)
                ).buildValueSettingOperations());

        historyBuilder.add(change);

        // 2. add new
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
        List<DataItemEntity> actives = new ArrayList<>();
        for (DataItemEntity tmp : dataItemEntityList) {
            System.out.println("\n***** dataItemEntity: " + tmp);
            if (tmp.getTemporalState().getSystemTo() == null) {
                actives.add(tmp);
            }
        }

        TestCase.assertEquals(1, actives.size());

        TestCase.assertEquals(effectiveFrom2, actives.get(0).getTemporalState().getEffectiveFrom().get());
        TestCase.assertEquals(effectiveTo2, actives.get(0).getTemporalState().getEffectiveTo().get());

        TestCase.assertEquals(value2, actives.get(0).getByPath(path).getValue().toString());

    }

}
