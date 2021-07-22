/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.refinitiv.edp.cnm.orca.cdf.utils;

import com.tr.cdf.datamodel.level2.core.BiTemporalDataItemEntityHistory;
import com.tr.cdf.datamodel.level2.core.ChangeOperation;
import com.tr.cdf.datamodel.level2.core.DataItemEntity;
import com.tr.cdf.datamodel.level2.core.DataItemPath;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

/**
 *
 * @author Binglin Yu
 */
public class CDFHistoryUtils {
    
    /**
     * Determine whether the history consists any drafted changes or not.
     * 
     * @param history
     * @return 
     */
    public boolean hasDrafts(BiTemporalDataItemEntityHistory history) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * Discard drafted changes.
     * 
     * !!!!! Not correct to set to systemTo only. !!!!
     */
    public BiTemporalDataItemEntityHistory discardDrafts(
            BiTemporalDataItemEntityHistory history) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * promote drafted changes
     * @param history
     */
    public BiTemporalDataItemEntityHistory promoteDrafts(
            BiTemporalDataItemEntityHistory history) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * calculate business history for the given data item path, in sequence which match the given dataItemPath.
     * @param history full biTemporal history
     * @param dataItemPath specified data item path 
     * @param includeDrafts whether to include drafted changes into calculation or not
     * @param systemDate system date
     * @return 
     */
    public List<List<DataItemEntity>> calculateBusinessHistory(
            BiTemporalDataItemEntityHistory history,
            List<DataItemPath> dataItemPath,
            boolean includeDrafts,
            Date systemDate
            ) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * calculate business history for all potential paths.
     * 
     * Call {@link #calculateBusinessHistory(com.tr.cdf.datamodel.level2.core.BiTemporalDataItemEntityHistory, java.util.List, boolean, java.util.Date) }
     * to calculate snapshots without empty items
     * 
     * @param history
     * @param includeDrafts
     * @param systemDate
     * @return 
     */
    public List<DataItemEntity> calculateBusinessHistory(
            BiTemporalDataItemEntityHistory history,
            boolean includeDrafts,
            Date systemDate ) {
        
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * get change logs
     */
    public Map<DataItemPath, List<CDFChange>> getChangeLogs() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * @return changeOperation (without temporal state info)
     */
    public ChangeOperation buildChangeOperation(CDFChange change) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
    /**
     * Similar to BiTemporalValue used in ORCA+
     */
    public static interface CDFChange {
        public DataItemPath getDataItemPath();
        /**
         * if null, indicate remove operation;
        **/
        public Object getValue();
        public boolean isDrafted();
        public JSONObject getProvenance();
        /**
         * if true, indicate the change is only appending provenance without data change 
         * (i.e. getValue() is meaningless)
         */
        public boolean hasNoValueChange();
    } 
}
