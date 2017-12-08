/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import simpledb.record.Schema;

/**
 *
 * @author Ashleigh
 */
public class SemiJoinPlan implements Plan{
    public SemiJoinPlan() {}
    public Scan open() {}
    public int blocksAccessed() {}
    public int recordsOutput() {}
    public int distinctValues(String fldname) {}
    public Schema schema() {}
}
