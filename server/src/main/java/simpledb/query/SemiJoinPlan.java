/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import java.util.Collection;
import simpledb.record.Schema;

/**
 *
 * @author Ashleigh
 */
public class SemiJoinPlan implements Plan{
    private Plan p1, p2;
    private Predicate pred;
    private Schema schema = new Schema();
    
    public SemiJoinPlan(Plan p1, Plan p2, Predicate pred) {
        this.p1 = p1;
        this.p2 = p2;
        this.pred = pred;
        this.schema.addAll(p1.schema());
    }
    
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        Collection<String> fields = p1.schema().fields();
        return new SemiJoinScan(s1, s2, this.pred, fields);
    }
    
    public int blocksAccessed() {
        Plan p = new ProductPlan(p1, p2);
        return p.blocksAccessed();
    }
    
    public int recordsOutput() {
        Plan p = new ProductPlan(p1, p2);
        return p.recordsOutput() / pred.reductionFactor(p);
    }
    
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname)) {
            return p1.distinctValues(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    
    public Schema schema() {
        return this.schema;
    }
}
