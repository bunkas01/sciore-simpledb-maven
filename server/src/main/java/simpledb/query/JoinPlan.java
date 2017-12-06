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
public class JoinPlan implements Plan {
    private Plan p1, p2;
    private Schema schema = new Schema();
    private Predicate pred;
    
    public JoinPlan(Plan p1, Plan p2, Predicate pred) {
        this.p1 = p1;
        this.p2 = p2;
        this.pred = pred;
        this.schema.addAll(p1.schema());
        this.schema.addAll(p2.schema());
    }
    
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new JoinScan(s1, s2, this.pred);
    }
    
    public Schema schema() {
        return schema;
    }
}
