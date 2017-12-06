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
    
    public int blocksAccessed() {
        Plan p = new ProductPlan(p1, p2);
      return p.blocksAccessed();
   }
    
    public int recordsOutput() {
      Plan p = new ProductPlan(p1, p2);
      return p.recordsOutput() / pred.reductionFactor(p);
   }
    
    public int distinctValues(String fldname) {
      Plan p = new ProductPlan(p1, p2);
      if (pred.equatesWithConstant(fldname) != null)
         return 1;
      else {
         String fldname2 = pred.equatesWithField(fldname);
         if (fldname2 != null) 
            return Math.min(p.distinctValues(fldname),
                            p.distinctValues(fldname2));
         else
            return Math.min(p.distinctValues(fldname),
                            recordsOutput());
      }
   }
}
