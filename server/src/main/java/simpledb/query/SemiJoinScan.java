/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import java.util.Collection;

/**
 *
 * @author Ashleigh
 */
public class SemiJoinScan implements Scan{
    private Scan prod;
    private Predicate pred;
    private Collection<String> fields;
    
    public SemiJoinScan(Scan s1, Scan s2, Predicate pred,
                        Collection<String> fieldlist) {
        this.prod = new ProductScan(s1, s2);
        this.pred = pred;
        this.fields = fieldlist;
    }
    
    public void beforeFirst() {
        prod.beforeFirst();
    }
    
    public boolean next() {
        while (prod.next())
            if (pred.isSatisfied(prod))
                return true;
        return false;
    }
    
    public void close() {
        prod.close();
    }
    
    public Constant getVal(String fldname) {
        if (hasField(fldname)) {
            return prod.getVal(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    public int getInt(String fldname) {
        if (hasField(fldname)) {
            return prod.getInt(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    
    public String getString(String fldname) {
        if (hasField(fldname)) {
            return prod.getString(fldname);
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    
    public boolean hasField(String fldname) {
        if (fields.contains(fldname)) {
            return true;
        } else {
            return false;
        }
    }
}
