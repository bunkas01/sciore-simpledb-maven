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
public class RenamePlan implements Plan {
    private Plan p;
    private Schema schema = new Schema();
    private String oldName, newName;
    private Collection<String> oldFields;
    
    public RenamePlan(Plan p, String oldName, String newName) {
        this.p = p;
        Schema old = p.schema();
        Collection<String> fieldlist = old.fields();
        oldFields = fieldlist;
        for (String fldname : fieldlist) {
            if (fldname == oldName) {
                schema.addField(newName, old.type(oldName), old.length(oldName));
            } else {
                schema.add(fldname, old);
            }
        }
        this.oldName = oldName;
        this.newName = newName;
    }
    
    public Scan open() {
        Scan s = p.open();
        return new RenameScan(s, schema.fields(), oldName, newName);
    }
    
    public int blocksAccessed() {
        return p.blocksAccessed();
    }
    
    public int recordsOutput() {
        return p.recordsOutput();
    }
    
    public int distinctValues(String fldname) {
        if (fldname == newName) {
            return p.distinctValues(oldName);
        } else {
            return p.distinctValues(fldname);
        }
    }
    
    public Schema schema() {
        return schema;
    }
}
