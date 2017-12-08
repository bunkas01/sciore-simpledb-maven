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
public class RenameScan implements Scan {
    private Scan s;
    private Collection<String> fieldlist;
    private String oldName, newName;
    
    public RenameScan(Scan s, Collection<String> fieldlist,
                      String old, String newName) {
        this.s = s;
        this.fieldlist = fieldlist;
        this.oldName = old;
        this.newName = newName;
    }
    
    public void beforeFirst() {
        s.beforeFirst();
    }
    
    public boolean next() {
        return s.next();
    }
    
    public void close() {
        s.close();
    }
    
    public Constant getVal(String fldname) {
        if (hasField(fldname)) {
            if (fldname == newName) {
                return s.getVal(oldName);
            } else {
                return s.getVal(fldname);
            }
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    
    public int getInt(String fldname) {
        if (hasField(fldname)) {
            if (fldname == newName) {
                return s.getInt(oldName);
            } else {
                return s.getInt(fldname);
            }
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    
    public String getString(String fldname) {
        if (hasField(fldname)) {
            if (fldname == newName) {
                return s.getString(oldName);
            } else {
                return s.getString(fldname);
            }
        } else {
            throw new RuntimeException("field " + fldname + " not found.");
        }
    }
    
    public boolean hasField(String fldname) {
        return fieldlist.contains(fldname);
    }
}
