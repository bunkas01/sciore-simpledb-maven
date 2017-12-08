/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author Ashleigh
 */
public class RenameTest {
    
    public RenameTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        SimpleDB.init("studentdb");
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of open method, of class RenamePlan.
     */
    @Test
    public void TestRename() {
        System.out.println("RENAME");
        Transaction tx = new Transaction();
        Plan deptTblPlan = new TablePlan("dept", tx);
        tx.commit();
        Plan rPlan = new RenamePlan(deptTblPlan, "did", "dept_id");
        Scan rScan = rPlan.open();
        System.out.println(rPlan.schema().fields());
        assertEquals(true, rScan.hasField("dept_id"));
    }
}