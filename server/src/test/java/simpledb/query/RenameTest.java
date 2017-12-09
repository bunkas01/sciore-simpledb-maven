/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import java.util.ArrayList;
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
        ArrayList<String> fields = new ArrayList();
        fields.add("did");
        fields.add("dname");
        Plan pPlan = new ProjectPlan(deptTblPlan, fields);
        Scan pScan = pPlan.open();
        System.out.println("Old fields");
        System.out.println(pPlan.schema().fields());
        System.out.println("Renamed Fields");
        System.out.println(rPlan.schema().fields());
        assertEquals(true, rScan.hasField("dept_id"));
        assertEquals(false, pScan.hasField("dept_id"));
        int pRecords = 0;
        int rRecords = 0;
        while (pScan.next()) {
            pRecords++;
        }
        while(rScan.next()) {
            rRecords++;
        }
        assertEquals(pRecords, rRecords);
    }
}