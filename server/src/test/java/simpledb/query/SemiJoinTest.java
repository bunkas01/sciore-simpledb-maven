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
public class SemiJoinTest {
    
    public SemiJoinTest() {
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

    @Test
    public void testSemiJoin() {
        System.out.println("SEMIJOIN");
        Transaction tx = new Transaction();
        Plan studentTblPlan = new TablePlan("student", tx);
        Plan deptTblPlan = new TablePlan("dept", tx);
        tx.commit();
        Plan semiJoinPlan = new SemiJoinPlan(studentTblPlan, deptTblPlan,
            new Predicate(
                    new Term(
                      new FieldNameExpression("majorid"),
                      new FieldNameExpression("did"))));
        Scan semiJoinScan = semiJoinPlan.open();
        int records = 0;
        while (semiJoinScan.next()) {
          for (String field: semiJoinPlan.schema().fields()) {
            System.out.printf("%10s", semiJoinScan.getVal(field).toString());
          }
          System.out.println();
          records++;
        }
        assertEquals(9, records);
        assertEquals(false, semiJoinScan.hasField("did"));
        assertEquals(false, semiJoinScan.hasField("dname"));
    }
}
