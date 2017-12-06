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
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.parse.Parser;

/**
 *
 * @author Ashleigh
 */
public class JoinScanTest {
    
    public JoinScanTest() {
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
    public void testJoinByConstantScan() {
//    System.out.println("SELECT");
//    Transaction tx = new Transaction();
//    String qry = "select sname from student join dept on majorid = did";
//    Parser p = new Parser(qry);
//    Plan studentTblPlan = new TablePlan("student", tx);
//    Plan deptTblPlan = new TablePlan("dept", tx);
//    Plan joinPlan = new JoinPlan(studentTblPlan, deptTblPlan,
//            new Predicate(new Term(
//            new FieldNameExpression("majorid"),
//            new FieldNameExpression("did"))));
//    Scan s = p.open();
//    tx.commit();
//    assertEquals(9, p.recordsOutput());
    }
    
}
