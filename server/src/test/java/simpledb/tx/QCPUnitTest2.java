/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author Ashleigh
 */
public class QCPUnitTest2 {
    public static void main(String[] args) {
        SimpleDB.init("testdb");
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
    }
}
