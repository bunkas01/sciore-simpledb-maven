/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.file.Block;

/**
 *
 * @author Ashleigh
 */
public class QCPUnitTest1 {
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
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
        new Thread(new tThread()).start();
    }
}

class tThread implements Runnable {
    public void run() {
        try {
            Transaction tx = new Transaction();
            Block blk1 = new Block("junk", 1);
            Block blk2 = new Block("junk", 2);
            tx.pin(blk1);
            tx.pin(blk2);
            Thread.sleep(10000);
            tx.commit();
        } catch(InterruptedException e) {}
    }
}

