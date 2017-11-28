/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx.recovery;

import java.util.logging.Level;
import java.util.logging.Logger;
import simpledb.tx.Transaction;
import simpledb.server.SimpleDB;

/**
 *
 * @author Ashleigh
 */
public class CheckpointThread implements Runnable{
    public static Object cLock = new Object();
    public static Boolean inProgress = new Boolean(true);
    
    public CheckpointThread() {}
    
    public void run() {
        while (!Transaction.running.isEmpty()) {
            try {
                cLock.wait();
            } catch (InterruptedException ex) {
                Logger.getLogger(CheckpointThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        // Start actual recovery process
        SimpleDB.bufferMgr().completeFlush();
    }
}
