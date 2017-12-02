package simpledb.buffer;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int strategy;
   private int time;  // Tracks the time at which pins/unpins occur.
   private int[] timeIn;  // Tracks the time at which the buffer at the same
   // index was pinned.
   private int[] timeOut; // Tracks the time at which the buffer at the same
   //index was unpinned.
   private int reference; // The most recently referenced Buffer, for use by the
   // clock strategy of Buffer replacement.

   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      time = 0;
      bufferpool = new Buffer[numbuffs];
      timeIn = new int[numbuffs];  // initialising timeIn array.
      timeOut = new int[numbuffs];  // initialising timeOut array.
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   synchronized public void completeFlush() {
       for (Buffer buff : bufferpool) {
           buff.flush();
       }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
        time++;
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null)
                return null;
            buff.assignToBlock(blk);
        }
      if (!buff.isPinned())
        numAvailable--;
      int i;
      for (i=0; i<bufferpool.length; i++) {
          if (buff == bufferpool[i])
              break;
      }
      timeIn[i] = time;
      reference = i;
      timeOut[i] = Integer.MAX_VALUE;  // Deals with a buffer being mistakenly
      // picked by LRU when timeOut hasn't yet been reset.
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      int i;
      for (i=0; i<bufferpool.length; i++) {
          if (buff == bufferpool[i])
              break;
      }
      timeIn[i] = time;
      reference = i;
      timeOut[i] = Integer.MAX_VALUE;  // Ensures LRU doesn't mistakenly pick
      // a buffer where timeout was inherited from an old one.
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      time++;
      int i;
      for (i=0; i<bufferpool.length; i++) {
          if (buff == bufferpool[i])
              break;
      }
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         timeOut[i] = time;  // Sets the correct timeout for the corresponding
         // buffer when it is fully unpinned.
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }
   
   private Buffer chooseUnpinnedBuffer() {
     switch (this.strategy) {
      case 0:
       return useNaiveStrategy();
      case 1:
        return useFIFOStrategy();
      case 2:
        return useLRUStrategy();
      case 3:
        return useClockStrategy();
      default:
        return null;
     }
   }
   /**
    * @return Allocated buffers
    */
   public Buffer[] getBuffers() {
     return this.bufferpool;
   }
   /**
    * Set buffer selection strategy
    * @param s (0 - Naive, 1 - FIFO, 2 - LRU, 3 - Clock)
    */
   public void setStrategy(int s) {
     this.strategy = s;
   }
   /**
    * Naive buffer selection strategy
    * @return
    */
   private Buffer useNaiveStrategy() {
      for (Buffer buff : bufferpool)
         if (!buff.isPinned())
            return buff;

      return null;
   }
   /**
    * FIFO buffer selection strategy
    * 
    * The first buffer is initially assumed to be the least recently pinned.
    * This is iteratively tested, with the index of the lowest entry of timeIn
    * saved as the index of the least recently pinned buffer. This is then used
    * to extract that buffer from the bufferpool, as the indices correspond.
    * finally, the buffer is double-checked to ensure that it isn't currently
    * pinned.
    * 
    * @return 
    */
   private Buffer useFIFOStrategy() {
        int first = timeIn[0];
        int firstDex = 0;
        for (int i=1; i<timeIn.length; i++) {
            if (timeIn[i] < first & !bufferpool[i].isPinned()) {
                first = timeIn[i];
                firstDex = i;
            }
        }
        Buffer buff = bufferpool[firstDex];
        if (!buff.isPinned()) {
            return buff;
        }
        return null;
   }
   /**
    * LRU buffer selection strategy
    * 
    * The first buffer is initially assumed to be the least recently unpinned.
    * This is iterativley tested, with the index of the lowest entry of timeOut
    * saved as the index of the least recently unpinned corresponding buffer.
    * A final double check that the corresponding buffer is unpinned verifies
    * that it is an acceptable return value.
    * 
    * @return 
    */
   private Buffer useLRUStrategy() {
      int first = timeOut[0];
      int firstDex = 0;
      for (int i=1; i<timeOut.length; i++) {
          if (timeOut[i] < first & !bufferpool[i].isPinned()) {
              first = timeOut[i];
              firstDex = i;
          }
      }
      Buffer buff = bufferpool[firstDex];
      if (!buff.isPinned()) {
          return buff;
      }
      return null;
   }
   /**
    * Clock buffer selection strategy
    * 
    * The reference int serves to denote the most recently referenced page, as
    * determined based on the most recent pinning of a buffer. The iterator of
    * possible buffers then goes through the bufferpool in circular fashion
    * looking for the first non-pinned buffer, stopping at the original
    * reference position.
    * 
    * @return 
    */
   private Buffer useClockStrategy() {
      Buffer buff;
      int i = reference+1;
      for (int c = 0; c<bufferpool.length; c++) {
          buff = bufferpool[i];
          if (!buff.isPinned()) {
              return buff;
          }
          i++;
          if (i>3)
              i=0;
      }
      return null;
   }
}
