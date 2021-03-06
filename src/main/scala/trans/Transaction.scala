
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller and Nick Klepp
  *  @version 1.1
  *  @date    Tue Jan 10 14:34:43 EST 2017
  *  @see     LICENSE (MIT style license file).
  *------------------------------------------------------------------------------
  */

package trans

import Operation._

import scala.collection.mutable.{ArrayBuffer, Map, Set}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.Breaks._
import java.util.concurrent.TimeUnit
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` companion object
  */
object Transaction
{
    private var count = -1

    def nextCount () = { count += 1; count }

    VDB.initCache ()

} // Transaction object

import Transaction._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
  *  @param sch  the schedule/order of operations for this transaction.
  */
class Transaction (sch: Schedule, concurrency: Int =0) extends Thread
{
    private val CLAIRVOYANCE = true
    private val DEBUG       = false					// debug flag
    private var tid         = nextCount ()        		    		// tarnsaction identifier
    private var rwSet       = Map[Int, Array[Int]]()		    		// the read write set : [oid, (num_reads, num_writes)]
    private var readLocks   = Map [Int, ReentrantReadWriteLock.ReadLock ]()	// (oid -> readLock)  read locks we haven't unlocked yet
    private var writeLocks  = Map [Int, ReentrantReadWriteLock.WriteLock]() 	// (oid -> writeLock) write locks we haven't unlocked yet 
    private val READ        = 0
    private val WRITE       = 1
    private val _2PL        = 1
    private val TSO         = 0
    private var ROLLBACK    = true
    private val debugSynch  = false
    private var numIgnores  = 0
    private var waitMult    = 80.0
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
      */
    override def run ()
    {
	while(ROLLBACK){						//note: ROLLBACK defaults to true so that we can initialize the running while loop
	    ROLLBACK = false
	    begin()
	    breakable{
		for (i <- sch.indices) {
		    //if(DEBUG) println(s"$tid starting op at sch index $i")
		    val (op, tid_1, oid) = sch(i)
		    val opString = new String(s"($op,$tid,$oid)")
		    //if(CLAIRVOYANCE)println(s"($op, $tid, $oid)")
            	    if (op == r) read (oid)
            	    //else         write (oid, VDB.str2record (sch(i).toString))
		    else 	 write(oid, VDB.str2record (opString))  
	    	    if(ROLLBACK) break
        	} // for
	    } // breakable
	} // while
        commit ()
    } // run


    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'. Redirect to different concurrency by ConcurrencyFlag setting
      *  @param oid  the object/record being read
      */
    def read (oid: Int): VDB.Record =
    {
        if (concurrency == TSO) readTSO2(oid)
        else read2PL(oid)
    }
    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'. Redirect to different concurrency control by ConcurrencyFlag setting
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write (oid:Int, value:VDB.Record  )
    {
        if (concurrency == TSO ) writeTSO2(oid, value)
        else write2PL (oid, value)
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Begin this transaction.
      */
    def begin ()=
    {
	import scala.util.Random
	Thread.sleep( 20 + Random.nextInt(waitMult.toInt) )
        if(concurrency == _2PL) fillReadWriteSet()
        VDB.begin (tid)
    } // begin

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Commit this transaction.
      */
    def commit ()
    {
	VDB.synchronized{
	    VDB.commit (tid)
	    releaseReadLocks()
	    releaseWriteLocks()
	    WaitsForGraph.removeNode(tid)
	    VDB.ignoredWrites += numIgnores
	    if( DEBUG ) println(s"$tid committed from transaction")
	    if( DEBUG ) println(s"Lock table after $tid commit: $LockTable")
	} // synch
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
      */
    def rollback ()
    {
	import java.lang.Math.floor
	VDB.synchronized{
	    if( DEBUG ) println(s"rollback($tid)")
	    ROLLBACK = true
            VDB.rollback (tid)
            releaseReadLocks()
            releaseWriteLocks()
	    emptyReadWriteSet()
	    tid = nextCount()
	    numIgnores = 0
	    if(concurrency == TSO) waitMult = floor(waitMult * 1.1)
	}
    } // rollback
    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Fills the read/write set for this transaction.
      */
    def fillReadWriteSet()
    {
	for(i <- sch.indices){
	      val (op,tid,oid) = sch(i)
	      if (rwSet contains oid){
	      	 if( op == r ) rwSet(oid)(READ)  += 1		//increment the read value for this object in the readWriteSet
		 else          rwSet(oid)(WRITE) += 1		//increment the write value
	      } // if
	      else{
		var tup = Array.fill(2)(0)				//add a new member to the read write set
		if( op == r ) tup(READ) = 1				//make it a read member
		else          tup(WRITE) = 1				//make it a write member
		
		rwSet += (oid -> tup) 
	      } // else
	}// for
    } // fillReadWriteSet

    def emptyReadWriteSet()
    {
	for(oid <- rwSet.keys) {
	    rwSet(oid)(READ)  = 0
	    rwSet(oid)(WRITE) = 0
	}
	
    }

    //::
    /**
     */
    def read2PL(oid: Int): VDB.Record =
    {

	var rec        	= Array.ofDim[Byte](128)
	var futureWrite = (rwSet contains oid) && rwSet(oid)(WRITE) > 0
	var locked = true
	if ( futureWrite ){
	   if( DEBUG ) println(s"$tid wants to writeLock $oid for reading/future write.")
	   locked = writeLockObj( oid )
	} // if
	else{
	    if( DEBUG ) println(s"$tid wants to readLock $oid")
	    locked = readLockObj( oid )
	} // else
	if (locked) {
	   houseKeeping(oid, READ)
	   rec = VDB.read(tid,oid)._1
	}
	else rollback()
	rec
    }


    def write2PL (oid: Int, value: VDB.Record)
    {

	if( DEBUG ) println(s"$tid wants to writeLock $oid")
	val locked = writeLockObj(oid)
	if( locked ){
		houseKeeping(oid,WRITE)
		VDB.write (tid,oid, value)
	} // if
	else rollback()
    }

    //::
    /**
     */
    def houseKeeping(oid: Int, readOrWrite: Int)
    {
	rwSet(oid)(readOrWrite) -= 1
	if( (rwSet(oid)(READ) == 0) && (rwSet(oid)(WRITE)==0) ) rwSet -= oid 
    }

    //::
    /**/
    def writeLockObj(oid: Int): Boolean =
    {
	//Thread.sleep(100)
	if(DEBUG){
	    println(s"readLocks for $tid entering writeLockObj: ")
	    for(oid <- readLocks.keys) print(s"$tid,$oid ")
	    println("")

	    println(s"writeLocks for $tid entering writeLockObj: ")
	    for(oid <- writeLocks.keys) print(s"$tid,$oid ")
	    println("")
	}
	var locked        = false
	val lock          = LockTable.getObjLock(oid)
	val writeLock     = lock.writeLock()
	val alreadyLocked = (writeLocks contains oid)
	if (debugSynch) println(s"$tid is entering locktable synch block in writeLockObj")
	LockTable.synchronized{
	    if(debugSynch) println(s"$tid entered LockTable synch block in writeLockObj")
	    val writeLocked = lock.isWriteLocked()
	    val noReaders   = lock.getReadLockCount() == 0
	    val noWait 	= noReaders && !writeLocked
	    if( noWait ){
		if( lock.isWriteLocked() ) println(s"MISTAKE : we say $oid is free to xlock but lock says it is writelocked")
		else if( !noReaders ) println(s"MISTAKE we say $oid is free to xlock but lock says it has readers")
		LockTable.addOwner(oid,tid)
		writeLock.lock()
		writeLocks += (oid -> writeLock)
		locked = true
		if( DEBUG ) println(s"$tid got to writeLock $oid b/c it had no owners.")
	    } // if
	    else if( writeLock.isHeldByCurrentThread() ){
		 if( DEBUG ) println(s"$tid already holds writeLock for $oid, don't lock again.")
		 locked = true
	    } // else if	
	} // synch
	if(debugSynch) println(s"$tid exited LockTable synch block in writeLockObj")
	if( !locked ){
												// get here means you can't lock without waiting
	    val noDeadlock = WaitsForGraph.checkForDeadlocks(tid, oid, lock, WRITE)		   // check to make sure waiting won't cause a deadlock
	    if( noDeadlock ){
	    	if( DEBUG ) println(s"$tid is waiting to reenter writeLockObj for $oid outside of synch obj synch block")
		LockTable.addWaiter(oid,tid)
		writeLock.lock()
		writeLock.unlock()
		LockTable.removeWaiter(oid, tid)
		locked = writeLockObj(oid)
	    } // if no deadLock
	    else if( DEBUG ) println(s"$tid not allowed to wait for lock for $oid b/c of deadlock.")
	} // if
	if(locked && CLAIRVOYANCE) println(s"writelock($tid,$oid)")
	if(DEBUG){
	    println(s"readLocks for $tid leaving writeLockObj: ")
	    for(oid <- readLocks.keys) print(s"$tid,$oid ")
	    println("")

	    println(s"writeLocks for $tid leaving writeLockObj: ")
	    for(oid <- writeLocks.keys) print(s"$tid,$oid ")
	    println("")
	}
	locked
    } // writeLockObj
    

    def readLockObj(oid: Int): Boolean = 
    {
	Thread.sleep(100)
	if(DEBUG){
	    println(s"readLocks for $tid entering writeLockObj: ")
	    for(oid <- readLocks.keys) print(s"$tid,$oid ")
	    println("")

	    println(s"writeLocks for $tid entering writeLockObj: ")
	    for(oid <- writeLocks.keys) print(s"$tid,$oid ")
	    println("")
	}
	var locked = false
	val lock = LockTable.getObjLock(oid)
	val readLock = lock.readLock()
    	if(debugSynch) println(s"$tid entering LOckTable synch block in readLockObj")
	LockTable.synchronized{
		if(debugSynch) println(s"$tid entered LockTable synch block in readLockObj")
		val writeLocked = lock.isWriteLocked()
		val noOwners = lock.getReadLockCount() == 0
		val alreadyReadLocked  =  readLocks contains oid
	     	val alreadyWriteLocked = writeLocks contains oid
		val alreadyLocked      = alreadyReadLocked || alreadyWriteLocked
		if( alreadyLocked )										//don't lock something you've already locked
		{
		    if( DEBUG ) println(s"tid already owns the writeLock for $oid, so go ahead and read.")
		    locked = true
		}
		else if( noOwners && !writeLocked){								//nobody owns the lock
		    if( lock.getReadLockCount() > 0 ) println(s"MISTAKE: $tid locking $oid bc we say it is unlocked but lock says it has readers.")
		    if( lock.isWriteLocked() )	println(s"MISTAKE: $tid locking $oid b/c we say it is unlocked but lock says it is writeLocked")
		    LockTable.addOwner(oid, tid)
		    readLock.tryLock()
		    readLocks += (oid -> readLock)
		    locked = true
		    if( DEBUG ) println(s"$tid got to readLockin $oid because it has no owners")
		} // if
		else if( !writeLocked ){
		     if( DEBUG ) println(s"$tid wants to share lock $oid")
		     readLock.tryLock() 
		     if( DEBUG ) println(s"$tid is readlocked on $oid")
		     LockTable.addOwner(oid,tid)
		     LockTable.updateWaiters(oid,tid)
		     readLocks += (oid -> readLock)
		     if( DEBUG ) println(s"$tid got to readLock $oid because it is shared locked.")	//we want to share in the locking
		     locked = true
		} // else if
	} // synchronized
	if(debugSynch) println(s"$tid exited LockTable synch block in readLockObj")
	if(!locked && !ROLLBACK){
												// get here means can't get lock without waiting
	    val noDeadlock = WaitsForGraph.checkForDeadlocks(tid, oid, lock, READ)		   // check to make sure waiting won't cause a deadlock
	    if( noDeadlock ){
	    	if( DEBUG ) println(s"$tid is waiting to reenter readLockObj for $oid outside of a synchronized block.")
		LockTable.addWaiter(oid, tid)
	    	readLock.lock() 
		readLock.unlock()
		LockTable.removeWaiter(oid,tid)
		locked = readLockObj(oid)
	    } // if
	    else if( DEBUG ) println(s"$tid not allowed to wait to readLock $oid b/c of deadlock.")
	} // if
	if(locked && CLAIRVOYANCE) println(s"readlocked($tid,$oid)")
	if(DEBUG){
	    println(s"readLocks for $tid leaving writeLockObj: ")
	    for(oid <- readLocks.keys) print(s"$tid,$oid ")
	    println("")

	    println(s"writeLocks for $tid leaving writeLockObj: ")
	    for(oid <- writeLocks.keys) print(s"$tid,$oid ")
	    println("")
	}
	locked
    } // readLockObj
    


    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      * Add Strict Timestamp Ordering
      *  @param oid  the object/record being read
      */
    def readTSO (oid: Int): VDB.Record =
    {
	var rec		= Array.ofDim[Byte](128)
	val writeTS 	= TSTable.writeTS(oid)
	val readTS	= TSTable.readTS(oid)
	if( DEBUG ) println(s"$tid wants to read $oid with readTS: $readTS and writeTS: $writeTS")
        if (writeTS <= tid){								// check if write_TS(X)<=TS(T), then we get to try to read  
	   rec = VDB.read(tid,oid)._1
	   if(readTS < tid) TSTable.readStamp(tid,oid)
	   if( DEBUG ) println(s"$tid got to read $oid. New read stamp: ${TSTable.readTS(oid)}")
	} // if
	else rollback()
	rec
    } // readTSO

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      * Add Strict Time Stamp Ordering
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def writeTSO (oid: Int, value: VDB.Record)
    {
	val readTS  = TSTable.readTS(oid) 
	val writeTS = TSTable.writeTS(oid)
      	if( DEBUG ) println(s"$tid wants to write $oid with readTS: $readTS and writeTS: $writeTS")
  	if( readTS > tid) rollback()
	else if( writeTS > tid) {									//Thomas's write rule
	    numIgnores += 1
	    if( DEBUG) println(s"$tid invoked Thomas's write rule to ignore a write operation.")
	}
	else {
	    VDB.write(tid,oid,value)
	    TSTable.writeStamp(tid,oid)
	    if( DEBUG ) println(s"$tid got to write $oid. New write stamp: ${TSTable.writeTS(oid)}") 
	}
    } // writeTSO

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      * Add Strict Timestamp Ordering
      *  @param oid  the object/record being read
      */
    def readTSO2 (oid: Int): VDB.Record =
    {
	var rec		= Array.ofDim[Byte](128)
	val writeTS 	= TSTable.writeTS(oid)
	val readTS	= TSTable.readTS(oid)
	var locked = false
        if (writeTS <= tid){								// check if write_TS(X)<=TS(T), then we get to try to read  
		if(writeTS < tid){							// check for STSO, then use locks
		    if( DEBUG ) println(s"$tid wants to readLock $oid")
		    locked = readLockObj(oid)
		    if(locked) {
		    	if( DEBUG ) println(s"$tid got to readLock $oid")
		        rec = VDB.read(tid,oid)._1
		    	releaseReadLocks()
		    }
		    else rollback()
		    } // if
            	else {
		    rec = VDB.read(tid,oid)._1
		    if(readTS < tid) TSTable.readStamp(tid,oid) 
		} //  else
	}
	else rollback()
	rec
    } // readTSO

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      * Add Strict Time Stamp Ordering
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def writeTSO2 (oid: Int, value: VDB.Record)
    {
	val readTS  = TSTable.readTS(oid)
	val writeTS = TSTable.writeTS(oid)
	var locked = false
        if(tid < readTS || tid < writeTS) rollback()			
	else {
	     if( tid > writeTS ){						// check for STSO
	     	if( DEBUG ) println(s"$tid wants to writeLock $oid")
	     	locked = writeLockObj(oid)
		if(locked) {
		    VDB.write(tid,oid,value)
		    if(DEBUG)println(s"$tid got to writeLock $oid")
		}
		else rollback()
	     } // if
	     else{
		VDB.write(tid,oid,value)
	      	TSTable.writeStamp(tid,oid)
	     }
	}
    } // writeTSO
    

    //::
    /*
    **/
    def releaseReadLocks()
    {
	if( DEBUG ) println(s"releasing read locks for tid: $tid")
	for(oid <- readLocks.keys) {
      		if( DEBUG ) println(s"$tid releasing readLock for ${oid}")
      		var readLock = readLocks(oid)
		readLock.unlock()
		if( CLAIRVOYANCE )println(s"unlock($tid,$oid)")
      		LockTable.unlock(tid,oid)
      		readLocks -= oid
	} // for
    } // releaseReadLocks

    //::
    /*
    **/
    def releaseWriteLocks()
    {
	if( DEBUG ) println(s"releasing write locks for tid: $tid")	
	for(oid <- writeLocks.keys) {
	      		if( DEBUG ) println(s"$tid is releasing writeLock for ${oid}")
	      		var writeLock = writeLocks(oid)
			writeLock.unlock()
			if( CLAIRVOYANCE )println(s"unlock($tid,$oid)")
	      		LockTable.unlock(tid,oid)
	      		writeLocks -= oid
	}
    }


    
} // Transaction class

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
  * > run-main trans.TransactionTest
  */

object TransactionTest extends App {
    private val _2PL = 0
    private val TSO = 1
    private val concurrency = _2PL

    private val numTrans = 400
    private val numOps   = 10
    private val numObjs  = 480
    
    private val totalOps = numOps * numTrans

    private val CSR_TESTING = true
    
    var start = 0L
    var end = 0L

    val transactions = Array.ofDim[Transaction](numTrans)

    for (i <- transactions.indices) transactions(i) = new Transaction(Schedule.genSchedule2(i,numOps,numObjs),concurrency)
    
    start = System.currentTimeMillis

    for (i <- transactions.indices){
        transactions(i).start()	
    } // for
    for (i <- transactions.indices){
        transactions(i).join()
    } // for

    end = System.currentTimeMillis

    
    if ( CSR_TESTING ){
       val sch = ScheduleTracker.getSchedule().toList
       val schedule = new Schedule( sch )
       println(s"$schedule")
       val csr = schedule.isCSR(Transaction.nextCount())
       println(s"Resulting schedule is CSR: $csr")
       var accounting = VDB.numWrites + totalOps - VDB.ignoredWrites == sch.size
       if(accounting) println(s"All ops accounted for")
       else if( VDB.numWrites + totalOps - VDB.ignoredWrites > sch.size ) println(s"Schedule tracker had too few operations in it.")
       else println("Schedule tracker had too many operations in it.")
    }
    
    if ( !CSR_TESTING) println(s"total time: ${end - start}")
    
    
}