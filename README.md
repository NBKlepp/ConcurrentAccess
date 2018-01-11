# ConcurrentAccess
This project is a simulated implementation of a database transaction manager allowing for concurrent access to the database by multiple transactions in parallel. The simulation is implemented in the following four files all located in the src/main/scala/transaction directory: Transaction.scala, VDB.scala, Graph.scala, and VDB.scala.

## Getting Started

To get started, first download and install scala and sbt - the simple build tool. Instillation instructions may be found [here](https://www.scala-lang.org/download/)

Once you have installed scala and sbt, clone the repository into some directory on your home machine, for instance::

> /some/directory/ConcurrentAccess/ $

To run the simulation, navigate to the project root directory and begin the sbt session with the 'sbt' command:

> /some/directory/ConcurrentAccess/ $ sbt

This should bring up the interactive sbt command line:

> /some/directory/ConcurrentAccess $ sbt
> [info] Set current project to trans (in build file:/Users/MacBot/Projects/DatabaseConcurrencyControl8370/Transactions/)
> \>

From here, the following command will run the simulation with default parameter settings:

> /some/directory/ConcurrentAccess $ sbt
> [info] Set current project to trans (in build file:/Users/MacBot/Projects/DatabaseConcurrencyControl8370/Transactions/)
> >run-main trans.VDBTest2

The default concurrent access protocol is the strict two-phase locking protocol. The time-stamp odering protocol has also been implemented, and the protocol employed in the simulation may be changed via the 'concurrency' instance variable in the VDBTest2 companion object in the VDB.scala file.

To change the number of transactions in the simulation, the number of operations per transaction, or the number of records in the database, modify the instance variables in the VDB object implemented in the VDB.scala file. Increasing the number of transactions in the simulation, increasing the number of operations per transaction, or decreasing the number of records in the simulation will all increase contention for the records among the various transactions thereby increasing the threat of deadlock in the concurrent access environment. 

## An Explanation of the System

The "database" consists of a persistent and volatile portion, implemented in the VDB.scala file.

The persistent portion of the database is implemented in the 'PDB' companion object to the VDB object. The "data" is represented by the 'store' variable in the PDB object, implemented with a java RandomAccessFile. The number of "pages" in the store, the number of "records" per page and the size of each "record" are controlled by the respective variables in the PDB object. The "log", or history, of transactions on the persistent database is represented by the log variable, and implemented with a java RandomAccessFile as well. The operations to write to the store, fetch a page from the store, and initialize the store are all implemented in the PDB object as well. 

The volatile portion of the database is represented by the 'cache' instance variable in the VDB object, and is implemented with an array of 'Page' objects. The 'Page' case class implements a cache page as an array of database 'Record' types. A 'Record' is defined as an array of raw Bytes. The "log buffer", which keeps track of recent write transactions run on the VDB, is implemented in the VDB object as well with an array of 'LogRecord' types. A 'LogRecord' is defined as a 4-tuple: (int object_id,int transaction_id,Record old_record,Record new_record). When a transaction is complete it is "committed" to the persistent database, whereupon the log buffer is flushed into the log file of the persistent database. The 'commit' method defined in the VDB object implements the commit procedure. Occassionally a transaction will need to be "rolled back" in order to gauruntee liveness of the system. The 'roleback' method defined in the VDB object implements this feature.

While the logic controlling the maintenance and accuracy of the volatile and persistent portions of the database (along with their respective log histories) is implemented in the VDB and PDB objects, the logic for granting read and write access to a record in the database according to the specified concurrent access protocol (2PL or TSO) is implemented in the Transaction object specifications in the Transaction.scala file. Specifically, the logic is implemented in the 'read2PL','write2PL','readTSO', and 'writeTSO' methods implemented in the Transaction object.

Schedule creation and maintenance of schedule for verification purposes is implemented in the Schedule object in the Schedule.scala file. 

