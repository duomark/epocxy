Erlang Patterns of Concurrency (epocxy)
=======================================

The version number format epocxy uses is Major.Minor.Rev (e.g., 1.1.0). If the Rev is an even number, this is a release version and will be tagged for builds. If it is an odd number, it is a modification of Master that is under development but available for testing, and will only be marked in the epocxy.app file, not as a tagged version. Only use Master in your dependency if you are testing partial fixes; always use an even number tag starting with 1.1.0 for stable release dependency.

NOTE:
  - 'make tests' is significantly slower in 1.1.0 because of cxy_fount PropEr tests. Enchancements are planned to speed testing in 1.1.1. Production performance is not impacted. Update: cxy_fount_SUITE now takes 30 seconds on my laptop, it won't get any faster.
  - cxy_regulator gives intermittent failures when running 'make tests'. This is a timing issue in the test itself. Run again and/or stop some CPU intensive operations before running. Hoping to fix this in 1.1.1

Erlang/OTP offers many components for distributed, concurrent, fault-tolerant, non-stop services. Many concurrent systems need common constructs which are not provided in the basic OTP system. This library is an Open Source set of concurrency tools which have been proven in a production environment and are being released publicly in anticipation that community interest and contributions will lead to the most useful tools being submitted for inclusion in Erlang/OTP.

This library is released under a "Modified BSD License". The library was sponsored by [TigerText](http://tigertext.com/) during 2013, and was validated on their HIPAA-compliant secure text XMPP servers in a production environment handling more than 1M messages per day.

A talk from Erlang Factory San Francisco 2014 on this library is in the 'doc' directory.

Concurrency Fount was added in 2016 and the 'doc' directory includes the slides from a talk at EUC 2016, as well as quick summary of the epocxy patterns.


ETS (Erlang Term Storage)
-------------------------

ETS tables offer fast access to very large amounts of mutable data, and unlike normal processes, have the added benefit of not being garbage collected. Under normal production situations, access to data stored in ets tables can be twice as fast as a synchronous request for the same information from another process.

ETS tables also offer a limited concurrency advantage. While they are not currently suited for scaling with manycore architectures, they do provide concurrent access because they employ internal record-level locking, whereas process message queues are inherently serial and therefore cannot service two requests by two separate processes. 

These benefits make ETS tables one of the most useful OTP constructs in building common concurrency idioms for multi-core processing systems.


Buffers
-------

FIFO, LIFO and Ring buffers are very useful in absorbing data from a source stream when the processing time is exceeded by the amount of data being supplied. If the difference in time and volume is not excessive, or the loss of data is inconsequential, an in-memory buffer can provide a smooth intermediate location for data in transit. If the volume becomes excessive, an alternative persistent data store may be needed.

OTP provides no explicit buffer tools, although it does provide queues and lists. These algorithms only allow serial access when they are embedded inside a process. Implementing buffers with an ets table allows multiple processes to access the buffer directly (although internal locks and the sequential nature of buffers prevent actual concurrent access) and operate in a more efficient and approximately concurrent manner.

The message queues of processes are naturally FIFO queues, with the additional feature of being able to scan through them using selective receive to find the first occurrence of a particular message pattern. LIFO queues cannot be efficiently modeled using just the built in message queue of an erlang process.

ETS buffers allow independent readers and writes to access data simultaneously through distinct read and write index counters which are atomically updated. A ring buffer is extremely useful under heavy load situations for recording a data window of the most recent activity. It is light weight in its processing needs, limits the amount of memory used at the cost of overwriting older data, and allows read access completely independent of any writers. It is often used to record information in realtime for later offline analysis.


Concurrency Control
-------------------

Most distributed systems need dynamic adaptation to traffic demand, however, there are limits to how much concurrency they can handle without overloading and causing failure. Most teams employ pooling as a solution to this problem, treating processes as a limited resource allocation problem. This approach is overly complicated, turns out to not be very concurrent because of the central management of resource allocation, can introduce latent errors by recycling previously used processes (beware of process dictionary garbage!), and leads to message queue overload and cascading timeout failures under heavy pressure.

A more lightweight approach is to limit concurrency via a governor on the number of processes active. When the limit is exceeded functions are executed inline rather than spawned, thus providing much needed backpressure on the source of the concurrency requests. The governor here is implemented as a counter in an ETS table with optional execution performance times recorded in a ring buffer residing in the same ETS table.


Concurrency Fount
-----------------

Concurrency fount (cxy_fount) offers an alternative control to avoid concurrency overload. It is a reservoir of one-shot pre-spawned processes which refresh at a controlled rate. The reservoir represents a pre-allocation of the total potential computational power for a single category of tasks on a node. Once it is consumed, no additional work should be started (or even accepted) until the compute potential is partially replenished. It is assumed that actual progress is made with allocated workers, so that they complete before too many new processes are available.

This pattern uses a supervisor and two gen_fsms, and does not rely on any ets tables. The cxy_fount API is a serial mailbox on the cxy_fount process, so it may become a limiting factor in some situations, but the overall structure is designed to avoid catastrophic failure in favor of surviving individual failures (even if occuring rapidly and repeatedly in the worker tasks).

The cxy_fount pattern is new as of Version 1.1.0, and more improvements will arrive before 1.2.0.

Caches
------

Most distributed systems consult information that is slow to generate or obtain. This is often because it is supplied by an external application such as a database or key/value store, or it comes via a TCP connection to an external system. In these situations, the built-in facilities of ETS key/value RAM storage can be used to retain previously retrieved information in anticipation of its reuse by other processes. Many development teams build their own application-specific caching on top of ETS because there is no basic caching capability provided in OTP.

The ets_cache provided here is a generational cache with two generations. New objects are inserted in the newest generation. When a generation change occurs, a new empty generation is created and the oldest generation is deleted. The previously active generation becomes the old generation. The pattern of access is: new generation -> old generation -> external data source. When an item is found in the old generation, it is copied to the new generation so that it will survive the next generation change. Everything residing solely in the old generation will be automatically eliminated in the single action of deleting that generation when it has aged.

Generation cycling can be performed based on elapsed time, number of access, an arbitrary function, or, when the entire dataset can comfortably fit in memory, never.


Batch Feeder
------------

Batch feeder is used for the lazy generation of large sets, context-aware iteration over sets, or paced iteration. It provides a behaviour framework for implementing a control structure that you may control via the generation of a continuation function at each step of the iteration. It may be combined with concurrency constructs to chop a large structure into segments which can be processed as batches in the background.
