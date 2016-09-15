Epocxy Explained
================

The epocxy library is a set of concurrency patterns that are useful for high-performance systems, but they also can be used on smaller systems to achieve a more disciplined architecture that is easier to maintain, understand, control, and monitor.

The following patterns are available:

  1. cxy_ctl: limits the number of spawned processes by category
  1. cxy_fount: limits the number of spawned processes with a paced replacement policy
  1. cxy_cache: generational caching for large numbers of objects
  1. cxy_synch: awaits for M of N replies from processes before continuing
  1. batch_feeder: provides a generalized pattern for continuation-based iteration
  1. ets_buffers: concurrently accessible LIFO, FIFO and Ring buffers

cxy_ctl
-------

A single ets table is used to hold metadata about all the concurrency categories that are limited. This table is indexed by category and contains the following fields for each category of limited concurrency:

  1. Max procs: the maximum number of simultaneous processes allowed of this type
  1. Active procs: the current number of simultaneous processes
  1. Max history: size of a circular buffer of historical spawns for debugging
  1. High water max procs: the highest number of processes since last reset

When a new process is spawned by category using the cxy_ctl module, it will only get created if the active processes is less than max processes.

cxy_fount
---------

Concurrency fount uses a reservoir and a regulator to maintain a potential computation capacity. The reservoir consists of a stack of slab-allocated processes. The regulator supplies each slab of processes, but no faster than one slab every 1/100th of a second. Processes in the reservoir are spawned before they are needed, and are linked to the cxy_fount so that it can be stopped and all unallocated processes are terminated.

While cxy_fount reduces latency by pre-spawning, the most important purpose is to limit the maximum computational resource consumed during overload situations. It is designed to accept but limit the impact of sudden spikes, so that a node doesn't crash under unexpected pressure. When the reservoir is empty, no new tasks can be started. The processes in a reservoir are expected to perform a task and then terminate within in a roughly predictable amount of time which doesn't dramatically exceed the reservoir refill time. If a tasked worker does not terminate, but the reservoir keeps refilling at a rate of one slab per 1/100th of a second, the reservoir will not be a capacity limit on processing potential.

There is an example that demonstrates using cxy_fount to perform a parallel hexdump on a blob of binary data.

cxy_cache
---------

Concurrency cache uses two ets tables (and a metatable) to track cached objects. There is a new generation and an old generation table. When an object is accessed, it is migrated forward from an older generation or retrieved for the first time and placed in the current generation. When a generation expires, the oldest generation will only contain those objects that have not been accessed once in the last 2 generations, therefore the table can be deleted and all resident objects removed in one action. This approach has far less overhead than tracking thousands of objects with indpendent timers for each one.

cxy_synch
---------

Synchronization barriers are under development. There is an example of collecting results from the first M of N processes, but this code will likely change soon now that cxy_fount is available. The existing code also demonstrates the use of Event Tracing to obtain message sequence diagrams for debugging concurrency.

batch_feeder
------------

Another experiment, this time using a behaviour to implement function continuations to chew through a batch of transactions sequentially. Still under development.

ets_buffers
-----------

As of 1.1.0 and earlier versions, the ets_buffers are not reliable with a high number of readers and writers. Improvements are not planned until 1.2.0. It is best to avoid using them until a replacement implementation is provided.
