Release History
===============

The following releases of epocxy are planned:

1.5.0 ()
--------

Requires OTP 19 or later to run.

1.2.0 ()
--------

New read-only ETS ring buffer
cxy_fount uses proc_lib thus allowing worker OTP tracing/debugging

1.1.1 ()
--------
cxy_fount tests run much faster due to clock skewing

=====

The following releases of epocxy have been made:


1.1.0 (15 Sep 2016)
-------------------

WARNING: The addition of cxy_fount slowed down 'make tests'
significantly because it uses PropEr and attempts many variations
of waiting on the cxy_regulator to slowly refill the reservoir.

Add RELEASES.md for overview of historical changes
Allow use on OTP 19.0
Update erlang.mk to latest version
Complete concurrency fount functionality and hexdump example
Add epocxy to Hex (courtesy of Fernando Bienevides)
Fix restart and refresh item issues with concurrency cache

1.0.0 ()
--------

Add high water mark to cxy_ctl (courtesy of David Hull)
Initial prototype of concurrency fount
Initial property-based testing of concurrency fount
Fix some warnings with batch feeder

0.9.9 ()
--------

0.9.8e
------
