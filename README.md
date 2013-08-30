OpenBeacon Log Parser and Extensions
=======================================

The OpenBeacon Parser is a software under development to parse log files produced by a backend server, receiving packets from the [OpenBeacon Ethernet EasyReader PoE II - Active 2.4GHz RFID Reader](http://www.openbeacon.org/OpenBeacon_Ethernet_EasyReader_PoE_II_-_Active_2.4GHz_RFID_Reader) devices. In addition, it contains useful extensions to analyze several algorithms on graphs.

Parser author: *Francesco Ficarola* - Extensions author: *Gianluca Amori*


Supported output graph formats
------------------------------

**AGGREGATED**

* Adjacency List (CSV)
* Adjacency Matrix (CSV)
* GEXF [[link](http://gexf.net/format/)] [[gexf4j](https://github.com/francesco-ficarola/gexf4j)]

**DYNAMIC**

* DNF [[link](https://github.com/francesco-ficarola/dnf)]
* GEXF [[link](http://gexf.net/format/)] [[gexf4j](https://github.com/francesco-ficarola/gexf4j)]
* JSON [[link](http://www.json.org/)]


How to compile
----------

	$ mvn clean compile


How to run the LogParser
------------------------

Execute the following command to run the parser help and see all accepted arguments:

	$ mvn exec:java -Dexec.mainClass="it.uniroma1.dis.wsngroup.parsing.LogParser" -Dexec.args="-h"

Examples:

* Adjacency List (default):

		$ mvn exec:java -Dexec.mainClass="it.uniroma1.dis.wsngroup.parsing.LogParser" -Dexec.args="-f logfile.txt -csv"

* Adjacency Matrix:

		$ mvn exec:java -Dexec.mainClass="it.uniroma1.dis.wsngroup.parsing.LogParser" -Dexec.args="-f logfile.txt -am -csv"

* DNF:

		$ mvn exec:java -Dexec.mainClass="it.uniroma1.dis.wsngroup.parsing.LogParser" -Dexec.args="-f logfile.txt -dnf"
		
* GEXF:

		$ mvn exec:java -Dexec.mainClass="it.uniroma1.dis.wsngroup.parsing.LogParser" -Dexec.args="-f logfile.txt -gexf"


Log example that can be parsed
------------------------------

	2012-06-20 19:55:39+0200 [__builtin__.Receiver (UDP)] S t=1340214939 ip=0xc0a85009 id=1264 boot_count=1 seq=0x000007a1 strgth=1 flgs=2 last_seen=0 
	2012-06-20 19:55:40+0200 [__builtin__.Receiver (UDP)] S t=1340214940 ip=0xc0a85009 id=1264 boot_count=1 seq=0x000007c1 strgth=2 flgs=2 last_seen=0
	2012-06-20 19:55:41+0200 [__builtin__.Receiver (UDP)] C t=1340214941 ip=0xc0a85013 id=1135 boot_count=4 seq=0x00000d61 flags=0 [1119(1) #1]
	2012-06-20 19:55:41+0200 [__builtin__.Receiver (UDP)] C t=1340214941 ip=0xc0a85016 id=1135 boot_count=4 seq=0x00000d61 flags=0 [1119(1) #1]
	2012-06-20 19:55:41+0200 [__builtin__.Receiver (UDP)] C t=1340214941 ip=0xc0a85009 id=1135 boot_count=4 seq=0x00000d61 flags=0 [1119(1) #1]
	2012-06-20 19:55:41+0200 [__builtin__.Receiver (UDP)] C t=1340214941 ip=0xc0a85009 id=1119 boot_count=2 seq=0x00004de1 flags=2 [1135(1) #3]
	2012-06-20 19:55:41+0200 [__builtin__.Receiver (UDP)] C t=1340214941 ip=0xc0a85016 id=1264 boot_count=1 seq=0x000007e1 flags=2
	2012-06-20 19:55:41+0200 [__builtin__.Receiver (UDP)] C t=1340214941 ip=0xc0a85009 id=1264 boot_count=1 seq=0x000007e1 flags=2
	2012-06-20 19:55:42+0200 [__builtin__.Receiver (UDP)] S t=1340214942 ip=0xc0a85009 id=1264 boot_count=1 seq=0x00000801 strgth=0 flgs=2 last_seen=0 
	2012-06-20 19:55:44+0200 [__builtin__.Receiver (UDP)] S t=1340214944 ip=0xc0a85009 id=1264 boot_count=1 seq=0x00000821 strgth=1 flgs=2 last_seen=0 
	2012-06-20 19:55:44+0200 [__builtin__.Receiver (UDP)] S t=1340214944 ip=0xc0a85009 id=1135 boot_count=4 seq=0x00000dc1 strgth=2 flgs=0 last_seen=0 
	2012-06-20 19:55:45+0200 [__builtin__.Receiver (UDP)] S t=1340214945 ip=0xc0a85009 id=1264 boot_count=1 seq=0x00000841 strgth=2 flgs=2 last_seen=0 

