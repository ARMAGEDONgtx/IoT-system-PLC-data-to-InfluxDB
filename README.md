# PLC_to_InfluxDB
fetching data from plcs (Siemens S7-300/400/1200/1500) and sending it do InfluxDB, BigData like data storing

# Setup steps

1. Setup machine with InfluxDB database stable realease (1.7.x). You can followe these steps
https://devconnected.com/how-to-install-influxdb-on-ubuntu-debian-in-2019/#II_Installing_InfluxDB_20

2. Install Python3.5+ on the same machine or on some other.

# Configuration 

The data which is fetched from PLCs and send to database is configured via ConfigApp. It generates xml file which is read at the start of program.

# Linux version - asynchronous

Program works as linux deamon with use of systemd (take a look at https://github.com/torfsen/python-systemd-tutorial). It allows to monitor current status and easily start and stop acqusition. For every PLC in configuration is started new process and data is being processed in it. For n PLC is started n Python processes to boost up performance. 

# Windows version - synchrnous

In windows version I decided to collect and send data as windows servise. It allows to monitor current status and easily start and stop acqusition. The asynchornous version was dropped due to problems with multiprocessing and service compatibility. So, data is collected one by one and send to database. Choose this version if you do not require fast acqusition.





