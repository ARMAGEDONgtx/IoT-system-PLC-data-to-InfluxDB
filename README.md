# PLC_to_InfluxDB

This project aim is to provide free software to fetch data from plcs (Siemens S7-300/400/1200/1500) and store it. Used stack is completly opensource. I used InfluDB as data storage, so application principle is following Big Data paradigm. InfluxDB allows to process your data in many interesting ways such as anomaly detecting, forecasting and of course basic data visualization. It also has retention policy, which allows to control your storage space.

For polish users you can read some paper we made about it (just basic explanation and some performance tests) -> https://www.overleaf.com/read/wfqmzmtnbmcb

Structure:
![Alt text](img/Structure.png?raw=true "Structure")


# Setup steps

1. Setup machine with InfluxDB database stable realease (1.7.x). You can followe these steps
https://devconnected.com/how-to-install-influxdb-on-ubuntu-debian-in-2019/#II_Installing_InfluxDB_20

2. Install Python3.5+ on the same machine or on some other. 
Requiremets for Linux and Windows version are in corresponding folders.

## Linux
3. Install Snap7. Here are the steps I followed:

Download  and unzip snap7-full-1.4.2
Compile the library
cd snap7-full-1.4.2/build/unix
sudo make -f x86_64_linux.mk

Copy the compiled library to lib directories
cd snap7-full-1.4.2/build/x86_64-linux
cp libsnap7.so /usr/lib
cp libsnap7.so /usr/local/lib
Install python-snap7 (pip install python-snap7)

It might be necessery to change common.py from snap7 package if you encouter problems (https://github.com/gijzelaerr/python-snap7/issues/68)


## Windows
3. Install Snap7. I recommend to followe this: https://python-snap7.readthedocs.io/en/latest/installation.html.


# Configuration 

The data which is fetched from PLCs and send to database is configured via ConfigApp. It generates xml file which is read at the start of program. In configuration you specify (looking from left on below picture, app gives you hints what should you write in field):
- IP address of PLC from which data will be fetched\
- RACK slot of the PLC
- Area of PLC Memory from which we get data:\
  -> S7AreaPE - inputs (IW0 etc.)\
  -> S7AreaPA - outputs (QW0 etc.)\
  -> S7AreaMK - standard memory (MW0 etc.)\
  -> S7AreaDB - data blocks (DB10.DBW0 etc.)\
  -> S7AreaCT/TM - not tested\
- Data Type of wanted varaible(Bit, Byte, Word ...)
- Actual address of variable (I0.0 or QW20 etc.)
- Data alias - alias by which variable will be visible in the system
- Activate - when activated data will be fetched, if not it will not be

![Alt text](img/configapp.PNG?raw=true "ConfigApp")

If you want to add new entry fill requiered fileds. In order to edit actual entries select PLC from drop down menu -> click find aliases -> select data from drop down menu -> clik find data and you will get actual config of selected data. You can edit it or modify. Remember that selected filed before "find data" click is the one that you are editing! 

## InfluDB endpoint

At the begining of InfluxConnector2.py there is small code fragment, which you need to adjust to your infludb service. Fill it with your confiuration, example below:

config_PATH = '/home/poziadmin/Documents/Python_projects/Linux/config.xml'\
influxDB_IP = '10.14.12.83'\
influxDB_user = 'admin'\
influxDB_pass = 'admin!'

# Linux version - asynchronous

Program works as linux deamon with use of systemd (take a look at https://github.com/torfsen/python-systemd-tutorial). It allows to monitor current status and easily start and stop acqusition. For every PLC in configuration is started new process and data is being processed in it. For n PLC is started n Python processes to boost up performance. 

To create service we have to make unit file - follow previous tutorial. It will look like this:

[Unit]\
Description=Python service to send data from PLC to InfluxDB\
PartOf=influxdb.service\
After=influxdb.service\
[Service]\
ExecStart=/home/poziadmin/Python-3.8.2/python /home/poziadmin/Documents/Python_projects/Linux/InfluxConnector2.py\
Restart=on-failure\
[Install]\
WantedBy=default.target

Adjust ExecStart variable with your python path and path to the service file.

As the unit file is created we can inspect logs in the future with:
sudo journalctl -u PLC2InfluxDB.service

# Windows version - synchrnous

In windows version I decided to collect and send data as windows servise. It allows to monitor current status and easily start and stop acqusition. The asynchornous version was dropped due to problems with multiprocessing and service compatibility. So, data is collected one by one and send to database. Choose this version if you do not require fast acqusition. 

In order to install service you have to generate exe file with PyInstaller, follow these steps:
https://gist.github.com/guillaumevincent/d8d94a0a44a7ec13def7f96bfb713d3f





