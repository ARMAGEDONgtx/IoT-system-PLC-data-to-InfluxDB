import xml.etree.ElementTree as ET
import re
import snap7
import time
from snap7.util import *
from snap7.snap7types import *
import influxdb
import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil
import multiprocessing


class my_data():
    def __init__(self, plc , type , area , address, alias , active, slot, opcua_var = None):
        self.m_plc = plc
        self.m_type = type
        self.m_area = area
        self.m_address = address
        self.m_alias = alias
        self.m_active = active
        self.m_slot = slot
        self.m_value = 0.0
        self.m_opcua_var = opcua_var

    def show(self):
        print("PLC IP: {0}, TYPE: {1}, AREA: {2}, ADDRESS: {3}, ALIAS: {4}, ACTIVE: {5}".format(
            self.m_plc, self.m_type, self.m_area, self.m_address, self.m_alias, self.m_active
        ))


class my_group():
    def __init__(self, data_list, lock):
        self.m_lock = lock
        self._stopev = False
        self.m_data_list= data_list
        print('creating plc clent')
        self.plc = snap7.client.Client()
        self.host = '10.14.12.83'
        self.port = 8086
        self.user = 'poziadmin'
        self.password = 'QpAlZm1!'
        self.db_name = 'PLC2InfluxDB'
        print('creating influx client')
        self.client = influxdb.InfluxDBClient(self.host, self.port, self.user, self.password, self.db_name)
        self.client.create_database(self.db_name)
        #if list no empty, create connection
        if len(self.m_data_list) > 0:
            print('connecting to the plc')
            print(self.m_data_list[0].m_plc)
            self.m_lock.acquire()
            self.plc.connect(self.m_data_list[0].m_plc, 0, eval(self.m_data_list[0].m_slot))
            self.m_lock.release()

    #assure to disconnect
    def __del__(self):
        self.plc.disconnect()

    def stop(self):
        self._stopev = True

    def add_one_data(self,data):
        self.m_data_list.append(data)

    def join_data_to_list(self,data_list):
        self.m_data_list = self.m_data_list + data_list

    # TODO: async
    def update_items(self):
        try:
            print('updating data')
            print(self.m_data_list[0].m_plc)
            #iterate through list
            for data in self.m_data_list:
                #extract numbers
                address = getNumbers(data.m_address)
                value = None
                if len(address) > 0:  # assure there was some address
                    if eval(data.m_area) == 132 and len(address) >= 2:  # DB
                        self.m_lock.acquire()
                        result = self.plc.read_area(eval(data.m_area), eval(address[0]), eval(address[1]),
                                                         eval(data.m_type))
                        self.m_lock.release()
                        if eval(data.m_type) == S7WLReal:
                            value = get_real(result, 0)
                        elif eval(data.m_type) == S7WLDWord:
                            value = get_dword(result, 0)
                        elif eval(data.m_type) == S7WLWord:
                            value = get_int(result, 0)
                        elif eval(data.m_type) == S7WLByte:
                                value = get_int(result, 0)
                        elif eval(data.m_type) == S7WLBit and len(address) == 3:
                                value = int(get_bool(result, 0, eval(address[2])))
                    elif (eval(data.m_area) == S7AreaPA or eval(data.m_area) == S7AreaPE or eval(
                                data.m_area) == S7AreaMK) and len(address) >= 1:  # Memory / In Out
                        self.m_lock.acquire()
                        result = self.plc.read_area(eval(data.m_area), 0, eval(address[0]), eval(data.m_type))
                        self.m_lock.release()
                        if eval(data.m_type) == S7WLReal:
                            value = get_real(result, 0)
                        elif eval(data.m_type) == S7WLDWord:
                            value = get_dword(result, 0)
                        elif eval(data.m_type) == S7WLWord:
                            value = get_int(result, 0)
                        elif eval(data.m_type) == S7WLByte:
                            value = get_int(result, 0)
                        elif eval(data.m_type) == S7WLBit and len(address) == 2:
                            value = int(get_bool(result, 0, eval(address[1])))
                #update value in every my_data object
                if value is not None:
                    data.m_value = value
                    # send data do InfluxDB
                    json_body1 = create_my_json(data.m_plc, data.m_alias, value)
                    #self.m_lock.acquire()
                    self.client.write_points(json_body1)
                    #self.m_lock.release()
        except Exception as e:
            print(str(e))
            print(self.m_data_list[0].m_plc)
            # error - try recconecting to plc
            self.m_lock.acquire()
            self.plc.disconnect()
            # assure, that there is some data in list
            if len(self.m_data_list) > 0:
                self.plc.connect(self.m_data_list[0].m_plc, 0, eval(self.m_data_list[0].m_slot))
            self.m_lock.release()


# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array


def create_my_json(mes, name, value):
    j = [{
            "measurement": mes,
            "tags": {
                "name": name
            },
            "fields": {
                "Float_value": value
            }
        }
        ]
    return j


# create my_data objects, group by PLC
def create_my_data_groups():
    #lock for threading
    my_lock = multiprocessing.Lock()
    tree = ET.parse('C:\config.xml')
    root = tree.getroot()
    groups = []
    # get all configured plc
    for p in root:  # PLC
        my_list = []
        # table for varaible grouping and updating
        for data in p:
            sl = p.get('slot')
            if sl is None:
                sl = "1"
            m = my_data(p.text, data[0].text, data[1].text, data[2].text, data[3].text, data[4].text, sl)
            #add reference to opc ua server variable
            if eval(data[4].text):
                my_list.append(m)
        group = my_group(my_list,my_lock)
        groups.append(group)
    return groups

class TestService(win32serviceutil.ServiceFramework):
    _svc_name_ = "InfluxConnector3.0"
    _svc_display_name_ = "InfluxConnector3.0"

    host = '10.14.12.83'
    port = 8086
    user = 'poziadmin'
    password = 'QpAlZm1!'
    db_name='PLC2InfluxDB'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        rc = None
        try:
            # create my_groups to update values later
            groups = create_my_data_groups()
        except Exception as e:
            with open('C:\\InfluxDBService.log', 'a') as f:
                f.write(str(e)+'\n')
        while rc != win32event.WAIT_OBJECT_0:
            try:
                for g in groups:
                    g.update_items()
            except Exception as e:
                with open('C:\\InfluxDBService.log', 'a') as f:
                    f.write(str(e) + '\n')
            rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)



groups = create_my_data_groups()

def test1():
    while not groups[0]._stopev:
        groups[0].update_items()


def test2():
    while not groups[1]._stopev:
        groups[1].update_items()

def test3():
    while not groups[2]._stopev:
        groups[2].update_items()

def test(n):
    while not groups[n]._stopev:
        groups[n].update_items()

if __name__ == '__main__':
    # create my_groups to update values later
    jobs = []
    try:
        no = 0
        for g in groups:
            process = multiprocessing.Process(target=test,args=(no,))
            jobs.append(process)
            no = no + 1
        for j in jobs:
                j.start()
                time.sleep(0.5)
    except Exception as e:
        print(str(e))
    finally:
        for g in groups:
            g.stop()
        for j in jobs:
            j.join()