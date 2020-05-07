import xml.etree.ElementTree as ET
import snap7
import time
from snap7.util import *
from snap7.snap7types import *
import influxdb
import multiprocessing
from collections import defaultdict
from threading import Thread, Timer

# CONFIGURATION --------------------------
config_PATH = '/home/poziadmin/Documents/Python_projects/Linux/config.xml'
influxDB_IP = '10.14.12.83'
influxDB_user = 'poziadmin'
influxDB_pass = 'QpAlZm1!'

class my_data():
    def __init__(self, plc , type , area , address, alias , active, slot, interval, opcua_var = None):
        self.m_plc = plc
        self.m_type = eval(type)
        self.m_area = eval(area)
        self.m_address = address
        self.m_alias = alias
        self.m_active = eval(active)
        self.m_slot = eval(slot)
        self.m_value = 0.0
        self.interval = interval
        self.m_opcua_var = opcua_var

    def show(self):
        print("PLC IP: {0}, TYPE: {1}, AREA: {2}, ADDRESS: {3}, ALIAS: {4}, ACTIVE: {5}".format(
            self.m_plc, self.m_type, self.m_area, self.m_address, self.m_alias, self.m_active
        ))


class my_group():
    def __init__(self, data_list, lock1, lock2):
        self.lock_plc = lock1
        self.lock_pipe = lock2
        self._stopev = False
        self.data_list= data_list
        self.plc = snap7.client.Client()
        self.host = influxDB_IP
        self.port = 8086
        self.user = influxDB_user
        self.password = influxDB_pass
        self.subgroups = defaultdict(list)
        self.group_by_interval()

    def group_by_interval(self):
        for data in self.data_list:
            self.subgroups[data.interval].append(data)


    def connect(self):
        #if list no empty, create connection
        if len(self.data_list) > 0:
            self.lock_plc.acquire()
            self.plc.connect(self.data_list[0].m_plc, 0, self.data_list[0].m_slot)
            self.lock_plc.release()
        self.db_name = self.data_list[0].m_plc
        self.client = influxdb.InfluxDBClient(self.host, self.port, self.user, self.password, self.db_name)
        self.client.create_database(self.db_name)

    #assure to disconnect
    def __del__(self):
        self.plc.disconnect()

    def stop(self):
        self._stopev = True

    def add_one_data(self,data):
        self.data_list.append(data)

    def join_data_to_list(self,data_list):
        self.data_list = self.data_list + data_list

    def update_items(self, group, interval_based = True):
        diff = 0.0
        # continous fucntion
        while (self._stopev != True):
            start = time.time()
            try:
                #iterate through list
                for data in group:
                    #extract numbers
                    address = getNumbers(data.m_address)
                    value = None
                    if len(address) > 0:  # assure there was some address
                        if data.m_area == 132 and len(address) >= 2:  # DB
                            self.lock_plc.acquire()
                            result = self.plc.read_area(data.m_area, eval(address[0]), eval(address[1]), data.m_type)
                            self.lock_plc.release()
                            if data.m_type == S7WLReal:
                                value = get_real(result, 0)
                            elif data.m_type == S7WLDWord:
                                value = get_dword(result, 0)
                            elif data.m_type == S7WLWord:
                                value = get_int(result, 0)
                            elif data.m_type == S7WLByte:
                                    value = get_int(result, 0)
                            elif data.m_type == S7WLBit and len(address) == 3:
                                    value = int(get_bool(result, 0, eval(address[2])))
                        elif (data.m_area == S7AreaPA or data.m_area == S7AreaPE or
                                    data.m_area == S7AreaMK) and len(address) >= 1:  # Memory / In Out
                            self.lock_plc.acquire()
                            result = self.plc.read_area(data.m_area, 0, eval(address[0]), data.m_type)
                            self.lock_plc.release()
                            if data.m_type == S7WLReal:
                                value = get_real(result, 0)
                            elif data.m_type == S7WLDWord:
                                value = get_dword(result, 0)
                            elif data.m_type == S7WLWord:
                                value = get_int(result, 0)
                            elif data.m_type == S7WLByte:
                                value = get_int(result, 0)
                            elif data.m_type == S7WLBit and len(address) == 2:
                                value = int(get_bool(result, 0, eval(address[1])))
                    #update value in every my_data object
                    if value is not None:
                        data.m_value = value
                        # send data do InfluxDB
                        json_body1 = create_my_json(data.m_plc, data.m_alias, value)
                        self.client.write_points(json_body1)
                end = time.time()
                if interval_based == True:
                    diff = end - start
                    upd_int = group[0].interval
                    #if not the minimum update time group, wait appropriate time
                    if upd_int != 'min':
                        wait_time = re.findall(r'[0-9]+', upd_int)
                        # check if we didnt exceeded time, if not lets wait
                        if float(wait_time[0])-diff >= 0.0:
                            time.sleep(float(wait_time[0]-diff))
            except Exception as e:
                self.lock_plc.acquire()
                print(str(e))
                # error - try recconecting to plc
                self.plc.disconnect()
                # assure, that there is some data in list
                if len(self.data_list) > 0:
                    self.plc.connect(self.data_list[0].m_plc, 0, self.data_list[0].m_slot)
                self.lock_plc.release()

    def update_items_by_interval(self):
        self.connect()
        #create thread for each subgroup
        for inter in self.subgroups.keys():
            group = self.subgroups[inter]
            th = Thread(target=self.update_items, args=(group, True,))
            #delay start
            time.sleep(1)
            th.start()


# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array


def create_my_json(mes, name, value):
    j = [{
            "measurement": mes,
            "tags": {

            },
            "fields": {
                name: value
            }
        }
        ]
    return j


# create my_data objects, group by PLC
def create_my_data_groups():

    tree = ET.parse(config_PATH)
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
            m = my_data(p.text, data[0].text, data[1].text, data[2].text, data[3].text, data[4].text, sl, data[5].text)
            #add reference to opc ua server variable
            if eval(data[4].text):
                my_list.append(m)
        # lock for threading
        my_lock1 = multiprocessing.Lock()
        my_lock2 = multiprocessing.Lock()
        group = my_group(my_list, my_lock1, my_lock2)
        groups.append(group)
    return groups


#function that will be called in separrrate process
def proc(group):
    group.update_items_by_interval()


#create groups by PLC
groups = create_my_data_groups()
jobs = []
try:
    no = 0
    #for each group start update process
    for g in groups:
        process = multiprocessing.Process(target=proc, args=(g,))
        jobs.append(process)
        no = no + 1
    for j in jobs:
        j.start()
        #delay start
        time.sleep(5)
except Exception as e:
    print(e)
    print('stopped')
    for g in groups:
        g.stop()
    for j in jobs:
        j.join()
