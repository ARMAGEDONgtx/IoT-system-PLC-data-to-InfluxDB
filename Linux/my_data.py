import xml.etree.ElementTree as ET
from threading import Thread
import re
import snap7
import time
from snap7.util import *
from snap7.snap7types import *
import random
import multiprocessing

class my_data():
    def __init__(self, plc , type , area , address, alias , active, slot, opcua_var_id = None):
        self.m_plc = plc
        self.m_type = eval(type)
        self.m_area = eval(area)
        self.m_address = address
        self.m_alias = alias
        self.m_active = active
        self.m_slot = eval(slot)
        self.m_value = 0.0
        self.m_opcua_var = opcua_var_id

    def show(self):
        print("PLC IP: {0}, TYPE: {1}, AREA: {2}, ADDRESS: {3}, ALIAS: {4}, ACTIVE: {5}".format(
            self.m_plc, self.m_type, self.m_area, self.m_address, self.m_alias, self.m_active
        ))


class my_group():
    def __init__(self, data_list, lock):
        self._stopev = False
        self.lock = lock
        self.m_data_list= data_list
        self.plc = snap7.client.Client()
        #if list no empty, create connection
        if len(self.m_data_list) > 0:
            self.plc.connect(self.m_data_list[0].m_plc, 0, self.m_data_list[0].m_slot)
            #pass

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
    def update_items(self, pipe):
        try:
            #continous function
            while(self._stopev != True):
                #iterate through list
                for data in self.m_data_list:
                    #extract numbers
                    address = getNumbers(data.m_address)
                    value = None
                    if len(address) > 0:  # assure there was some address
                        if data.m_area == 132 and len(address) >= 2:  # DB
                            result = self.plc.read_area(data.m_area, eval(address[0]), eval(address[1]), data.m_type)
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
                            result = self.plc.read_area(data.m_area, 0, eval(address[0]), data.m_type)
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
                        if data.m_opcua_var is not None:
                            pipe.send(data.m_opcua_var)
                            pipe.send(data.m_value)
        except Exception as e:
            print(str(e))
            # error - try recconecting to plc
            self.plc.disconnect()
            # assure, that there is some data in list
            if len(self.m_data_list) > 0:
                self.plc.connect(self.m_data_list[0].m_address, 0, self.m_data_list[0].m_slot)

    def sim_update(self, pipe):
        while(self._stopev != True):
            for d in self.m_data_list:
                d.value = random.uniform(0.0,100.0)
                d.show()
                if d.m_opcua_var is not None:
                    pipe.send(d.m_opcua_var)
                    pipe.send(d.value)


# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array


