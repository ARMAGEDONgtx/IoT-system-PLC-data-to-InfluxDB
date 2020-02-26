import xml.etree.ElementTree as ET
import re
import snap7
import time
from snap7.util import *
from snap7.snap7types import *
import asyncio

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
    def __init__(self, data_list):
        self.m_data_list= data_list
        self.plc = snap7.client.Client()
        #if list no empty, create connection
        if len(self.m_data_list) > 0:
            #self.plc.connect(self.m_data_list[0].m_address, 0, self.m_data_list[0].m_slot)
            pass

    #assure to disconnect
    def __del__(self):
        self.plc.disconnect()

    def add_one_data(self,data):
        self.m_data_list.append(data)

    def join_data_to_list(self,data_list):
        self.m_data_list = self.m_data_list + data_list

    # TODO: async
    def update_items(self):
        try:
            for data in self.m_data_list:
                address = getNumbers(data.m_address)
                value = None
                if len(address) > 0:  # assure there was some address
                    if eval(data.m_area) == 132 and len(address) >= 2:  # DB
                        result = self.plc.read_area(eval(data.m_area), eval(address[0]), eval(address[1]),
                                                     eval(data.m_type))
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
                        result = self.plc.read_area(eval(data.m_area), 0, eval(address[0]), eval(data.m_type))
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
                    #if there is reference to opcua var update it also
                    if data.m_opcua_var is not None:
                        data.m_opcua_var.set_value(value)
        except Exception as e:
            print(str(e))
            # error - try recconecting to plc
            self.plc.disconnect()
            # assure, that there is some data in list
            if len(self.m_data_list) > 0:
                self.plc.connect(self.m_data_list[0].m_address, 0, self.m_data_list[0].m_slot)


# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array

# create my_data objects, group by PLC
def create_my_data_groups():
    tree = ET.parse('config.xml')
    root = tree.getroot()
    groups = []
    # get all configured plc
    for p in root:  # PLC
        my_list = []
        for data in p:
            sl = p.get('slot')
            if sl is None:
                sl = "1"
            m = my_data(p.text, data[0].text, data[1].text, data[2].text, data[3].text, data[4].text, sl)
            if eval(data[4].text):
                my_list.append(m)
        group = my_group(my_list)
        groups.append(group)
    return  groups


groups = create_my_data_groups()

for g in groups:
    for d in g.m_data_list:
        d.show()