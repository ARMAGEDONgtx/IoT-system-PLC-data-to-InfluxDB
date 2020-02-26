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
import datetime

class my_data():
    def __init__(self, plc , type , area , address, alias , active, slot):
        self.m_plc = plc
        self.m_type = type
        self.m_area = area
        self.m_address = address
        self.m_alias = alias
        self.m_active = active
        self.m_slot = slot

    def show(self):
        print("PLC IP: {0}, TYPE: {1}, AREA: {2}, ADDRESS: {3}, ALIAS: {4}, ACTIVE: {5}".format(
            self.m_plc, self.m_type, self.m_area, self.m_address, self.m_alias, self.m_active
        ))

# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array

def create_my_data():
    tree = ET.parse('C:/config.xml')
    root = tree.getroot()
    list_plc = []
    # get all configured plc
    for p in root:  # PLC
        tmp_list = []
        for data in p:
            sl = p.get('slot')
            if sl is None:
                sl = "1"
            m = my_data(p.text, data[0].text, data[1].text, data[2].text, data[3].text, data[4].text, sl)
            #m.show()
            if eval(data[4].text):
                tmp_list.append(m)
        list_plc.append(tmp_list)
    return list_plc

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


class TestService(win32serviceutil.ServiceFramework):
    _svc_name_ = "InfluxConnector2.0"
    _svc_display_name_ = "InfluxConnector2.0"

    host = '10.14.12.83'
    port = 8086
    user = 'poziadmin'
    password = 'QpAlZm1!'
    db_name='PLC2InfluxDB'

    def main_loop(self):
        for plc in self.my_list:
            self.plc.connect(plc[0].m_plc, 0, eval(plc[0].m_slot))
            for data in plc:
                address = getNumbers(data.m_address)
                if len(address) > 0:  # assure tere was some address
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
                    elif (eval(data.m_area) == S7AreaPA or eval(data.m_area) == S7AreaPE or eval(data.m_area) == S7AreaMK) and len(address) >= 1:  # Memory / In Out
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
                # send data do InfluxDB
                json_body1 = create_my_json(data.m_plc, data.m_alias, value)
                self.client.write_points(json_body1)
            self.plc.disconnect()


    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.plc.disconnect()
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        rc = None
        try:
            self.my_list = create_my_data()
            self.plc = snap7.client.Client()
            self.client = influxdb.InfluxDBClient(self.host, self.port, self.user, self.password, self.db_name)
            self.client.create_database(self.db_name)
        except Exception as e:
            with open('C:\\InfluxDBService.log', 'a') as f:
                f.write(str(e)+'\n')
        while rc != win32event.WAIT_OBJECT_0:
            try:
                self.main_loop()
            except Exception as e:
                with open('C:\\InfluxDBService.log', 'a') as f:
                    f.write(str(e) + '\n')
            rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TestService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TestService)