import xml.etree.ElementTree as ET
import re

class my_data():
    def __init__(self, plc , type , area , address, alias , active):
        self.m_plc = plc
        self.m_type = type
        self.m_area = area
        self.m_address = address
        self.m_alias = alias
        self.m_active = active
        self.m_value = 0.0

    def show(self):
        print("PLC IP: {0}, TYPE: {1}, AREA: {2}, ADDRESS: {3}, ALIAS: {4}, ACTIVE: {5}".format(
            self.m_plc, self.m_type, self.m_area, self.m_address, self.m_alias, self.m_active
        ))

# Function to extract all the numbers from the given string
def getNumbers(str):
    array = re.findall(r'[0-9]+', str)
    return array


def create_my_data():
    my_list = []
    tree = ET.parse('config.xml')
    root = tree.getroot()
    plc_list = []
    # get all configured plc
    for p in root:  # PLC
        for data in p:
            m = my_data(p.text, data[0].text, data[1].text, data[2].text, data[3].text, data[4].text)
            m.show()
            my_list.append(m)
    return  my_list




create_my_data()