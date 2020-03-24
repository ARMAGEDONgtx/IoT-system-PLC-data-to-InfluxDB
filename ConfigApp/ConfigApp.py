import xml.etree.ElementTree as ET
import PySimpleGUI as sg
import xml.dom.minidom
import os
try:
    from Tkinter import Tk
except ImportError:
    from tkinter import Tk

config_PATH = '/home/poziadmin/Documents/Python_projects/Linux/config.xml'


def remove_empty_lines(filename):
    if not os.path.isfile(filename):
        print("{} does not exist ".format(filename))
        return
    with open(filename) as filehandle:
        lines = filehandle.readlines()

    with open(filename, 'w') as filehandle:
        lines = filter(lambda x: x.strip(), lines)
        filehandle.writelines(lines)

def update_config(ip,slot,type,area,address_,alias,activ):
    root = None
    #check if file is not empty, if it's not - get actual root element
    try:
        tree = ET.parse(config_PATH)
        root = tree.getroot()
    except Exception as e:
        print(str(e))
        root = ET.Element("communication")

    #check if plc is already defined, if it is - append data tag
    duplicate = False
    for child in root:
        if child.text== ip:
            duplicate = True
            data = ET.SubElement(child, 'data')
            ET.SubElement(data, 'data_type').text = type
            ET.SubElement(data, 'data_area').text = area
            ET.SubElement(data, 'data_address').text = address_
            ET.SubElement(data, 'data_alias').text = alias
            ET.SubElement(data, 'active').text = activ

    #if plc is not duplicate - create new plc tag with other elements
    if not duplicate:
        plc = ET.SubElement(root,'plc')
        plc.text = ip
        plc.set('slot',slot)
        data = ET.SubElement(plc,'data')
        ET.SubElement(data,'data_type').text = type
        ET.SubElement(data,'data_area').text = area
        ET.SubElement(data,'data_address').text = address_
        ET.SubElement(data,'data_alias').text = alias
        ET.SubElement(data,'active').text = activ

    #save xml file
    tree = ET.ElementTree(root)
    tree.write(config_PATH)

def get_actual_plcs():
    tree = ET.parse(config_PATH)
    root = tree.getroot()
    plc_list = ['']
    # get all configured plc
    for r in root:  # PLC
        plc_list.append(r.text)
    if len(plc_list) == 0:
        return tuple()
    else:
        return tuple(plc_list)

def get_data_aliases(plc_name):
    tree = ET.parse(config_PATH)
    root = tree.getroot()
    all_list = []
    for plc in root:
        if plc.text == plc_name:
            for data in plc:
                all_list.append(data[3].text)
    if len(all_list) == 0:
        return tuple()
    else:
        return tuple(all_list)

def get_data_by_alias(plc_name, alias):
    tree = ET.parse(config_PATH)
    root = tree.getroot()
    all_list = []
    for plc in root:
        if plc.text == plc_name:
            for data in plc:
                if data[3].text == alias:
                    for d in data:
                        all_list.append(d.text)
    return all_list

def delete_element(plc_name, alias):
    tree = ET.parse(config_PATH)
    root = tree.getroot()
    for plc in root:
        if plc.text == plc_name:
            for data in plc:
                if data[3].text == alias:
                    plc.remove(data)
    #save xml file
    tree.write(config_PATH)

def update_element(plc_name, alias, param):
    tree = ET.parse(config_PATH)
    root = tree.getroot()
    for plc in root:
        if plc.text == plc_name:
            for data in plc:
                if data[3].text == alias:
                    data[0].text = param[0]
                    data[1].text = param[1]
                    data[2].text = param[2]
                    data[3].text = param[3]
                    data[4].text = param[4]
    #save xml file
    tree.write(config_PATH)



# -------- PROGRAM -----------------------------------------------------------------------------------------------------
#change to nicer theme
#sg.theme('Dark2')
tree = ET.parse(config_PATH)
root = tree.getroot()
ar = ['S7AreaPE','S7AreaPA','S7AreaMK','S7AreaDB','S7AreaCT','S7AreaTM']
areas = tuple(ar)
ty = ['S7WLBit','S7WLByte','S7WLWord','S7WLDWord','S7WLReal','S7WLCounter','S7WLTimer']
types = tuple(ty)

#prepare layout
data_row = [sg.Input('0.0.0.0',size=(15,1),key='PLC_NEW',tooltip='enter PLC IP address'),
            sg.Input('1',size=(2,1),key="SLOT_NEW",tooltip='enter plc slot number'),
            sg.Drop(values=areas,key='AREA_NEW',size=(10,1),tooltip='select data area',readonly=True,default_value=areas[0]),
            sg.Drop(values=types,key='TYPE_NEW',size=(14,1),tooltip='select data type',readonly=True,default_value=types[0]),
            sg.Input('0',size=(10,1),key='ADR_NEW',tooltip='enter data address'),
            sg.Input('data_alias',size=(20,1),key='ALIAS_NEW',tooltip='enter data alias'),sg.Checkbox('Active',key='ACTIVATE_NEW')]
column0 = [
    [sg.Text('New entry',font=("Helvetica", 25))],
    [sg.Text('Update below fields with desiered configuration to fetch data from PLC')],
    data_row.copy(),
    [sg.Button("Add"),sg.Button('Clear')],
    [sg.Text('Edit actual entries',font=("Helvetica", 25))],
    [sg.Text('Select PLC IP address to get configured data aliases, then choose one to get all parameters connected with it')]]

plc_list = []
alias_list = []
plc_list = get_actual_plcs()
column1=[[sg.Text('PLC IP address\t\t\tData alias')],
         [sg.DropDown(values=tuple(plc_list),size=(15,1),key='PLC',tooltip='select PLC to find information',readonly=True,default_value=plc_list[0]),sg.Button('Find aliases'),
          sg.DropDown(values=tuple(),size=(15,1),key='ALIAS',tooltip='select alias to find information'),sg.Button('Find data')]]

# add row to enable editing row associated with the selected alias
data_row = [sg.Drop(values=areas, auto_size_text=True,key='AREA_EDIT',size=(10,1),tooltip='update data area',readonly=True),
            sg.Drop(values=types, key='TYPE_EDIT',size=(14,1),tooltip='update data type',readonly=True),
            sg.Input('', size=(15, 1),key='ADR_EDIT',tooltip='update data address'),
            sg.Input('', size=(20, 1),key='ALIAS_EDIT',tooltip='update data alias'), sg.Checkbox('Active',key='ACTIVATE_EDIT')]
column1.append(data_row)
# add buttons to enable actions
column1.append([sg.Button("Update"), sg.Button('Delete')])
column2 = [[sg.Image('xml.png',pad=(200,0))],
               [sg.Button("Show raw configuration file"), sg.Button('Exit',)]]


main_layout = [[sg.Column(column0)],[sg.Column(column1)],
               [sg.Column(column2)]]

window = sg.Window('Configuration').Layout(main_layout)

data_plc = None
#main gui loop
while(1):
    button, values = window.Read()
    if button == None or button == "Exit":
        break
    elif button == "Add":
        update_config(values['PLC_NEW'],values['SLOT_NEW'],values['TYPE_NEW'],values['AREA_NEW'],values['ADR_NEW'],values['ALIAS_NEW'],str(values['ACTIVATE_NEW']))
        plcs = get_actual_plcs()
        window.find_element('PLC').update(values=plcs)
    elif button == "Clear":
        window.find_element('PLC_NEW').update("")
        window.find_element('SLOT_NEW').update("")
        window.find_element('TYPE_NEW').update("")
        window.find_element('AREA_NEW').update("")
        window.find_element('ADR_NEW').update("")
        window.find_element('ALIAS_NEW').update("")
        window.find_element('ACTIVATE_NEW').update(False)
    elif button == "Show raw configuration file":
        #get contect of xml file
        tree = ET.parse(config_PATH)
        root = tree.getroot()
        xml_str = ET.tostring(root,encoding='utf-8',method='xml')
        reparsed = xml.dom.minidom.parseString(xml_str)
        pretty = reparsed.toprettyxml(encoding='utf-8')
        conf_layout = [[sg.Multiline(pretty,size=(100,20))]]
        window_config = sg.Window('Raw configuration').Layout(conf_layout)
        conf_button, conf_val = window_config.Read(timeout=0)
        if button == None:
            window_config.close()

    elif button == 'Find aliases':
        aliases_plc = get_data_aliases(values['PLC'])
        window.find_element('ALIAS').update(values=aliases_plc)

    elif button == 'Find data':
        data_plc = get_data_by_alias(values['PLC'], values['ALIAS'])
        # check if there is any data for choosen plc and alias
        if len(data_plc):
            window.find_element('TYPE_EDIT').update(data_plc[0])
            window.find_element('AREA_EDIT').update(data_plc[1])
            window.find_element('ADR_EDIT').update(data_plc[2])
            window.find_element('ALIAS_EDIT').update(data_plc[3])
            window.find_element('ACTIVATE_EDIT').update(eval(data_plc[4]))
        else:
            window.find_element('TYPE_EDIT').update("")
            window.find_element('AREA_EDIT').update("")
            window.find_element('ADR_EDIT').update("")
            window.find_element('ALIAS_EDIT').update("")
            window.find_element('ACTIVATE_EDIT').update(False)
    elif button == 'Update':
        params = [values['TYPE_EDIT'], values['AREA_EDIT'],values['ADR_EDIT'],values['ALIAS_EDIT'],str(values['ACTIVATE_EDIT'])]
        update_element(values['PLC'], values['ALIAS'], params)
        aliases_plc = get_data_aliases(values['PLC'])
        window.find_element('ALIAS').update(values=aliases_plc)
    elif button == 'Delete':
        delete_element(values['PLC'], values['ALIAS'])
        aliases_plc = get_data_aliases(values['PLC'])
        window.find_element('ALIAS').update(values=aliases_plc)






