import xml.etree.ElementTree as ET
import copy
import logging
from datetime import datetime
import time
import sys
import random
import asyncio
import my_data
import multiprocessing
import opcua

sys.path.insert(0, "..")

try:
    from IPython import embed
except ImportError:
    import code

    def embed():
        myvars = globals()
        myvars.update(locals())
        shell = code.InteractiveConsole(myvars)
        shell.interact()

from opcua import ua, uamethod, Server


def config_to_opc_nodes(server):
    #open xml config
    tree = ET.parse('config.xml')
    root = tree.getroot()
    # namespace number, for nodeid creation
    ns = 2
    # get all configured plc
    for p in root:  # PLC
        # First a folder to organise our nodes and setup our own namespace
        idx = server.register_namespace(p.text)
        myfolder = server.nodes.objects.add_folder(idx, p.text)
        # table for varaible grouping and updating
        group = []
        for data in p:
            dev = myfolder.add_object(idx, data[3].text)
            var1 = dev.add_variable(idx, "PLC", p.text)
            var2 = dev.add_variable(idx, "Type", data[0].text)
            var4 = dev.add_variable(idx, "Address", data[2].text)
            nodeid = "ns={0};s={1}".format(ns,data[3].text)
            var6 = dev.add_variable(nodeid, "Value", 0.0)
            var6.set_writable()
            group.append(var6)
        #next plc, next namespace -> increment ns
        ns = ns + 1


# create my_data objects, group by PLC
def create_my_data_groups(server):
    global var2
    tree = ET.parse('config.xml')
    root = tree.getroot()
    # namespace number, for nodeid creation
    ns = 2
    groups = []
    # get all configured plc
    for p in root:  # PLC
        my_list = []
        # First a folder to organise our nodes and setup our own namespace
        idx = server.register_namespace(p.text)
        myfolder = server.nodes.objects.add_folder(idx, p.text)
        # table for varaible grouping and updating
        for data in p:
            # OPC UA PART ------------------------------------
            dev = myfolder.add_object(idx, data[3].text)
            dev.add_variable(idx, "PLC", p.text)
            dev.add_variable(idx, "Type", data[0].text)
            dev.add_variable(idx, "Address", data[2].text)
            nodeid = "ns={0};s={1}".format(ns, data[3].text)
            var1 = dev.add_variable(nodeid, "Value", 0.0)
            var1.set_writable()
            # ------------------------------------------------
            sl = p.get('slot')
            if sl is None:
                sl = "1"
            m = my_data.my_data(p.text, data[0].text, data[1].text, data[2].text, data[3].text, data[4].text, sl)
            #add nodid - will be updated in another process by nodeid
            m.m_opcua_var = nodeid
            if eval(data[4].text):
                my_list.append(m)
        # next plc, next namespace -> increment ns
        ns = ns + 1
        group = my_data.my_group(my_list)
        groups.append(group)
    return groups


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """
    def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)

    def event_notification(self, event):
        print("Python: New event", event)


# method to be exposed through server
def func(parent, variant):
    ret = False
    if variant.Value % 2 == 0:
        ret = True
    return [ua.Variant(ret, ua.VariantType.Boolean)]


# method to be exposed through server
# uses a decorator to automatically convert to and from variants
@uamethod
def multiply(parent, x, y):
    print("multiply method call with parameters: ", x, y)
    return x * y


class VarUpdater():
    def __init__(self, vars):
        self._stopev = False
        self.vars = vars

    def stop(self):
        self._stopev = True

    async def run(self):
        while not self._stopev:
            for v in self.vars:
                v.set_value(random.uniform(0.0,100.0))
                #print(v)
                await asyncio.sleep(0.05)


# optional: setup logging
logging.basicConfig(level=logging.WARN)
# now setup our server
server = Server()
# server.disable_clock()
server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
server.set_server_name("M.W opcua-influxdb")
# set all possible endpoint policies for clients to connect through
server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity,
        ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ua.SecurityPolicyType.Basic256Sha256_Sign])

# creating a default event object
# The event object automatically will have members for all events properties
# you probably want to create a custom event type, see other examples
myevgen = server.get_event_generator()
myevgen.event.Severity = 300

# starting!
server.start()

# Initialize OPC UA server with variables
#create my_groups to update values later
groups = create_my_data_groups(server)


def test():
    groups[0].sim_update()



if __name__ == "__main__":

    print("Available loggers are: ", logging.Logger.manager.loggerDict.keys())
    jobs = []
    #groups[1].sim_update()
    try:
        # create variable updater for each group and start it
        for g in groups:
            process = multiprocessing.Process(target=test) # blokada
            jobs.append(process)
        for j in jobs:
            j.start()
            print(j.get())
        embed()
    finally:
        # stop all variable updaters
        for g in groups:
            g.stop()
        for j in jobs:
            j.join()
        server.stop()
