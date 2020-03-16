import xml.etree.ElementTree as ET
import copy
import logging
from datetime import datetime
import threading
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

    #interactive console to the opc ua server
    def embed():
        myvars = globals()
        myvars.update(locals())
        shell = code.InteractiveConsole(myvars)
        shell.interact()

from opcua import ua, uamethod, Server


#get variables from configuration and setup server
# create my_data objects, group by PLC
def create_my_data_groups(server):
    my_lock = multiprocessing.Lock()
    # open xml config
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
        group = my_data.my_group(my_list,my_lock)
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

#simple variable updater (not for multiprocessing)
class VarUpdater(threading.Thread):
    def __init__(self, server, pipe):
        threading.Thread.__init__(self)
        self._stopev = False
        self.server = server
        self.pipe = pipe

    def stop(self):
        self._stopev = True

    #recive data through pipe and update server variable
    def run(self):
        while not self._stopev:
            var = self.pipe.recv()
            val = self.pipe.recv()
            opc_var = server.get_node(var)
            opc_var.set_value(val)
            print(var)



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

# Initialize OPC UA server with variables
#create my_groups to update values later
groups = create_my_data_groups(server)


def data_process_group(no, pipe):
    groups[no].update_items(pipe)

def update_server_vars(pipe , server):
    while True:
        var = pipe.recv()
        val = pipe.recv()
        opc_var = server.get_node(var)
        opc_var.set_value(val)


if __name__ == "__main__":

    # creating a default event object
    # The event object automatically will have members for all events properties
    # you probably want to create a custom event type, see other examples
    myevgen = server.get_event_generator()
    myevgen.event.Severity = 300

    # starting!
    server.start()


    print("Available loggers are: ", logging.Logger.manager.loggerDict.keys())
    jobs = []
    vups = []
    try:
        # create variable updater for each group and start it
        no = 0
        for g in groups:
            pipe1, pipe2 = multiprocessing.Pipe()
            process = multiprocessing.Process(target=data_process_group, args=(no, pipe1,))
            jobs.append(process)
            vup = VarUpdater(server,pipe2).start()
            vups.append(vup)
            no = no + 1
        for j in jobs:
            j.start()
            time.sleep(0.5)
        #embed()
    except Exception as e:
        print(str(e))
    finally:
        # stop all variable updaters
        for g in groups:
            g.stop()
        for j in jobs:
            j.join()
        for v in vups:
            v.stop()
        server.stop()
