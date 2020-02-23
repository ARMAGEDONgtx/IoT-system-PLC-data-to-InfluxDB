#!C:\Users\WrobleM\PycharmProjects\PP\OPC\venv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'opcua==0.98.9','console_scripts','uadiscover'
__requires__ = 'opcua==0.98.9'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('opcua==0.98.9', 'console_scripts', 'uadiscover')()
    )
