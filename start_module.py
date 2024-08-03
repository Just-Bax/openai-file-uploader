import sys
import subprocess

installed_dependencies = subprocess.check_output(
    [sys.executable, '-m', 'pip', 'install', '-r', 'python_dependencies.ini']).decode().strip()
if 'Successfully installed' in installed_dependencies:
    raise Exception('Some required dependent libraries were installed. ' \
        'Module execution has to be terminated now to use installed libraries on the next scheduled launch.')

import json
import re
from jsonschema import validate
from onevizion import IntegrationLog, LogLevel
from module import Module


with open('settings.json', 'rb') as settings_file:
    settings_data = json.loads(settings_file.read().decode('utf-8'))

with open('settings_schema.json', 'rb') as settings_schema_file:
    data_schema = json.loads(settings_schema_file.read().decode('utf-8'))

try:
    validate(instance = settings_data, schema = data_schema)
except Exception as exceptiion:
    raise Exception(f'Incorrect value in the settings file\n{str(exceptiion)}') from exceptiion

with open('ihub_parameters.json', 'rb') as module_run:
    module_run_data = json.loads(module_run.read().decode('utf-8'))

module_log = IntegrationLog(
    module_run_data['processId'], 
    settings_data['ovUrl'], 
    settings_data['ovAccessKey'], 
    settings_data['ovSecretKey'], 
    None, 
    True, 
    module_run_data['logLevel'])

module = Module(module_log, settings_data)

try:
    module.start()
except Exception as e:
    module_log.add(LogLevel.ERROR, str(e))
    raise e
