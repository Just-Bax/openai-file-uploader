import subprocess
import sys

installed_dependencies = (
    subprocess.check_output([sys.executable, '-m', 'pip', 'install', '-r', 'python_dependencies.ini']).decode().strip()
)
if 'Successfully installed' in installed_dependencies:
    raise Exception(
        'Some required dependent libraries were installed. '
        'Module execution has to be terminated now to use installed libraries on the next scheduled launch.'
    )

from onevizion import IntegrationLog, LogLevel
from module_error import ModuleError
from jsonschema import validate
from module import Module
import json
import re

with open('settings.json', 'rb') as settings_file:
    settings_data = json.loads(settings_file.read().decode('utf-8'))

with open('settings_schema.json', 'rb') as settings_schema_file:
    settings_schema = json.loads(settings_schema_file.read().decode('utf-8'))

try:
    validate(instance=settings_data, schema=settings_schema)
except Exception as exception:
    raise Exception(f'Incorrect value in the settings file: {str(exception)}') from exception

with open('ihub_parameters.json', 'rb') as module_run:
    module_run_data = json.loads(module_run.read().decode('utf-8'))

REGEXP_PROTOCOLS = r'^(https|http)://'
url_without_protocol = re.sub(REGEXP_PROTOCOLS, '', settings_data['ovUrl']).strip('/')

module_log = IntegrationLog(
    processId=module_run_data['processId'],
    URL=url_without_protocol,
    userName=settings_data['ovAccessKey'],
    password=settings_data['ovSecretKey'],
    isTokenAuth=True,
    logLevelName=module_run_data['logLevel'],
)

module = Module(module_log, settings_data)
try:
    module.start()
except ModuleError as module_error:
    module_log.add(LogLevel.ERROR, str(module_error))
    raise module_error
