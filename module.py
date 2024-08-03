from onevizion import LogLevel


class Module:

    def __init__(self, ov_module_log: IntegrationLog, settings_data: dict):
        self._module_log = ov_module_log
        self._settings = settings_data
        
    def start(self):
        self._module_log.add(LogLevel.INFO, 'Module is started')
