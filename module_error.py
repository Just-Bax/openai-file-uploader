class ModuleError(Exception):
    def __init__(self, error_message: str, description):
        self._message = error_message
        self._description = description

    @property
    def message(self) -> str:
        return self._message

    @property
    def description(self) -> str:
        return self._description