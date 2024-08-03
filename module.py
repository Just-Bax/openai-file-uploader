from typing import Optional, Any, Dict, List
import re
import io

from onevizion import Trackor, IntegrationLog, LogLevel
from module_error import ModuleError
from openai import OpenAI


class Uploader:
    def __init__(self, api_key: str, purpose: str = 'assistants') -> None:
        self.client = OpenAI(api_key=api_key)
        self.purpose = purpose

    def upload_file(self, file_data: str, file_name: Optional[str]) -> str:
        file_io = io.BytesIO(file_data.encode('utf-8'))
        if file_name:
            file_io.name = file_name
        response = self.client.files.create(
            file=file_io,
            purpose=self.purpose,
        )
        return response.id


class OVAccessParameters:
    REGEXP_PROTOCOLS = r'^(https|http)://'

    def __init__(self, ov_url: str, ov_access_key: str, ov_secret_key: str) -> None:
        self.ov_url_without_protocol = re.sub(self.REGEXP_PROTOCOLS, '', ov_url).strip('/')
        self.ov_access_key = ov_access_key
        self.ov_secret_key = ov_secret_key


class OVTrackor:
    def __init__(self, ov_access_parameters: OVAccessParameters) -> None:
        self._ov_url_without_protocol = ov_access_parameters.ov_url_without_protocol
        self._ov_access_key = ov_access_parameters.ov_access_key
        self._ov_secret_key = ov_access_parameters.ov_secret_key
        self._trackor_wrapper = Trackor()

    @property
    def trackor_wrapper(self) -> Trackor:
        return self._trackor_wrapper

    @trackor_wrapper.setter
    def trackor_wrapper(self, trackor_type_name: str) -> None:
        self._trackor_wrapper = Trackor(
            trackorType=trackor_type_name,
            URL=self._ov_url_without_protocol,
            userName=self._ov_access_key,
            password=self._ov_secret_key,
            isTokenAuth=True,
        )

    def get_trackors_by_filters(self, fields: List[str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.trackor_wrapper.read(fields=fields, filters=filters)
        errors = self.trackor_wrapper.errors
        if len(errors) > 0:
            raise ModuleError(
                'Failed to get trackors',
                f'fields={fields}, filters={filters}, errors={errors}',
            )
        return list(self.trackor_wrapper.jsonData)

    def update_fields_by_trackor_id(self, trackor_id: int, fields: Dict[str, Any]) -> Dict[str, Any]:
        self.trackor_wrapper.update(trackorId=trackor_id, fields=fields)
        errors = self.trackor_wrapper.errors
        if len(errors) > 0:
            raise ModuleError(
                'Failed to update trackor fields',
                f'trackor_id={trackor_id}, fields={fields}, errors={errors}',
            )
        return self.trackor_wrapper.jsonData


class Module:
    TRACKOR_ID = 'TRACKOR_ID'

    def __init__(self, ov_module_log: IntegrationLog, settings_data: dict):
        self._module_log = ov_module_log
        self._ov_access_parameters = OVAccessParameters(
            settings_data['ovUrl'],
            settings_data['ovAccessKey'],
            settings_data['ovSecretKey'],
        )
        self._ov_trackor = OVTrackor(self._ov_access_parameters)
        self._uploader = Uploader(settings_data['openAIApiKey'])
        self._trackor_type = settings_data['trackorType']
        self._file_field = settings_data['fileField']
        self._file_id_field = settings_data['fileIdField']
        self._load_checkbox_field = settings_data['loadCheckboxField']

    def start(self):
        self._module_log.add(LogLevel.INFO, 'Module is started')
        self._ov_trackor.trackor_wrapper = self._trackor_type
        read_fields = [self._file_field]
        read_filters = {
            self._load_checkbox_field: '1'
        }
        trackors = self._ov_trackor.get_trackors_by_filters(
            fields=read_fields,
            filters=read_filters,
        )
        for trackor in trackors:
            try:
                trackor_id = trackor[self.TRACKOR_ID]
                file = trackor[self._file_field]
                if file:
                    file_data = file["data"]
                    file_name = file["file_name"]
                    file_id = self._uploader.upload_file(file_data, file_name)
                    update_fields = {
                        self._load_checkbox_field: '0',
                        self._file_id_field: file_id,
                    }
                    self._ov_trackor.update_fields_by_trackor_id(
                        trackor_id=trackor_id,
                        fields=update_fields,
                    )
                    self._module_log.add(
                        LogLevel.INFO,
                        'The file has been successfully uploaded',
                        f'trackor_id={trackor_id}, file_id={file_id}',
                    )
            except ModuleError as error:
                self._module_log.add(
                    LogLevel.ERROR,
                    'The file could not be uploaded',
                    f'trackor_id={trackor_id}, error={error}',
                )
