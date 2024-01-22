from time import sleep
from datetime import datetime
from requests import request as http_request
from requests.exceptions import ConnectionError

from appmetrica_logs_api.constants import APIResources
from appmetrica_logs_api.schemas.events import EventsSchema
from appmetrica_logs_api.schemas.installations import InstallationsSchema

from appmetrica_logs_api.exceptions import AppmetricaClientError, AppmetricaApiError


RESOURCES_SCHEMA = {
    APIResources.EVENTS: EventsSchema,
    APIResources.INSTALLATIONS: InstallationsSchema,
}


class AppMetrica:
    def __init__(self, app_token: str) -> None:
        self.__app_token = app_token
        self._api_endpoint = 'https://api.appmetrica.yandex.ru/logs/v1/export'

    def _make_request(self, url: str, params: dict, headers: dict):
        """
        Общая функция отправки запросов к API.
        :param url: Конечная точка запроса.
        :param params: Параметры запросы.
        :param headers: Заголовки запроса.
        :return:
        """
        # Параметры для регулирования скорости выполнения запросов на экспорт
        retry_count = 0
        base_delay = 10  # секунды

        headers.update({
            'Authorization': f'OAuth {self.__app_token}'
        })

        while True:
            try:
                response = http_request('GET', url=url, params=params, headers=headers)

                if response.status_code == 200:
                    return response
                elif response.status_code in (201, 202):
                    # Увеличение задержки с каждой неудачной попыткой
                    retry_count += 1
                    sleep(base_delay * 2 ^ retry_count)
                else:
                    raise AppmetricaApiError(response.text)
            except ConnectionError:
                raise AppmetricaClientError(ConnectionError)

    def export(self, resource: str, application_id: str, fields: list[str] = None,
               date_from: datetime = None, date_to: datetime = None, **kwargs):
        """
        Экспорт данных из ресурса.
        :param resource: Название ресурса.
        :param application_id: Идентификатор приложения в AppMetrica.
        :param fields: Список полей для выборки. Если не задан, запрашиваются все доступные поля ресурса.
        :param date_from: Начало интервала дат в формате yyyy-mm-dd hh:mm:ss.
        :param date_to: Конец интервала дат в формате yyyy-mm-dd hh:mm:ss.
        :param kwargs: Другие параметры ресурса и заголовков (Cache-Control и Accept-Encoding) в формате snake_case.
        Также доступен кастомный параметр export_format, который определяет формат данных (csv/json).
        :return:
        """
        # Формат даты и времени, требуемый для параметров запроса.
        dt_format = '%Y-%m-%d %H:%M:%S'
        # Формат данных
        export_format = kwargs.pop('export_format', 'csv')

        api_url = '/'.join([self._api_endpoint, f'{resource}']) + f'.{export_format}'

        if resource in RESOURCES_SCHEMA.keys():
            fields = ','.join(list(RESOURCES_SCHEMA[resource].model_fields.keys())) if fields is None else ','.join(fields)
        else:
            raise AppmetricaClientError(f'Ресурс {resource} не доступен для экспорта.')

        headers = {}
        # Отвечает за то, будет сформирован новый файл при повторном запросе или отдан сформированный ранее.
        if cache_control := kwargs.pop('cache_control', None):
            headers.update({'Cache-Control': cache_control})
        # Сжатие gzip.
        if accept_encoding := kwargs.pop('accept_encoding', None):
            headers.update({'Accept-Encoding': accept_encoding})

        params = {
            'application_id': application_id,
            'fields': fields,
            **kwargs
        }

        # Для всех ресурсов, кроме profiles и push_tokens надо указать диапазон дат.
        if resource not in ('profiles', 'push_tokens'):
            if all([date_from, date_to]):
                params.update({'date_since': date_from.strftime(dt_format), 'date_until': date_to.strftime(dt_format)})
            else:
                raise AppmetricaClientError(f'Для ресурса {resource} требуется указать диапазон дат - '
                                            f'параметры date_from и date_to')

        response = self._make_request(api_url, params, headers)

        if export_format == 'csv':
            return response.text
        else:
            return response.json()
