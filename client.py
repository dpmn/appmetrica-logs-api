import time
from schemas import schemas
from requests import Session


API_HOST = 'https://api.appmetrica.yandex.ru'
API_URL = 'logs/v1/export'
API_ENDPOINT = '/'.join([API_HOST, API_URL])


class AppMetrica:
    def __init__(self, access_token) -> None:
        self._req_session = Session()
        self._req_session.headers.update({
            'Authorization': f'OAuth {access_token}'
        })
    
    def export(self, point: str, application_id: str, date_from: str = None, date_to: str = None, fields: list = None):
        report_fields = ','.join(fields) if fields is not None else ','.join([field['name'] for field in schemas[point]])

        req_point = '/'.join([API_ENDPOINT, f'{point}.json'])
        params = {
            'application_id': application_id,
            'fields': report_fields
        }

        if date_from and date_to:
            params['date_since'] = date_from + ' 00:00:00'
            params['date_until'] = date_to + ' 23:59:59'

        response = self._req_session.get(url=req_point, params=params)
        response.encoding = 'utf-8'

        if response.status_code == 202:
            print(f'Response ({req_point}) 202: waiting...')

            time.sleep(30)
            return self.export(
                point=point,
                application_id=application_id,
                date_from=date_from,
                date_to=date_to,
                fields=report_fields.split(',')
            )
        elif response.status_code == 200:
            print(f'Response ({req_point}) 200')
            return response.json()
        else:
            print(f'Response ({req_point}) {response.status_code}: {response.text}')
            return {'data': []}
    
    # Кастомный метод для получения описания схем данных точек запроса
    @staticmethod
    def get_schema(point: str):
        return schemas.get(point, [])
