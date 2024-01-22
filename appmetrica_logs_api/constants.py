class APIResources(object):
    """
    Ресурсы API AppMetrica.
    Подробнее: https://appmetrica.yandex.ru/docs/mobile-api/logs/ref/index.html
    """
    CLICKS = 'clicks'
    INSTALLATIONS = 'installations'
    POSTBACKS = 'postbacks'
    EVENTS = 'events'
    PROFILES = 'profiles'
    REVENUE_EVENTS = 'revenue_events'
    DEEPLINKS = 'deeplinks'
    CRASHES = 'crashes'
    ERRORS = 'errors'
    PUSH_TOKENS = 'push_tokens'
    SESSIONS_STARTS = 'sessions_starts'


class ExportFormat(object):
    """
    Формат экспорта данных: csv или json.
    """
    CSV = 'csv'
    JSON = 'json'


class DateDimension(object):
    """
    Определяет, относительно какого события считается дата:
    default — относительно момента, когда событие произошло на устройстве пользователя;
    receive — относительно момента, когда информация о событии была получена сервером.
    """
    DEFAULT = 'default'
    RECEIVE = 'receive'
