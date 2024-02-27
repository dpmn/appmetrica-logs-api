from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional


class PostbacksSchema(BaseModel):
    """
    Постбеки.
    https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints#postback
    """
    # Трекинговая информация
    application_id: Optional[int] = Field(
        default=None,
        description='Уникальный числовой идентификатор приложения в AppMetrica.'
    )
    attributed_touch_type: Optional[int] = Field(
        default=None,
        description='Тип рекламного взаимодействия: click | impression | unknown.'
    )
    click_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время клика/показа в формате yyyy-mm-dd hh:mm:ss.'
    )
    click_id: Optional[str] = Field(
        default=None,
        description='Идентификатор клика/показа или '' (пустая строка, если недоступен).'
    )
    click_ipv6: Optional[str] = Field(
        default=None,
        description='IP-адрес в момент клика/показа в формате IPv6.'
    )
    click_timestamp: Optional[int] = Field(
        default=None,
        description='Время клика/показа в формате UNIX-time в секундах.'
    )
    click_url_parameters: Optional[str] = Field(
        default=None,
        description='Параметры, как они представлены в ссылке.'
    )
    click_user_agent: Optional[str] = Field(
        default=None,
        description='User-agent клика/показа.'
    )
    profile_id: Optional[int] = Field(
        default=None,
        description='Идентификатор пользовательского профиля.'
    )
    publisher_id: Optional[int] = Field(
        default=None,
        description='ID партнера в AppMetrica.'
    )
    publisher_name: Optional[str] = Field(
        default=None,
        description='Название партнёра AppMetrica.'
    )
    tracker_name: Optional[str] = Field(
        default=None,
        description='Название трекера, который добавляется в интерфейсе AppMetrica.'
    )
    tracking_id: Optional[int] = Field(
        default=None,
        description='ID трекера в AppMetrica.'
    )
    touch_type: Optional[str] = Field(
        default=None,
        description='Тип: клик или показ. Возможные значения: click | impression | unknown.'
    )
    # Информация об установке и re-engagement
    install_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время установки в формате yyyy-mm-dd hh:mm:ss.'
    )
    install_ipv6: Optional[str] = Field(
        default=None,
        description='IP-адрес в момент установки в формате IPv6.'
    )
    install_timestamp: Optional[int] = Field(
        default=None,
        description='Время установки в формате UNIX-time в секундах.'
    )
    match_type: Optional[str] = Field(
        default=None,
        description='Способ атрибуции: fingerprint | referrer | identifier | '' (пустая строка).'
    )
    # Информация об устройстве и ОС (определённая по клику)
    appmetrica_device_id: Optional[str] = Field(
        default=None,
        description='Уникальный идентификатор устройства, который устанавливает AppMetrica.'
    )
    city: Optional[str] = Field(
        default=None,
        description='Название города на английском языке, где был произвёден клик.'
    )
    country_iso_code: Optional[str] = Field(
        default=None,
        description='ISO-код страны.'
    )
    device_manufacturer: Optional[str] = Field(
        default=None,
        description='Производитель устройства, определяется сервисом AppMetrica.'
    )
    device_model: Optional[str] = Field(
        default=None,
        description='Модель устройства, определяется сервисом AppMetrica.'
    )
    device_type: Optional[str] = Field(
        default=None,
        description='Тип устройства, определяется сервисом AppMetrica. Возможные значения: phone | tablet | unknown.'
    )
    google_aid: Optional[str] = Field(
        default=None,
        description='Google AID устройства в формате, в котором получен от устройства.'
    )
    ios_ifa: Optional[str] = Field(
        default=None,
        description='IFA устройства в формате, в котором получен от устройства.'
    )
    ios_ifv: Optional[str] = Field(
        default=None,
        description='IFV для приложения в формате, в котором получен от устройства.'
    )
    windows_aid: Optional[str] = Field(
        default=None,
        description='Windows AID устройства в формате, в котором получен от устройства.'
    )
    oaid: Optional[str] = Field(
        default=None,
        description='Huawei OAID устройства в формате, в котором получен от устройства.'
    )
    os_name: Optional[str] = Field(
        default=None,
        description='Операционная система на устройстве пользователя: ios | android | windows.'
    )
    os_version: Optional[str] = Field(
        default=None,
        description='Версия операционной системы на устройстве пользователя.'
    )
    # Информация о приложении
    app_package_name: Optional[str] = Field(
        default=None,
        description='Имя пакета для Android или Bundle ID для iOS.'
    )
    app_version_name: Optional[str] = Field(
        default=None,
        description='Версия приложения в виде, как указана разработчиком.'
    )
    # Информация о событии
    conversion_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время события (для CPA postback), установки (для CPI postback) или re-engagement (для CPR postback) в формате yyyy-mm-dd hh:mm:ss.'
    )
    conversion_timestamp: Optional[int] = Field(
        default=None,
        description='Время события (для CPA postback), установки (для CPI postback) или re-engagement (для CPR postback) в формате UNIX-time.'
    )
    event_name: Optional[str] = Field(
        default=None,
        description='Имя события (как передано в SDK).'
    )
    # Информация о postback
    attempt_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время отправки в формате yyyy-mm-dd hh:mm:ss.'
    )
    attempt_timestamp: Optional[int] = Field(
        default=None,
        description='Время отправки в формате UNIX-time.'
    )
    cost_model: Optional[str] = Field(
        default=None,
        description='Тип постбека. Возможные значения: cpi (установка) | cpa (событие) | cpr (re-engagement).'
    )
    notifying_status: Optional[str] = Field(
        default=None,
        description='Статус отправки postback (failed | sent). Один и тот же postback может присутствовать в ответе несколько раз, если его отправка не была успешной.'
    )
    postback_url: Optional[str] = Field(
        default=None,
        description='Полный Postback URL.'
    )
    postback_url_parameters: Optional[str] = Field(
        default=None,
        description='Параметры Postback URL, как они представлены в URL.'
    )
    response_body: Optional[str] = Field(
        default=None,
        description='Данные, полученные от сервера.'
    )
    response_code: Optional[int] = Field(
        default=None,
        description='HTTP-код отправки postback.'
    )
