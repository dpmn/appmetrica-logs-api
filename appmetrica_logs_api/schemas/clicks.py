from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional


class ClicksSchema(BaseModel):
    """
    Трекинговые клики и показы.
    https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints#clicks
    """
    # Трекинговая информация
    application_id: Optional[int] = Field(
        default=None,
        description='Уникальный числовой идентификатор приложения в AppMetrica.'
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
    is_bot: Optional[bool] = Field(
        default=None,
        description='Признак клика, совершённого не из браузера.'
    )
    # Информация об устройстве и ОС (определённая по клику)
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
