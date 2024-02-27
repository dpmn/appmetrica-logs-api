from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional


class DeeplinksSchema(BaseModel):
    """
    Открытия приложения через deeplink.
    https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints#deeplinks
    """
    # Трекинговая информация
    deeplink_url_host: Optional[str] = Field(
        default=None,
        description='Доменное имя deeplink.'
    )
    deeplink_url_parameters: Optional[str] = Field(
        default=None,
        description='Параметры, которые передаются в deeplink.'
    )
    deeplink_url_path: Optional[str] = Field(
        default=None,
        description='URL-путь deeplink.'
    )
    deeplink_url_scheme: Optional[str] = Field(
        default=None,
        description='URL-схема deeplink.'
    )
    event_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время события в формате yyyy-mm-dd hh:mm:ss.'
    )
    event_timestamp: Optional[int] = Field(
        default=None,
        description='Время события в формате UNIX-time.'
    )
    event_receive_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время получения сервером события.'
    )
    event_receive_timestamp: Optional[int] = Field(
        default=None,
        description='Время получения сервером события в формате UNIX-time.'
    )
    is_reengagement: Optional[bool] = Field(
        default=None,
        description='Признак, который определяет, что трекер создан для ремаркетинг-кампании.'
    )
    profile_id: Optional[str] = Field(
        default=None,
        description='Идентификатор пользовательского профиля.'
    )
    publisher_id: Optional[int] = Field(
        default=None,
        description='ID партнёра в AppMetrica.'
    )
    publisher_name: Optional[str] = Field(
        default=None,
        description='Название партнера AppMetrica.'
    )
    session_id: Optional[int] = Field(
        default=None,
        description='Идентификатор сессии.'
    )
    tracker_name: Optional[str] = Field(
        default=None,
        description='Название трекера, который добавляется в интерфейсе AppMetrica.'
    )
    tracking_id: Optional[int] = Field(
        default=None,
        description='ID трекера в AppMetrica.'
    )
    # Информация об устройстве и ОС
    android_id: Optional[str] = Field(
        default=None,
        description='Идентификатор Android.'
    )
    appmetrica_device_id: Optional[str] = Field(
        default=None,
        description='Уникальный идентификатор устройства, который устанавливает AppMetrica.'
    )
    appmetrica_sdk_version: Optional[int] = Field(
        default=None,
        description='Версия AppMetrica SDK.'
    )
    city: Optional[str] = Field(
        default=None,
        description='Название города на английском языке, где был произвёден клик.'
    )
    connection_type: Optional[str] = Field(
        default=None,
        description='Тип подключения устройства. Возможные значения: wifi | cell | unknown.'
    )
    country_iso_code: Optional[str] = Field(
        default=None,
        description='ISO-код страны.'
    )
    device_ipv6: Optional[str] = Field(
        default=None,
        description='IP-адрес в момент совершения события в формате IPv6.'
    )
    device_locale: Optional[str] = Field(
        default=None,
        description='Язык интерфейса устройства.'
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
    original_device_model: Optional[str] = Field(
        default=None,
        description='Заводская модель устройства.'
    )
    os_version: Optional[str] = Field(
        default=None,
        description='Версия операционной системы на устройстве пользователя.'
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
    mcc: Optional[int] = Field(
        default=None,
        description='Мобильный код страны.'
    )
    mnc: Optional[int] = Field(
        default=None,
        description='Код мобильной сети.'
    )
    # Информация о приложении
    app_build_number: Optional[int] = Field(
        default=None,
        description='Номер сборки приложения.'
    )
    app_package_name: Optional[str] = Field(
        default=None,
        description='Имя пакета для Android или Bundle ID для iOS.'
    )
    app_version_name: Optional[str] = Field(
        default=None,
        description='Версия приложения в виде, как указана разработчиком.'
    )
