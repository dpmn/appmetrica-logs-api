from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional


class SessionsStartsSchema(BaseModel):
    """
    Начало сессий.
    https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints#sessions_starts
    """
    # Информация о сессиях
    session_id: Optional[int] = Field(
        default=None,
        description='Идентификатор сессии.'
    )
    session_start_datetime: Optional[datetime] = Field(
        default=None,
        description='Дата и время начала сессии в формате yyyy-mm-dd hh:mm:ss.'
    )
    session_start_timestamp: Optional[int] = Field(
        default=None,
        description='Время начала сессии в формате UNIX-time.'
    )
    session_start_receive_datetime: Optional[datetime] = Field(
        default=None,
        description='Время получения сервером сообщения о начале сессии в формате yyyy-mm-dd hh:mm:ss.'
    )
    session_start_receive_timestamp: Optional[int] = Field(
        default=None,
        description='Время получения сервером сообщения о начале сессии в формате UNIX-time.'
    )
    # Информация об устройстве и ОС
    appmetrica_device_id: Optional[str] = Field(
        default=None,
        description='Уникальный идентификатор устройства, который устанавливает AppMetrica.'
    )
    connection_type: Optional[str] = Field(
        default=None,
        description='Тип подключения устройства. Возможные значения: wifi | cell | unknown.'
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
    original_device_model: Optional[str] = Field(
        default=None,
        description='Заводская модель устройства.'
    )
    device_ipv6: Optional[str] = Field(
        default=None,
        description='IP-адрес в момент совершения события в формате IPv6.'
    )
    device_locale: Optional[str] = Field(
        default=None,
        description='Язык интерфейса устройства.'
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
    mcc: Optional[int] = Field(
        default=None,
        description='Мобильный код страны.'
    )
    mnc: Optional[int] = Field(
        default=None,
        description='Код мобильной сети.'
    )
    operator_name: Optional[str] = Field(
        default=None,
        description='Имя оператора сотовой связи.'
    )
    os_name: Optional[str] = Field(
        default=None,
        description='Операционная система на устройстве пользователя: ios | android | windows.'
    )
    os_version: Optional[str] = Field(
        default=None,
        description='Версия операционной системы на устройстве пользователя.'
    )
    profile_id: Optional[str] = Field(
        default=None,
        description='Идентификатор пользовательского профиля.'
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
    application_id: Optional[int] = Field(
        default=None,
        description='Уникальный числовой идентификатор приложения в AppMetrica.'
    )
