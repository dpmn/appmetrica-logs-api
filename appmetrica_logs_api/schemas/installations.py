from datetime import datetime
from pydantic import BaseModel, Field


class InstallationsSchema(BaseModel):
    """
    Установки приложения.
    https://appmetrica.yandex.ru/docs/mobile-api/logs/endpoints.html#installations
    """
    application_id: int | None = Field(
        description='Уникальный числовой идентификатор приложения в AppMetrica.'
    )
    installation_id: str | None = Field(
        description='Идентификатор установки.'
    )
    attribution_touch_type: str | None = Field(
        description='Тип рекламного взаимодействия: click | impression | unknown.'
    )
    click_datetime: datetime | None = Field(
        description='Дата и время клика/показа в формате yyyy-mm-dd hh:mm:ss.'
    )
    click_id: str | None = Field(
        description='Идентификатор клика/показа или '' (пустая строка, если недоступен).'
    )
    click_ipv6: str | None = Field(
        description='IP-адрес в момент клика/показа в формате IPv6.'
    )
    click_timestamp: int | None = Field(
        description='Время клика/показа в формате UNIX-time в секундах.'
    )
    click_url_parameters: str | None = Field(
        description='Параметры, как они представлены в ссылке.'
    )
    click_user_agent: str | None = Field(
        description='User-agent клика/показа.'
    )
    profile_id: str | None = Field(
        description='Идентификатор пользовательского профиля.'
    )
    publisher_id: int | None = Field(
        description='ID партнера в AppMetrica.'
    )
    publisher_name: str | None = Field(
        description='Название партнера AppMetrica.'
    )
    tracker_name: str | None = Field(
        description='Название трекера, который добавляется в интерфейсе AppMetrica.'
    )
    tracking_id: int | None = Field(
        description='ID трекера в AppMetrica. '
    )
    install_datetime: datetime | None = Field(
        description='Дата и время установки в формате yyyy-mm-dd hh:mm:ss.'
    )
    install_ipv6: str | None = Field(
        description='IP-адрес в момент установки в формате IPv6.'
    )
    install_receive_datetime: datetime | None = Field(
        description='Дата и время получения сервером события об установке.'
    )
    install_receive_timestamp: int | None = Field(
        description='Время получения сервером события об установке в формате UNIX-time.'
    )
    install_timestamp: int | None = Field(
        description='Время установки в формате UNIX-time в секундах.'
    )
    is_reattribution: bool | None = Field(
        description='Признак реатрибутирования установки новому источнику. Возможные значения: true | false.'
    )
    is_reinstallation: bool | None = Field(
        description='Признак переустановки приложения. Возможные значения: true | false.'
    )
    match_type: str | None = Field(
        description='Способ атрибуции: fingerprint | referrer | identifier | '' (пустая строка).'
    )
    appmetrica_device_id: str | None = Field(
        description='Уникальный идентификатор устройства, который устанавливает AppMetrica.'
    )
    city: str | None = Field(
        description='Название города на английском языке, где был произведен клик.'
    )
    connection_type: str | None = Field(
        description='Тип подключения устройства. Возможные значения: wifi | cell | unknown.'
    )
    country_iso_code: str | None = Field(
        description='ISO-код страны.'
    )
    device_locale: str | None = Field(
        description='Язык интерфейса устройства.'
    )
    device_manufacturer: str | None = Field(
        description='Производитель устройства, определяется сервисом AppMetrica.'
    )
    device_model: str | None = Field(
        description='Модель устройства, определяется сервисом AppMetrica.'
    )
    device_type: str | None = Field(
        description='Тип устройства, определяется сервисом AppMetrica. Возможные значения: phone | tablet | unknown.'
    )
    google_aid: str | None = Field(
        description='Google AID устройства в формате, в котором получен от устройства.'
    )
    oaid: str | None = Field(
        description='Huawei OAID устройства в формате, в котором получен от устройства.'
    )
    ios_ifa: str | None = Field(
        description='IFA устройства в формате, в котором получен от устройства.'
    )
    ios_ifv: str | None = Field(
        description='IFV для приложения в формате, в котором получен от устройства.'
    )
    mcc: int | None = Field(
        description='Мобильный код страны.'
    )
    mnc: int | None = Field(
        description='Код мобильной сети.'
    )
    operator_name: str | None = Field(
        description='Имя оператора сотовой связи.'
    )
    os_name: str | None = Field(
        description='Операционная система на устройстве пользователя: ios | android | windows.'
    )
    os_version: str | None = Field(
        description='Версия операционной системы на устройстве пользователя.'
    )
    windows_aid: str | None = Field(
        description='Windows AID устройства в формате, в котором получен от устройства.'
    )
    app_package_name: str | None = Field(
        description='Имя пакета для Android или Bundle ID для iOS.'
    )
    app_version_name: str | None = Field(
        description='Версия приложения в виде, как указана разработчиком.'
    )
