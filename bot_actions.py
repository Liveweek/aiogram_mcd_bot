from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from main import connection
from db_actions import get_current_errors


def generate_help_list() -> str:
    """
    Метод для генерации списка помощи к боту
    :return: Текст сообщения-помощи
    """
    txt = '<b>HELP:</b>\n'
    txt += '<b>/start</b> Ну че, народ, погнали?!\n'
    # txt += '<b>/request_rtp</b> Показать последнюю сводку по краткосрочному прогнозу\n'
    # txt += '<b>/request_vf</b> Показать последюю сводку по долгосрочному прогнозу\n'
    # txt += '<b>/request</b> Показать ошибки с начала цикла (17:00)\n'
    txt += '<b>INFO</b> Бот для получения запросов к конфигурационной базе данных PG проекта Macdonalds\n'
    txt += '<b>Жми кнопки ниже</b>\n'
    return txt


def get_errors_list_nms() -> List[str]:
    """
    Получение списка упавших процессов
    :return: Список строк с названиеми процессов
    """
    query_results = get_current_errors(conn=connection)
    return [tup[0] for tup in query_results]


async def errors_keyboard() -> Optional[InlineKeyboardMarkup]:
    """
    Создание встроенной клавиатуры со всеми упавшими процессами
    :return: InlineKeyboardMarkup с кнопка для каждого упавшего ресурса
    """
    markup = InlineKeyboardMarkup()
    err_processes = get_errors_list_nms()
    if err_processes:
        for resource in err_processes:
            markup.add(InlineKeyboardButton(resource, callback_data=resource))
        return markup
    return None

