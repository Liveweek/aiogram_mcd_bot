from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db_actions import get_current_errors


def generate_help_list() -> str:
    """
    Метод для генерации списка помощи к боту
    :return: Текст сообщения-помощи
    """
    txt = '<b>HELP:</b>\n'
    txt += '<b>/start</b> Ну че, народ, погнали?!\n'
    txt += '<b>INFO</b> Бот для получения запросов к конфигурационной базе данных PG проекта Macdonalds\n'
    txt += '<b>Жми кнопки ниже</b>\n'
    return txt


def get_errors_list_nms() -> List[str]:
    """
    Получение списка упавших процессов
    :return: Список строк с названиеми процессов
    """
    query_results = get_current_errors()
    return [tup[0] for tup in query_results] if query_results else []


async def errors_keyboard() -> Optional[InlineKeyboardMarkup]:
    """
    Создание встроенной клавиатуры со всеми упавшими процессами
    :return: InlineKeyboardMarkup с кнопка для каждого упавшего ресурса
    """
    markup = InlineKeyboardMarkup(row_width=2)
    err_processes = get_errors_list_nms()
    if err_processes:
        for resource in err_processes:
            markup.add(InlineKeyboardButton(resource, callback_data=resource))
        return markup
    return None


def get_keyboard(lst, type_nm=None) -> Optional[InlineKeyboardMarkup]:
    """
    Генерация клавиатуры из исписка с типом выполняемой команды
    :param lst: Список ресурсов для отображения
    :param type_nm: Тип исполняемой команды над ресурсом
    :return: Встроенную в сообщение клавиатуру (Класс InlineKeyboardMarkup), опционально
    """
    markup = InlineKeyboardMarkup(row_width=2)
    if lst:
        for obj in lst:
            markup.insert(InlineKeyboardButton(obj, callback_data=f"{obj}+{type_nm}" if type_nm else obj))
        return markup
    else:
        return None
