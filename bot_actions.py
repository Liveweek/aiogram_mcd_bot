from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from db_actions import get_current_errors, update_resource_status, close_resource_status, open_resource_status, \
    get_modules_list, show_resource_status, delete_resource_status
from main import bot


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


def get_modules_list_nms() -> List[str]:
    """
    Получение списка модулей
    :return: Список строк с названиями модулей
    """
    query_results = get_modules_list()
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
            markup.add(InlineKeyboardButton(resource, callback_data=f'err@{resource}'))
        return markup
    return None


def get_keyboard(lst, type_nm) -> Optional[InlineKeyboardMarkup]:
    """
    Генерация клавиатуры из исписка с типом выполняемой команды
    :param lst: Список ресурсов для отображения
    :param type_nm: Тип исполняемой команды над ресурсом
    :return: Встроенную в сообщение клавиатуру (Класс InlineKeyboardMarkup), опционально
    """
    markup = InlineKeyboardMarkup(row_width=2)
    if lst:
        for obj in lst:
            markup.insert(InlineKeyboardButton(obj, callback_data=f"{type_nm}@{obj}"))
        return markup
    else:
        return None


async def err_res_menu(resource_nm, callback):
    """
    Отправка пользователю сообщения с кнопками меню для данного ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :param resource_nm: Наименование ресурса
    :return: None
    """
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton('Перезапустить', callback_data=f'err@rerun@{resource_nm}'))
    markup.add(InlineKeyboardButton('Закрыть', callback_data=f'err@close@{resource_nm}'))
    markup.add(InlineKeyboardButton('Пропустить', callback_data=f'err@skip@{resource_nm}'))
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f'Как обработать статус ресурса {resource_nm}?\n',
        reply_markup=markup
    )


async def err_res_rerun(resource_nm, callback):
    """
    Перезапуск упавшего ресурса (Перевод из статуса Е в статус А)
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    update_resource_status(resource_nm=resource_nm)
    err_lst = get_errors_list_nms()
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Статус процесса {resource_nm} был обновлен с 'E' на 'А'.</b>\n\n" + \
             "Процессы,завершившиеся с ошибками: \n" if err_lst else "<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
        reply_markup=await errors_keyboard()
    )


async def err_res_close(resource_nm, callback):
    """
    Закрытие ошибочного ресурса (перевод из статуса E в статус С)
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    close_resource_status(resource_nm=resource_nm)
    err_lst = get_errors_list_nms()
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Статус процесса {resource_nm} был обновлен с 'E' на 'C'.</b>\n\n" + \
             "Процессы,завершившиеся с ошибками: \n" if err_lst else "<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
        reply_markup=await errors_keyboard()
    )


async def err_res_skip(resource_nm, callback):
    """
    Пропуск ресурса (никаких действий над ним не производится)
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    err_lst = get_errors_list_nms()
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Действий к ресурсу {resource_nm} не применялось.</b>\n\n" + \
             "Процессы,завершившиеся с ошибками: \n" if err_lst else "<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
        reply_markup=await errors_keyboard()
    )


async def res_menu(resource_nm, callback):
    """
    Сообщение пользователю с кнопками действий над соответствующим ресурсом
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton('Запустить', callback_data=f"resource@run@{resource_nm}"),
        InlineKeyboardButton('Узнать статус', callback_data=f"resource@status@{resource_nm}"),
        InlineKeyboardButton('Закрыть', callback_data=f"resource@close@{resource_nm}"),
        InlineKeyboardButton('Удалить', callback_data=f"resource@delete@{resource_nm}"),
        InlineKeyboardButton('Пропустить', callback_data=f"resource@close@{resource_nm}")
    )
    # заряжаем новую
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"Как обработать ресурс {resource_nm}?\n",
        reply_markup=markup
    )


async def res_run(resource_nm, callback):
    """
    Полный перезапуск ресурса со всеми зависимостями
    :param resource_nm: Наимнование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    open_resource_status(resource_nm=resource_nm)
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Ресурс {resource_nm} запустится при следующей регламетной проверке статусов (каждые 15 минут).</b>\n\n"
             f"Как поступим с другими ресурсами? \n"
             f"<b>Выберите модуль: </b> \n",
        reply_markup=get_keyboard(get_modules_list_nms(), type_nm='module')
    )


async def res_status(resource_nm, callback):
    """
    Получение текущего статуса ресура
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Текущий статус ресурса {resource_nm}: '{show_resource_status(resource_nm=resource_nm)}'.</b>\n\n"
             f"Как поступим с другими ресурсами? \n"
             f"<b>Выберите модуль: </b>\n",
        reply_markup=get_keyboard(get_modules_list_nms(), type_nm='module')
    )


async def res_close(resource_nm, callback):
    """
    Закрытие выбранного ресурса
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    close_resource_status(resource_nm=resource_nm)
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Текущий статус ресурса {resource_nm}: '{show_resource_status(resource_nm=resource_nm)}'.</b>\n\n"
             f"Как поступим с другими ресурсами? \n"
             f"<b>Выберите модуль: </b>\n",
        reply_markup=get_keyboard(get_modules_list_nms(), type_nm='module')
    )


async def res_skip(resource_nm, callback):
    """
    Пропуск выбранного ресурса
    :param resource_nm: Наименование ресурса (хоть и не используется, но для единообразия
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Пропуск хода.</b>\n\n"
             f"Как поступим с другими ресурсами? \n"
             f"<b>Выберите модуль: </b>\n",
        reply_markup=get_keyboard(get_modules_list_nms(), type_nm='module')
    )


async def res_delete(resource_nm, callback):
    """
    Удаление выбранного ресурса из таблицы статусов
    :param resource_nm: Наименование ресурса
    :param callback: Call-back данные, пришедшие с сообщением
    :return: None
    """
    delete_resource_status(resource_nm=resource_nm)
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Ресурс {resource_nm} был удален из таблицы etl_cfg.cfg_status_table.</b>\n\n"
             f"Как поступим с другими ресурсами? \n"
             f"<b>Выберите модуль: </b>\n",
        reply_markup=get_keyboard(get_modules_list_nms(), type_nm='module')
    )

