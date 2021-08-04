from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.dispatcher.filters import Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from main import bot, dp
from filters import IsAdmin
from config import button_request_errors_text, button_request_status_text, \
    button_request_rtp_text, button_request_vf_text, button_show_errors, \
    button_show_resources

from bot_actions import generate_help_list, get_errors_list_nms, errors_keyboard, get_keyboard
from db_actions import get_error_stats, get_status, get_rtp_status, get_vf_status, get_current_errors, \
    update_resource_status, close_resource_status, get_modules_list, get_resources_list, delete_resource_status, \
    open_resource_status, show_resource_status


REPLY_MARKUP_ADMIN = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=button_show_resources),
            KeyboardButton(text=button_request_errors_text),
            KeyboardButton(text=button_request_status_text),
            KeyboardButton(text=button_request_rtp_text),
            KeyboardButton(text=button_request_vf_text),
            KeyboardButton(text=button_show_errors)
        ],
    ],
    resize_keyboard=True,
)

REPLY_MARKUP_USER = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=button_request_errors_text),
            KeyboardButton(text=button_request_status_text),
            KeyboardButton(text=button_request_rtp_text),
            KeyboardButton(text=button_request_vf_text),
        ],
    ],
    resize_keyboard=True,
)


@dp.message_handler(commands=['start'])
async def show_help(message: Message):
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Бот запущен',
        reply_markup=REPLY_MARKUP_ADMIN if await IsAdmin().check(message) else REPLY_MARKUP_USER
    )


@dp.message_handler(commands=['help'])
async def show_help(message: Message):
    await bot.send_message(
        chat_id=message.from_user.id,
        text=generate_help_list(),
        reply_markup=REPLY_MARKUP_ADMIN if await IsAdmin().check(message) else REPLY_MARKUP_USER
    )


@dp.message_handler(Text(equals=button_request_errors_text))
async def show_errors(message: Message):
    await message.answer(text='Я отправил реквест в базу данных для извлечения статистики ошибок в системе за '
                              'сегодня, ждите!')

    query_results = get_error_stats()

    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if text else 'Отсутствуют свежие данные по ошибкам в системе.'
    )


@dp.message_handler(Text(equals=button_request_status_text))
async def show_status(message: Message):
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики в системе за сегодня (ограничение = '
             '30 записей), ждите!')

    query_results = get_status()

    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if text else 'Отсутствуют свежие данные по общей статистике системы.'
    )


@dp.message_handler(Text(equals=button_request_rtp_text))
async def show_rtp(message: Message):
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу за сегодня '
             '(ограничение = '
             '30 записей), ждите!')

    query_results = get_rtp_status()

    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if text else 'Отсутствуют свежие данные по краткосрочному процессу прогнозирования.'
    )


@dp.message_handler(Text(equals=button_request_vf_text))
async def show_vf(message: Message):
    query_results = get_vf_status()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу '
             'прогнозирования за сегодня '
             '(ограничение = '
             '30 записей), ждите!')

    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if text else 'Отсутствуют свежие данные по долгосрочному процессу прогнозирования.'
    )


# WARNING: вы остановились здесь


@dp.message_handler(IsAdmin(), Text(equals=button_show_resources))
async def show_modules(message: Message):
    modules = get_modules_list()
    await bot.send_message(
        chat_id=message.from_user.id,
        text='<b>Выберите модуль: </b>\n' if modules else 'Модули отсутствуют.\n\n<b>Сообщите администратору!</b>',
        reply_markup=get_keyboard(modules))


@dp.message_handler(IsAdmin(), Text(equals=button_show_errors))
async def show_errors(message: Message):
    errors_list = get_errors_list_nms()
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Процессы, завершившиеся с ошибками: \n' if errors_list \
            else 'Отсутствуют процессы, завершившиеся с ошибками. \n',
        reply_markup=await errors_keyboard()
    )


@dp.callback_query_handler(lambda call: True)
async def callback_inline(call):
    call_data = call.data
    if call.data in get_errors_list_nms():
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Перезапустить', callback_data=f'{call.data}+res_act_rerun'))
        markup.add(InlineKeyboardButton('Закрыть', callback_data=f'{call.data}+res_act_close'))
        markup.add(InlineKeyboardButton('Пропустить', callback_data=f'{call.data}+res_act_skip'))
        await bot.edit_message_text(
            chat_id=call.from_user.id,
            message_id=call.message.message_id,
            text=f'Как обработать статус ресурса {call_data}?\n',
            reply_markup=markup
        )
    elif call.data in get_modules_list():
        await bot.edit_message_text(
            chat_id=call.from_user.id,
            message_id=call.message.message_id,
            text=f"<b>Выберите ресурсы, доступные для модуля {call.data}: </b>\n",
            reply_markup=get_keyboard(get_resources_list(module_nm=call.data), type_nm='single_resource')
        )
    elif call_data.find('+') != -1:
        if call_data.split('+')[1] == 'res_act_rerun':
            update_resource_status(resource_nm=call_data.split('+')[0])
            err_lst = get_errors_list_nms()
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на 'А'.</b>\n\n" + \
                     "Процессы,завершившиеся с ошибками: \n" if err_lst else "<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
                reply_markup=await errors_keyboard()
            )
        elif call_data.split('+')[1] == 'res_act_close':
            close_resource_status(resource_nm=call_data.split('+')[0])
            err_lst = get_errors_list_nms()
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на 'С'.</b>\n\n" + \
                    "Процессы,завершившиеся с ошибками: \n" if err_lst else "<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
                reply_markup=await errors_keyboard()
            )
        elif call_data.split('+')[1] == 'res_act_skip':
            err_lst = get_errors_list_nms()
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Действий к ресурсу {call_data.split('+')[0]} не применялось.</b>\n\n" + \
                     "Процессы,завершившиеся с ошибками: \n" if err_lst else "<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
                reply_markup=await errors_keyboard()
            )
        # Блок обработчика для управления отдельными процессами
        elif call_data.split('+')[1] == 'single_resource':
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton('Запустить', callback_data=f"{call_data.split('+')[0]}+single_res_act_run"),
                InlineKeyboardButton('Узнать статус', callback_data=f"{call_data.split('+')[0]}+single_res_act_status"),
                InlineKeyboardButton('Закрыть', callback_data=f"{call_data.split('+')[0]}+single_res_act_close"),
                InlineKeyboardButton('Удалить', callback_data=f"{call_data.split('+')[0]}+single_res_act_delete"),
                InlineKeyboardButton('Пропустить', callback_data=f"{call_data.split('+')[0]}+single_res_act_skip")
            )
            # заряжаем новую
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"Как обработать ресурс {call_data.split('+')[0]}?\n",
                reply_markup=markup
            )
        elif call_data.split('+')[1] == 'single_res_act_run':
            open_resource_status(resource_nm=call_data.split('+')[0])
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Ресурс {call_data.split('+')[0]} запустится при следующей регламетной проверке статусов (каждые 15 минут).</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b> \n",
                reply_markup=get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_status':
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Текущий статус ресурса {call_data.split('+')[0]}: '{show_resource_status(resource_nm=call_data.split('+')[0])}'.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_close':
            close_resource_status(resource_nm=call_data.split('+')[0])
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Текущий статус ресурса {call_data.split('+')[0]}: '{show_resource_status(resource_nm=call_data.split('+')[0])}'.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_skip':
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Пропуск хода.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_delete':
            delete_resource_status(resource_nm=call_data.split('+')[0])
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Ресурс {call_data.split('+')[0]} был удален из таблицы etl_cfg.cfg_status_table.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=get_keyboard(get_modules_list())
            )
    else:
        await bot.send_message(
            chat_id=call.from_user.id,
            text=f'УУУУУХ Я ХЗ ЧЕ ЗА КОМАНДА ПАЦАНЫ!!!!!!! callback={call_data}\n'
        )
