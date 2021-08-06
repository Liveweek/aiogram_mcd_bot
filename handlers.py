from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.dispatcher.filters import Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from main import bot, dp
from filters import IsAdmin
from config import button_request_errors_text, button_request_status_text, \
    button_request_rtp_text, button_request_vf_text, button_show_errors, \
    button_show_resources

from bot_actions import generate_help_list, get_errors_list_nms, errors_keyboard, get_keyboard, err_res_menu, \
    err_res_rerun, err_res_close, err_res_skip, res_menu, res_run, res_status, res_skip, res_delete, res_close, \
    get_modules_list_nms
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
    modules = get_modules_list_nms()
    await bot.send_message(
        chat_id=message.from_user.id,
        text='<b>Выберите модуль: </b>\n' if modules else 'Модули отсутствуют.\n\n<b>Сообщите администратору!</b>',
        reply_markup=get_keyboard(modules, type_nm='module'))


@dp.message_handler(IsAdmin(), Text(equals=button_show_errors))
async def show_errors(message: Message):
    errors_list = get_errors_list_nms()
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Процессы, завершившиеся с ошибками: \n' if errors_list \
            else 'Отсутствуют процессы, завершившиеся с ошибками. \n',
        reply_markup=await errors_keyboard()
    )


@dp.callback_query_handler(lambda callback: callback.data.split('@')[0] == 'err')
async def error_res_action(callback):
    commands = {
        'rerun':   err_res_rerun,
        'close':   err_res_close,
        'skip':    err_res_skip,
        'default': err_res_menu
    }

    parsed_callback_data = callback.data.split('@')[1:]
    if parsed_callback_data[0] in commands:
        func, resource_nm = parsed_callback_data
        await commands[func](resource_nm, callback)
    else:
        resource_nm = parsed_callback_data[0]
        await commands['default'](resource_nm, callback)


@dp.callback_query_handler(lambda callback: callback.data.split('@')[0] == 'module')
async def show_module_menu(callback):
    module_nm = callback.data.split('@')[1]
    await bot.edit_message_text(
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        text=f"<b>Выберите ресурсы, доступные для модуля {module_nm}: </b>\n",
        reply_markup=get_keyboard(get_resources_list(module_nm=module_nm), type_nm='resource')
    )


@dp.callback_query_handler(lambda callback: callback.data.split('@')[0] == 'resource')
async def show_res_action(callback):
    commands = {
        'run':     res_run,
        'status':  res_status,
        'close':   res_close,
        'delete':  res_delete,
        'skip':    res_skip,
        'default': res_menu
    }

    parsed_callback_data = callback.data.split('@')[1:]
    if parsed_callback_data[0] in commands:
        func, resource_nm = parsed_callback_data
        await commands[func](resource_nm, callback)
    else:
        resource_nm = parsed_callback_data[0]
        await commands['default'](resource_nm, callback)


