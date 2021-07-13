from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.dispatcher.filters import Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from main import bot, dp, connection
from config import ADMIN_ID, button_request_errors_text, button_request_status_text, \
    button_request_rtp_text, button_request_vf_text, button_show_errors

from bot_actions import generate_help_list, get_errors_list_nms, errors_keyboard
from db_actions import get_error_stats, get_status, get_rtp_status, get_vf_status, get_current_errors, \
    update_resource_status, close_resource_status


# TODO: Написать систему защиты от "незнакомцев" в виде декоратора (и применить его к БЛ)
# TODO: Вынести системные параметры подключения и токенов в переменные окружения
# TODO: Кнопка с волками

reply_markup = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=button_request_errors_text),
            KeyboardButton(text=button_request_status_text),
            KeyboardButton(text=button_request_rtp_text),
            KeyboardButton(text=button_request_vf_text),
            KeyboardButton(text=button_show_errors)
        ],
    ],
    resize_keyboard=True,
)


@dp.message_handler(commands=['start'])
async def show_start(message: Message):
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Бот запущен',
        reply_markup=reply_markup
    )


@dp.message_handler(commands=['help'])
async def show_help(message: Message):
    txt = generate_help_list()
    await bot.send_message(
        chat_id=message.from_user.id,
        text=txt,
        reply_markup=reply_markup
    )


@dp.message_handler(Text(equals=button_request_errors_text))
async def show_errors(message: Message):
    await message.answer(text='Я отправил реквест в базу данных для извлечения статистики ошибок в системе за '
                              'сегодня, ждите!')

    query_results = get_error_stats(conn=connection)
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if len(text) > 0 else 'Отсутствуют свежие данные по ошибкам в системе.'
    )


@dp.message_handler(Text(equals=button_request_status_text))
async def show_status(message: Message):
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики в системе за сегодня (ограничение = '
             '30 записей), ждите!')

    query_results = get_status(connection)

    # TODO: Подумать, как можно адапировать этот блок кода
    if len(query_results) > 0:
        text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    elif len(query_results) == 0:
        text = 'Отсутствуют свежие данные по общей статистике системы.'
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_rtp_text))
async def show_rtp_errors(message: Message):
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу за сегодня '
             '(ограничение = '
             '30 записей), ждите!')

    query_results = get_rtp_status(connection)
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if len(text) > 0 else 'Отсутствуют свежие данные \
                                        по краткосрочному процессу прогнозирования.'
    )


@dp.message_handler(Text(equals=button_request_vf_text))
async def show_vf_errors(message: Message):
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по долгосрочному процессу \
             прогнозирования за сегодня (ограничение = 30 записей), ждите!')

    query_results = get_vf_status(connection)
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text if len(text) > 0 else 'Отсутствуют свежие данные \
                                        по долгосрочному процессу прогнозирования.'
    )


@dp.message_handler(Text(equals=button_show_errors))
async def show_errors(message: Message):
    errors_list = get_errors_list_nms()
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Процессы, завершившиеся с ошибками: \n' \
            if errors_list else \
            'Отсутствуют процессы, завершившиеся с ошибками. \n',
        reply_markup=await errors_keyboard()
    )


@dp.callback_query_handler(lambda call: True)
async def callback_inline(call: CallbackQuery):
    call_data = call.data
    if call.data in get_errors_list_nms():
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Перезапустить', callback_data=f'{call.data}+res_act_rerun'))
        markup.add(InlineKeyboardButton('Закрыть', callback_data=f'{call.data}+res_act_close'))
        markup.add(InlineKeyboardButton('Пропустить', callback_data=f'{call.data}+res_act_skip'))
        # удаляем клавиатуру старую
        # await bot.edit_message_reply_markup(call.from_user.id, message_id=call.message.message_id,
        #                                      reply_markup='')
        # заряжаем новую
        await bot.edit_message_text(
            chat_id=call.from_user.id,
            message_id=call.message.message_id,
            text=f'Как обработать статус ресурса {call_data}?\n',
            reply_markup=markup
        )
    elif call_data.find('+'):
        if call_data.split('+')[1] == 'res_act_rerun':
            update_resource_status(call_data.split('+')[0], connection)
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на 'А'.</b>\n\nПроцессы, завершившиеся с ошибками: \n",
                reply_markup=await errors_keyboard()
            )
        elif call_data.split('+')[1] == 'res_act_close':
            close_resource_status(call_data.split('+')[0], connection)
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на 'С'.</b>\n\nПроцессы, завершившиеся с ошибками: \n",
                reply_markup=await errors_keyboard()
            )
        elif call_data.split('+')[1] == 'res_act_skip':
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Действий к ресурсу {call_data.split('+')[0]} не применялось.</b>\n\nПроцессы, завершившиеся с ошибками: \n",
                reply_markup=await errors_keyboard()
            )
