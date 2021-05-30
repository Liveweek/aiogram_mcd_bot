from main import bot, dp, connection
from config import ADMIN_ID, button_help_text, button_request_errors_text, button_request_status_text, \
    button_request_rtp_text, button_request_vf_text, DATABASELINK_POSTGRES, button_show_errors
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, ReplyKeyboardRemove
from aiogram.dispatcher.filters import Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List

reply_markup = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=button_help_text),
            KeyboardButton(text=button_request_errors_text),
            KeyboardButton(text=button_request_status_text),
            KeyboardButton(text=button_request_rtp_text),
            KeyboardButton(text=button_request_vf_text),
            KeyboardButton(text=button_show_errors)
        ],
    ],
    resize_keyboard=True,
)


async def send_to_admin(dp):
    await bot.send_message(chat_id=ADMIN_ID, text='Бот запущен', reply_markup=reply_markup)


@dp.message_handler(Text(equals=button_help_text))
async def show_help(message: Message):
    txt = '<b>HELP:</b>\n'
    # txt += '<b>/start</b> Ну че, народ, погнали?!\n'
    # txt += '<b>/request_rtp</b> Показать последнюю сводку по краткосрочному прогнозу\n'
    # txt += '<b>/request_vf</b> Показать последюю сводку по долгосрочному прогнозу\n'
    # txt += '<b>/request</b> Показать ошибки с начала цикла (17:00)\n'
    txt += '<b>INFO</b> Бот для получения запросов к конфигурационной базе данных PG проекта Macdonalds\n'
    txt += '<b>Жми кнопки ниже</b>\n'
    # await message.answer(text=txt)
    await bot.send_message(
        chat_id=message.from_user.id,
        text=txt,
        reply_markup=reply_markup
    )


@dp.message_handler(Text(equals=button_request_errors_text))
async def show_errors(message: Message):
    cursor = connection.cursor()
    await message.answer(text='Я отправил реквест в базу данных для извлечения статистики ошибок в системе за '
                              'сегодня, ждите!')
    cursor.execute(""" select t1.* from 
        (
        select cast(lower(process_nm) as varchar(32)) as process_nm,
        				case when status_description='' and end_dttm is null then 'In progress' 
    					when status_description='' and end_dttm is not null then 'Success' 
        				else cast(status_description as varchar(50)) 
        				end as status_description
        				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
        				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
        				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
        					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
        					end as exec_tm
        				, cast(user_nm as varchar(15)) as user
        				from etl_cfg.cfg_log_event 
        				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
        				and status_cd=1
        				order by start_dttm desc 
        				limit 40
        				) t1 
    			order by start_dttm
        				""")

    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_status_text))
async def show_errors(message: Message):
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики в системе за сегодня (ограничение = '
             '30 записей), ждите!')

    cursor.execute(""" select t1.* from 
           (
           select cast(lower(process_nm) as varchar(32)) as process_nm,
           				case when status_description='' and end_dttm is null then 'In progress' 
       					when status_description='' and end_dttm is not null then 'Success' 
           				else cast(status_description as varchar(50)) 
           				end as status_description
           				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
           				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
           				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
           					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
           					end as exec_tm
           				, cast(user_nm as varchar(15)) as user
           				from etl_cfg.cfg_log_event 
           				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
           				order by start_dttm desc 
           				limit 40
           				) t1 
       			order by start_dttm
           				""")
    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_rtp_text))
async def show_errors(message: Message):
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу за сегодня '
             '(ограничение = '
             '30 записей), ждите!')
    cursor.execute("""select t1.* from
    (
    select cast(lower(process_nm) as varchar(32)) as process_nm,
        				case when status_description='' and end_dttm is null then 'In progress' 
    					when status_description='' and end_dttm is not null then 'Success' 
        				else cast(status_description as varchar(50)) 
        				end as status_description
        				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
        				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
        				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
        					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
        					end as exec_tm
        				, cast(user_nm as varchar(15)) as user
        				from etl_cfg.cfg_log_event 
        				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
     					and lower(process_nm) like 'rtp_%'
        				order by start_dttm desc 
        				limit 30
    	) t1 order by start_dttm """)
    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_vf_text))
async def show_errors(message: Message):
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу за сегодня '
             '(ограничение = '
             '30 записей), ждите!')
    cursor.execute("""select t1.* from
(
select cast(lower(process_nm) as varchar(32)) as process_nm,
    				case when status_description='' and end_dttm is null then 'In progress' 
					when status_description='' and end_dttm is not null then 'Success' 
    				else cast(status_description as varchar(50)) 
    				end as status_description
    				,cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm
    				, cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm
    				, case when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
    					else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
    					end as exec_tm
    				, cast(user_nm as varchar(15)) as user
    				from etl_cfg.cfg_log_event 
    				where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
 					and lower(process_nm) like 'vf_%'
    				order by start_dttm desc 
    				limit 30
	) t1 order by start_dttm """)
    query_results = cursor.fetchall()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


def get_errors_list_nms() -> List[str]:
    cursor = connection.cursor()
    cursor.execute("select resource_nm\n"
                   "from etl_cfg.cfg_status_table\n"
                   "where status_cd='E'\n")
    query_results = cursor.fetchall()
    list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    return list_results


def cnt_list_obj(list):
    count_of_objects = 0
    for obj in list:
        count_of_objects = count_of_objects + 1

    return count_of_objects


async def errors_keyboard():
    markup = InlineKeyboardMarkup()
    err_processes = get_errors_list_nms()
    if cnt_list_obj(get_errors_list_nms()) > 0:
        for resource in err_processes:
            button_text = resource
            print(button_text)
            markup.add(InlineKeyboardButton(button_text, callback_data=button_text))
    return markup


def close_resource_status(resource_nm):
    cursor = connection.cursor()
    cursor.execute(
        f"update etl_cfg.cfg_status_table set status_cd='C' where status_cd='E' and resource_nm = '{resource_nm}'")
    connection.commit()


def update_resource_status(resource_nm):
    cursor = connection.cursor()
    cursor.execute(
        f"update etl_cfg.cfg_status_table set status_cd='A' where status_cd='E' and resource_nm = '{resource_nm}'")
    connection.commit()


@dp.message_handler(Text(equals=button_show_errors))
async def show_errors(message: Message):
    markup = InlineKeyboardMarkup()
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Процессы, завершившиеся с ошибками: \n',
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
        await bot.send_message(
            chat_id=call.from_user.id,
            text='Как обработать статус ресурса?\n',
            reply_markup=markup
        )
    elif call_data.find('+'):
        if call_data.split('+')[1] == 'res_act_rerun':
            await bot.send_message(
                chat_id=call.from_user.id,
                text='Статус процесса {} был обновлен с "E" на "А".'.format(call_data.split('+')[0]))
            await bot.edit_message_reply_markup(call.from_user.id, message_id=call.message.message_id,
                                                reply_markup='')
            update_resource_status(call_data.split('+')[0])
        elif call_data.split('+')[1] == 'res_act_close':
            await bot.send_message(
                chat_id=call.from_user.id,
                text='Статус процесса {} был обновлен с "E" на "С".'.format(call_data.split('+')[0]))
            await bot.edit_message_reply_markup(call.from_user.id, message_id=call.message.message_id,
                                                reply_markup='')
            close_resource_status(call_data.split('+')[0])
        elif call_data.split('+')[1] == 'res_act_skip':
            await bot.send_message(
                chat_id=call.from_user.id,
                text='Действий не применялось')
            await bot.edit_message_reply_markup(call.from_user.id, message_id=call.message.message_id,
                                                reply_markup='')