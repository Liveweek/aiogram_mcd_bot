from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.dispatcher.filters import Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from main import bot, dp, connection
from config import button_request_errors_text, button_request_status_text, \
    button_request_rtp_text, button_request_vf_text, button_show_errors, \
    button_show_resources

from bot_actions import generate_help_list, get_errors_list_nms, errors_keyboard
from db_actions import get_error_stats, get_status, get_rtp_status, get_vf_status, get_current_errors, \
    update_resource_status, close_resource_status

# TODO: Написать систему защиты от "незнакомцев" в виде декоратора (и применить его к БЛ)
# TODO: Вынести системные параметры подключения и токенов в переменные окружения
# TODO: Кнопка с волками

reply_markup = ReplyKeyboardMarkup(
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


async def send_to_admin(dp):
    await bot.send_message(chat_id=ADMIN_ID, text='Бот запущен', reply_markup=reply_markup)


@dp.message_handler(commands=['start'])
async def show_help(message: Message):
    await bot.send_message(
        chat_id=message.from_user.id,
        text='Бот запущен',
        reply_markup=reply_markup
    )


@dp.message_handler(commands=['help'])
async def show_help(message: Message):
    txt = '<b>HELP:</b>\n'
    txt += '<b>/start</b> Ну че, народ, погнали?!\n'
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
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
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
    cursor.close()
    connection.close()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    if len(text) > 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text=text
        )
    elif len(text) == 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Отсутствуют свежие данные по ошибкам в системе.'
        )


@dp.message_handler(Text(equals=button_request_status_text))
async def show_errors(message: Message):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
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
    cursor.close()
    connection.close()
    if len(query_results) > 0:
        text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    elif len(query_results) == 0:
        text = 'Отсутствуют свежие данные по общей статистике системы.'
    await bot.send_message(
        chat_id=message.from_user.id,
        text=text
    )


@dp.message_handler(Text(equals=button_request_rtp_text))
async def show_errors(message: Message):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
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
    cursor.close()
    connection.close()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    if len(text) > 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text=text
        )
    elif len(text) == 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Отсутствуют свежие данные по краткосрочному процессу прогнозирования.'
        )


@dp.message_handler(Text(equals=button_request_vf_text))
async def show_errors(message: Message):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    await message.answer(
        text='Я отправлю реквест в базу данных для извлечения свежей статистики по краткосрочному процессу '
             'прогнозирования за сегодня '
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
    cursor.close()
    connection.close()
    text = '\n\n'.join([', '.join(map(str, x)) for x in query_results])
    if len(text) > 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text=text
        )
    elif len(text) == 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Отсутствуют свежие данные по долгосрочному процессу прогнозирования.'
        )


def get_errors_list_nms() -> List[str]:
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    cursor.execute("select resource_nm\n"
                   "from etl_cfg.cfg_status_table\n"
                   "where status_cd='E'\n")
    query_results = cursor.fetchall()
    cursor.close()
    connection.close()
    if len(query_results) > 0:
        list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    elif len(query_results) == 0:
        list_results = ''
    return list_results


async def errors_keyboard():
    markup = InlineKeyboardMarkup(row_width=2)
    err_processes = get_errors_list_nms()
    if len(get_errors_list_nms()) > 0:
        for resource in err_processes:
            markup.add(InlineKeyboardButton(resource, callback_data=resource))
        return markup


async def get_keyboard(list, type_nm=''):
    markup = InlineKeyboardMarkup(row_width=2)
    #print(f'type_nm = {type_nm}')
    if len(list) > 0:
        for obj in list:
            #print(obj)
            if len(type_nm) > 0:
                markup.insert(InlineKeyboardButton(obj, callback_data=f"{obj}+{type_nm}"))
            elif len(type_nm) == 0:
                markup.insert(InlineKeyboardButton(obj, callback_data=obj))
        return markup


def close_resource_status(resource_nm):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    cursor.execute(
        f"update etl_cfg.cfg_status_table set status_cd='C' where status_cd='E' and resource_nm = '{resource_nm}'")
    connection.commit()
    cursor.close()
    connection.close()

def delete_resource_status(resource_nm):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    cursor.execute(
        f"delete from etl_cfg.cfg_status_table where resource_nm = '{resource_nm}'")
    connection.commit()
    cursor.close()
    connection.close()

def update_resource_status(resource_nm):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    cursor.execute(
        f"update etl_cfg.cfg_status_table set status_cd='A' where status_cd='E' and resource_nm = '{resource_nm}'")
    connection.commit()
    cursor.close()
    connection.close()


def open_resource_status(resource_nm):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    #print('resource_nm=====', resource_nm)
    cursor.execute(
        f"select max(t1.ex_flg) as max_ex_flg from (select 0 as ex_flg union ( select 1012 as ex_flg from etl_cfg.cfg_status_table	where lower(resource_nm) = '{resource_nm}')) t1")
    # Если ресурс УЖЕ имеется в статусной таблице за сегодня (значит, зависимые ресурсы также имеются в статусе "L"
    # if int(cursor.fetchone()[0]) == 1:
    query_rc = cursor.fetchall()
    query_list = ', '.join([', '.join(map(str, x)) for x in query_rc]).split((', '))
    if len(query_list[0]) >= 4:
        #print('Ветка, если полученный ответ не типа НонТайп')
        # Если ресурс уже есть в системе, то надо выкинуть предупреждение и другую клавиатуру - перезапускать ли
        # в этом случае. Если да: Удаляем текущий ресурс из таблицы статусов
        cursor.execute(f"delete from etl_cfg.cfg_status_table where lower(resource_nm) = '{resource_nm}'")
        # находим зависимые ресурсы, статус которых нужно проставить в "А", чтобы основной ресурс запустился
        cursor.execute(
            f"select replace(rule_cond, '/A','') from etl_cfg.cfg_schedule_rule where lower(rule_nm) = '{resource_nm}'")
        query_results = cursor.fetchall()
        if len(query_results) > 0:
            #print('Ветка, если полученный ответ для определения зависимых ресурсов не нулевой')
            list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
            #print('зависимые ресурсы: ', list_results)
            i = 1
            for obj in list_results:
                i = i + 1
                #print(f'зависимый ресурс #{i}: ', obj)
                # Обновляем зависимый ресурс в "А" для запуска главного ресурса
                cursor.execute(f"update etl_cfg.cfg_status_table set status_cd = 'A' where resource_nm = '{obj}'")
                #print(f"Статус ресурса {obj} обновлен в 'A'.")
        else:
            print(f'Для указанного ресурса {resource_nm} нет правила запуска! Обратитесь к администратору')
        connection.commit()
    # Если ресурса НЕТ в статусной таблице за сегодня (но, возможно, он загружается - надо проверить)
    elif len(query_list[0]) <= 1:
        # находим зависимые ресурсы, статус которых нужно проставить в "А", чтобы основной ресурс запустился
        cursor.execute(
            f"select replace(rule_cond, '/A','') from etl_cfg.cfg_schedule_rule where lower(rule_nm) = '{resource_nm}'")
        query_results = cursor.fetchall()
        #print(query_results)
        if len(query_results) > 0:
            #print('Ветка, если есть зависимые ресурсы')
            list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((' '))
            #print('list_results ===== ', list_results)
            i = 0
            for obj in list_results:
                i = i + 1
                #print(f'зависимый ресурс #{i}: ', obj)
                # Проверка на существование ресурса в таблице статусов за сегодня
                cursor.execute(
                        f"select max(t1.ex_flg) as max_ex_flg from (select 0 as ex_flg union ( select 1012 as ex_flg from etl_cfg.cfg_status_table	where lower(resource_nm) = '{obj}')) t1")
                #cursor.execute(f"select 1012 as ex_flg from etl_cfg.cfg_status_table where resource_nm = '{obj}'")
                query_rc = cursor.fetchall()
                query_list_inter = ', '.join([', '.join(map(str, x)) for x in query_rc]).split((', '))
                # print('type=',type(query_list_inter))
                # print('Value',query_list_inter)
                if len(query_list_inter[0]) >= 4:
                    # Если ресурс УЖЕ имеется в статусной таблице за сегодня (значит, зависимые ресурсы также имеются в статусе "L"
                    if int(query_list_inter[0]) == 1:
                        #print(
                        #    f"Зависимый ресурс {obj} уже имеется в таблице статусов за сегодня. Ожидайте регламентного запуска процесса (каждые 15 мин).")
                        cursor.execute(
                            f"update etl_cfg.cfg_status_table set status_cd = 'A' where resource_nm = '{obj}' and status_cd not in ('P', 'E')")
                        #print(
                        #    f"Статус зависимого ресурса {obj} обновлен в 'A', на случай, если его текущий статус был закрыт вручную.")
                elif len(query_list_inter[0]) <= 1:
                    cursor.execute(f"select resource_id from  etl_cfg.cfg_resource where lower(resource_nm) = '{obj}'")
                    resource_id = cursor.fetchall()
                    resource_id_fmt = ', '.join([', '.join(map(str, x)) for x in resource_id]).split((', '))
                    # print('======================',resource_id_fmt)
                    # print('type of first object in a list ===', type(int(resource_id_fmt[0].split('.')[0])))
                    # print('res_id====',int(resource_id_fmt[0].split('.')[0]))
                    postgres_insert_query = """ INSERT INTO etl_cfg.cfg_status_table(resource_id, resource_nm, status_cd, processed_dttm, retries_cnt) 	VALUES (%s,%s,%s,%s,%s)"""
                    record_to_insert = (int(resource_id_fmt[0].split('.')[0]), obj, 'A', 'now()', 0)
                    cursor.execute(postgres_insert_query, record_to_insert)
                    #print(f"Добавлена новая строка - статус ресурс {obj} = 'A'")
                else:
                    pass
        connection.commit()
    else:
        print('ЕЕЕ ПАРНИ ХЗ ЧТО ПРОИСХОДИТ ЕЕЕЕЕЕ')
    cursor.close()
    connection.close()


def show_resource_status(resource_nm):
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    # проверка на существование статуса для указанного ресурса
    cursor.execute(f"select 1012 as ex_flg from etl_cfg.cfg_status_table where lower(resource_nm) = '{resource_nm}'")
    query_rc = cursor.fetchall()
    query_list = ', '.join([', '.join(map(str, x)) for x in query_rc]).split((', '))
    if len(query_list[0]) >= 4:
        cursor.execute(f"select status_cd from etl_cfg.cfg_status_table where lower(resource_nm) = '{resource_nm}'")
        query_rc = cursor.fetchall()
        query_list = ', '.join([', '.join(map(str, x)) for x in query_rc]).split((', '))
        return query_list[0]
    else:
        return 'Ресурс отсутствует в таблице статусов'
    cursor.close()
    connection.close()


def get_modules_list() -> List[str]:
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    cursor.execute("select distinct lower(module_nm) from etl_cfg.cfg_resource")
    query_results = cursor.fetchall()
    cursor.close()
    connection.close()
    if len(query_results) > 0:
        list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    elif len(query_results) == 0:
        list_results = ''
    return list_results


def get_resources_list(module_nm) -> List[str]:
    connection = psycopg2.connect(database=db_info.get('NAME'),
                      user=db_info.get('USER'),
                      password=db_info.get('PASSWORD'),
                      host=db_info.get('HOST'),
                      port=db_info.get('PORT'))
    cursor = connection.cursor()
    cursor.execute(
        f"select distinct lower(resource_nm) from etl_cfg.cfg_resource where lower(module_nm) = '{module_nm}'")
    query_results = cursor.fetchall()
    cursor.close()
    connection.close()
    if len(query_results) > 0:
        list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    elif len(query_results) == 0:
        list_results = ''
    return list_results


@dp.message_handler(Text(equals=button_show_resources))
async def show_modules(message: Message):
    if len(get_modules_list()) > 0:
        markup = InlineKeyboardMarkup()
        await bot.send_message(
            chat_id=message.from_user.id,
            text='<b>Выберите модуль: </b>\n',
            reply_markup=await get_keyboard(get_modules_list())
        )
    elif len(get_modules_list()) == 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Модули отсутствуют.\n\n<b>Сообщите администратору!</b>'
        )


@dp.message_handler(Text(equals=button_show_errors))
async def show_errors(message: Message):
    if len(get_errors_list_nms()) > 0:
        markup = InlineKeyboardMarkup()
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Процессы, завершившиеся с ошибками: \n',
            reply_markup=await errors_keyboard()
        )
    elif len(get_errors_list_nms()) == 0:
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Отсутствуют процессы, завершившиеся с ошибками. \n'
        )


@dp.callback_query_handler(lambda call: True)
async def callback_inline(call):
    call_data = call.data
    #print(call_data)
    if call.data in get_errors_list_nms():
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Перезапустить', callback_data=f'{call.data}+res_act_rerun'))
        markup.add(InlineKeyboardButton('Закрыть', callback_data=f'{call.data}+res_act_close'))
        markup.add(InlineKeyboardButton('Пропустить', callback_data=f'{call.data}+res_act_skip'))
        # заряжаем новую
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
            reply_markup=await get_keyboard(get_resources_list(call.data), type_nm='single_resource')
        )
    elif call_data.find('+') != -1:
        #print(f'CALLBACK={call_data}')
        #print(f"Результат поиска плюсика = {call_data.find('+')}")
        if call_data.split('+')[1] == 'res_act_rerun':
            update_resource_status(call_data.split('+')[0])
            if len(get_errors_list_nms()) > 0:
                markup = InlineKeyboardMarkup()
                await bot.edit_message_text(
                    chat_id=call.from_user.id,
                    message_id=call.message.message_id,
                    text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на 'А'.</b>\n\nПроцессы, "
                         f"завершившиеся с ошибками: \n",
                    reply_markup=await errors_keyboard()
                )
            elif len(get_errors_list_nms()) == 0:
                # Если ошибочный ресурсов нет, то убираем предыдущую клавиатуру
                await bot.edit_message_text(
                    chat_id=call.from_user.id,
                    message_id=call.message.message_id,
                    text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на "
                         f"'А'.</b>\n\n<b>Отсутствуют процессы, завершившиеся с ошибками.</b>",
                    reply_markup=''
                )
        elif call_data.split('+')[1] == 'res_act_close':
            close_resource_status(call_data.split('+')[0])
            if len(get_errors_list_nms()) > 0:
                markup = InlineKeyboardMarkup()
                await bot.edit_message_text(
                    chat_id=call.from_user.id,
                    message_id=call.message.message_id,
                    text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на 'С'.</b>\n\nПроцессы, "
                         f"завершившиеся с ошибками: \n",
                    reply_markup=await errors_keyboard()
                )
            elif len(get_errors_list_nms()) == 0:
                await bot.edit_message_text(
                    chat_id=call.from_user.id,
                    message_id=call.message.message_id,
                    text=f"<b>Статус процесса {call_data.split('+')[0]} был обновлен с 'E' на "
                         f"'С'.</b>\n\n<b>Отсутствуют "
                         f"процессы, завершившиеся с ошибками.</b>\n",
                    reply_markup=await errors_keyboard()
                )
        elif call_data.split('+')[1] == 'res_act_skip':
            if len(get_errors_list_nms()) > 0:
                markup = InlineKeyboardMarkup()
                await bot.edit_message_text(
                    chat_id=call.from_user.id,
                    message_id=call.message.message_id,
                    text=f"<b>Действий к ресурсу {call_data.split('+')[0]} не применялось.</b>\n\nПроцессы, "
                         f"завершившиеся с ошибками: \n",
                    reply_markup=await errors_keyboard()
                )
            elif len(get_errors_list_nms()) == 0:
                await bot.edit_message_text(
                    chat_id=call.from_user.id,
                    message_id=call.message.message_id,
                    text=f"<b>Действий к ресурсу {call_data.split('+')[0]} не применялось.</b>\n\n<b>Отсутствуют "
                         f"процессы, завершившиеся с ошибками.</b>\n",
                    reply_markup=await errors_keyboard()
                )
        # Блок обработчика для управления отдельными процессами
        elif call_data.split('+')[1] == 'single_resource':
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(InlineKeyboardButton('Запустить', callback_data=f"{call_data.split('+')[0]}+single_res_act_run"),
                       (
                           InlineKeyboardButton('Узнать статус',
                                                callback_data=f"{call_data.split('+')[0]}+single_res_act_status")),
                       InlineKeyboardButton('Закрыть', callback_data=f"{call_data.split('+')[0]}+single_res_act_close"),
                       InlineKeyboardButton('Удалить', callback_data=f"{call_data.split('+')[0]}+single_res_act_delete"),
                       InlineKeyboardButton('Пропустить',
                                            callback_data=f"{call_data.split('+')[0]}+single_res_act_skip")
                       )
            # заряжаем новую
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"Как обработать ресурс {call_data.split('+')[0]}?\n",
                reply_markup=markup
            )
        elif call_data.split('+')[1] == 'single_res_act_run':
            open_resource_status(call_data.split('+')[0])
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Ресурс {call_data.split('+')[0]} запустится при следующей регламетной проверке статусов (каждые 15 минут).</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b> \n",
                reply_markup=await get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_status':
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Текущий статус ресурса {call_data.split('+')[0]}: '{show_resource_status(call_data.split('+')[0])}'.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=await get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_close':
            open_resource_status(call_data.split('+')[0])
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Текущий статус ресурса {call_data.split('+')[0]}: '{show_resource_status(call_data.split('+')[0])}'.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=await get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_skip':
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Пропуск хода.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=await get_keyboard(get_modules_list())
            )
        elif call_data.split('+')[1] == 'single_res_act_delete':
            delete_resource_status(call_data.split('+')[0])
            await bot.edit_message_text(
                chat_id=call.from_user.id,
                message_id=call.message.message_id,
                text=f"<b>Ресурс {call_data.split('+')[0]} был удален из таблицы etl_cfg.cfg_status_table.</b>\n\n"
                     f"Как поступим с другими ресурсами? \n"
                     f"<b>Выберите модуль: </b>\n",
                reply_markup=await get_keyboard(get_modules_list())
            )
        else:
            #print(f'УУУУУХ Я ХЗ ЧЕ ЗА КОМАНДА ПАЦАНЫ!!!!!!!')
            await bot.send_message(
                chat_id=call.from_user.id,
                text=f'УУУУУХ Я ХЗ ЧЕ ЗА КОМАНДА ПАЦАНЫ!!!!!!! callback={call_data}\n'
            )
    else:
        #print(f'УУУУУХ Я ХЗ ЧЕ ЗА КОМАНДА ПАЦАНЫ!!!!!!!')
        await bot.send_message(
            chat_id=call.from_user.id,
            text='УУУУУХ Я ХЗ ЧЕ ЗА КОМАНДА ПАЦАНЫ!!!!!!! callback={call_data}\n'
        )
