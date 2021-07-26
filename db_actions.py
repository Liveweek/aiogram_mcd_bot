from typing import List, Tuple
from decorators import on_connection
from main import db_info


def serialize(query):
    return

@on_connection(db_info=db_info)
def get_error_stats(*, conn, **kwargs) -> List[Tuple]:
    """
    Получение статуса ошибок из базы данных
    :param conn: Используемое подключение к БД
    :return: Результат запроса в формате списка кортежей
    """
    cursor = conn.cursor()
    cursor.execute("""
            select t1.* 
            from (
                select 
                    cast(lower(process_nm) as varchar(32)) as process_nm,
            		case 
            		    when status_description='' and end_dttm is null then 'In progress' 
        				when status_description='' and end_dttm is not null then 'Success' 
            			else cast(status_description as varchar(50)) 
        			end as status_description,
        			cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm,
        			cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm,
        			case 
        			    when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
            			else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
            		end as exec_tm,
            		cast(user_nm as varchar(15)) as user
            	from etl_cfg.cfg_log_event 
    			where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
            		    and status_cd=1
            	order by start_dttm desc 
    			limit 40
            	) t1 
        	order by start_dttm
        """)
    return cursor.fetchall()


def get_status(*, conn, **kwargs) -> List[Tuple]:
    """
    Получение свежений информации о статусе процессов из базы данных
    :param conn: Используемое подключение к БД
    :return: Результат запроса в формате списка кортежей
    """
    cursor = conn.cursor()
    cursor.execute("""
            select t1.* 
            from (
               select 
                    cast(lower(process_nm) as varchar(32)) as process_nm,
               		case 
               		    when status_description='' and end_dttm is null then 'In progress' 
           				when status_description='' and end_dttm is not null then 'Success' 
               			else cast(status_description as varchar(50)) 
               		end as status_description,
                    cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm,
                    cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm,
                    case 
                        when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
               			else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
               		end as exec_tm,
               		cast(user_nm as varchar(15)) as user
               from etl_cfg.cfg_log_event 
               where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
               order by start_dttm desc 
               limit 40
               ) t1 
           	order by start_dttm""")
    return cursor.fetchall()


def get_rtp_status(*, conn, **kwargs) -> List[Tuple]:
    """
    Получение свежей информации о краткосрочном прогнозе из Базы Данных
    :param conn: Используемое подклюение к БД
    :return: Результат запроса в формате списка кортежей
    """
    cursor = conn.cursor()
    cursor.execute("""
            select t1.* 
            from (
                select 
                    cast(lower(process_nm) as varchar(32)) as process_nm,
            		case 
            		    when status_description='' and end_dttm is null then 'In progress' 
        				when status_description='' and end_dttm is not null then 'Success' 
            			else cast(status_description as varchar(50)) 
            		end as status_description,
            		cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm,
            		cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm,
            		case 
            		    when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
            			else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
            		end as exec_tm,
            		cast(user_nm as varchar(15)) as user
            				from etl_cfg.cfg_log_event 
            	where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
         			and lower(process_nm) like 'rtp_%'
            	order by start_dttm desc 
    			limit 30
        	) t1 
        	order by start_dttm
        """)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_vf_status(*, conn, **kwargs) -> List[Tuple]:
    """
    Получение свежей информации о долгосрочном прогнозе из Базы данных
    :param conn: Используемое подключение к БД
    :return: Результат запроса в формате списка кортежей
    """
    cursor = conn.cursor()
    cursor.execute("""
            select t1.*
            from (
                select 
                    cast(lower(process_nm) as varchar(32)) as process_nm,
                    case
                        when status_description='' and end_dttm is null then 'In progress' 
    					when status_description='' and end_dttm is not null then 'Success' 
        				else cast(status_description as varchar(50)) 
        			end as status_description,
        			cast(start_dttm + interval'8 hour' as timestamp (0)) as start_dttm,
        			cast(end_dttm + interval'8 hour' as timestamp (0)) as end_dttm,
        			case 
        			    when end_dttm is null then cast(current_timestamp as timestamp (0)) - cast(start_dttm as timestamp (0)) 
        				else cast(end_dttm as timestamp (0)) - cast(start_dttm as timestamp (0))
        			end as exec_tm,
        			cast(user_nm as varchar(15)) as user
        		from etl_cfg.cfg_log_event 
        		where cast(current_date as timestamp (0)) - interval'8 hour' <= start_dttm + interval'8 hour'
     			    and lower(process_nm) like 'vf_%'
        		order by start_dttm desc 
        		limit 30
    	    ) t1 
    	    order by start_dttm
        """)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_current_errors(*, conn, **kwargs) -> List[Tuple]:
    """
    Получение всех процессов со статусом 'E' из Базы данных
    :param conn: Используемое подключение к БД
    :return: Результат запроса в формате списка кортежей
    """
    cursor = conn.cursor()
    cursor.execute("""
        select 
            resource_nm
        from etl_cfg.cfg_status_table
        where status_cd='E'
    """)
    result = cursor.fetchall()
    cursor.close()
    return result


def close_resource_status(*, resource_nm, conn, **kwargs):
    """
    Закрытие ресурса с ошибкой
    :param resource_nm: Наименование ресурса
    :param conn: Используемое подключение к БД
    :return: None
    """
    cursor = conn.cursor()
    cursor.execute(
        f"update etl_cfg.cfg_status_table "
        f"set status_cd='C' "
        f"where status_cd='E' "
        f"      and resource_nm = '{resource_nm}'")
    conn.commit()
    cursor.close()


def update_resource_status(*, resource_nm, conn, **kwargs):
    """
    Открытие ресурса с ошибкой
    :param resource_nm: Наименование ресурса
    :param conn: Используемое подключение к БД
    :return: None
    """
    cursor = conn.cursor()
    cursor.execute(
        f"update etl_cfg.cfg_status_table "
        f"set status_cd='A' "
        f"where status_cd='E' "
        f"      and resource_nm = '{resource_nm}'")
    conn.commit()
    cursor.close()


def delete_resource_status(*, resource_nm, conn, **kwargs):
    """
    Удаление ресурса из таблицы etl_cfg.etl_status_table
    :param resource_nm: Наименование ресурса
    :param conn: Используемое подключение к БД
    :return: None
    """
    cursor = conn.cursor()
    cursor.execute(f"delete from etl_cfg.cfg_status_table"
                   f" where resource_nm = '{resource_nm}'")
    conn.commit()
    cursor.close()


def get_resources_list(*, module_nm, conn, **kwargs) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(
        f"select distinct lower(resource_nm) from etl_cfg.cfg_resource where lower(module_nm) = '{module_nm}'")
    query_results = cursor.fetchall()
    cursor.close()
    if len(query_results) > 0:
        list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    elif len(query_results) == 0:
        list_results = ''
    return list_results


def get_modules_list(*, conn, **kwargs) -> List[str]:
    cursor = conn.cursor()
    cursor.execute("select distinct lower(module_nm) from etl_cfg.cfg_resource")
    query_results = cursor.fetchall()
    cursor.close()
    if len(query_results) > 0:
        list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    elif len(query_results) == 0:
        list_results = ''
    return list_results


def open_resource_status(*, resource_nm, conn, **kwargs):
    """
    Открывает ресурс для загрузки. Алгоритм:
    Удаляется его запись из cfg_status_table
    Найти все зависимые ресурсы для resource_nm
    Для каждого:
        -- Проверка на наличие в cfg_status_table
        -- Если есть -> обновляем статус на "А"
        -- Иначе:
            --- Получаем resource_id из cfg_resource
            --- Добавляем запись в таблицу с этим ресурсом
    Получаем для resource_nm его resource_id из cfg_resource
    Добавляем запись в таблицу
    :param resource_nm: Название ресурса, который мы собираемся открыть
    :param conn: Используемое подключение к базе данных
    :return:
    """
    
    cursor = conn.cursor()
    # Удаляем ресурс из таблицы статусов
    cursor.execute(f"""
        delete
            from etl_cfg.cfg_status_table
        where lover(resource_nm) = '{resource_nm}'
    """)
    conn.commit()

    # Получаем все зависимые ресурсы
    cursor.execute(f"""
        select replace(rule_cond, '/A', '')
        from etl_cfg.cfg_schedule_rule
        where lower(rule_nm) = '{resource_nm}'
    """)

    rule_cond = cursor.fetchone()
    if rule_cond:
        rule_cond_list = rule_cond[0].split()
        for res in rule_cond_list:
            cursor.execute(f"""
                select 1
                from etl_cfg.cfg_status_table
                where lower(resource_nm) = '{res}'
            """)
            result = cursor.fetchone()[0]
            if result:
                # Если ресурс есть в таблице статусов -> обновить
                cursor.execute(f"""
                    update etl_cfg.cfg_status_table 
                    set status_cd = 'A' 
                    where resource_nm = '{res}' 
                        and status_cd not in ('P', 'E')
                """)
                conn.commit()
            else:
                # Если ресурса нет в таблице статусов -> добавить
                cursor.execute(f"""
                    select resource_id 
                    from etl_cfg.cfg_resource
                    where lower(resource_nm) = '{res}'
                """)

                resource_id = cursor.fetchone()[0]

                cursor.execute(f"""
                    insert into etl_cfg.cfg_status_table(resource_id, resource_nm, status_cd, processed_dttm, retries_cnt)
                    VALUES ({resource_id},{res},'A', now() ,0)
                """)
                conn.commit()

    cursor.execute(f"""
        select resource_id
        from elt_cfg.cfg_resource
        where lower(resource_nm) = '{resource_nm}'
    """)

    resource_id = cursor.fetchone()[0]
    cursor.execute(f"""
                        insert into etl_cfg.cfg_status_table(resource_id, resource_nm, status_cd, processed_dttm, retries_cnt)
                        VALUES ({resource_id},{resource_nm},'A', now() ,0)
                    """)
    conn.commit()



    
    
    
    # connection = psycopg2.connect(database=db_info.get('NAME'),
    #                               user=db_info.get('USER'),
    #                               password=db_info.get('PASSWORD'),
    #                               host=db_info.get('HOST'),
    #                               port=db_info.get('PORT'))
    # cursor = connection.cursor()
    # # print('resource_nm=====', resource_nm)
    # cursor.execute(
    #     f"select max(t1.ex_flg) as max_ex_flg from (select 0 as ex_flg union ( select 1012 as ex_flg from etl_cfg.cfg_status_table	where lower(resource_nm) = '{resource_nm}')) t1")
    # # Если ресурс УЖЕ имеется в статусной таблице за сегодня (значит, зависимые ресурсы также имеются в статусе "L"
    # # if int(cursor.fetchone()[0]) == 1:
    # query_rc = cursor.fetchall()
    # query_list = ', '.join([', '.join(map(str, x)) for x in query_rc]).split((', '))
    # if len(query_list[0]) >= 4:
    #     # Если ресурс уже есть в системе, то надо выкинуть предупреждение и другую клавиатуру - перезапускать ли
    #     # в этом случае. Если да: Удаляем текущий ресурс из таблицы статусов
    #     cursor.execute(f"delete from etl_cfg.cfg_status_table where lower(resource_nm) = '{resource_nm}'")
    #     # находим зависимые ресурсы, статус которых нужно проставить в "А", чтобы основной ресурс запустился
    #     cursor.execute(
    #         f"select replace(rule_cond, '/A','') from etl_cfg.cfg_schedule_rule where lower(rule_nm) = '{resource_nm}'")
    #     query_results = cursor.fetchall()
    #     if len(query_results) > 0:
    #         list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    #         i = 1
    #         for obj in list_results:
    #             i = i + 1
    #             # Обновляем зависимый ресурс в "А" для запуска главного ресурса
    #             cursor.execute(f"update etl_cfg.cfg_status_table set status_cd = 'A' where resource_nm = '{obj}'")
    #     else:
    #         print(f'Для указанного ресурса {resource_nm} нет правила запуска! Обратитесь к администратору')
    #     connection.commit()
    # # Если ресурса НЕТ в статусной таблице за сегодня (но, возможно, он загружается - надо проверить)
    # elif len(query_list[0]) <= 1:
    #     # находим зависимые ресурсы, статус которых нужно проставить в "А", чтобы основной ресурс запустился
    #     cursor.execute(
    #         f"select replace(rule_cond, '/A','') from etl_cfg.cfg_schedule_rule where lower(rule_nm) = '{resource_nm}'")
    #     query_results = cursor.fetchall()
    #     if len(query_results) > 0:
    #         list_results = ', '.join([', '.join(map(str, x)) for x in query_results]).split((', '))
    #         i = 0
    #         for obj in list_results:
    #             i = i + 1
    #             # Проверка на существование ресурса в таблице статусов за сегодня
    #             cursor.execute(
    #                 f"select max(t1.ex_flg) as max_ex_flg from (select 0 as ex_flg union ( select 1012 as ex_flg from etl_cfg.cfg_status_table	where lower(resource_nm) = '{obj}')) t1")
    #             query_rc = cursor.fetchall()
    #             query_list_inter = ', '.join([', '.join(map(str, x)) for x in query_rc]).split((', '))
    #             if len(query_list_inter[0]) >= 4:
    #                 # Если ресурс УЖЕ имеется в статусной таблице за сегодня (значит, зависимые ресурсы также имеются в статусе "L"
    #                 if int(query_list_inter[0]) == 1:
    #                     # print(
    #                     #    f"Зависимый ресурс {obj} уже имеется в таблице статусов за сегодня. Ожидайте регламентного запуска процесса (каждые 15 мин).")
    #                     cursor.execute(
    #                         f"update etl_cfg.cfg_status_table set status_cd = 'A' where resource_nm = '{obj}' and status_cd not in ('P', 'E')")
    #             elif len(query_list_inter[0]) <= 1:
    #                 cursor.execute(f"select resource_id from  etl_cfg.cfg_resource where lower(resource_nm) = '{obj}'")
    #                 resource_id = cursor.fetchall()
    #                 resource_id_fmt = ', '.join([', '.join(map(str, x)) for x in resource_id]).split((', '))
    #                 postgres_insert_query = """ INSERT INTO etl_cfg.cfg_status_table(resource_id, resource_nm, status_cd, processed_dttm, retries_cnt) 	VALUES (%s,%s,%s,%s,%s)"""
    #                 record_to_insert = (int(resource_id_fmt[0].split('.')[0]), obj, 'A', 'now()', 0)
    #                 cursor.execute(postgres_insert_query, record_to_insert)
    #             else:
    #                 pass
    #     connection.commit()
    # else:
    #     print('ЕЕЕ ПАРНИ ХЗ ЧТО ПРОИСХОДИТ ЕЕЕЕЕЕ')
    # cursor.close()
    # connection.close()

    def show_resource_status(*, resource_nm, conn):
        connection = psycopg2.connect(database=db_info.get('NAME'),
                                      user=db_info.get('USER'),
                                      password=db_info.get('PASSWORD'),
                                      host=db_info.get('HOST'),
                                      port=db_info.get('PORT'))
        cursor = connection.cursor()
        # проверка на существование статуса для указанного ресурса
        cursor.execute(
            f"select 1012 as ex_flg from etl_cfg.cfg_status_table where lower(resource_nm) = '{resource_nm}'")
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