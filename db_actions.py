from typing import List, Tuple

import psycopg2

from decorators import on_connection
from main import db_info


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
    			where status_cd=1
            	order by start_dttm desc
            	limit 20 
            	) t1 
        	order by start_dttm
        """)
    return cursor.fetchall()


@on_connection(db_info=db_info)
def get_status(*, conn, **kwargs) -> List[Tuple]:
    """
    Получение свеженей информации о статусе процессов из базы данных
    :param conn: Используемое подключение к БД
    :return: Результат запроса в формате списка кортежей
    """
    # conn = psycopg2.connect(database=db_info.get('NAME'),
    #                               user=db_info.get('USER'),
    #                               password=db_info.get('PASSWORD'),
    #                               host=db_info.get('HOST'),
    #                               port=db_info.get('PORT'))
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
               order by start_dttm desc 
               limit 40
               ) t1 
           	order by start_dttm""")
    query_results = cursor.fetchall()
    cursor.close()
    # conn.close()
    return query_results


@on_connection(db_info=db_info)
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
            	where lower(process_nm) like 'rtp_%'
            	order by start_dttm desc 
    			limit 30
        	) t1 
        	order by start_dttm
        """)
    result = cursor.fetchall()
    cursor.close()
    return result


@on_connection(db_info=db_info)
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
        		where lower(process_nm) like 'vf_%'
        		order by start_dttm desc 
        		limit 30
    	    ) t1 
    	    order by start_dttm
        """)
    result = cursor.fetchall()
    cursor.close()
    return result


@on_connection(db_info=db_info)
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


@on_connection(db_info=db_info)
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


@on_connection(db_info=db_info)
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


@on_connection(db_info=db_info)
def delete_resource_status(*, resource_nm, conn, **kwargs):
    """
    Удаление ресурса из таблицы etl_cfg.etl_status_table
    :param resource_nm: Наименование ресурса
    :param conn: Используемое подключение к БД
    :return: None
    """
    cursor = conn.cursor()
    cursor.execute(f"delete from etl_cfg.cfg_status_table"
                   f" where lower(resource_nm) = '{resource_nm}'")
    conn.commit()
    cursor.close()


@on_connection(db_info=db_info)
def get_resources_list(*, module_nm, conn, **kwargs) -> List[str]:
    """
    Получение списка ресурсов в соответствии с рассматриваемым модулем
    :param module_nm: наименование модуля
    :param conn: Используемое подключение к БД
    :return: Список названий ресурсов
    """
    cursor = conn.cursor()
    cursor.execute(
        f"select distinct lower(resource_nm) from etl_cfg.cfg_resource where lower(module_nm) = '{module_nm}'")
    query_results = cursor.fetchall()
    cursor.close()
    return [tup[0] for tup in query_results] if query_results else None


@on_connection(db_info=db_info)
def get_modules_list(*, conn, **kwargs) -> List[str]:
    """
    Получение списка модулей ресурсов из БД
    :param conn: Используемое подключение к БД
    :return: Список названий модулей
    """
    cursor = conn.cursor()
    cursor.execute("select distinct lower(module_nm) from etl_cfg.cfg_resource")
    query_results = cursor.fetchall()
    cursor.close()
    return query_results


@on_connection(db_info=db_info)
def open_resource_status(*, resource_nm, conn, **kwargs) -> None:
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
    :param resource_nm: Название ресурса, который мы собираемся открыть
    :param conn: Используемое подключение к базе данных
    :return:
    """
    cursor = conn.cursor()
    # Удаляем ресурс из таблицы статусов
    cursor.execute(f"""
        delete from etl_cfg.cfg_status_table
        where lower(resource_nm) = '{resource_nm}'
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
            result = cursor.fetchone()
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
                    VALUES ({resource_id},'{res}','A', now() ,0)
                """)
                conn.commit()
    cursor.close()


@on_connection(db_info=db_info)
def show_resource_status(*, resource_nm, conn, **kwargs) -> str:
    """
    Метод определяет статус рассматриваемого ресурса
    :param resource_nm: наименование ресурса
    :param conn: используемое подключение
    :return:
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        select status_cd
        from etl_cfg.cfg_status_table
        where lower(resource_nm) = '{resource_nm}'
    """)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else "Ресурс отсутствует в таблице статусов"
