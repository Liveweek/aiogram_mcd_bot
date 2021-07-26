from typing import List, Tuple
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


def update_resource_status(*, resource_nm, conn):
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


def delete_resource_status(*, resource_nm, conn):
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
