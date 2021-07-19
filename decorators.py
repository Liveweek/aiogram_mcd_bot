import psycopg2

from config import ADMIN_IDS
from main import bot


def security(func):
    def wrapper(**kwargs):
        if kwargs['user_id'] in ADMIN_IDS:
            func(**kwargs)
        else:
            bot.send_message(
                chat_id=kwargs['user_id'],
                text='<b>ДОСТУП ОГРАНИЧЕН</b> \n \
                Обратитесь к администратору бота, чтобы получить доступ к функционалу'
            )

    return wrapper


def on_connection(*, db_info):
    def decorator(func):
        def wrapper(**kwargs):
            connection = psycopg2.connect(database=db_info.get('NAME'),
                                          user=db_info.get('USER'),
                                          password=db_info.get('PASSWORD'),
                                          host=db_info.get('HOST'),
                                          port=db_info.get('PORT'))
            func(**kwargs, conn=connection)
            connection.close()

        return wrapper

    return decorator
