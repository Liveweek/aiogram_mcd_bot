from config import ADMIN_IDS
from main import bot

def security_wrapper(func):
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
