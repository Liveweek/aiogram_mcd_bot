from aiogram import Bot, Dispatcher, executor
import asyncio
import dj_database_url
import psycopg2


from config import TG_TOKEN, DATABASELINK_POSTGRES
from filters import IsAdmin


loop = asyncio.get_event_loop()
bot = Bot(token=TG_TOKEN, parse_mode='HTML')
dp = Dispatcher(bot=bot, loop=loop)

db_info = dj_database_url.config(default=DATABASELINK_POSTGRES)
connection = psycopg2.connect(database=db_info.get('NAME'),
                              user=db_info.get('USER'),
                              password=db_info.get('PASSWORD'),
                              host=db_info.get('HOST'),
                              port=db_info.get('PORT'))

if __name__ == '__main__':
    from handlers import dp
    dp.bind_filter(IsAdmin)
    executor.start_polling(dispatcher=dp, skip_updates=True)

