import asyncpg
from loguru import logger
import asyncio
import schedule
from auth import *
from db_code import *


async def main():
    conn = await asyncpg.connect(user=config['user'], host=config['host'], port=config['port'], password=config['password'],
                                            database=config['database']) # Коннектимся к DB через указаные в auth.py атрибуты
    try:
        await conn.execute(deactivate_expired)
        logger.debug('Successfuly deactivated expired devices')
    except Exception as e:
        logger.error(str(e))

    try:
        await conn.execute(delete_exp_duplicates)
        logger.debug('Successfully deleted expired duplicates')
    except Exception as e:
        logger.error(str(e))

    try:
        await db_delete_duplicates(conn)
        logger.debug('Successfully deleted duplicates on every user`s account')
    except Exception as e:
        logger.error(str(e))

    await conn.close()

def run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())


# Настройка schedule-скрипта, который раз в промежуток времени,
# указанный в auth.py выполняет выше написанные функции


schedule.every(seconds_amount).seconds.do(run_loop)
while True:
    schedule.run_pending()
