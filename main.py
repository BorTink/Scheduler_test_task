import asyncpg
from loguru import logger
import asyncio
import schedule
from auth import seconds_amount, config
from db_code import db_delete_expired_empty_devices, db_deactivate_expired, db_deactivate_duplicates


async def main():
    conn = await asyncpg.connect(user=config['user'], host=config['host'], port=config['port'], password=config['password'],
                                            database=config['database']) # Коннектимся к DB через указаные в auth.py атрибуты
    logger.debug('Deactivating expired devices')
    try:
        result = await db_deactivate_expired(conn)
        result = list(result)[0]
        ans = str(result) + ' devices were successfully deactivated'
        logger.info(ans)
    except Exception as e:
        logger.error(f'Error while deactivating expired devices - {str(e)}')

    logger.debug('Deleting expired devices with no instance_id')
    try:
        result = await db_delete_expired_empty_devices(conn)
        result = list(result)[0]
        ans = str(result) + ' expired empty devices were successfully deleted'
        logger.info(ans)
    except Exception as e:
        logger.error(f'Error while deleting expired devices with no instance_id - {str(e)}')

    logger.debug('Deactivating user duplicates')
    try:
        result = await db_deactivate_duplicates(conn)
        ans = str(result[0]) + ' user duplicates were deactivated from ' + str(result[1]) + ' profiles'
        logger.info(ans)
    except Exception as e:
        logger.error(f'Error while deactivating user duplicates - {str(e)}')

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
