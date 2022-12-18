import asyncpg
from loguru import logger
import asyncio
import aioschedule
from auth import db_link
from db_code import deactivate_expired, delete_exp_duplicates, select_ids, deactivate_duplicates, db_delete_duplicates

async def main():
    conn = await asyncpg.connect(db_link)
    try:
        await conn.execute(deactivate_expired)
        logger.debug('Successfuly deactivated expired devices')
    except:
        logger.error(str(Exception))

    try:
        await conn.execute(delete_exp_duplicates)
        logger.debug('Successfully deleted expired duplicates')
    except:
        logger.error(str(Exception))

    try:
        await db_delete_duplicates(conn)
        logger.debug('Successfully deleted duplicates on every user`s account')
    except Exception as e:
        logger.error(e)

    await conn.close()




asyncio.get_event_loop().run_until_complete(main())
