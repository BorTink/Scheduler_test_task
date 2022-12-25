deactivate_expired = """with upd as (
UPDATE devices SET active = false WHERE exp_date<now() at time zone 'utc' and active <> false RETURNING 1
) SELECT count(*) from upd"""


delete_expired_empty_devices = """with upd as (
DELETE FROM devices WHERE exp_date<now() at time zone 'utc' and instance_id = NULL RETURNING 1
) SELECT count(*) from upd"""


deactivate_duplicates = """with upd as (
UPDATE devices SET active = false
WHERE exp_date<(SELECT max(exp_date)
FROM devices WHERE profile_id = $1) and profile_id = $1 and active <> false RETURNING 1
) SELECT count(*) from upd"""


select_ids = """SELECT DISTINCT profile_id from devices"""


async def db_deactivate_duplicates(conn):
    ids = await conn.fetch(select_ids) # Вычленяю все уникальные id профилей
    result=0
    #result_temp=[]
    for i in range(len(ids)):
        id = int(str(ids[i]).split('=')[1].replace('>', '')) # Перевожу уникальные id в int
        # Для каждого id профиля оставляю лишь последнее по exp_date устройство
        result_temp = await conn.fetchrow(deactivate_duplicates, id)
        #print(id)
        #print(result_temp)
        result+=result_temp[0]
    return result, len(ids)


async def db_delete_expired_empty_devices(conn):
    result = await conn.fetchrow(delete_expired_empty_devices)
    return result


async def db_deactivate_expired(conn):
    result = await conn.fetchrow(deactivate_expired)
    return result


