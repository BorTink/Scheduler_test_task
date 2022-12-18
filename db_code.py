deactivate_expired = """UPDATE devices SET active = false WHERE exp_date<NOW()"""


delete_exp_duplicates = """DELETE FROM devices WHERE exp_date<NOW() and instance_id = NULL"""


deactivate_duplicates = """UPDATE devices SET active = false
WHERE exp_date<(SELECT max(exp_date)
FROM devices WHERE profile_id = $1) AND profile_id = $1"""


select_ids = """SELECT DISTINCT profile_id from devices"""


async def db_delete_duplicates(conn):
    ids = await conn.fetch(select_ids)
    for i in range(len(ids)):
        id = int(str(ids[i]).split('=')[1].replace('>', ''))
        await conn.execute(deactivate_duplicates, id)