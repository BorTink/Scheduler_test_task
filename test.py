from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio

async def job():
    print(1)

async def start():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job, "interval", seconds=3)
    scheduler.start()


loop = asyncio.get_event_loop()
task = loop.create_task(start)
loop.run_until_complete(task)
