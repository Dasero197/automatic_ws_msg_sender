import asyncio
from uuid import uuid4
import logging
from src.config import Config as c

class _Worker_id:
    """
    NE PAS M'IMPORTER HORS DU WORKER CONTEXT
    """
    def __init__(self,logger: logging.Logger, init_id:str):
        self.worker_id = str(uuid4())
        logger.info(f"Worker_id initialized init_id: {init_id}")

    async def __heartbeat_loop(self):
        while True:
            await c().worker_redis_db.set(f"worker:{self.worker_id}", "active", ex=c().heartbeat_interval * 2) # *2 pour majorer en cas de simple retard d'un worker pour ne pas supprimer ces connexions pour rien
            await asyncio.sleep(c().heartbeat_interval)

    def start(self):
        asyncio.create_task(self.__heartbeat_loop())
