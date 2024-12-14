import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Optional
from uuid import uuid4
from src.core_modules.worker_network.worker_request import _Worker_request
from src.core_modules.worker_network.worker_response import _Worker_response
from src.core_modules.worker_network.worker_id import _Worker_id
from src.config import Config as c
import os, logging, redis


class __Worker_context:

    def __init__(self):
        self.__id_instance : _Worker_id = None
        self.__request_instance : _Worker_request = None
        self.__response_instance : _Worker_response = None
        self.__worker_id :str = None
        self.__started = False
        self.__redis_client = c().worker_redis_db

        self.__logger = self.__setup_logger()
        self.__logger.info("Logger configuré pour Handler.")

    async def is_redis_online(self, logger: logging.Logger):
        try:
            if await self.__redis_client.ping():
                return True
            else:
                return False
        except redis.exceptions.ConnectionError:
            logger.error("Redis server not started or can be access!")
            return False

    def __setup_logger(self):
        
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            log_dir = f"logs/real_time_data/{self.__class__.__name__}"
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"{self.__class__.__name__}.log")

            handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=90)
            handler.suffix = "%Y-%m-%d"
            handler.setLevel(logging.DEBUG)

            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            handler.namer = lambda name: name + ".log"

            logger.addHandler(handler)
            logger.propagate = False

        return logger

    async def start(self, fastapi_log: Optional[logging.Logger]):
        """
        Doit etre lancer pour introuduire le worker au reseau de worker
        Est une fonction Asynchrone.
        """
        redis_started = False
        while redis_started == False:
            redis_started = await self.is_redis_online(fastapi_log)
            if redis_started == False:
                fastapi_log.error("Worker context is waiting for redis server to start...")
                await asyncio.sleep(5)
        
        init_id = str(uuid4())
        self.__logger.info(f"Starting Worker Context initiating ini_id: {init_id}")
        self.__id_instance = _Worker_id(self.__logger, init_id)
        self.__request_instance = _Worker_request(self.__logger, init_id)
        self.__response_instance = _Worker_response(self.__logger, init_id)
        self.__worker_id = self.__id_instance.worker_id
        await self.__response_instance.start(worker_id= self.__worker_id)
        await self.__request_instance.start(worker_id= self.__worker_id)
        self.__id_instance.start()
        self.__started = True
       
        self.__logger.info(f"Worker context started for worker with id : {self.__worker_id}")
        if fastapi_log:
            fastapi_log.info(f"Worker context started for worker with id : {self.__worker_id}")

    
    @property
    def have_started(self):
        """
        informe du demarrage de Worker context
        """
        return self.__started
    
    @property
    def get_worker_id(self):

        """
        Permet d'obtenir le worker id
        """
        if not self.__started:
            self.__logger.error("Module Worker context not started. call Start method in main loop")
        return self.__worker_id if self.__started else None
    
    @property
    def get_request_inst(self):

        """
        Permet d'obtenir une instance de Worker request
        """
        if not self.__started:
            self.__logger.error("Module Worker context not started. call Start method in main loop")
        return self.__request_instance if self.__started else None
    
    @property
    def get_response_inst(self):

        """
        Permet d'obtenir une instance de Worker response
        """
        if not self.__started:
            self.__logger.error("Module Worker context not started. call Start method in main loop")
        return self.__response_instance if self.__started else None

    @property
    async def get_active_workers(self):

        """
        Permet d'obtenir une liste des id de tous les workers actifs du reseau de workers
        """
        if not self.__started:
            self.__logger.error("Module Worker context not started. call Start method in main loop")
        active_workers = await self.__redis_client.keys("worker:*")
        if not active_workers:
            return None
        return [worker.split(":")[1] for worker in active_workers]

worker_context = __Worker_context()

"""
Point d'entrer du reseau de worker
"""

