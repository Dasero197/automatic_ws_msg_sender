import json
import logging, asyncio
import os
from typing import Optional
from src.config import Config as c
from src.core_modules.ws_comm import WS_comm as WS

class _Worker_request:
    """
    NE PAS M'IMPORTER HORS DU WORKER CONTEXT
    """
    def __init__(self, logger:logging.Logger, init_id:str):
        self.__worker_id: str = None
        config = c()
        config.init_redis_client()
        self.__redis = config.worker_redis_db
        self.__pub_sub = self.__redis.pubsub()
        self.__rqst_channel = c().requests_chanel
        self.__logger = logger
        logger.info(f"Worker_request initialized init_id: {init_id}")

    async def start(self, worker_id: str):
        self.__logger.info(f"Worker_request started on worker: {worker_id}")
        self.__worker_id = worker_id
        await self.__pub_sub.subscribe(self.__rqst_channel)
        self.__logger.info(f"Worker_request subscribed on worker: {worker_id}")
        asyncio.create_task(self._listen())

    async def _listen(self):
        self.__logger.info(f"Worker_request listening process start on channel '{self.__rqst_channel}' with worker: {self.__worker_id}")
        try:
            async for message in self.__pub_sub.listen():
                if message["type"] == "message":
                    self._handle_incoming_rqst(message)
        except Exception as e:
            self.__logger.info(f"Exception occured with Worker_request on worker : {self.__worker_id} , error: {e} -- trying restart listening...")
            asyncio.create_task(self._listen())

    def _handle_incoming_rqst(self, message: dict):
        message_dict = json.loads(message["data"])
        target_id = message_dict.pop("target_id", None)
        if target_id == self.__worker_id:
            sender_id = message_dict.pop("sender_id")
            self.__logger.info(f"Request receive from worker: {sender_id} on worker : {self.__worker_id}")
            if message_dict["operation"] == "shutdown":
                print("Signal d'arret de l'orchestrateur reçu -- arrêt du worker")
                self.__logger.info("Signal d'arret de l'orchestrateur reçu -- arrêt du worker")
                os._exit(0)

            else:
                comm_man = WS(sender_id=sender_id,  
                            logger= self.__logger)  #initialiser la class qui doir gerer la requete
                
                comm_man.handle_rqst(msg= message_dict) #créer la tache de gestion de la requete
                

    async def send_rqst(self, request: dict, target_worker_id: str, logger: Optional[logging.Logger] = None, flux_id: Optional[str]=None):
        self.__logger.info(f"Trying to sent request to worker {target_worker_id} on worker : {self.__worker_id} | flux_id: {flux_id}")
        request["sender_id"] = self.__worker_id
        request["target_id"] = target_worker_id
        await self.__redis.publish(channel=self.__rqst_channel, message=json.dumps(request))
        if logger:
            logger.info(f"request from worker {self.__worker_id} sent to worker {target_worker_id} with flux: {flux_id}")

