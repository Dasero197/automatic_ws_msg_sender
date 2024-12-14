import json, asyncio
import logging
from src.config import Config as c
from src.core_modules.ws_comm import WS_comm as WS


class _Worker_response:
    """
    NE PAS M'IMPORTER HORS DU WORKER CONTEXT
    """
    def __init__(self, logger:logging.Logger, init_id:str):
        self.__worker_id :str = None
        self.__redis = c().worker_redis_db
        self.__pub_sub = self.__redis.pubsub()
        self.__resp_chanel = c().responses_chanel
        self.__logger = logger
        logger.info(f"Worker_response initialized init_id: {init_id}")

    async def start(self, worker_id: str):
        self.__logger.info(f"Worker_response started on worker: {worker_id}")
        self.__worker_id = worker_id
        await self.__pub_sub.subscribe(self.__resp_chanel)
        self.__logger.info(f"Worker_response subscribed on worker: {worker_id}")
        asyncio.create_task(self._listen())

    async def _listen(self):
        self.__logger.info(f"Worker_response listening process start on channel '{self.__resp_chanel}' with worker: {self.__worker_id}")
        try:
            async for message in self.__pub_sub.listen():
                if message["type"] == "message":
                    await self._handle_incoming_resp(message)
        except Exception as e:
            self.__logger.info(f"Exception occured with Worker_response on worker : {self.__worker_id} , error: {e} -- trying restart listening...")
            asyncio.create_task(self._listen())

    async def _handle_incoming_resp(self, message: dict):
        message_dict: dict = json.loads(message["data"])
        target_id = message_dict.pop("target_id", None)
        if target_id == self.__worker_id:
            sender_id = message_dict.pop("sender_id")
            self.__logger.info(f"Response received from worker: {sender_id} on worker: {self.__worker_id}")
            comm_man = WS(sender_id=sender_id,
                            logger=self.__logger)
                        
            asyncio.create_task(comm_man.handle_rsp(message=message_dict))

    async def send_resp(self, response: dict, target_worker_id: str):
        self.__logger.info(f"Trying to sent response to worker {target_worker_id} from worker : {self.__worker_id} ")
        response["sender_id"] = self.__worker_id
        response["target_id"] = target_worker_id
        await self.__redis.publish(channel= self.__resp_chanel,
                             message= json.dumps(response))
        
        self.__logger.info(f"response from worker {self.__worker_id} sent to worker {target_worker_id}")
