from redis.asyncio import client as redis
import json, os

class Config:

    def __init__(self):
        self.__config_file_path = "config.json"
        self.__host = "localhost"
        self.__port = 6379

        if os.path.exists(self.__config_file_path):
            with open(self.__config_file_path, "r", encoding="utf-8") as raw:
                file = raw.read()
                data = json.loads(file)
                self.__host = data.get("redis_host", "localhost")
                self.__port = data.get("redis_port", 6379)
        else:
            print("\nError from config class: unable to detect config.json file -- default localhost redis server used -- run setup\n")
        
        self.worker_redis_db = redis.Redis(host=self.__host, port=self.__port, decode_responses=True)
        self.requests_chanel = "wrk-rqst-chan"
        self.responses_chanel = "wrk-resp-chan"
        self.heartbeat_interval = 10