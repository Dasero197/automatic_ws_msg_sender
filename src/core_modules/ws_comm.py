




from logging import Logger


class WS_comm:
    def __init__(self, sender_id:str, logger: Logger):
        self.sender_id = sender_id
        self.logger = logger

    def handle_rqst(self, message: dict):
        pass

    def handle_rsp(self, message: dict):
        pass