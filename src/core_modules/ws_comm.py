from logging import Logger
import pywhatkit
import pywhatkit.whats


class WS_comm:
    def __init__(self, sender_id:str, logger: Logger):
        self.sender_id = sender_id
        self.logger = logger

    def handle_rqst(self, message: dict):
        pass

    def handle_rsp(self, message: dict):
        pass

    def send_ws_message(self, message:str, number:str):
        pywhatkit.whats.sendwhatmsg_instantly(phone_no= number, message=message, tab_close= True,close_time=2)
