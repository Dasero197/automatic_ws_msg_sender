import asyncio
import json, time
from logging import Logger
"""import pywhatkit
import pywhatkit.whats"""
from src.core_modules.state import state



class WS_comm:
    def __init__(self, sender_id:str, logger: Logger):
        from src.core_modules.worker_network.worker_context import worker_context
        self.sender_id = sender_id
        self.logger = logger
        self.config = None
        self.my_id = worker_context.get_worker_id
        with open("_internal/config.json", 'r', encoding='utf-8') as config_file:
                self.config = json.load(config_file)

    def handle_rqst(self, msg: dict):
        state.main._run_async_task(self.rqst_handler(msg= msg), False)

    async def rqst_handler(self, msg: dict):
        try:
            self.logger.info(f"request received from orch:{self.sender_id}")
            print("\nrequest received from orch...")
            contact = msg.get("contact", None)
            message = msg.get("message", None)

            start_time = time.time()
            message = message.replace("{{contact}}", contact.get("name") if contact.get("name",'')!= '' else "Monsieur/Madame" )

            for num in contact.get("phone",[]):
                self.send_ws_message(message= message,number= num)
                self.logger.info(f"Message sent to: {num}")
            
            self.logger.info("request processed sucessfully")
            print("request processed sucessfully")

            end_time = time.time()
            processing_time = end_time - start_time
            response = {
                "error": False,
                "processing_t" : processing_time,
                "my_id": self.my_id
            }
            
        except Exception as e:
            self.logger.exception(f"_send_msg error: {str(e)}")
            response = {
                "error": True,
                "error_msg" : str(e),
                "contact":  msg.get("contact", None),
                "my_id": self.my_id
            }

        finally:
            from src.core_modules.worker_network.worker_context import worker_context
            inst =  worker_context.get_response_inst
            if not inst:
                self.logger.error("Error on geting response instance, cannot send response to orch")
                print("Error on geting response instance, cannot send response to orch")
            else:
                await inst.send_resp(response=response, target_worker_id= str(self.sender_id))
                self.logger.info("report sent to orch")
                print("report sent to orch")


    def handle_rsp(self, message: dict):
        error = message.get("error")
        if error == True:
            state.fail_send.append(message.get("contact"))
            state.error.append(message.get("error_msg"))
            state.busy_worker[message.get("my_id")] = False
        elif error == False:
            state.total_sent += 1
            state.process_times.append(float(message.get("processing_t")))
            state.busy_worker[message.get("my_id")] = False
        else:
            self.logger.error(f"handle response error: unknow error state -- message: {message}")
            state.busy_worker[message.get("my_id")] = False


    def send_ws_message(self, message:str, number:str):
        """pywhatkit.whats.sendwhatmsg_instantly(phone_no= number, 
                                              message=message, 
                                              tab_close= True,
                                              close_time= self.config.get("window_closing_timeout",3), 
                                              wait_time=self.config.get("message_sending_timeout",15) )"""
        time.sleep(30)
