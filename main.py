import asyncio, logging, os, vobject, json, time
from threading import Thread
from logging.handlers import TimedRotatingFileHandler
from typing import Optional
from src.core_modules.worker_network.worker_context import worker_context
from src.core_modules.ws_comm import WS_comm as WS
from src.core_modules.state import state
from src.core_modules.gui import Gui #designer pour etre initialisé à chaque utilisation et abandonner pour le garbage collector plus tard


class Main:
    def __init__(self):
        self.context = worker_context
        self.logger = self.__setup_logger()
        self.config_path = "config.json"
        self.loop: asyncio.AbstractEventLoop = None

    def __setup_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            log_dir = f"logs/main/{self.__class__.__name__}"
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
    
    def _start_async_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            print("Async event loop started.")
            self.loop.run_forever()  # Maintient la boucle asynchrone en cours d'exécution
        finally:
            print("Async event loop stopped.")
            self.loop.close()

    def _stop_async_loop(self):
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)

    async def _wait_for_future_result(self, future, sleep_time:Optional[float]=0.05):
        while not future.done() :
            await asyncio.sleep(sleep_time)
        return future

    def _run_async_task(self, coro, wait_future:Optional[bool]= False, wait_time:Optional[int]=None):
        if self.loop:
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            if not wait_future:
                return future
            else:
                return future.result(timeout= wait_time)
        else:
            self.logger.error("No running async event loop -- call _start_async_loop()")
            print("No running async event loop -- call _start_async_loop()")
            return None

    def run_sync_in_async_loop(self, sync_function, *args):
        if self.loop:
            return self.loop.run_in_executor(None, sync_function, *args)

    def _load_vcf(self, file_path: str):
   
        contacts = []
        errors = [] 

        with open(file_path, 'r', encoding='utf-8') as file:
            vcf_data = file.read()

        # Split the VCF file into individual vCards using BEGIN:VCARD and END:VCARD
        vcard_blocks = vcf_data.split("BEGIN:VCARD")[1:]  # Skip any header before the first vCard
        for index, vcard_text in enumerate(vcard_blocks, start=1):
            try:
                vcard_text = "BEGIN:VCARD\n" + vcard_text.strip()  # Re-add the BEGIN:VCARD
                vcard = vobject.readOne(vcard_text)  # Parse one vCard at a time
                
                contact = {}
                if hasattr(vcard, 'fn'):  # Full name
                    contact['name'] = vcard.fn.value
                if hasattr(vcard, 'tel_list'):  # Telephone numbers
                    contact['phone'] = [tel.value for tel in vcard.tel_list]
                if hasattr(vcard, 'email_list'):  # Emails
                    contact['email'] = [email.value for email in vcard.email_list]
                
                contacts.append(contact)
            except Exception as e:
                # Log the error for the current vCard block
                error_msg = f"Error parsing vCard #{index}: {str(e)}"
                self.logger.error(error_msg)
                errors.append(error_msg)

        os.remove(file_path)  # Clean up the file after processing

        # Summary
        self.logger.info(f"Finished loading VCF file. Total valid contacts: {len(contacts)}")
        if errors:
            self.logger.warning(f"Encountered {len(errors)} errors while parsing the VCF file.")
            for error in errors:
                self.logger.warning(error)

        return contacts

    def _get_app_type(self):
        print("\n")
        while True:
            try:
                app_type = input("Pour lancer l'orchestrateur appuyer sur '1'\nPour lancer un worker appuyer sur '2'\n")
                if int(app_type) in [1,2]:
                    return int(app_type)
                print("\nSélection invalide\n")
            except ValueError:
                print("\nSélection invalide, votre choix doit etre 1 ou 2\n")

    def _get_contact_and_msg(self):
        print("\nle placeholder pour le nom et prenom du contact est: '{{contact}}'\n")
        msg = None
        path = None
        path_resp = False
        while not msg or not path_resp:
            try:
                if not msg:
                    msg = input("\nRenseigner le message à envoyer\n")
                if not path:
                    print("\nSéléctionner le fichier .vcf dans la boite de dialogue\n")
                    path_resp,path = Gui(self.logger).select_and_copy_file(file_categorie="Fichier de contacts",
                                                        destination_path=".temp/contact_file",
                                                        new_name= "contact",
                                                        file_type= [("Fichier contact", "*.vcf")])
                if msg and path and path_resp == True:
                    return msg, path
                print("\nImpossible de récuperer les contacts et/ou le message, reessayer!\n")
            except Exception as e:
                self.logger.exception(f" exception occured in _get_contact_and_msg: {str(e)}")
                os._exit(0)
        self.logger.error("Impossible de recuperer les contact et le message")
        os._exit(0)

    def _filter_contacts(self, contacts:list[dict]):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as config_file:
                config = json.load(config_file)

            excluded_names = set(config.get("excluded_name", []))
            excluded_numbers = set(config.get("excluded_number", []))
            excluded_words_in_name = config.get("excludes_word_in_name", [])

            filtered_contacts = []

            for contact in contacts:
                name = contact.get('name', '')
                numbers = contact.get('phone', [])
                
                # Vérifier si le nom est dans la liste des exclus
                if name in excluded_names:
                    continue
                
                # Vérifier si le nom contient un mot exclu
                if any(word.lower() in name.lower() for word in excluded_words_in_name):
                    continue
                
                valid_numbers = []
                for number in numbers:
                    # Extraire les 8 derniers chiffres du numéro
                    last_8_digits = number[-8:].replace('-', '').replace(' ', '')

                    # Vérifier si le numéro est dans la liste des exclus
                    if last_8_digits not in excluded_numbers and f"+229{last_8_digits}" not in excluded_numbers:
                        valid_numbers.append(f"+229{last_8_digits}")
                
                if valid_numbers:
                    filtered_contact = {
                        "name": name,
                        "phone": valid_numbers
                    }
                    filtered_contacts.append(filtered_contact)

            return filtered_contacts
        except Exception as e:
            self.logger.exception(f"filter_contact exception: {str(e)}")
            os._exit(0)

    def _contact_generator(self, contacts:list[dict]):
        for contact in contacts:
            yield contact
    
    async def _shutdown_workers(self):
        my_id = self.context.get_worker_id
        active_workers = await self.context.get_active_workers

        rqst_inst = self.context.get_request_inst
        if active_workers and len(active_workers)>1 and rqst_inst:
            print("Arrêt des workers!")
            for worker in active_workers:
                if str(worker) != my_id :
                    request ={"operation":"shutdown"}
                    await rqst_inst.send_rqst(request=request,target_worker_id= worker, logger= self.logger)
                    print(f"worker {worker} arrêter")
                    self.logger.info(f"worker {worker} arrêter")

    async def _send_msg_by_wrkr(self, contact:dict, message:str, target:str):
        try:
            self.logger.info(f"tentative d'envois de requete au worker {target}")
            rqst_inst = self.context.get_request_inst
            if not rqst_inst:
                return self.logger.error(f"Unable to get request instance for worker {target}")
            request ={"contact":contact, "message":message, "operation": "send_msg"}
            await rqst_inst.send_rqst(request=request,target_worker_id= target, logger= self.logger)
            

        except Exception as e:
            self.logger.exception(f"_send_msg_by_wrkr error: {str(e)}")
            raise e

    def _send_msg(self, contact:dict, message:str, sender_id:str):
        try:
            start_time = time.time()
            message = message.replace("{{contact}}", contact.get("name") if contact.get("name",'')!= '' else "Monsieur/Madame" )
            ws = WS(sender_id= sender_id, logger= self.logger)
            for num in contact.get("phone",[]):
                ws.send_ws_message(message= message,number= num)
                self.logger.info(f"Message sent to: {num}")
            state.total_sent += 1

            end_time = time.time()
            processing_time = end_time - start_time
            state.process_times.append(processing_time)
        except Exception as e:
            self.logger.exception(f"_send_msg error: {str(e)}")
            state.fail_send.append(contact)
            raise e
        
    async def _broadcast_message(self, contact:list[dict], message:str, use_local_worker:Optional[bool]= False):

        self.logger.info("Démarrage du processus d'envois des messages")
        print("Démarrage du processus d'envois des messages")
        start_time = time.time()  
        my_id = self.context.get_worker_id
        contact_nb = len(contact)
        contact_gen = self._contact_generator(contact)
        last_prompt = None
        while True:
            try:
                active_workers = self._run_async_task( self.context.get_active_workers, True)
                if active_workers and len(active_workers)>1:
                    
                    for worker in active_workers:
                        if str(worker) != my_id and (not str(worker) in state.busy_worker.keys() or state.busy_worker.get(worker) == False):
                            state.busy_worker[worker]= True
                            # executer  la tache d'envois au worker dans le thread gerant redis vu que asyncio n'aura pas le temps de s'executer ici vu que le while ne s'arretera pas
                            self._run_async_task(self._send_msg_by_wrkr(contact= next(contact_gen),message=message, target= worker), False)
                    
                if (active_workers and not use_local_worker and len(active_workers) <= 1) or use_local_worker:
                    if not use_local_worker:
                        print("\nAucun worker externe actif -- utilisation du worker interne nécéssaire!\n")
                        self.logger.info("\nAucun worker externe actif -- utilisation du worker interne nécéssaire!\n")
                    self._send_msg(contact= next(contact_gen),message=message, sender_id= my_id)

                if last_prompt:
                    if time.time() - last_prompt > 1.5 :
                        print(f"\n{len(active_workers)-1} workers externes actifs")
                        self.logger.info(f"\n{len(active_workers)-1} workers externes actifs")

                        if len(state.process_times)==0:
                            remaining_t = "Inconnu"
                        else:
                            moy_process = (sum(state.process_times) / len(state.process_times))/60
                            remaining_t = f"{moy_process*abs(contact_nb-state.total_sent)} minutes..."
                        print(f"\n{state.total_sent} messages envoyés sur {contact_nb}...\nExtimation du temps restant: {remaining_t}")
                        self.logger.info(f"\n{state.total_sent} messages envoyés sur {contact_nb}...")
                        last_prompt = time.time()
                else:
                    last_prompt = time.time()
            
            except StopIteration:
                while state.total_sent + len(state.fail_send)!= contact_nb:
                    await asyncio.sleep(10)
                    print('waiting for all task to finish to continue...')
                end_time = time.time()
                processing_time = (end_time - start_time)/60
                print(f"\nFin des iterations sur la liste de contact\nTemps de traitement: {processing_time} minutes.\n")
                self.logger.info(f"Fin des iterations sur la liste de contact\nTemps de traitement: {processing_time}")
                break
        
    async def main(self):
    
        app_type = self._get_app_type()
        if app_type == 1:
            #initialisation du client redis dans le thread separer creer au demrrage de l'app
            self.run_sync_in_async_loop(self.context.init_redis_client)
            await self.orch_main()
        elif app_type == 2:
            #lancer redis dans le tread principal et maintenir le tread secondaire actif pour gerer les taches d'envois de message sans bloquer le thread de redis
            self.context.init_redis_client()
            await self.wrkr_main()
        else:
            print(f"App_type : {app_type} not implemented yet")
            os._exit(0)

    async def orch_main(self):
        msg, path = self._get_contact_and_msg()
        essai = 20
        redis_is_up = False
        while essai > 0:
            if not self._run_async_task(self.context.is_redis_online(self.logger), True) :
                print("waiting for redis server to go up...")
                await asyncio.sleep(5)
                essai -= 1
                continue
            redis_is_up = True
            break
        if not redis_is_up:
            print("\nStopping for timemout error: Redis server is down!")
            os._exit(0)
        
        self._run_async_task(self.context.start(self.logger), True)

        contacts = self._load_vcf(file_path= path)
        print(f"\n{len(contacts)} contacts brutes chargé\n")
        
        filtered_contact = self._filter_contacts(contacts)

        while True:
            resp = input(f"\n{len(filtered_contact)} contact restant après application des filtres\nVoulez-vous les consulter? (O/N)")
            if not resp.lower() in ["o", "n"]:
                print("Choix invalide!")
            elif resp.lower() == "o":
                for contact in filtered_contact:
                    print(contact)
                break
            elif resp.lower() == "n":
                break
        
        use_int_worker = False
        while True:
            resp = input("\nVoulez-vous utiliser le worker interne de l'ochestrateur? (O/N): ")
            if not resp.lower() in ["o", "n"]:
                print("Choix invalide!")
            elif resp.lower() == "o":
                use_int_worker = True
                break
            elif resp.lower() == "n":
                use_int_worker = False
                break

        await self._broadcast_message(contact= filtered_contact, message=msg, use_local_worker= use_int_worker)

        self._run_async_task(self._shutdown_workers(),True)

    async def wrkr_main(self):
        essai = 20
        redis_is_up = False
        while essai > 0:
            if not await self.context.is_redis_online(self.logger):
                print("\nwaiting for redis server to go up...")
                await asyncio.sleep(5)
                essai -= 1
                continue
            redis_is_up = True
            break
        if not redis_is_up:
            print("\nStopping for timemout error: Redis server is down!")
            os._exit(0)

        await self.context.start(self.logger)

        my_id = self.context.get_worker_id
        self.logger.info(f"Lancement de du worker id: {my_id}")
        print(f"\nWorker {my_id} lancer et prêt!")

        while True:
            await asyncio.sleep(30) # redis sera interompu chaque 30s pour executer cette ligne qui maintient le thread principal actif

if __name__ == "__main__":
    state.main = Main()
    async_loop_thread = Thread(target=state.main._start_async_loop, daemon=True)
    async_loop_thread.start()
    asyncio.run(state.main.main())