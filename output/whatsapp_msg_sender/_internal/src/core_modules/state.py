class State:
    def __init__(self):
        self.fail_send:list[dict] = []
        self.busy_worker = {}
        self.total_sent = 0
        self.error = []
        self.process_times:list[float]=[]
        # flemme de tout refactoriser j'ai juste besoin que ça marche pour l'instant donc je vais stocker la ref de l'instance de la classe main ici pour permettre a ws_comm d'acceder aux fonctionnalité passerelles
        self.main = None


state = State()