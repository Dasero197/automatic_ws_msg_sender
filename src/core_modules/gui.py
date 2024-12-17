from typing import Optional
from customtkinter import *
from PIL import Image, ImageTk
from tkinter import filedialog
from typing import Optional
import logging, os, customtkinter as t, shutil


class Gui:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        #self.icon_path = "files/media/gui_icon.ico"
        t.set_appearance_mode("dark")
        t.set_default_color_theme("dark-blue")
        self.root = t.CTk() # meme si on ne compte pas utiliser le fenetre principale il est necessaire de la creer sinon on a un bug
        self.root.withdraw()

    def prompt_user(self, prompt:str):
        dialog = t.CTkInputDialog(text=prompt, title= "Config")
        dialog.geometry("250x250")
        # image = ImageTk.PhotoImage(Image.open(self.icon_path))
        #dialog.after(250, dialog.iconphoto, False, image) # honnetement vive stack overflow https://stackoverflow.com/questions/78272957/how-to-change-the-icon-in-ctkinputdialog#:~:text=If%20you%20want%20to%20change,it%20will%20be%20override%20implicitly.
        value = dialog.get_input()
        self.root.destroy()
        return value

    def select_and_copy_file(self, file_categorie: str, 
                            destination_path: str, 
                            new_name: Optional[str]= None, 
                            file_type: Optional[list] = [("Fichiers image", "*.png;*.jpg;*.jpeg;*.bmp;*.gif"), ("Tous les fichiers", "*.*")] ):
        
        file_path = filedialog.askopenfilename(
            title=f"Select {file_categorie}",
            filetypes=file_type)
        
        if file_path:
            try:
                if not os.path.exists(destination_path):
                    os.makedirs(destination_path)
                extension = file_path.split(".")[-1]
                if not new_name:
                    file_name = os.path.basename(file_path)
                else:
                    file_name = f"{new_name}.{extension}"

                full_path = f"{destination_path}/{file_name}"

                shutil.copy(file_path, full_path)
                self.logger.info(f"Image copiée avec succès dans : {destination_path}")
                return True, full_path
            except Exception as e:
                self.logger.Exception(f"Erreur lors de la copie : {e}")
                return False, None
        else:
            return False, None
