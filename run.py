import tkinter as tk
from tkinter import messagebox
import mysql.connector
import logging
import sys
import os

# 將 src 目錄加入到 Python 路徑中
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

try:
    import ttkbootstrap as ttk
    from app import MainApplication
except ImportError as e:
    messagebox.showerror("缺少套件", f"必要的套件未找到: {e}\n\n請執行 'pip install -r requirements.txt' 來安裝相依套件。")
    sys.exit(1)

class LoginFrame(ttk.Frame):
    """登入畫面的框架"""
    def __init__(self, master, success_callback):
        super().__init__(master, padding="20")
        self.master = master
        self.success_callback = success_callback

        # 設定主視窗標題和大小
        self.master.title("MySQL 登入")
        self.master.geometry("350x240")
        self.master.resizable(False, False)

        ttk.Label(self, text="使用者名稱:", font=("-size", 10)).pack(fill="x", pady=(0, 5))
        self.user_entry = ttk.Entry(self, font=("-size", 10))
        self.user_entry.pack(fill="x", ipady=4)
        self.user_entry.focus_set()

        ttk.Label(self, text="密碼:", font=("-size", 10)).pack(fill="x", pady=(10, 5))
        self.password_entry = ttk.Entry(self, show="*", font=("-size", 10))
        self.password_entry.pack(fill="x", ipady=4)

        self.status_label = ttk.Label(self, text="", bootstyle="danger")
        self.status_label.pack(fill="x", pady=(10, 0))

        login_button = ttk.Button(self, text="登入", command=self.attempt_login, style="Success.TButton")
        login_button.pack(fill="x", ipady=5, pady=(10, 0))
        
        self.master.bind('<Return>', self.attempt_login)

    def attempt_login(self, event=None):
        user = self.user_entry.get().strip()
        password = self.password_entry.get()

        if not user or not password:
            self.status_label.config(text="使用者名稱和密碼不能為空。")
            return

        self.status_label.config(text="正在嘗試連線...", bootstyle="info")
        self.update_idletasks()

        temp_config = {
            "host": "mysql.theaken.com",
            "port": 33306,
            "user": user,
            "password": password,
            "connect_timeout": 10
        }

        try:
            conn = mysql.connector.connect(**temp_config)
            conn.close()
            
            db_config = temp_config
            db_config['database'] = f"db_{user}"
            
            # 登入成功，呼叫回呼函式
            self.success_callback(db_config)

        except mysql.connector.Error as err:
            logging.error(f"登入失敗: {err}")
            self.status_label.config(text=f"登入失敗: {err.msg}", bootstyle="danger")
        except Exception as e:
            logging.error(f"發生未知錯誤: {e}")
            self.status_label.config(text="發生未知錯誤。", bootstyle="danger")

def run_app():
    root = ttk.Window(themename="litera")

    def on_login_success(db_config):
        # 1. 清除登入畫面
        for widget in root.winfo_children():
            widget.destroy()
        
        # 解除 Return 鍵的綁定，避免影響主程式
        root.unbind('<Return>')

        # 2. 載入主應用程式
        root.resizable(True, True) # 允許調整視窗大小
        root.state('zoomed') # 預設最大化
        app = MainApplication(root, db_config)
        app.pack(expand=True, fill="both")
        
        def on_closing():
            logging.info("應用程式關閉。")
            if hasattr(app, 'db_pool'):
                try:
                    app.db_pool.close()
                    logging.info("MySQL 連線池已關閉。")
                except Exception as e:
                    logging.error(f"關閉連線池時發生錯誤: {e}")
            root.destroy()
        
        root.protocol("WM_DELETE_WINDOW", on_closing)

    # 初始顯示登入畫面
    login_frame = LoginFrame(root, on_login_success)
    login_frame.pack(expand=True, fill="both")

    root.mainloop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run_app()
