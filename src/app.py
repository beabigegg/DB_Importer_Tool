import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import sqlite3
import mysql.connector
from mysql.connector import pooling
import threading
import logging
import queue
import os
import pandas as pd
import re

# --- 日誌設定 ---
def setup_logging(log_queue):
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # 將日誌檔案儲存在專案根目錄
    log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db_importer_debug.log')
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    
    queue_handler = QueueHandler(log_queue)
    queue_handler.setFormatter(log_formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(queue_handler)

class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
    def emit(self, record):
        self.log_queue.put(self.format(record))

# --- 主應用程式 ---
class MainApplication(tk.Frame):
    def __init__(self, root, db_config, *args, **kwargs):
        tk.Frame.__init__(self, root, *args, **kwargs)
        self.root = root
        self.root.title(f"資料庫管理工具 (v14.0) - 使用者: {db_config['user']}")
        self.root.geometry("1200x800")
        
        style = ttk.Style()
        style.configure("Custom.Treeview.Heading", background="#f0f0f0", relief="flat", font=('Microsoft JhengHei UI', 9, 'bold'))
        style.map("Custom.Treeview.Heading", background=[('active', '#e0e0e0')])

        self.log_queue = queue.Queue()
        self.raw_data_queue = queue.Queue()
        setup_logging(self.log_queue)

        try:
            self.db_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **db_config)
            logging.info(f"MySQL 連線池建立成功 (使用者: {db_config['user']}, 資料庫: {db_config['database']})。")
        except mysql.connector.Error as err:
            logging.error(f"無法建立 MySQL 連線池: {err}")
            messagebox.showerror("連線池錯誤", f"無法建立 MySQL 連線池: {err}")
            root.destroy()
            return
            return
        
        self.notebook = ttk.Notebook(root)
        self.tab1 = ttk.Frame(self.notebook)
        self.tab2 = ttk.Frame(self.notebook)
        self.tab3 = ttk.Frame(self.notebook)
        self.tab4 = ttk.Frame(self.notebook)
        self.tab5 = ttk.Frame(self.notebook)
        
        self.notebook.add(self.tab1, text='SQLite 複製到 MySQL')
        self.notebook.add(self.tab2, text='MySQL 資料表管理')
        self.notebook.add(self.tab4, text='檔案匯入工具 (Excel/CSV)')
        self.notebook.add(self.tab5, text='操作日誌 (Action Log)')
        self.notebook.add(self.tab3, text='偵錯日誌 (Debug Log)')
        self.notebook.pack(expand=True, fill="both", padx=10, pady=10)

        self.init_copier_tab()
        self.init_manager_tab()
        self.init_log_tab()
        self.init_importer_tab()
        self.init_action_log_tab()
        
        self.root.after(100, self.process_log_queue)
        self.root.after(100, self.process_raw_data_queue)

    def process_log_queue(self):
        while not self.log_queue.empty():
            message = self.log_queue.get_nowait()
            if hasattr(self, 'log_text') and self.log_text.winfo_exists():
                self.log_text.insert(tk.END, message + '\n')
                self.log_text.see(tk.END)
        self.root.after(100, self.process_log_queue)

    def process_raw_data_queue(self):
        try:
            df = self.raw_data_queue.get_nowait()
            logging.info("主線程：從原始資料佇列中取到資料。")
            self.raw_df = df
            self.transformed_df = df.copy()
            self._populate_preview_tree(self.transformed_df)
        except queue.Empty:
            pass
        except Exception as e:
            logging.error(f"處理原始資料佇列時發生錯誤: {e}", exc_info=True)
        finally:
            self.root.after(100, self.process_raw_data_queue)

    # ======================================================================
    # 頁籤四：檔案匯入工具 (Importer Tab)
    # ======================================================================
    def init_importer_tab(self):
        self.raw_df = None
        self.transformed_df = None
        self.headers_promoted = False
        self.all_files_in_folder = []
        self.selected_file_path = tk.StringVar()
        self.is_preview_loading = False
        self.preview_tree = None

        importer_pane = ttk.PanedWindow(self.tab4, orient=tk.HORIZONTAL)
        importer_pane.pack(expand=True, fill="both", padx=5, pady=5)

        settings_container = ttk.Frame(importer_pane, width=400)
        settings_canvas = tk.Canvas(settings_container, highlightthickness=0)
        settings_scrollbar = ttk.Scrollbar(settings_container, orient="vertical", command=settings_canvas.yview)
        settings_canvas.configure(yscrollcommand=settings_scrollbar.set)
        
        settings_scrollbar.pack(side="right", fill="y")
        settings_canvas.pack(side="left", fill="both", expand=True)

        settings_pane = ttk.Frame(settings_canvas, padding=(10, 10, 20, 10))
        settings_canvas.create_window((0, 0), window=settings_pane, anchor="nw")
        
        def on_frame_configure(event):
            settings_canvas.configure(scrollregion=settings_canvas.bbox("all"))
        settings_pane.bind("<Configure>", on_frame_configure)
        
        importer_pane.add(settings_container, weight=1)

        preview_pane = ttk.Frame(importer_pane, padding=10)
        importer_pane.add(preview_pane, weight=4)

        mode_frame = ttk.LabelFrame(settings_pane, text="1. 選擇模式", padding="10")
        mode_frame.pack(fill="x", pady=5, anchor="n")
        self.import_mode = tk.StringVar(value="single")
        ttk.Radiobutton(mode_frame, text="單一檔案", variable=self.import_mode, value="single", command=self._on_mode_change).pack(anchor="w")
        ttk.Radiobutton(mode_frame, text="資料夾", variable=self.import_mode, value="folder", command=self._on_mode_change).pack(anchor="w")

        source_frame = ttk.LabelFrame(settings_pane, text="2. 選擇來源", padding="10")
        source_frame.pack(fill="x", pady=5, anchor="n")
        self.source_path_var = tk.StringVar()
        ttk.Entry(source_frame, textvariable=self.source_path_var, state="readonly").pack(fill="x", expand=True, side="left", padx=(0, 5))
        ttk.Button(source_frame, text="瀏覽...", command=self.browse_source).pack(side="left")

        self.file_selection_frame = ttk.LabelFrame(settings_pane, text="3. 檔案設定", padding="10")
        self.file_selection_frame.pack(fill="x", pady=5, anchor="n")
        
        self.folder_widgets_frame = ttk.Frame(self.file_selection_frame)
        ttk.Label(self.folder_widgets_frame, text="檔名關鍵字篩選:").pack(anchor="w")
        self.file_keyword = tk.StringVar()
        self.file_keyword.trace_add("write", self._update_file_list_view)
        ttk.Entry(self.folder_widgets_frame, textvariable=self.file_keyword).pack(fill="x", pady=(0, 5))
        
        list_frame = ttk.Frame(self.folder_widgets_frame)
        list_frame.pack(fill='x')
        h_scroll = ttk.Scrollbar(list_frame, orient="horizontal")
        v_scroll = ttk.Scrollbar(list_frame, orient="vertical")
        self.file_listbox = tk.Listbox(list_frame, height=6, xscrollcommand=h_scroll.set, yscrollcommand=v_scroll.set)
        h_scroll.config(command=self.file_listbox.xview)
        v_scroll.config(command=self.file_listbox.yview)
        h_scroll.pack(side='bottom', fill='x')
        v_scroll.pack(side='right', fill='y')
        self.file_listbox.pack(side='left', fill='both', expand=True)
        self.file_listbox.bind("<<ListboxSelect>>", self._on_file_selected_from_list)

        self.excel_options_frame = ttk.Frame(self.file_selection_frame)
        ttk.Label(self.excel_options_frame, text="選擇工作表 (Sheet):").pack(anchor="w")
        self.sheet_name = tk.StringVar()
        self.sheet_menu = ttk.Combobox(self.excel_options_frame, textvariable=self.sheet_name, state="readonly")
        self.sheet_menu.pack(fill="x")
        self.sheet_menu.bind("<<ComboboxSelected>>", self._start_raw_data_load_thread)
        
        processing_frame = ttk.LabelFrame(settings_pane, text="4. 資料轉換工具", padding="10")
        processing_frame.pack(fill="x", pady=5, anchor="n")
        
        transform_frame1 = ttk.Frame(processing_frame)
        transform_frame1.pack(fill="x", pady=2)
        ttk.Label(transform_frame1, text="移除頂端 N 行:").pack(side="left")
        self.rows_to_remove = tk.IntVar(value=0)
        ttk.Spinbox(transform_frame1, from_=0, to=100, textvariable=self.rows_to_remove, width=5).pack(side="left", padx=5)
        
        transform_frame2 = ttk.Frame(processing_frame)
        transform_frame2.pack(fill="x", pady=(5,8))
        ttk.Button(transform_frame2, text="使用第一行作為標題", command=self.promote_headers).pack(side="left", expand=True, fill="x", padx=(0,5))
        ttk.Button(transform_frame2, text="套用並更新預覽", command=self._apply_transformations_and_refresh_preview, style="Accent.TButton").pack(side="left", expand=True, fill="x")

        self.encoding_frame = ttk.Frame(processing_frame)
        ttk.Label(self.encoding_frame, text="CSV 編碼:").pack(side="left")
        self.csv_encoding = tk.StringVar(value="utf-8")
        self.csv_encoding_menu = ttk.Combobox(self.encoding_frame, textvariable=self.csv_encoding, values=['utf-8', 'big5', 'gbk'], width=10, state="readonly")
        self.csv_encoding_menu.pack(side="left", padx=5)
        self.csv_encoding_menu.bind("<<ComboboxSelected>>", self._start_raw_data_load_thread)

        self.add_filename = tk.BooleanVar()
        ttk.Checkbutton(processing_frame, text="新增 '檔案來源' 欄位", variable=self.add_filename).pack(anchor="w", pady=(5,0))
        self.deduplicate = tk.BooleanVar()
        ttk.Checkbutton(processing_frame, text="去除重複的資料行", variable=self.deduplicate).pack(anchor="w")

        dest_frame = ttk.LabelFrame(settings_pane, text="5. 匯入目標", padding="10")
        dest_frame.pack(fill="x", pady=5, anchor="n")
        ttk.Label(dest_frame, text="MySQL 資料表名稱:").pack(anchor="w")
        self.mysql_target_table = tk.StringVar()
        ttk.Entry(dest_frame, textvariable=self.mysql_target_table).pack(fill="x", pady=(0, 5))
        ttk.Label(dest_frame, text="若資料表已存在:").pack(anchor="w")
        self.import_action = tk.StringVar(value="覆蓋 (Overwrite)")
        ttk.Combobox(dest_frame, textvariable=self.import_action, values=['覆蓋 (Overwrite)', '附加 (Append)', '失敗 (Fail)'], state="readonly").pack(fill="x")

        action_frame = ttk.LabelFrame(settings_pane, text="6. 執行", padding="10")
        action_frame.pack(fill="x", pady=5, anchor="n")
        
        self.importer_progress_var = tk.DoubleVar()
        self.importer_progressbar = ttk.Progressbar(action_frame, variable=self.importer_progress_var, maximum=100)
        self.importer_progressbar.pack(fill="x", pady=(0,5))
        self.importer_status_label = ttk.Label(action_frame, text="")
        self.importer_status_label.pack(fill="x")

        self.importer_button = ttk.Button(action_frame, text="開始匯入", command=self.start_import_thread, style="Success.TButton")
        self.importer_button.pack(fill="x", ipady=5, pady=(10,0))

        self.preview_frame = ttk.LabelFrame(preview_pane, text="資料預覽 (最多顯示前 50 筆)", padding="10")
        self.preview_frame.pack(expand=True, fill="both")
        
        status_bar_frame = tk.Frame(preview_pane, bg="#f0f0f0")
        status_bar_frame.pack(fill='x', side='bottom', pady=(5,0))
        self.preview_status_label = ttk.Label(status_bar_frame, text="請選擇檔案以產生預覽", background="#f0f0f0", padding=(5,3))
        self.preview_status_label.pack(fill='x')
        
        self._on_mode_change()
    
    def _on_mode_change(self, *args):
        mode = self.import_mode.get()
        self.source_path_var.set("")
        self.selected_file_path.set("")
        self.all_files_in_folder.clear()
        self.file_listbox.delete(0, tk.END)
        self.sheet_menu['values'] = []
        self.sheet_name.set("")
        self.raw_df = None
        self.transformed_df = None
        self.headers_promoted = False
        self._clear_and_recreate_preview_tree()
        if mode == 'single':
            self.folder_widgets_frame.pack_forget()
        else:
            self.folder_widgets_frame.pack(fill="x")
            self.excel_options_frame.pack_forget()
            self.encoding_frame.pack_forget()

    def browse_source(self):
        mode = self.import_mode.get()
        path = ""
        if mode == 'single':
            path = filedialog.askopenfilename(filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv")])
            if path:
                self.source_path_var.set(path)
                self.selected_file_path.set(path)
                self._handle_file_type(path)
        else:
            path = filedialog.askdirectory()
            if path:
                self.source_path_var.set(path)
                self.all_files_in_folder = sorted([f for f in os.listdir(path) if f.endswith(('.csv', '.xlsx', '.xls'))])
                self._update_file_list_view()

    def _update_file_list_view(self, *args):
        keyword = self.file_keyword.get().lower()
        self.file_listbox.delete(0, tk.END)
        for filename in self.all_files_in_folder:
            if keyword in filename.lower():
                self.file_listbox.insert(tk.END, filename)
    
    def _on_file_selected_from_list(self, event):
        selected_indices = self.file_listbox.curselection()
        if not selected_indices: return
        filename = self.file_listbox.get(selected_indices[0])
        folder_path = self.source_path_var.get()
        full_path = os.path.join(folder_path, filename)
        self.selected_file_path.set(full_path)
        self._handle_file_type(full_path)

    def _handle_file_type(self, file_path):
        self.excel_options_frame.pack_forget()
        self.encoding_frame.pack_forget()
        self.headers_promoted = False
        if file_path.endswith(('.xlsx', '.xls')):
            self.excel_options_frame.pack(fill="x", pady=5)
            try:
                xls = pd.ExcelFile(file_path)
                sheets = xls.sheet_names
                self.sheet_menu['values'] = sheets
                if sheets:
                    self.sheet_name.set(sheets[0])
                    self._start_raw_data_load_thread()
            except Exception as e:
                messagebox.showerror("讀取錯誤", f"無法讀取 Excel 工作表: {e}")
        elif file_path.endswith('.csv'):
            self.encoding_frame.pack(fill="x", pady=2)
            self._start_raw_data_load_thread()

    def _start_raw_data_load_thread(self, *args):
        if self.is_preview_loading: return
        file_to_load = self.selected_file_path.get()
        if not file_to_load: return

        self.is_preview_loading = True
        self.preview_status_label.config(text="正在載入原始資料...")
        self.raw_df = None
        self.transformed_df = None
        self.headers_promoted = False
        self._clear_and_recreate_preview_tree()
        
        thread = threading.Thread(target=self._run_raw_data_load, args=(file_to_load,))
        thread.start()

    def _run_raw_data_load(self, file_path):
        try:
            logging.info(f"背景：開始讀取原始檔案 '{os.path.basename(file_path)}'")
            df = self._read_file_raw(file_path, preview=True)
            logging.info(f"背景：成功讀取原始檔案。")
            self.raw_data_queue.put(df)
        except Exception as e:
            logging.error(f"背景讀取原始檔案失敗: {e}", exc_info=True)
            self.raw_data_queue.put(pd.DataFrame())
        finally:
            self.is_preview_loading = False
    
    def _read_file_raw(self, file_path, preview=False, sheet_name_override=None):
        if not file_path: return None
        nrows = 200 if preview else None 
        df = None
        sheet_to_use = sheet_name_override if sheet_name_override else self.sheet_name.get()

        if file_path.endswith(('.xlsx', '.xls')):
            if not sheet_to_use: return None
            df = pd.read_excel(file_path, sheet_name=sheet_to_use, header=None, nrows=nrows)
        elif file_path.endswith('.csv'):
            encoding = self.csv_encoding.get()
            df = pd.read_csv(file_path, encoding=encoding, header=None, low_memory=False, skipinitialspace=True, nrows=nrows)
        
        if df is not None:
            df.dropna(how='all', axis=0, inplace=True)
            df.dropna(how='all', axis=1, inplace=True)
            df.reset_index(drop=True, inplace=True)
            df.columns = [f"Column_{i}" for i in range(df.shape[1])]
        return df

    def _apply_transformations_and_refresh_preview(self, *args):
        if self.raw_df is None:
            messagebox.showinfo("提示", "請先選擇一個檔案以載入資料。")
            return

        logging.info("正在套用轉換並更新預覽...")
        try:
            df = self.raw_df.copy()
            rows_to_skip = self.rows_to_remove.get()
            if rows_to_skip > 0 and rows_to_skip < len(df):
                df = df.iloc[rows_to_skip:].reset_index(drop=True)

            self.transformed_df = df
            self._populate_preview_tree(df)
        except Exception as e:
            logging.error(f"套用轉換時出錯: {e}", exc_info=True)
            messagebox.showerror("轉換錯誤", f"套用轉換時出錯: {e}")

    def promote_headers(self, *args):
        current_df = self.transformed_df
        if current_df is None or current_df.empty:
            messagebox.showinfo("提示", "沒有可提升為標頭的資料。")
            return

        logging.info("正在提升標題列...")
        try:
            df = current_df.copy()
            new_header = df.iloc[0].astype(str)
            df = df[1:]
            df.columns = new_header
            df.reset_index(drop=True, inplace=True)
            
            self.raw_df = df
            self.transformed_df = df
            self.headers_promoted = True
            self._populate_preview_tree(df)
        except Exception as e:
            logging.error(f"提升標題列時出錯: {e}", exc_info=True)
            messagebox.showerror("操作錯誤", f"提升標題列時出錯: {e}")

    def _populate_preview_tree(self, df):
        self._clear_and_recreate_preview_tree()
        if df is None: return

        df_preview = df.copy()
        df_preview = self._sanitize_and_deduplicate_columns(df_preview)
        
        if self.deduplicate.get():
            df_preview.drop_duplicates(inplace=True)
        if self.add_filename.get():
            filename = os.path.basename(self.selected_file_path.get())
            if '檔案來源' not in df_preview.columns:
                df_preview.insert(0, '檔案來源', filename)

        if df_preview.empty:
            self.preview_status_label.config(text="預覽更新：0 筆資料列 × 0 個欄位")
            return
        
        rows, cols = df_preview.shape
        self.preview_status_label.config(text=f"預覽更新：{rows} 筆資料列 × {cols} 個欄位")
        
        for col in df_preview.columns: df_preview[col] = df_preview[col].fillna('').astype(str)
        
        self.preview_tree["columns"] = list(df_preview.columns)
        self.preview_tree["displaycolumns"] = list(df_preview.columns)
        
        for col in df_preview.columns:
            self.preview_tree.heading(col, text=col)
        
        self._autofit_treeview_columns(self.preview_tree)

        for index, row in df_preview.head(50).iterrows():
            self.preview_tree.insert("", "end", values=list(row))

    def _autofit_treeview_columns(self, treeview):
        for col in treeview["columns"]:
            header_width = len(str(col)) * 12 + 20
            treeview.column(col, width=header_width, minwidth=50, stretch=tk.NO)

    def _clear_and_recreate_preview_tree(self):
        if self.preview_tree is not None and self.preview_tree.winfo_exists():
            self.preview_tree.destroy()
            if hasattr(self, 'preview_vsb'): self.preview_vsb.destroy()
            if hasattr(self, 'preview_hsb'): self.preview_hsb.destroy()
        
        self.preview_tree = ttk.Treeview(self.preview_frame, show="headings", style="Custom.Treeview")
        self.preview_vsb = ttk.Scrollbar(self.preview_frame, orient="vertical", command=self.preview_tree.yview)
        self.preview_hsb = ttk.Scrollbar(self.preview_frame, orient="horizontal", command=self.preview_tree.xview)
        self.preview_tree.configure(yscrollcommand=self.preview_vsb.set, xscrollcommand=self.preview_hsb.set)
        
        self.preview_vsb.pack(side="right", fill="y")
        self.preview_hsb.pack(side="bottom", fill="x")
        self.preview_tree.pack(expand=True, fill="both")

        self.preview_status_label.config(text="請選擇檔案或套用轉換")

    def _sanitize_and_deduplicate_columns(self, df):
        original_columns = df.columns.tolist()
        new_columns = []
        seen_counts = {}
        for col in original_columns:
            clean_col = re.sub(r'[\s\n\r\t　]+', ' ', str(col)).strip()
            if not clean_col: clean_col = "Unnamed_Column"
            if clean_col in seen_counts:
                seen_counts[clean_col] += 1
                new_columns.append(f"{clean_col}_{seen_counts[clean_col]}")
            else:
                seen_counts[clean_col] = 0
                new_columns.append(clean_col)
        df.columns = new_columns
        return df

    def start_import_thread(self):
        self.importer_button.config(state=tk.DISABLED)
        self.importer_progress_var.set(0)
        self.importer_status_label.config(text="")
        thread = threading.Thread(target=self.run_import)
        thread.start()
        
    def run_import(self):
        logging.info("="*20 + " 開始新的檔案匯入任務 " + "="*20)
        target_table = self.mysql_target_table.get().strip()
        if not target_table:
            messagebox.showerror("錯誤", "請填寫目標 MySQL 資料表名稱。")
            self.importer_button.config(state=tk.NORMAL)
            return
        
        conn = None
        try:
            all_files_to_process = []
            mode = self.import_mode.get()
            source = self.source_path_var.get()
            if mode == 'single':
                all_files_to_process.append(self.selected_file_path.get())
            else:
                filtered_filenames = self.file_listbox.get(0, tk.END)
                all_files_to_process = [os.path.join(source, f) for f in filtered_filenames]
            if not all_files_to_process: raise ValueError("找不到任何要處理的檔案。")
            
            logging.info(f"找到 {len(all_files_to_process)} 個待處理檔案。")
            
            all_dfs = []
            final_columns = None
            rows_to_skip = self.rows_to_remove.get()
            sheet_name_to_use = self.sheet_name.get()
            
            for i, f_path in enumerate(all_files_to_process):
                logging.info(f"正在完整讀取檔案: {f_path}")
                df = self._read_file_raw(file_path=f_path, sheet_name_override=sheet_name_to_use, preview=False)
                if df is None or df.empty:
                    continue

                if rows_to_skip > 0 and rows_to_skip < len(df):
                    df = df.iloc[rows_to_skip:].reset_index(drop=True)
                
                if df.empty:
                    continue

                if i == 0:
                    if self.headers_promoted:
                        new_header = df.iloc[0].astype(str)
                        df = df[1:]
                        df.columns = new_header
                    
                    df.reset_index(drop=True, inplace=True)
                    df = self._sanitize_and_deduplicate_columns(df)
                    final_columns = df.columns.tolist()
                else:
                    if self.headers_promoted:
                        df = df[1:]
                    
                    df.reset_index(drop=True, inplace=True)
                    
                    if len(final_columns) == df.shape[1]:
                        df.columns = final_columns
                    else:
                        logging.warning(f"檔案 '{os.path.basename(f_path)}' 的欄位數 ({df.shape[1]}) 與第一個檔案 ({len(final_columns)}) 不符，將跳過此檔案。")
                        continue

                if self.add_filename.get():
                    df.insert(0, '檔案來源', os.path.basename(f_path))
                
                all_dfs.append(df)

            if not all_dfs: raise ValueError("所有檔案都無法讀取或為空。")
            
            master_df = pd.concat(all_dfs, ignore_index=True)

            if self.deduplicate.get():
                master_df.drop_duplicates(inplace=True)
            
            total_rows = len(master_df)
            logging.info(f"最終準備匯入 {total_rows} 筆資料到資料表 '{target_table}'")
            self.importer_progressbar['maximum'] = total_rows
            
            master_df = master_df.where(pd.notnull(master_df), None)
            
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES LIKE %s", (target_table,))
            table_exists = cursor.fetchone()
            action = self.import_action.get().split(' ')[0]
            
            if table_exists:
                if action == '失敗': raise ValueError(f"資料表 '{target_table}' 已存在，操作已取消。")
                if action == '覆蓋':
                    cursor.execute(f"DROP TABLE `{target_table}`")
                    table_exists = False
            
            if not table_exists:
                cols_with_types = [f"`{col}` {self.map_pandas_dtype_to_mysql(dtype)}" for col, dtype in master_df.dtypes.items()]
                create_sql = f"CREATE TABLE `{target_table}` ({', '.join(cols_with_types)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
                cursor.execute(create_sql)
            
            if not master_df.empty:
                data_to_insert = [tuple(row) for row in master_df.itertuples(index=False)]
                insert_sql = f"INSERT INTO `{target_table}` ({', '.join([f'`{c}`' for c in master_df.columns])}) VALUES ({', '.join(['%s'] * len(master_df.columns))})"
                
                CHUNK_SIZE = 1000
                rows_written = 0
                for i in range(0, total_rows, CHUNK_SIZE):
                    chunk = data_to_insert[i:i + CHUNK_SIZE]
                    status_text = f"正在寫入資料... {rows_written + len(chunk)} / {total_rows}"
                    self.importer_status_label.config(text=status_text)
                    self.importer_progress_var.set(rows_written + len(chunk))
                    self.root.update_idletasks()
                    
                    cursor.executemany(insert_sql, chunk)
                    conn.commit()
                    rows_written += len(chunk)

            self.importer_status_label.config(text=f"匯入成功！共 {total_rows} 筆資料。", bootstyle="success")
            logging.info("所有資料成功寫入資料庫！")
            messagebox.showinfo("成功", f"成功將 {total_rows} 筆資料匯入到資料表 '{target_table}'。")
        except Exception as e:
            logging.error(f"匯入任務失敗: {e}", exc_info=True)
            self.importer_status_label.config(text=f"任務失敗: {e}", bootstyle="danger")
            messagebox.showerror("匯入失敗", f"任務失敗，請查看日誌。\n\n錯誤: {e}")
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
            self.importer_button.config(state=tk.NORMAL)

    def map_pandas_dtype_to_mysql(self, dtype):
        dtype_str = str(dtype).lower()
        if "int" in dtype_str: return "BIGINT"
        if "float" in dtype_str: return "DOUBLE"
        if "datetime" in dtype_str: return "DATETIME"
        if "bool" in dtype_str: return "TINYINT(1)"
        return "TEXT"

    def init_action_log_tab(self):
        action_log_frame = ttk.LabelFrame(self.tab5, text="資料庫操作日誌", padding="10")
        action_log_frame.pack(expand=True, fill="both", padx=5, pady=5)
        self.action_log_text = scrolledtext.ScrolledText(action_log_frame, state='normal', wrap=tk.WORD, height=10, font=("Courier New", 9))
        self.action_log_text.pack(expand=True, fill="both")

    def log_action(self, message):
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"
        if hasattr(self, 'action_log_text') and self.action_log_text.winfo_exists():
            self.action_log_text.insert(tk.END, log_entry)
            self.action_log_text.see(tk.END)

    # ======================================================================
    # 既有功能頁籤
    # ======================================================================
    def init_log_tab(self):
        log_frame = ttk.LabelFrame(self.tab3, text="偵錯日誌", padding="10")
        log_frame.pack(expand=True, fill="both", padx=5, pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, state='normal', wrap=tk.WORD, height=10, font=("Courier New", 9))
        self.log_text.pack(expand=True, fill="both")

    def init_copier_tab(self):
        main_frame = ttk.Frame(self.tab1, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        sqlite_frame = ttk.LabelFrame(main_frame, text="來源 SQLite 資料庫", padding="10")
        sqlite_frame.pack(fill=tk.X, pady=5)
        ttk.Label(sqlite_frame, text="資料庫檔案路徑:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.sqlite_file_path = tk.StringVar()
        ttk.Entry(sqlite_frame, textvariable=self.sqlite_file_path, width=60, state="readonly").grid(row=0, column=1, padx=5, pady=5)
        ttk.Button(sqlite_frame, text="瀏覽...", command=self.browse_sqlite_file).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(sqlite_frame, text="選擇來源資料表:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.selected_sqlite_table = tk.StringVar()
        self.sqlite_table_combobox = ttk.Combobox(sqlite_frame, textvariable=self.selected_sqlite_table, state="readonly", width=57)
        self.sqlite_table_combobox.grid(row=1, column=1, padx=5, pady=5)
        self.sqlite_table_combobox.bind("<<ComboboxSelected>>", self.update_new_table_name)
        mysql_frame = ttk.LabelFrame(main_frame, text="目標 MySQL 資料庫", padding="10")
        mysql_frame.pack(fill=tk.X, pady=5)
        ttk.Label(mysql_frame, text="設定新資料表名稱:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.mysql_table_name = tk.StringVar()
        ttk.Entry(mysql_frame, textvariable=self.mysql_table_name, width=60).grid(row=0, column=1, padx=5, pady=5)
        self.convert_button = ttk.Button(main_frame, text="開始複製", command=self.start_conversion_thread)
        self.convert_button.pack(pady=10, ipady=4, fill='x')
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=5)
        self.progress_var = tk.DoubleVar()
        self.progressbar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100)
        self.progressbar.pack(fill=tk.X, expand=True)
        self.copier_status_label = ttk.Label(main_frame, text="請先選擇 SQLite 資料庫檔案", bootstyle="info")
        self.copier_status_label.pack(pady=5)

    def convert_database(self):
        logging.info("="*20 + " 開始新的複製任務 " + "="*20)
        self.progress_var.set(0)
        self.copier_status_label.config(text="", bootstyle="info")
        sqlite_file = self.sqlite_file_path.get()
        sqlite_table = self.selected_sqlite_table.get()
        new_mysql_table = self.mysql_table_name.get().strip()
        if not all([sqlite_file, sqlite_table, new_mysql_table]):
            messagebox.showerror("輸入錯誤", "請確認所有欄位都已正確填寫。")
            return
        sqlite_conn = None
        mysql_conn = None
        try:
            self.copier_status_label.config(text="正在連接 SQLite...", bootstyle="warning")
            sqlite_conn = sqlite3.connect(sqlite_file)
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute(f"SELECT COUNT(*) FROM `{sqlite_table}`")
            total_rows = sqlite_cursor.fetchone()[0]
            self.progressbar['maximum'] = total_rows
            sqlite_cursor.execute(f"PRAGMA table_info(`{sqlite_table}`)")
            columns_info = sqlite_cursor.fetchall()
            if not columns_info: raise ValueError(f"在 SQLite 中找不到資料表 '{sqlite_table}' 或該表沒有欄位。")
            self.copier_status_label.config(text="正在從連線池取得 MySQL 連線...", bootstyle="warning")
            mysql_conn = self.db_pool.get_connection()
            mysql_cursor = mysql_conn.cursor()
            self.copier_status_label.config(text=f"正在建立資料表 '{new_mysql_table}'...", bootstyle="warning")
            column_definitions = [f"`{col[1]}` {self.map_sqlite_type_to_mysql(col[2])}" for col in columns_info]
            create_table_query = f"CREATE TABLE `{new_mysql_table}` ({', '.join(column_definitions)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            mysql_cursor.execute(f"DROP TABLE IF EXISTS `{new_mysql_table}`")
            mysql_cursor.execute(create_table_query)
            if total_rows > 0:
                sqlite_cursor.execute(f"SELECT * FROM `{sqlite_table}`")
                column_names_str = ", ".join([f"`{col[1]}`" for col in columns_info])
                placeholders = ", ".join(["%s"] * len(columns_info))
                insert_query = f"INSERT INTO `{new_mysql_table}` ({column_names_str}) VALUES ({placeholders})"
                CHUNK_SIZE = 500
                rows_written = 0
                while True:
                    chunk = sqlite_cursor.fetchmany(CHUNK_SIZE)
                    if not chunk: break
                    mysql_cursor.executemany(insert_query, chunk)
                    mysql_conn.commit()
                    rows_written += len(chunk)
                    self.progress_var.set(rows_written)
                    status_text = f"正在寫入資料... {rows_written} / {total_rows}"
                    self.copier_status_label.config(text=status_text, bootstyle="info")
                    self.root.update_idletasks()
            self.copier_status_label.config(text="複製成功！", bootstyle="success")
            messagebox.showinfo("成功", f"資料表 '{sqlite_table}' 的 {total_rows} 筆資料已成功複製到 '{new_mysql_table}'。")
        except Exception as e:
            logging.error(f"任務失敗！錯誤訊息: {e}", exc_info=True)
            self.copier_status_label.config(text="任務失敗！請查看日誌。", bootstyle="danger")
            messagebox.showerror("任務失敗", f"發生錯誤，請切換到「偵錯日誌」頁籤查看詳細資訊。\n\n錯誤摘要: {e}")
        finally:
            if sqlite_conn: sqlite_conn.close()
            if mysql_conn and mysql_conn.is_connected():
                mysql_cursor.close()
                mysql_conn.close() 
            self.convert_button.config(state=tk.NORMAL)
            self.progress_var.set(0)
            logging.info("="*22 + " 複製任務結束 " + "="*23 + "\n")

    def map_sqlite_type_to_mysql(self, sqlite_type):
        sqlite_type_upper = sqlite_type.upper()
        if "INT" in sqlite_type_upper: return "BIGINT"
        if "CHAR" in sqlite_type_upper or "TEXT" in sqlite_type_upper: return "TEXT"
        if "REAL" in sqlite_type_upper or "FLOAT" in sqlite_type_upper: return "REAL"
        if "DOUBLE" in sqlite_type_upper: return "DOUBLE"
        if "BLOB" in sqlite_type_upper: return "LONGBLOB"
        if "DATE" in sqlite_type_upper: return "DATETIME"
        return "VARCHAR(255)"

    def start_conversion_thread(self):
        self.convert_button.config(state=tk.DISABLED)
        thread = threading.Thread(target=self.convert_database)
        thread.start()
        
    def browse_sqlite_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("SQLite files", "*.sqlite *.db *.sqlite3")])
        if file_path:
            self.sqlite_file_path.set(file_path)
            self.load_sqlite_tables()
            
    def load_sqlite_tables(self):
        db_path = self.sqlite_file_path.get()
        if not db_path: return
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [table[0] for table in cursor.fetchall()]
            self.sqlite_table_combobox['values'] = tables
            if tables:
                self.selected_sqlite_table.set(tables[0])
                self.update_new_table_name(None)
            else:
                self.selected_sqlite_table.set("")
                self.mysql_table_name.set("")
            self.copier_status_label.config(text="已成功讀取資料表，請選擇要複製的資料表", bootstyle="info")
            conn.close()
        except Exception as e:
            messagebox.showerror("讀取錯誤", f"無法讀取 SQLite 資料表: {e}")
            
    def update_new_table_name(self, event):
        self.mysql_table_name.set(self.selected_sqlite_table.get())
        
    def init_manager_tab(self):
        self.current_page = 1
        self.rows_per_page = 100
        self.total_rows = 0
        self.total_pages = 1
        self.current_table_for_data = None

        manager_frame = ttk.Frame(self.tab2, padding="10")
        manager_frame.pack(expand=True, fill="both")
        
        main_pane = ttk.PanedWindow(manager_frame, orient=tk.HORIZONTAL)
        main_pane.pack(expand=True, fill="both")

        left_frame = ttk.Frame(main_pane)
        main_pane.add(left_frame, weight=1)
        
        right_frame = ttk.Frame(main_pane)
        main_pane.add(right_frame, weight=3)

        table_list_frame = ttk.LabelFrame(left_frame, text="資料表列表", padding="10")
        table_list_frame.pack(expand=True, fill="both")
        cols = ('Table Name',)
        self.table_tree = ttk.Treeview(table_list_frame, columns=cols, show='headings', style="Custom.Treeview")
        self.table_tree.heading('Table Name', text='資料表名稱')
        self.table_tree.pack(side="left", expand=True, fill="both")
        self.table_tree.bind('<<TreeviewSelect>>', self.show_table_details)
        scrollbar_y = ttk.Scrollbar(table_list_frame, orient="vertical", command=self.table_tree.yview)
        scrollbar_y.pack(side="right", fill="y")
        self.table_tree.configure(yscrollcommand=scrollbar_y.set)
        
        button_frame = ttk.Frame(left_frame)
        button_frame.pack(fill="x", pady=5)
        ttk.Button(button_frame, text="重新整理", command=self.refresh_mysql_tables).pack(side="left", expand=True, fill="x", padx=2)
        ttk.Button(button_frame, text="創建新表", command=self.create_table_window).pack(side="left", expand=True, fill="x", padx=2)
        ttk.Button(button_frame, text="刪除選項", command=self.delete_table).pack(side="left", expand=True, fill="x", padx=2)

        # --- Right Frame Paned Window ---
        right_pane = ttk.PanedWindow(right_frame, orient=tk.VERTICAL)
        right_pane.pack(expand=True, fill="both")

        column_details_frame = ttk.LabelFrame(right_pane, text="欄位詳細資訊", padding="10")
        right_pane.add(column_details_frame, weight=1)
        
        data_preview_frame = ttk.LabelFrame(right_pane, text="資料預覽", padding="10")
        right_pane.add(data_preview_frame, weight=2)

        # --- Column Details ---
        cols = ('Field', 'Type', 'Null', 'Key', 'Default', 'Extra')
        self.column_tree = ttk.Treeview(column_details_frame, columns=cols, show='headings', style="Custom.Treeview")
        for col in cols:
            self.column_tree.heading(col, text=col)
            self.column_tree.column(col, width=100, anchor='w')
        self.column_tree.pack(expand=True, fill="both")

        # --- Data Preview ---
        self.data_tree = ttk.Treeview(data_preview_frame, show="headings", style="Custom.Treeview")
        data_vsb = ttk.Scrollbar(data_preview_frame, orient="vertical", command=self.data_tree.yview)
        data_hsb = ttk.Scrollbar(data_preview_frame, orient="horizontal", command=self.data_tree.xview)
        self.data_tree.configure(yscrollcommand=data_vsb.set, xscrollcommand=data_hsb.set)
        data_vsb.pack(side="right", fill="y")
        data_hsb.pack(side="bottom", fill="x")
        self.data_tree.pack(expand=True, fill="both")

        pagination_frame = ttk.Frame(data_preview_frame)
        pagination_frame.pack(fill="x", pady=(5, 0))
        
        self.prev_page_button = ttk.Button(pagination_frame, text="<< 上一頁", command=lambda: self.change_page(-1))
        self.prev_page_button.pack(side="left", padx=5)
        
        self.page_status_label = ttk.Label(pagination_frame, text="頁數: 1 / 1")
        self.page_status_label.pack(side="left", padx=5)
        
        self.next_page_button = ttk.Button(pagination_frame, text="下一頁 >>", command=lambda: self.change_page(1))
        self.next_page_button.pack(side="left", padx=5)

        ttk.Separator(pagination_frame, orient=tk.VERTICAL).pack(side="left", fill="y", padx=10)

        self.delete_button = ttk.Button(pagination_frame, text="刪除選定資料", command=self.delete_selected_data, style="Danger.TButton")
        self.delete_button.pack(side="left", padx=5)

        self.add_button = ttk.Button(pagination_frame, text="新增資料", command=self.add_new_data_window, style="Success.TButton")
        self.add_button.pack(side="left", padx=5)

        self.refresh_mysql_tables()

    def run_query(self, query, params=None, fetch=None):
        conn = None
        try:
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            if fetch == 'all': return cursor.fetchall()
            elif fetch == 'one': return cursor.fetchone()
            else:
                conn.commit()
                return True
        except mysql.connector.Error as err:
            logging.error(f"MySQL 查詢錯誤: {err}", exc_info=True)
            messagebox.showerror("MySQL 查詢錯誤", f"錯誤: {err}")
            return None
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def refresh_mysql_tables(self):
        for i in self.table_tree.get_children(): self.table_tree.delete(i)
        for i in self.column_tree.get_children(): self.column_tree.delete(i)
        tables = self.run_query("SHOW TABLES", fetch='all')
        if tables is not None:
            for table in tables: self.table_tree.insert("", "end", values=(table[0],))

    def show_table_details(self, event=None):
        selected_item = self.table_tree.focus()
        if not selected_item: return
        table_name = self.table_tree.item(selected_item)['values'][0]
        self.current_primary_keys = []
        
        # 載入欄位資訊並尋找主鍵
        for i in self.column_tree.get_children(): self.column_tree.delete(i)
        columns = self.run_query(f"DESCRIBE `{table_name}`", fetch='all')
        if columns:
            for col in columns:
                self.column_tree.insert("", "end", values=col)
                if 'PRI' in col[3]: # col[3] is the Key column
                    self.current_primary_keys.append(col[0]) # col[0] is the Field name

        # 載入資料
        self.current_table_for_data = table_name
        self.current_page = 1
        self.load_table_data()

    def load_table_data(self):
        if not self.current_table_for_data:
            return

        table_name = self.current_table_for_data

        # 清空舊資料
        for i in self.data_tree.get_children():
            self.data_tree.delete(i)

        # 取得總筆數和總頁數
        count_result = self.run_query(f"SELECT COUNT(*) FROM `{table_name}`", fetch='one')
        self.total_rows = count_result[0] if count_result else 0
        self.total_pages = (self.total_rows + self.rows_per_page - 1) // self.rows_per_page
        if self.total_pages == 0:
            self.total_pages = 1

        # 取得欄位名稱
        columns_info = self.run_query(f"DESCRIBE `{table_name}`", fetch='all')
        if not columns_info:
            self.data_tree["columns"] = []
            return
            
        column_names = [col[0] for col in columns_info]
        self.data_tree["displaycolumns"] = "#all"
        self.data_tree["columns"] = column_names

        # 取得當前頁面的資料
        offset = (self.current_page - 1) * self.rows_per_page
        data = self.run_query(f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s", params=(self.rows_per_page, offset), fetch='all')

        from tkinter import font
        style_font = ttk.Style().lookup("Treview", "font")
        font_obj = font.Font(font=style_font or ("TkDefaultFont", 9))

        for i, col in enumerate(column_names):
            self.data_tree.heading(col, text=col)
            
            # 自動計算欄寬
            header_width = font_obj.measure(col)
            max_width = header_width
            
            if data:
                for row in data:
                    cell_value = row[i]
                    if cell_value is not None:
                        cell_width = font_obj.measure(str(cell_value))
                        if cell_width > max_width:
                            max_width = cell_width
            
            # 設定欄寬，加入邊距，並禁止伸縮
            self.data_tree.column(col, width=max_width + 30, minwidth=50, stretch=tk.NO)

        if data:
            for row in data:
                self.data_tree.insert("", "end", values=row)

        # 更新分頁狀態
        self.page_status_label.config(text=f"頁數: {self.current_page} / {self.total_pages} (共 {self.total_rows} 筆)")
        self.prev_page_button.config(state="normal" if self.current_page > 1 else "disabled")
        self.next_page_button.config(state="normal" if self.current_page < self.total_pages else "disabled")

    def change_page(self, delta):
        new_page = self.current_page + delta
        if 1 <= new_page <= self.total_pages:
            self.current_page = new_page
            self.load_table_data()

    def delete_selected_data(self):
        selected_items = self.data_tree.selection()
        if not selected_items:
            messagebox.showwarning("未選擇", "請先在資料預覽中選擇要刪除的資料列。")
            return

        if not messagebox.askyesno("確認刪除", f"您確定要永久刪除選定的 {len(selected_items)} 筆資料嗎？\n此操作無法復原！"):
            return

        table_name = self.current_table_for_data
        column_names = self.data_tree["columns"]
        use_primary_key = bool(self.current_primary_keys)

        if use_primary_key:
            pk_indices = [column_names.index(pk) for pk in self.current_primary_keys]
        
        deleted_count = 0
        for item in selected_items:
            item_values = self.data_tree.item(item)['values']
            where_clauses = []
            params = []

            if use_primary_key:
                # 主鍵模式
                for i in pk_indices:
                    where_clauses.append(f"`{self.current_primary_keys[pk_indices.index(i)]}` = %s")
                    params.append(item_values[i])
            else:
                # 全欄位比對模式
                for i, col_name in enumerate(column_names):
                    # 注意：對於 NULL 值，需要使用 IS NULL 而不是 = NULL
                    if item_values[i] is None or str(item_values[i]).upper() == 'NAN':
                        where_clauses.append(f"`{col_name}` IS NULL")
                    else:
                        where_clauses.append(f"`{col_name}` = %s")
                        params.append(item_values[i])
            
            sql = f"DELETE FROM `{table_name}` WHERE {' AND '.join(where_clauses)} LIMIT 1" # Limit 1 增加安全性
            
            if self.run_query(sql, params=params):
                deleted_count += 1
                self.log_action(f"執行刪除: {sql} | 參數: {params} | 結果: 成功")
            else:
                self.log_action(f"執行刪除: {sql} | 參數: {params} | 結果: 失敗")

        messagebox.showinfo("操作完成", f"成功刪除 {deleted_count} 筆資料。")
        self.load_table_data() # 重新載入資料

    def add_new_data_window(self):
        if not self.current_table_for_data:
            messagebox.showwarning("無操作對象", "請先選擇一個資料表。")
            return

        self.add_win = tk.Toplevel(self.root)
        self.add_win.title(f"新增資料到 {self.current_table_for_data}")
        self.add_win.geometry("500x600")
        self.add_win.transient(self.root)
        self.add_win.grab_set()

        canvas = tk.Canvas(self.add_win)
        scrollbar = ttk.Scrollbar(self.add_win, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, padding=20)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        column_names = self.data_tree["columns"]
        self.add_entries = {}

        for col_name in column_names:
            row_frame = ttk.Frame(scrollable_frame)
            row_frame.pack(fill="x", pady=5)
            ttk.Label(row_frame, text=f"{col_name}:", width=20).pack(side="left")
            entry = ttk.Entry(row_frame, width=40)
            entry.pack(side="left", expand=True, fill="x")
            self.add_entries[col_name] = entry

        button_frame = ttk.Frame(self.add_win, padding=10)
        button_frame.pack(fill="x", side="bottom")
        ttk.Button(button_frame, text="儲存", command=self.save_new_data, style="Success.TButton").pack(side="right", padx=5)
        ttk.Button(button_frame, text="取消", command=self.add_win.destroy).pack(side="right")

    def save_new_data(self):
        table_name = self.current_table_for_data
        column_names = list(self.add_entries.keys())
        values = []
        for col_name in column_names:
            value = self.add_entries[col_name].get()
            # 如果使用者未輸入，我們將其視為 NULL
            values.append(None if value == "" else value)

        placeholders = ", ".join(["%s"] * len(column_names))
        sql = f"INSERT INTO `{table_name}` ({', '.join(f'`{c}`' for c in column_names)}) VALUES ({placeholders})"

        if self.run_query(sql, params=values):
            self.log_action(f"執行新增: {sql} | 參數: {values} | 結果: 成功")
            messagebox.showinfo("成功", "資料已成功新增。")
            self.add_win.destroy()
            self.load_table_data()
        else:
            self.log_action(f"執行新增: {sql} | 參數: {values} | 結果: 失敗")
            messagebox.showerror("失敗", "新增資料失敗，請檢查日誌。")

    def delete_table(self):
        selected_item = self.table_tree.focus()
        if not selected_item:
            messagebox.showwarning("未選擇", "請先從左側列表中選擇一個要刪除的資料表。")
            return
        table_name = self.table_tree.item(selected_item)['values'][0]
        if messagebox.askyesno("確認刪除", f"您確定要永久刪除資料表 '{table_name}' 嗎？\n此操作無法復原！"):
            if self.run_query(f"DROP TABLE `{table_name}`"):
                messagebox.showinfo("成功", f"資料表 '{table_name}' 已被成功刪除。")
                self.refresh_mysql_tables()

    def create_table_window(self):
        self.create_win = tk.Toplevel(self.root)
        self.create_win.title("創建新表")
        self.create_win.geometry("500x400")
        tk.Label(self.create_win, text="資料表名稱:", font=("Arial", 10, "bold")).pack(pady=(10,0))
        self.new_table_name_entry = tk.Entry(self.create_win, width=40)
        self.new_table_name_entry.pack(pady=5)
        columns_frame = tk.LabelFrame(self.create_win, text="欄位定義", padx=10, pady=10)
        columns_frame.pack(expand=True, fill="both", padx=10, pady=5)
        self.columns_canvas = tk.Canvas(columns_frame)
        self.columns_canvas.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(columns_frame, orient="vertical", command=self.columns_canvas.yview)
        scrollbar.pack(side="right", fill="y")
        self.scrollable_frame = tk.Frame(self.columns_canvas)
        self.columns_canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.scrollable_frame.bind("<Configure>", lambda e: self.columns_canvas.configure(scrollregion=self.columns_canvas.bbox("all")))
        self.column_rows = []
        self.add_column_row()
        btn_frame = tk.Frame(self.create_win)
        btn_frame.pack(pady=10)
        tk.Button(btn_frame, text="+ 新增欄位", command=self.add_column_row).pack(side="left", padx=5)
        tk.Button(btn_frame, text="創建資料表", command=self.execute_create_table, bg="#007BFF", fg="white").pack(side="left", padx=5)

    def add_column_row(self):
        row_frame = tk.Frame(self.scrollable_frame)
        row_frame.pack(fill="x", pady=2)
        tk.Label(row_frame, text="名稱:").pack(side="left", padx=2)
        name_entry = tk.Entry(row_frame, width=15)
        name_entry.pack(side="left", padx=2)
        tk.Label(row_frame, text="型態:").pack(side="left", padx=2)
        data_types = ['INT', 'VARCHAR(255)', 'TEXT', 'DATE', 'DATETIME', 'DOUBLE', 'BOOLEAN', 'BIGINT', 'LONGBLOB']
        type_var = tk.StringVar()
        type_menu = ttk.Combobox(row_frame, textvariable=type_var, values=data_types, width=15, state="readonly")
        type_menu.pack(side="left", padx=2)
        type_menu.set('VARCHAR(255)')
        pk_var = tk.BooleanVar()
        pk_check = tk.Checkbutton(row_frame, text="主鍵(PK)", variable=pk_var)
        pk_check.pack(side="left", padx=2)
        self.column_rows.append({'frame': row_frame, 'name': name_entry, 'type': type_var, 'pk': pk_var})

    def execute_create_table(self):
        table_name = self.new_table_name_entry.get().strip()
        if not table_name:
            messagebox.showerror("錯誤", "請輸入資料表名稱。")
            return
        definitions = []
        primary_keys = []
        for row in self.column_rows:
            col_name = row['name'].get().strip()
            col_type = row['type'].get()
            is_pk = row['pk'].get()
            if col_name and col_type:
                definitions.append(f"`{col_name}` {col_type} NOT NULL")
                if is_pk: primary_keys.append(f"`{col_name}`")
        if not definitions:
            messagebox.showerror("錯誤", "請至少定義一個有效的欄位。")
            return
        query = f"CREATE TABLE `{table_name}` ("
        query += ", ".join(definitions)
        if primary_keys:
            query += f", PRIMARY KEY ({', '.join(primary_keys)})"
        query += ");"
        if self.run_query(query):
            messagebox.showinfo("成功", f"資料表 '{table_name}' 已成功創建。")
            self.refresh_mysql_tables()
            self.create_win.destroy()
