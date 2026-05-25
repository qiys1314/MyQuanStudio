# ==============================================================================
# 模块名称: ui/main_window.py
# 核心功能: 创建应用程序图形界面 (GUI)。接收用户的参数配置，发起后端计算，
#           并对返回的结果数据集进行表格展示、异常过滤和 Excel 文件导出。
#           包含系统防崩溃、强制退出拦截以及文件安全导出保护机制。
# ==============================================================================

import os
import json
import pandas as pd
from datetime import datetime, timedelta
# 导入 PyQt5 用于构建图形化窗口、布局管理器及各类输入控件
from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QGridLayout, QLabel, QComboBox, QSpinBox, 
                             QDoubleSpinBox, QPushButton, QTextEdit, QFrame, 
                             QDateEdit, QTableWidget, QTableWidgetItem, 
                             QHeaderView, QAbstractItemView, QSplitter, QCheckBox,
                             QProgressBar, QMessageBox, QMenu)
from PyQt5.QtCore import Qt, QDate, QThread, pyqtSignal
# ====== 导入 QCloseEvent 用于拦截窗口右上角的关闭 (X) 按钮 ======
from PyQt5.QtGui import QCloseEvent

# 导入业务类：数据拉取线程、量化计算引擎、数据库工具以及导出路径
from core.data_updater import DataUpdateThread
from core.calc_engine import CalcEngine
from database.db_manager import DBManager 
from utils.config import EXPORT_DIR, SETTINGS_PATH


# ------------------------------------------------------------------------------
# 独立的后台计算线程类
# 架构意图: Numpy 和 Pandas 的大量数组运算若在主线程执行，会导致界面完全卡死无响应。
# 因此封装在 QThread 中处理。
# ------------------------------------------------------------------------------
class CalcThread(QThread):
    # 定义传递文本日志的信号
    log_signal = pyqtSignal(str)           
    # 定义传递列表格式计算结果的信号
    result_signal = pyqtSignal(list)      
    # 定义进度数值信号
    progress_signal = pyqtSignal(int) 
    
    def __init__(self, params): 
        super().__init__()
        # 保存主界面通过构造函数传入的用户设定参数字典
        self.params = params
        # 线程自身的运行状态标志
        self.is_running = True              
        
    def run(self):
        try:
            # 通过 lambda 将自己的 is_running 状态打包成函数传给引擎
            results = CalcEngine.run_filter(
                self.params, 
                self.log_signal, 
                self.progress_signal, 
                check_running=lambda: self.is_running
            )
            self.result_signal.emit(results)
        except Exception as e: 
            self.log_signal.emit(f"❌ 计算引擎运行时抛出异常: {e}")

# ------------------------------------------------------------------------------
# 软件主界面类
# ------------------------------------------------------------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        # 继承 QMainWindow 的初始化设定
        super().__init__()
        
        # 设定操作系统显示的程序窗口标题
        self.setWindowTitle("个人量化筛选终端 V1.0")
        # 设定初始化时的窗口默认宽高 (宽1150像素，高800像素)
        self.resize(1150, 800)
        
        # ====== 定义一个全局状态变量，用于标记后台是否正在执行任务 ======
        # 该变量用于控制“中止任务”按钮的状态和“拦截退出”弹窗的触发
        self.is_task_running = False
        
        # 设定界面整体的 QSS 样式表 (类似于网页前端的 CSS)
        # 统一规范控件的背景颜色、字体(微软雅黑)、卡片描边和悬停效果
        self.setStyleSheet("""
            QMainWindow { background-color: #f8fafc; }
            * { font-family: "Segoe UI", "Microsoft YaHei"; }
            QFrame#RowCard { background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px; }
            QLabel#HeaderLabel { color: #94a3b8; font-size: 12px; font-weight: bold; }
            QLabel#MetricName, QCheckBox#MetricName { color: #1e293b; font-size: 13px; font-weight: bold; }
            QLabel#SubDesc { color: #64748b; font-size: 12px; }
            QComboBox, QSpinBox, QDoubleSpinBox, QDateEdit { border: 1px solid #cbd5e1; border-radius: 4px; padding: 4px 8px; font-size: 12px; }

            # =======================================================
            # 1. 全局按钮核心样式统一（全面推向极简白底 + 生态绿交互）
            # =======================================================
            QPushButton { 
                background-color: #ffffff; 
                border: 1px solid #cbd5e1; 
                /* 统一维持 2px 底部厚度，产生轻微的 3D 浮雕质感 */
                border-bottom: 2px solid #94a3b8; 
                border-radius: 6px; 
                padding: 6px 12px; 
                font-weight: bold; 
                color: #334155; 
            }
            
            /* 鼠标悬停：所有按钮（含筛选、云同步）高亮时统一呈现精致的生态绿风格 */
            QPushButton:hover { 
                background-color: #f0fdf4;   /* 浅绿背景 */
                border-color: #86efac;       /* 翠绿边框 */
                border-bottom-color: #166534;/* 深绿底边 */
                color: #166534;              /* 深绿文字 */
            }
            
            /* 鼠标按下：所有按钮下沉幅度完全一致，触发弹簧机械反馈 */
            QPushButton:pressed { 
                background-color: #dcfce7; 
                border-bottom: 1px solid #bbf7d0; 
                margin-top: 1px;             /* 机械下沉 */
            }

            # =======================================================
            # 2. 危险操作按钮（任务运行中，“清空”切换为“中止”时的预警状态）
            # =======================================================
            QPushButton#DangerBtn {
                background-color: #fef2f2;
                color: #dc2626;
                border: 1px solid #fca5a5;
                border-bottom: 2px solid #ef4444;
            }
            QPushButton#DangerBtn:hover {
                background-color: #fee2e2;
                border-color: #ef4444;
                color: #b91c1c;
            }
            QPushButton#DangerBtn:pressed {
                background-color: #fca5a5;
                border-bottom: 1px solid #fca5a5;
                margin-top: 1px;
            }

            # =======================================================
            # 3. 下拉菜单样式统一（视觉调性与常规按钮悬停完美呼应）
            # =======================================================
            QMenu {
                background-color: #ffffff;
                border: 1px solid #cbd5e1;
                border-radius: 4px;
                padding: 4px 0px;
                font-family: "Segoe UI", "Microsoft YaHei";
                font-size: 12px;
            }
            QMenu::item {
                padding: 8px 24px;
                color: #475569;
            }
            /* 下拉菜单项被鼠标悬停选中的状态 */
            QMenu::item:selected {
                background-color: #f0fdf4;   /* 契合全局的生态绿 */
                color: #166534;
                font-weight: bold;
            }

            QTextEdit { background-color: #0f172a; color: #38bdf8; border-radius: 6px; font-family: "Consolas", monospace; padding: 10px; }
            QTableWidget { background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px; gridline-color: #f1f5f9; }
            QHeaderView::section { background-color: #f8fafc; padding: 4px; border: none; border-bottom: 1px solid #cbd5e1; font-weight: bold; color: #475569; }
        """)
        
        # 调用搭建 DOM 布局和插入控件的方法
        self.setup_ui()        
        # 调用事件绑定方法，将按钮操作关联到对应函数
        self.connect_signals() 
        
        # 软件刚打开时，默认禁用所有按钮并显示进度条
        self.set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        # ======设置系统自检的动态进度文字 ======
        self.progress_bar.setFormat("🔍 系统数据健康自检中... %p%")
        
        # 实例化数据下载线程，并传入特殊的 'self_check' 指令。
        # 目的是在软件刚被双击打开展示出界面时，就在后台静默运行自检程序
        self.checker_thread = DataUpdateThread("self_check")
        # 将线程内的打印信号连接到当前 UI 界面的 log_info 函数
        self.checker_thread.log_signal.connect(self.log_info)
        
        # 将自检线程的进度连接到进度条
        self.checker_thread.progress_signal.connect(self.progress_bar.setValue)
        
        # 自检完成后，隐藏进度条并恢复按钮可用状态
        self.checker_thread.finished_signal.connect(lambda: self.progress_bar.setVisible(False))
        self.checker_thread.finished_signal.connect(lambda: self.set_buttons_enabled(True))
        # 启动自检线程
        self.checker_thread.start()
        # ====== 加载上次UI保存的配置 ======
        self.load_settings()

    def create_row_card(self, metric_name, sub_desc, cycle_widget, threshold_widget):
        """
        创建用于参数设置的标准化 UI 行组件卡片。
        将多个零散的输入控件打包为一个带有网格布局的 QFrame 容器。
        """
        # 实例化卡片容器并设置对象名称以匹配 QSS 样式表
        card = QFrame()
        card.setObjectName("RowCard")
        
        # 实例化网格布局管理器，并将其挂载至卡片容器
        layout = QGridLayout(card)
        
        # 配置卡片内部边距：左15，上2，右15，下2，以压缩卡片的整体垂直高度
        layout.setContentsMargins(15, 2, 15, 2) 
        
        # 创建左侧的参数指标名称标签
        name_label = QLabel(metric_name)
        name_label.setObjectName("MetricName")
        name_label.setFixedWidth(100) 
        # 将该标签放置在网格的第 0 行第 0 列
        layout.addWidget(name_label, 0, 0)
        
        # 创建中间周期设置区域的水平布局管理器
        cycle_box = QHBoxLayout()
        lbl_sub = QLabel(sub_desc)
        lbl_sub.setObjectName("SubDesc")
        lbl_sub.setFixedWidth(55) 
        
        # 将描述文本和对应的交互控件按序加入水平布局，并在末尾添加弹性拉伸空间
        cycle_box.addWidget(lbl_sub)
        cycle_box.addWidget(cycle_widget)
        cycle_box.addStretch() 
        # 将构建好的水平布局嵌套存放在网格的第 0 行第 1 列
        layout.addLayout(cycle_box, 0, 1)
        
        # 设置右侧的阈值输入控件宽度，并放置于网格的第 0 行第 2 列
        threshold_widget.setFixedWidth(150) 
        layout.addWidget(threshold_widget, 0, 2)
        
        # 设定第 1 列（即中间的周期设置区）具备延伸权重，自动吸收多余宽度
        layout.setColumnStretch(1, 1) 
        
        return card
    def setup_ui(self):
        """
        初始化主界面 UI，完成所有控件的实例化、层级排布与布局参数初始化。
        """
        # 初始化主窗口的中心基础部件
        self.main_widget = QWidget(self)
        self.setCentralWidget(self.main_widget) 
        self.main_layout = QVBoxLayout(self.main_widget) 
        self.main_layout.setContentsMargins(15, 5, 15, 5)
        self.main_layout.setSpacing(4)
        
        # 1 & 2. 顶部综合配置行：市场板块 + 剔除亏损
        top_card = QFrame()
        top_card.setObjectName("RowCard")
        top_layout = QHBoxLayout(top_card)
        # 压缩内边距
        top_layout.setContentsMargins(15, 4, 15, 4) 
        
        # [前置] 市场板块
        lbl_mk = QLabel("市场板块：")
        lbl_mk.setObjectName("MetricName")
        self.market_combo = QComboBox()
        self.market_combo.addItems(["主板", "创业板", "科创板"])
        self.market_combo.setFixedWidth(120)
        
        # [后置] 剔除亏损 (依然是勾选框形式)
        self.filter_bad_finance_cb = QCheckBox("剔除亏损及异常财报")
        self.filter_bad_finance_cb.setChecked(True)
        # 赋予相同的样式 ID，实现字体一致性
        self.filter_bad_finance_cb.setObjectName("MetricName") 
        self.filter_bad_finance_cb.stateChanged.connect(self.refresh_table_display)
        
        # 组装：排在同一行
        top_layout.addWidget(lbl_mk)
        top_layout.addWidget(self.market_combo)
        top_layout.addSpacing(30) # 物理隔离间距
        top_layout.addWidget(self.filter_bad_finance_cb)
        top_layout.addStretch() # 推至左侧对齐
        
        self.main_layout.addWidget(top_card)

        # 3. 构建参数表头提示区域
        header_widget = QWidget()
        h_layout = QGridLayout(header_widget)
        h_layout.setContentsMargins(15, 0, 15, 0)
        
        lbl_col1 = QLabel("参数类型")
        lbl_col1.setObjectName("HeaderLabel")
        lbl_col1.setFixedWidth(100) 
        lbl_col2 = QLabel("时间区间")
        lbl_col2.setObjectName("HeaderLabel")
        lbl_col3 = QLabel("阈值界限")
        lbl_col3.setObjectName("HeaderLabel")
        lbl_col3.setFixedWidth(150) 
        
        h_layout.addWidget(lbl_col1, 0, 0)
        h_layout.addWidget(lbl_col2, 0, 1)
        h_layout.addWidget(lbl_col3, 0, 2)
        h_layout.setColumnStretch(1, 1)
        self.main_layout.addWidget(header_widget)

        # 4. 实例化各参数控制行
        # 4.1 复现比参数设定
        period_rec = QWidget()
        pr_layout = QHBoxLayout(period_rec)
        pr_layout.setContentsMargins(0, 0, 0, 0)
        self.start_date = QDateEdit(); self.start_date.setCalendarPopup(True); self.start_date.setDate(QDate(2020, 1, 1)); self.start_date.setMaximumDate(QDate.currentDate()) 
        self.end_date = QDateEdit(); self.end_date.setCalendarPopup(True); self.end_date.setDate(QDate.currentDate()); self.end_date.setMaximumDate(QDate.currentDate())
        pr_layout.addWidget(self.start_date); pr_layout.addWidget(QLabel("-")); pr_layout.addWidget(self.end_date)
        
        self.recur_spin = QDoubleSpinBox(); self.recur_spin.setRange(0.0, 100.0); self.recur_spin.setDecimals(1); self.recur_spin.setSingleStep(0.1); self.recur_spin.setValue(2.0); self.recur_spin.setSuffix(" 倍")
        self.main_layout.addWidget(self.create_row_card("复现比", "起止时间", period_rec, self.recur_spin))

        # 4.2 价格位置比参数设定
        self.price_period = QComboBox(); self.price_period.addItems(["半年", "一年", "两年", "三年"]); self.price_period.setCurrentIndex(1)  
        self.price_spin = QSpinBox(); self.price_spin.setRange(1, 100); self.price_spin.setValue(20); self.price_spin.setSuffix(" %")
        self.main_layout.addWidget(self.create_row_card("价格位置比", "回溯", self.price_period, self.price_spin))

        # 4.3 成交量位置比参数设定
        self.vol_period = QSpinBox(); self.vol_period.setRange(1, 30); self.vol_period.setValue(5); self.vol_period.setSuffix(" 天")
        vol_combo_widget = QWidget()
        vol_combo_layout = QHBoxLayout(vol_combo_widget)
        vol_combo_layout.setContentsMargins(0, 0, 0, 0)  # 边距清零，保证严丝合缝
        vol_combo_layout.setSpacing(6)                  # 天数框和文字的微调间距
        vol_combo_layout.addWidget(self.vol_period)     # 塞入天数框
        
        lbl_avg_text = QLabel("平均值")
        lbl_avg_text.setObjectName("SubDesc")           # 沿用系统自带的灰色小字样式
        vol_combo_layout.addWidget(lbl_avg_text)        # 塞入固定标签
        
        self.vol_spin = QSpinBox(); self.vol_spin.setRange(1, 1000); self.vol_spin.setValue(20); self.vol_spin.setSuffix(" %")
        self.main_layout.addWidget(self.create_row_card("成交量位置比", "最近", vol_combo_widget, self.vol_spin))

        # 5. 横排全局操作按钮组
        btn_layout = QHBoxLayout()
        
        # 5.1 核心主推按钮：云同步
        self.btn_sync = QPushButton("☁️ 极速更新数据")
        self.btn_sync.setObjectName("SyncBtn")
        
        # 5.2 次要功能折叠：手动更新菜单
        self.btn_manual_update = QPushButton("⚙️ 手动更新数据 ")
        self.manual_update_menu = QMenu(self)
        
        # 将原先的三个独立按钮转换为菜单动作 (QAction)
        self.action_up_k = self.manual_update_menu.addAction("📊 更新 K线 (Baostock)")
        self.action_up_f = self.manual_update_menu.addAction("💰 更新财报 (AkShare)")
        self.action_up_d = self.manual_update_menu.addAction("🎁 更新分红 (AkShare)")
        
        # 将菜单挂载到按钮上
        self.btn_manual_update.setMenu(self.manual_update_menu)
        
        # 5.3 筛选与清空按钮
        self.btn_ok = QPushButton("🚀 启动筛选"); self.btn_ok.setObjectName("PrimaryBtn") 
        self.btn_clear = QPushButton("🗑️ 清空显示")
        
        #5.4 打开本地导出目录按钮：实例化按钮对象
        self.btn_open_dir = QPushButton("📁 筛选结果目录")
        
        # 将新层级的按钮按序加入布局
        for b in [self.btn_sync, self.btn_manual_update, self.btn_ok, self.btn_clear, self.btn_open_dir]: 
            btn_layout.addWidget(b)
            
        self.main_layout.addLayout(btn_layout)

        # 6. 构建核心数据展示区
        self.splitter = QSplitter(Qt.Vertical)
        self.log_box = QTextEdit(); self.log_box.setReadOnly(True) 
        self.splitter.addWidget(self.log_box)

        self.result_table = QTableWidget(); self.result_table.setColumnCount(10)
        self.result_table.setHorizontalHeaderLabels(["代码", "名称", "净值", "现价", "后复权价", "复现比", "价格比(%)", "成交量比(%)", "静态市盈率", "市盈率TTM"])
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.splitter.addWidget(self.result_table)
        
        self.splitter.setSizes([250, 300]) 
        self.main_layout.addWidget(self.splitter, stretch=1)

        # 7. 构建底层状态栏及进度条
        self.statusBar().showMessage("系统准备就绪！")
        self.statusBar().setStyleSheet("QStatusBar { background-color: #ffffff; border-top: 1px solid #cbd5e1; color: #64748b; }")
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100); self.progress_bar.setValue(0); self.progress_bar.setTextVisible(True); self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar { border: 1px solid #cbd5e1; border-radius: 6px; background-color: #f1f5f9; text-align: center; font-weight: bold; color: #0f172a; min-height: 20px; max-height: 20px; }
            QProgressBar::chunk { background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #2563eb, stop:1 #0ea5e9); border-radius: 5px; }
        """)
        self.statusBar().addPermanentWidget(self.progress_bar, 1)

    def log_info(self, text):
        """
        向文本框注入带有当前系统时间标记的字符串，并自动实现页面的向下翻滚。
        """
        # 获取当前时间并插入文本框尾部
        self.log_box.append(f"[{datetime.now().strftime('%H:%M:%S')}] {text}")
        # 获取文本框右侧的垂直滚动条，并强制将滑块移至底部的最大可能值处
        scrollbar = self.log_box.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        
    def connect_signals(self):
        """
        信号枢纽：将按钮点击等触发事件与类内部处理方法进行路由映射。
        """
        # 通过 lambda 匿名函数实现对公共方法的参数动态传入分发
        # 注意：QAction 使用 triggered 信号，而不是 clicked
        self.action_up_k.triggered.connect(lambda: self.start_update_task("kline"))
        self.action_up_f.triggered.connect(lambda: self.start_update_task("finance"))
        self.action_up_d.triggered.connect(lambda: self.start_update_task("dividend"))
        
        self.btn_sync.clicked.connect(lambda: self.start_update_task("sync_cloud"))
        
        self.btn_ok.clicked.connect(self.start_calc_task) 
        self.btn_clear.clicked.connect(self.on_clear_or_abort_clicked)
        # 将打开目录按钮的点击信号连接到新声明的类成员方法上
        self.btn_open_dir.clicked.connect(self.open_export_dir)
        # =============================================================
        
        # 安全性设定：当用户修改起点日期后，将终点日历的最小值设为起点（阻止选择比起点早的日子）
        self.start_date.dateChanged.connect(lambda date: self.end_date.setMinimumDate(date))
        # 当终点被修改后，同样去限制起点的最大边界
        self.end_date.dateChanged.connect(lambda date: self.start_date.setMaximumDate(min(date, QDate.currentDate())))
        
    
    # 参数持久化核心方法
    def save_settings(self):
        """
        动作记忆保存：只有在点击筛选且通过验证时调用。
        将当前界面的 7 个核心参数固化为 JSON 文件。
        """
        settings = {
            "market": self.market_combo.currentText(),
            "recur_start": self.start_date.date().toString("yyyy-MM-dd"),
            "recur_end": self.end_date.date().toString("yyyy-MM-dd"),
            "recur_val": self.recur_spin.value(),
            "price_period": self.price_period.currentText(),
            "price_pct": self.price_spin.value(),
            "vol_period": self.vol_period.value(),
            "vol_pct": self.vol_spin.value()
        }
        try:
            with open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
                # 使用 indent=4 让 JSON 文件具有可读性，方便高级用户手动微调
                json.dump(settings, f, ensure_ascii=False, indent=4)
            self.log_info("💾 筛选参数已成功持久化至本地。")
        except Exception as e:
            self.log_info(f"⚠️ 参数保存失败: {e}")

    def load_settings(self):
        """
        启动自动加载：从本地 JSON 读取上一次执行成功的参数并回填控件。
        """
        if not os.path.exists(SETTINGS_PATH):
            return # 首次运行或文件丢失时，保持界面默认缺省值
            
        try:
            with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            
            # 1. 还原市场板块
            self.market_combo.setCurrentText(settings.get("market", "主板"))
            
            # 2. 还原复现比起止日期 (需将 String 转回 QDate)
            if "recur_start" in settings:
                self.start_date.setDate(QDate.fromString(settings["recur_start"], "yyyy-MM-dd"))
            #if "recur_end" in settings:
                #self.end_date.setDate(QDate.fromString(settings["recur_end"], "yyyy-MM-dd"))
                
            # 3. 还原复现比阈值
            self.recur_spin.setValue(settings.get("recur_val", 2.0))
            
            # 4. 还原价格位置比回溯周期
            self.price_period.setCurrentText(settings.get("price_period", "三年"))
            
            # 5. 还原价格位置比阈值
            self.price_spin.setValue(settings.get("price_pct", 20))
            
            # 6. 还原成交量位置比天数
            self.vol_period.setValue(settings.get("vol_period", 5))
            
            # 7. 还原成交量位置比阈值
            self.vol_spin.setValue(settings.get("vol_pct", 20))
            
            self.log_info("⚙️ 系统已为您自动加载上一次运行成功的筛选配置。")
        except Exception as e:
            self.log_info(f"⚠️ 历史配置读取失败，已恢复系统缺省值。({e})")

    # ====== 通用中止及清空任务处理器 (防死锁方案) ======
    def on_clear_or_abort_clicked(self):
        """
        根据当前系统的任务状态，决定该按钮是执行“清空”还是“强制中止任务”。
        """
        if self.is_task_running:
            self.log_info("🛑 正在请求中止任务，请稍候...")
            # 中止更新线程
            if hasattr(self, 'updater_thread') and self.updater_thread.isRunning():
                # 更改子线程的内部状态标记，使其在下一个安全循环口自动退出
                self.updater_thread.is_running = False
                
                # ====== 【针对底层网络死锁的暴力强杀逻辑】 ======
                # 等待 2 秒，给线程自己关闭的机会
                if not self.updater_thread.wait(2000): 
                    # 如果 2 秒后线程还在卡死，直接强行切断系统级资源！
                    self.updater_thread.terminate() 
                    self.log_info("🔪 检测到底层网络死锁，已执行强制阻断！")
                    self.on_calc_finished(None, aborted=True)
            
            # 中止计算线程
            if hasattr(self, 'calc_thread') and self.calc_thread.isRunning():
                # Pandas 纯内存操作计算，可直接强制终止释放资源
                self.calc_thread.is_running = False
                self.log_info("🛑 正在中断计算并安全释放内存，请稍候...")
        else:
            # 正常清空界面
            self.log_box.clear()
            self.result_table.setRowCount(0)
    # =========================================================

    def start_update_task(self, task_type):
        """
        接收更新请求类型，实例化异步后台线程。
        """
        # 首先调用方法禁用全部按钮响应，避免重复触发并发异常
        self.set_buttons_enabled(False)
        # 显示进度条并清零
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        # ====== 动态设置更新任务的进度条文字 ======
        text_map = {
            "kline": "正在更新历史 K 线数据... %p%",
            "finance": "正在同步最新财务报表... %p%",
            "dividend": "正在扫描分红除权规则... %p%",
            "sync_cloud": "☁️ 正在全量补齐数据... %p%"
        }
        self.progress_bar.setFormat(text_map.get(task_type, "正在处理... %p%"))
        # 生成更新线程对象
        self.updater_thread = DataUpdateThread(task_type)
        # 将线程内部抛出的回传文本与自身 log_info 拼接显示
        self.updater_thread.log_signal.connect(self.log_info)
        # 连接进度条更新信号
        self.updater_thread.progress_signal.connect(self.progress_bar.setValue) 
        # 结束时不仅要解锁按钮，还要隐藏进度条
        self.updater_thread.finished_signal.connect(lambda: self.set_buttons_enabled(True))
        self.updater_thread.finished_signal.connect(lambda: self.progress_bar.setVisible(False))
        
        # ====== 任务完成后的专属弹窗逻辑 ======
        def show_update_popup():
            msg_map = {
                "kline": "历史 K 线数据已成功同步至最新！",
                "finance": "财务报表基本面数据更新完毕！",
                "dividend": "分红除权规则扫描入库完成！"
            }
            # 仅对这三种任务类型弹窗 (若已被中止，则不弹出成功)
            if task_type in msg_map and not getattr(self.updater_thread, 'is_running', True) == False:
                QMessageBox.information(self, "更新完成", msg_map[task_type])
                
        self.updater_thread.finished_signal.connect(show_update_popup)
        # 下达线程起跑指令
        self.updater_thread.start()

    def set_buttons_enabled(self, enabled):
        """
        批量控制主界面的核心操作按钮状态是否允许鼠标点击响应。
        ====== 全局状态和动态“中止按钮”逻辑 ======
        """
        self.is_task_running = not enabled # 更新全局任务状态标志
        
        # 锁定或解锁除了“清空”以外的业务触发按钮
        for b in [self.btn_sync, self.btn_manual_update, self.btn_ok, self.btn_open_dir]:
            b.setEnabled(enabled)
            
        # 根据系统运行状态，智能切换最后一个按钮的作用
        if enabled:
            # 恢复为空闲状态：清空显示按钮
            self.btn_clear.setText("🗑️ 清空显示")
            # 移除危险标识，恢复为我们在 QSS 里定义的基础白色按钮形态
            self.btn_clear.setObjectName("") 
            # 必须调用这两句，强制 PyQt 刷新样式表才能立即生效
            self.btn_clear.style().unpolish(self.btn_clear)
            self.btn_clear.style().polish(self.btn_clear)
        else:
            # 进入运行状态：变身为中止任务按钮
            self.btn_clear.setText("🛑 中止任务")
            # 挂载我们在 QSS 里写好的 #DangerBtn 红色立体样式
            self.btn_clear.setObjectName("DangerBtn")
            # 强制刷新样式表
            self.btn_clear.style().unpolish(self.btn_clear)
            self.btn_clear.style().polish(self.btn_clear)

    def start_calc_task(self):
        """
        运算执行序列入口。负责从各类控件捕获数值、完成预处理并投喂给计算引擎。
        """
        # 将用户下拉的自然年描述，转化为粗略计算所耗用的整天数
        period_map = {"半年": 180, "一年": 365, "两年": 730, "三年": 1095}
        price_period_str = self.price_period.currentText()
        period_days = period_map.get(price_period_str, 365)
        # 利用当前时间反推算出真正的切片测算起始日期
        price_start = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d")
        
        # 提取日历控件指定的字符串日期
        recur_start = self.start_date.date().toString("yyyy-MM-dd")
        ui_end_date = self.end_date.date().toString("yyyy-MM-dd")
        
        # 动态纠错审计机制：检查用户所选计算截止日是否超过本地存储记录中最新一天的数据边界
        max_local_date = DBManager.get_latest_kline_date()
        
        # ====== 空库硬拦截防御网 ======
        if not max_local_date:
            self.log_info("❌ 严重错误：未能读取到本地 K 线数据！")
            QMessageBox.warning(
                self, 
                "拦截提示", 
                "本地数据库为空或未初始化！\n\n请先点击左侧的【更新 K线/财报/分红】按钮，建立基础数据底座后再执行计算。"
            )
            return  # 直接阻断函数运行，不启动计算线程
        
        
        # 在这里保存用户在 UI 界面上输入的原始参数。
        self.save_settings()
        
        actual_end_date = ui_end_date

        if max_local_date and ui_end_date > max_local_date:
            self.log_info(f"⚠️ [系统自动修正] 您选择的截至日期 ({ui_end_date}) 超出本地数据范围， 自动调整为最新有效日期({max_local_date})")
            # 越界即被回滚为数据库能够支撑的最大日期
            actual_end_date = max_local_date
            
        # 将经过底层校验的真实截止日期固化为实例属性，供导出报告使用
        self.actual_calc_end_date = actual_end_date    
        
        # 计算总体历史调取起点：从价格跨度与复现区间的两端，选取一个更早的基准日
        calc_start = min(price_start, recur_start)

        # 封装将要发往后台的请求字典包
        params = {
            'market': self.market_combo.currentText(),
            'recur_val': self.recur_spin.value(),
            'recur_start_date': recur_start,
            'recur_end_date': actual_end_date,   
            'price_pct': self.price_spin.value(),
            'vol_pct': self.vol_spin.value(),
            'vol_days': self.vol_period.value(),
            'calc_start_date': calc_start,       
            'price_start_date': price_start      
        }
        
        self.log_info("\n" + "═" * 45)
        self.log_info("🚀 [系统准备] 筛选配置已确认，开始执行任务...")
        
       
        # 禁用按钮群，触发中止按钮变换
        self.set_buttons_enabled(False)
        # 清除显示区域的遗留表格行
        self.result_table.setRowCount(0) 
        
        # ====== ======
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        # ====== 设置筛选任务的专属进度文字 ======
        self.progress_bar.setFormat("🚀 正在筛选符合条件的股票... %p%")
        
        # 初始化计算专用线程实例
        self.calc_thread = CalcThread(params)
        self.calc_thread.log_signal.connect(self.log_info)
        # 连接进度条
        self.calc_thread.progress_signal.connect(self.progress_bar.setValue) 
        # 将线程结算数据回调绑定至内部封装的显示及保存方法中
        self.calc_thread.result_signal.connect(self.on_calc_finished)
        self.calc_thread.start()

    def get_filtered_results(self):
        """
        基于界面上的亏损拦截复选框，进行内存数据集的实时提取和清洗。
        """
        if not hasattr(self, 'current_full_results') or not self.current_full_results:
            return []

        if not self.filter_bad_finance_cb.isChecked():
            return self.current_full_results

        filtered = []
        
        # 【优化】：将审计函数和列表移出循环体外，节省数千次内存分配开销
        bad_keywords = ["亏损", "逾期", "暴雷", "数据不足", "无年报", "利润为0", "计算失败", "异常", "无数据"]
        
        def is_bad_stock(val):
            if any(kw in val for kw in bad_keywords): return True
            if val.strip().startswith("-"): return True 
            return False

        # 仅执行极速遍历
        for row in self.current_full_results:
            pe_static = str(row.get("市盈率", ""))
            pe_ttm = str(row.get("市盈率TTM", ""))
            
            if is_bad_stock(pe_static) or is_bad_stock(pe_ttm):
                continue 
                
            filtered.append(row) 
            
        return filtered

    def refresh_table_display(self):
        """
        提取洗刷完成的数据源，根据长度划定控件空间并逐步注入单元格进行排版。
        """
        if not hasattr(self, 'current_full_results'):
            return
            
        display_data = self.get_filtered_results()
        self.result_table.setRowCount(len(display_data))
        
        # 【优化】：将静态的键值列表移出循环体外
        keys = ["代码", "名称", "净值", "现价", "后复权价", "复现比", "价格比", "成交量比", "市盈率", "市盈率TTM"]
        
        for row, data in enumerate(display_data):
            for col, key in enumerate(keys):
                val = data.get(key, "无数据")
                item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignCenter) 
                self.result_table.setItem(row, col, item)

    def on_calc_finished(self, results, aborted=False):
        """
        接收计算结果并执行导出。
        """
        # 1. 恢复按钮状态，隐藏进度条
        self.set_buttons_enabled(True) 
        self.progress_bar.setVisible(False) 
        
        # 如果是用户中止，则不执行后续逻辑
        if aborted: return
        
        # 结果为空检测
        if not results:
            self.log_info("筛选完成：未发现符合当前条件股票。")
            QMessageBox.warning(self, "筛选结果", "未发现符合当前条件的股票。")
            return

        # 2. 缓存全量结果并刷新 UI 表格
        self.current_full_results = results 
        self.refresh_table_display()
        self.log_info(f"✅ [筛选完成] 选出 {len(results)} 只符合条件的股票")

        # ======================================================================
        # 【增强导出逻辑开始】：提取参数并构造 Excel 报告
        # ======================================================================
        try:
            # A. 提取界面当前的筛选参数，用于存证
            criteria_summary = {
                "筛选时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "市场板块": self.market_combo.currentText(),
                "复现比起点": self.start_date.date().toString("yyyy-MM-dd"),
                "复现比终点": getattr(self, 'actual_calc_end_date', self.end_date.date().toString("yyyy-MM-dd")),
                "复现比阈值": f"< {self.recur_spin.value()} 倍",
                "价格位置回溯": self.price_period.currentText(),
                "价格位置阈值": f"< {self.price_spin.value()} %",
                "成交量位置回溯": self.price_period.currentText(),
                "成交量近期天数": f"{self.vol_period.value()} 天",
                "成交量位置阈值": f"< {self.vol_spin.value()} %"
            }
            # 将字典转换为 DataFrame，方便写入 Excel 前几行
            df_criteria = pd.DataFrame(list(criteria_summary.items()), columns=["筛选参数项目", "设定值"])

            # B. 准备筛选后的结果数据
            export_data = self.get_filtered_results()
            if not export_data:
                self.log_info("提示：剔除亏损/异常后，无剩余股票可导出。")
                return
            # 安全提取：即使底层数据偶尔缺失某个字段，也能强行生成 Excel 而不报错
            export_columns = ["代码", "名称", "净值", "现价", "后复权价", "复现比", "价格比", "成交量比", "市盈率", "市盈率TTM"]
            df_results = pd.DataFrame(export_data).reindex(columns=export_columns)

            # C. 动态生成文件名（带上核心参数和时分秒）
            market_name = self.market_combo.currentText()
            recur_val = self.recur_spin.value()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_base_name = f"{market_name}_{timestamp}.xlsx"
            
            os.makedirs(EXPORT_DIR, exist_ok=True) 
            file_name = os.path.join(EXPORT_DIR, file_base_name)

            # D. 使用 ExcelWriter 实现多段写入（参数摘要 + 结果表格）
            # 我们引入防护网，防止文件被打开时报错
            try:
                with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
                    # 在第一行写入参数摘要
                    df_criteria.to_excel(writer, index=False, sheet_name='筛选报告')
                    # 在摘要下方空出两行，写入正式结果
                    df_results.to_excel(writer, index=False, sheet_name='筛选报告', startrow=len(df_criteria) + 2)
                
                msg_text = f"成功选出 {len(export_data)} 只股票！\n报告已保存至：{file_base_name}"
            except PermissionError:
                # 触发另存为逻辑
                # 极端情况下如果秒级文件名也冲突（如1秒内多次点击），增加额外备份标识
                extra_time = datetime.now().strftime("%f")[:3] # 取毫秒前3位
                file_base_name = f"{market_name}_{timestamp}_备份_{extra_time}.xlsx"
                file_name = os.path.join(EXPORT_DIR, file_base_name)
                
                with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
                    df_criteria.to_excel(writer, index=False, sheet_name='筛选报告')
                    df_results.to_excel(writer, index=False, sheet_name='筛选报告', startrow=len(df_criteria) + 2)
                
                msg_text = f"⚠️ 原报告被占用，已另存为：\n{file_base_name}"
                self.log_info(f"警告：文件被占用，已另存 -> {file_base_name}")

            self.log_info(f"输出：结果报告已保存至  {file_base_name}")
            QMessageBox.information(self, "筛选完成", msg_text)

        except Exception as e:
            self.log_info(f"异常：Excel 报告生成失败。错误信息: {e}")
        # ======================================================================
        # 【增强导出逻辑结束】
        # ======================================================================
    # ====== 【新增】底层拦截系统级别的窗口关闭事件防崩溃 ======
    def closeEvent(self, event: QCloseEvent):
        """
        重写 PyQt 的关闭事件回调。当用户点击窗口右上角的关闭 [X] 按钮时触发。
        用于防止用户在大量数据写入 SQLite 的途中强制杀进程导致数据库文件彻底损坏。
        """
        # 判断全局标志位：后台是否有任务正在运行？
        if self.is_task_running:
            # 挂起一个最高优先级的严重警告弹窗
            reply = QMessageBox.warning(
                self, "危险操作警告", 
                "后台正在高速执行数据更新或计算任务！\n此时强行退出极易导致本地数据库文件 (.db) 结构永久损坏。\n\n是否仍要强制中止并退出软件？", 
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            
            # 如果用户执意要退出
            if reply == QMessageBox.Yes:
                self.log_info("🛑 接收到强制退出指令，正在尝试安全切断数据库连接...")
                # 通知底层爬虫和写入循环尽快停止
                if hasattr(self, 'updater_thread') and self.updater_thread.isRunning():
                    self.updater_thread.is_running = False
                    # 强制主线程等候 1.5 秒，给底层 SQLite 执行 commit 和 close 预留最后喘息时间
                    self.updater_thread.wait(1500) 
                
                # 放行关闭事件，彻底关闭窗口销毁应用
                event.accept()
            else:
                # 忽略用户的点击操作，窗口不关闭，继续运行任务
                event.ignore()
        else:
            # 处于空闲状态，完全放行正常的应用关闭
            event.accept()
    
    def open_export_dir(self):
        """
        [业务方法] 调起本地操作系统资源管理器直接打开 Excel 筛选报告输出目录。
        """
        try:
            # 检验并确保目标导出文件夹在物理磁盘上确实存在，若不存在则递归建立
            os.makedirs(EXPORT_DIR, exist_ok=True)
            # 调用 Windows 操作系统级接口，使用关联的资源管理器程序原生打开该绝对路径
            os.startfile(EXPORT_DIR)
            # 在软件下方的文本框中记录本次操作日志
            self.log_info(f"📂 已成功调起系统资源管理器打开本地目录: {EXPORT_DIR}")
        except Exception as e:
            # 捕获由于系统权限不足或链路阻断导致的异常，防止主线程崩溃
            self.log_info(f"❌ 调起本地导出目录失败，原因: {e}")
    # ==========================================================