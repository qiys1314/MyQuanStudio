# ==============================================================================
# 模块名称: utils/config.py
# 代码功能: 负责解析程序的运行目录，并集中定义所有的文件夹和数据库绝对路径。
# ==============================================================================

import os  # 导入操作系统接口模块，用于处理文件路径、创建文件夹等
import sys # 导入系统环境模块，用于获取 Python 解释器和当前进程的状态

# ==============================================================================
# 动态识别运行环境与解析项目根目录
# ==============================================================================
# 使用 getattr 安全获取 sys 模块的 frozen 属性。
# 当代码被 PyInstaller 打包为独立的 EXE 时，系统会自动向 sys 注入 frozen=True 属性。
# 这一步是为了防止打包后相对路径失效，导致程序找不到本地的数据库。
if getattr(sys, 'frozen', False):
    # 【EXE 运行模式】
    # sys.executable 返回当前执行的 EXE 文件的绝对物理路径（如 D:\app\main.exe）
    # os.path.dirname 剔除文件名，保留 EXE 所在的文件夹路径（如 D:\app），将其设为根目录
    BASE_DIR = os.path.dirname(sys.executable)
else:
    # 【Python 源码运行模式】
    # __file__ 指代当前 config.py 文件本身
    # os.path.abspath(__file__) 获取当前文件的绝对路径
    # 第一次 os.path.dirname 提取到上一级的 utils 目录
    # 第二次 os.path.dirname 提取到整个项目的根目录
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ==============================================================================
# 文件夹路径拼接与自动创建
# ==============================================================================
# os.path.join 根据不同操作系统（Windows/Mac）自动使用正确的斜杠拼接路径
# 定义数据库统一存放的 data 文件夹路径
DATA_DIR = os.path.join(BASE_DIR, 'data')
# 定义导出 Excel 文件的 export 文件夹路径
EXPORT_DIR = os.path.join(BASE_DIR, 'export')

# 防御性编程：os.makedirs 用于递归创建目录
# exist_ok=True 表示如果该物理文件夹已经存在，则直接跳过且不抛出报错
# 确保在程序后续写入文件时，目标路径一定可用
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

# ==============================================================================
# 核心数据库文件绝对路径定义
# ==============================================================================
# 定义历史 K 线原生数据库文件的绝对路径
DB_HISTORY_PATH = os.path.join(DATA_DIR, 'stock_history_wfq.db')
# 定义最新财务报表指标数据库文件的绝对路径
DB_FINANCE_PATH = os.path.join(DATA_DIR, 'stock_finance.db')
# 定义历史分红与送配规则数据库文件的绝对路径
DB_DIVIDEND_PATH = os.path.join(DATA_DIR, 'stock_dividend.db')
# ======UI 参数持久化配置文件路径 ======
# 将参数保存在软件根目录下，确保绿色版移动时配置随行
SETTINGS_PATH = os.path.join(BASE_DIR, 'user_settings.json')