# ==============================================================================
# 模块名称: database/db_manager.py
# 代码功能: 数据访问对象（DAO）基础层。
#           负责初始化本地 SQLite 数据库的所有表结构，并提供用于前端自检的基础查询接口。
# ==============================================================================

import sqlite3          # 导入 Python 内置的轻量级数据库接口
import os               # 导入系统模块，用于检测本地物理文件状态
import pandas as pd     # 导入 Pandas 用于将 SQL 查询结果直接解析为 DataFrame
# 从配置文件中导入声明好的各数据库文件的物理路径
from utils.config import DB_HISTORY_PATH, DB_FINANCE_PATH, DB_DIVIDEND_PATH

class DBManager:
    """
    数据库管理静态工具类。
    所有方法使用 @staticmethod 装饰，无需实例化对象即可通过类名直接调用。
    """

    @staticmethod
    def init_all_tables():
        """
        执行数据库定义语言（DDL），初始化所有必需的数据表结构。
        """
        # --- 1. K 线数据库表结构初始化 ---
        # 建立连接。若对应路径的文件不存在，sqlite3 会自动生成一个新的空数据库文件。
        conn_h = sqlite3.connect(DB_HISTORY_PATH)
        # 执行建表 SQL。CREATE TABLE IF NOT EXISTS 确保如果表已存在则静默跳过，不影响原有数据。
        # 字段类型：TEXT为字符串，REAL为浮点数，INTEGER为整数。
        # PRIMARY KEY (日期, 代码) 声明联合主键：强制约束数据库层面不允许同一天、同一只股票的数据被重复插入。
        conn_h.execute('''CREATE TABLE IF NOT EXISTS history_kline (
            日期 TEXT, 代码 TEXT, 开盘 REAL, 最高 REAL, 最低 REAL, 收盘 REAL, 
            昨收 REAL, 成交量 INTEGER, 成交额 REAL, 换手率 REAL, 状态 TEXT,
            PRIMARY KEY (日期, 代码))''')
        # 关闭连接，释放文件锁
        conn_h.close()

        # --- 2. 财务指标数据库表结构初始化 ---
        conn_f = sqlite3.connect(DB_FINANCE_PATH)
        # PRIMARY KEY (代码, 报告期) 联合主键：确保某只股票某个季度的报表具有唯一性。
        conn_f.execute('''CREATE TABLE IF NOT EXISTS all_financials (
            代码 TEXT, 名称 TEXT, 每股收益 REAL, 每股净资产 REAL, 
            净利润 REAL, 净利润同比增长 REAL, 报告期 TEXT,
            PRIMARY KEY (代码, 报告期))''')
        conn_f.close()

        # --- 3. 分红除权数据库表结构初始化 ---
        conn_d = sqlite3.connect(DB_DIVIDEND_PATH)
        # PRIMARY KEY (代码, ex_date) 联合主键：确保某只股票在同一个除权日只有一条操作规则。
        conn_d.execute('''CREATE TABLE IF NOT EXISTS dividend_rules (
            代码 TEXT, ex_date TEXT, S REAL, D REAL,
            PRIMARY KEY (代码, ex_date))''')
        conn_d.close()

    @staticmethod
    def get_latest_kline_date():
        """
        查询 K 线历史库中记录的最大日期，用于判断本地数据库更新到了哪一天。
        """
        # 拦截机制：若物理文件不存在，直接返回 None，防止 sqlite 自动新建空文件导致误判
        if not os.path.exists(DB_HISTORY_PATH): return None
        
        conn = sqlite3.connect(DB_HISTORY_PATH)
        try:
            # 聚合查询 MAX(日期)。由于日期是 YYYY-MM-DD 格式，字符串比较可以正确找出最晚的日期。
            # .fetchone() 返回包含单行数据的元组，[0] 用于提取元组中的第一个值（即具体日期字符串）。
            res = conn.execute("SELECT MAX(日期) FROM history_kline").fetchone()[0]
        except Exception:
            # 捕获表结构损坏等异常情况，防止代码崩溃中断
            res = None
        finally:
            # 确保即使抛出异常，文件连接也会被安全关闭
            conn.close()
        return res

    @staticmethod
    def get_kline_count_on_date(target_date):
        """
        统计指定日期的数据行数（股票数量），用于自检模块验证数据断层。
        """
        if not os.path.exists(DB_HISTORY_PATH): return 0
        conn = sqlite3.connect(DB_HISTORY_PATH)
        try:
            # 参数化查询：使用 (?) 占位符并通过元组传入 target_date，有效防止 SQL 注入。
            # COUNT(DISTINCT 代码)：只统计唯一的股票代码数量。
            res = conn.execute("SELECT COUNT(DISTINCT 代码) FROM history_kline WHERE 日期 = ?", (target_date,)).fetchone()[0]
        except Exception:
            res = 0
        finally:
            conn.close()
        return res

    @staticmethod
    def get_all_latest_dates_dict():
        """
        获取库中每只股票各自的最大日期记录。为高水位断点续传提供起始点依据。
        """
        if not os.path.exists(DB_HISTORY_PATH): return {}
        conn = sqlite3.connect(DB_HISTORY_PATH)
        try:
            # 执行分组聚合查询：按股票代码进行分组，提取组内日期的最大值。
            # 结果通过 pandas 直接解析为包含两列（代码, max_date）的 DataFrame。
            df = pd.read_sql("SELECT 代码, MAX(日期) as max_date FROM history_kline GROUP BY 代码", conn)
            # 矢量化字符串处理：剔除日期中的 '-' 连字符，统一格式。
            df['max_date'] = df['max_date'].str.replace('-', '')
            # zip 组合两列数据为迭代器，并强制转换为 Python 内置的字典格式。
            # 最终返回结构如: {'600000': '20260428', '000001': '20260428'}
            return dict(zip(df['代码'], df['max_date']))
        except Exception:
            return {}
        finally:
            conn.close()