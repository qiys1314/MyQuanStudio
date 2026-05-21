# ==============================================================================
# 模块名称: server/auto_fetcher.py
# 代码功能: 云端无人值守数据采集中心 (原子化事务版)
# 架构设计: 引入 "临时表 (Temp Table) + 原子化合并" 机制。
#           所有增量数据先入临时表，确保全量抓取无误后，再通过单次事务
#           (Transaction) 瞬间合并至主表。彻底杜绝宕机导致的本地数据断层。
# ==============================================================================

import os
import sys
import time
import random
import sqlite3
import logging
import pandas as pd
from datetime import datetime, timedelta

import akshare as ak
import baostock as bs

# ==============================================================================
# 1. 基础配置与日志引擎初始化
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

DB_HISTORY_PATH = os.path.join(DATA_DIR, "history_kline.db")
DB_FINANCE_PATH = os.path.join(DATA_DIR, "all_financials.db")
DB_DIVIDEND_PATH = os.path.join(DATA_DIR, "dividend_rules.db")

log_file = os.path.join(LOG_DIR, f"fetcher_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class CloudDataFetcher:
    
    @staticmethod
    def get_safe_end_date():
        """测算当前安全的 T-1 结算基准日"""
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        start_check = (now - timedelta(days=30)).strftime('%Y-%m-%d')
        
        rs = bs.query_trade_dates(start_date=start_check, end_date=today_str)
        trade_dates = []
        while (rs.error_code == '0') & rs.next(): 
            trade_dates.append(rs.get_row_data())
            
        df_calendar = pd.DataFrame(trade_dates, columns=rs.fields)
        df_calendar = df_calendar[df_calendar['is_trading_day'] == '1']
        
        is_today_trading = today_str in df_calendar['calendar_date'].values
        if is_today_trading and now.hour < 18:
            safe_end_date = df_calendar[df_calendar['calendar_date'] < today_str]['calendar_date'].max()
        else:
            safe_end_date = df_calendar['calendar_date'].max()
            
        return safe_end_date

    @classmethod
    def update_kline(cls):
        """
        更新历史 K 线数据。
        采用增量同步策略与临时表原子化写入机制，确保数据一致性。
        引入 kline_status 状态表以提升增量水位查询的性能。
        """
        logging.info("🚀 [K线] 开始独立获取全市场通讯录并更新 K 线数据...")
        
        lg = bs.login()
        if lg.error_code != '0':
            logging.error(f"❌ [K线] Baostock 接口鉴权失败: {lg.error_msg}")
            return
            
        try:
            # 获取安全的 T-1 结算日
            safe_end_date = cls.get_safe_end_date()
            logging.info(f"📅 [K线] 更新目标安全结算日锁定为: {safe_end_date}")

            # 获取全市场股票代码列表
            rs_stocks = bs.query_all_stock(day=safe_end_date)
            if rs_stocks.error_code != '0':
                logging.error(f"❌ [K线] 获取通讯录失败: {rs_stocks.error_msg}")
                return
                
            stock_df = rs_stocks.get_data()
            if stock_df.empty: return

            # 过滤提取标准 A 股交易代码
            valid_codes = [
                code.replace('sh.', '').replace('sz.', '') 
                for code in stock_df['code'] 
                if code.startswith(('sh.60', 'sh.68', 'sz.00', 'sz.30'))
            ]
            logging.info(f"✅ [K线] 发现 {len(valid_codes)} 只交易态 A 股。")

            conn = sqlite3.connect(DB_HISTORY_PATH)
            cursor = conn.cursor()
            
            # 定义主表与临时表的表结构规范
            table_schema = '''
                (
                    日期 TEXT, 代码 TEXT, 开盘 REAL, 最高 REAL, 最低 REAL, 
                    收盘 REAL, 昨收 REAL, 成交量 INTEGER, 成交额 REAL, 
                    换手率 REAL, 状态 TEXT,
                    PRIMARY KEY (日期, 代码) 
                )
            '''
            # 初始化主表与临时表
            cursor.execute(f"CREATE TABLE IF NOT EXISTS history_kline {table_schema}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS temp_history_kline {table_schema}")
            
            # 初始化增量更新状态表，支撑毫秒级断点续传查询
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kline_status (
                    代码 TEXT PRIMARY KEY,
                    最新日期 TEXT
                )
            ''')
            
            # 清空临时表数据，防止崩溃残留导致的脏读
            cursor.execute("DELETE FROM temp_history_kline")
            conn.commit()

            # 从状态表读取本地最新水位线数据字典，替代全表扫描
            df_watermark = pd.read_sql("SELECT 代码, 最新日期 as max_date FROM kline_status", conn)
            watermark_dict = dict(zip(df_watermark['代码'], df_watermark['max_date']))

            total_inserted = 0
            updated_stocks = 0

            # 遍历并请求每只股票的增量切片
            for idx, code in enumerate(valid_codes, 1):
                bs_code = f"sh.{code}" if code.startswith(('60', '68')) else f"sz.{code}"

                # 判断拉取起点日期
                if code in watermark_dict:
                    last_date = watermark_dict[code]
                    start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    start_date = "2000-01-01" 

                # 若起点大于安全截止日，判定为已最新，跳过当次拉取
                if start_date > safe_end_date: continue

                if idx % 200 == 0 or idx == 1:
                    logging.info(f"🌐 [K线] 正在推进: [{idx}/{len(valid_codes)}] 标的 {code}")

                # 执行网络数据请求，包含防抖重试逻辑
                rs = None
                for attempt in range(3):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code, 
                            "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus",
                            start_date=start_date, end_date=safe_end_date, 
                            frequency="d", adjustflag="3"
                        )
                        if rs.error_code == '0': break 
                        time.sleep(1)
                    except Exception:
                        if attempt < 2: time.sleep(1)
                
                if rs is None or rs.error_code != '0': continue

                # 提取返回结果
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

                if not data_list: continue 
                
                # 数据清洗与类型转换映射
                df = pd.DataFrame(data_list, columns=rs.fields)
                df.rename(columns={'date': '日期', 'code': '代码', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'preclose': '昨收', 'volume': '成交量', 'amount': '成交额', 'turn': '换手率', 'tradestatus': '状态'}, inplace=True)
                
                num_cols = ['开盘', '最高', '最低', '收盘', '昨收', '成交额', '换手率']
                for c in num_cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
                df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce').fillna(0).astype(int)
                df['代码'] = df['代码'].str.replace('sh.', '').str.replace('sz.', '')
                
                # 滤除停牌及未产生成交的数据
                df = df[(df['状态'] == '1') & (df['成交量'] > 0)] 
                
                if df.empty: continue
                data_tuples = list(df.itertuples(index=False, name=None))
                
                try:
                    # 将清洗后的结构化增量数据批量写入临时表
                    cursor.executemany('''
                        INSERT OR IGNORE INTO temp_history_kline 
                        (日期, 代码, 开盘, 最高, 最低, 收盘, 昨收, 成交量, 成交额, 换手率, 状态) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', data_tuples)
                    conn.commit() 
                    total_inserted += len(data_tuples)
                    updated_stocks += 1
                except Exception as e:
                    logging.error(f"⚠️ [K线] 写入临时表 {code} 异常: {e}")

            # 开启数据库原子化事务，执行主表合并及状态表批量更迭
            logging.info("⏳ [K线] 爬取结束，正在执行原子化主表合并...")
            cursor.execute("BEGIN TRANSACTION;")
            
            # 第一阶段：将临时表暂存数据合入主历史表
            cursor.execute('''
                INSERT OR REPLACE INTO history_kline 
                SELECT * FROM temp_history_kline
            ''')
            
            # 第二阶段：提取临时表内各标的的最新日期，覆盖式写入状态表
            cursor.execute('''
                INSERT OR REPLACE INTO kline_status (代码, 最新日期)
                SELECT 代码, MAX(日期) FROM temp_history_kline GROUP BY 代码
            ''')
            
            # 第三阶段：清除临时沙盒缓存
            cursor.execute("DELETE FROM temp_history_kline") 
            conn.commit() 

            conn.close()
            logging.info(f"🎉 [K线] 战役结束! 为 {updated_stocks} 只股票无缝合入 {total_inserted} 行新数据。")

        finally:
            bs.logout() 

    @classmethod
    def update_finance(cls):
        """财务报表原子化抓取"""
        logging.info("📊 [财报] 开始同步全市场财务报告数据...")
        try:
            conn = sqlite3.connect(DB_FINANCE_PATH)
            cursor = conn.cursor()
            
            table_schema = '''
                (
                    代码 TEXT, 名称 TEXT, 每股收益 REAL, 每股净资产 REAL,
                    净利润 REAL, 净利润同比增长 REAL, 报告期 TEXT,
                    PRIMARY KEY (代码, 报告期)
                )
            '''
            cursor.execute(f"CREATE TABLE IF NOT EXISTS all_financials {table_schema}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS temp_all_financials {table_schema}")
            cursor.execute("DELETE FROM temp_all_financials")
            conn.commit()

            now = datetime.now()
            years = [now.year, now.year - 1, now.year - 2]
            periods = ["1231", "0930", "0630", "0331"]
            
            valid_scan_list = [f"{y}{p}" for y in years for p in periods if not (y == now.year and int(p[:2]) > now.month)]
            
            for r_date in valid_scan_list:
                logging.info(f"🌐 [财报] 请求 AkShare: 扫描 {r_date} 季度业绩...")
                
                df = None
                for attempt in range(3):
                    try:
                        df = ak.stock_yjbb_em(date=r_date)
                        break 
                    except Exception:
                        if attempt < 2: time.sleep(random.uniform(1.5, 3.0))
                
                if df is None or df.empty: continue
                    
                try:    
                    cols = df.columns.tolist()
                    col_code = next((c for c in cols if '代码' in c), None)
                    col_name = next((c for c in cols if '简称' in c or '名称' in c), None)
                    col_eps  = next((c for c in cols if '每股收益' in c), None)
                    col_bps  = next((c for c in cols if '每股净资产' in c), None)
                    col_profit = next((c for c in cols if '利润' in c and '同比' not in c and '扣非' not in c), None)
                    col_yoy = next((c for c in cols if '利润' in c and '同比' in c and '扣非' not in c), None)
                    
                    if not all([col_code, col_name, col_eps, col_bps, col_profit, col_yoy]): continue

                    df = df[[col_code, col_name, col_eps, col_bps, col_profit, col_yoy]].copy()
                    df.columns = ['代码', '名称', '每股收益', '每股净资产', '净利润', '净利润同比增长']
                    df['报告期'] = r_date
                    df['代码'] = df['代码'].astype(str)
                    
                    records = df.to_dict('records')
                    # 写入临时表
                    cursor.executemany('''
                        INSERT OR REPLACE INTO temp_all_financials 
                        (代码, 名称, 每股收益, 每股净资产, 净利润, 净利润同比增长, 报告期)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', [(r['代码'], r['名称'], r['每股收益'], r['每股净资产'], r['净利润'], r['净利润同比增长'], r['报告期']) for r in records])
                    conn.commit()
                    time.sleep(1.5)
                except Exception as e:
                    logging.warning(f"⚠️ [财报] 季度 {r_date} 处理异常: {e}")
            
            # 执行原子化合并
            logging.info("⏳ [财报] 扫描完毕，执行原子化主表合并...")
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("INSERT OR REPLACE INTO all_financials SELECT * FROM temp_all_financials")
            cursor.execute("DELETE FROM temp_all_financials")
            conn.commit()
            
            conn.close()
            logging.info("🎉 [财报] 全市场业绩底座安全同步完毕！")
        except Exception as e:
            logging.error(f"❌ [财报] 核心逻辑崩溃: {e}")

    @classmethod
    def update_dividend(cls):
        """分红送配原子化抓取"""
        logging.info("🎁 [分红] 启动分红送配除权引擎...")
        try:
            conn = sqlite3.connect(DB_DIVIDEND_PATH) 
            cursor = conn.cursor()
            
            table_schema = '''
                (
                    代码 TEXT NOT NULL, ex_date TEXT NOT NULL,
                    S REAL, D REAL,
                    PRIMARY KEY (代码, ex_date)
                )
            '''
            cursor.execute(f"CREATE TABLE IF NOT EXISTS dividend_rules {table_schema}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS temp_dividend_rules {table_schema}")
            cursor.execute("DELETE FROM temp_dividend_rules")
            conn.commit()

            cursor.execute("SELECT MIN(ex_date) FROM dividend_rules")
            min_date = cursor.fetchone()[0]
            
            now = datetime.now()
            scan_periods = []
            
            if not min_date or min_date > "2006-01-01":
                logging.info("⚠️ [分红] 触发底座全量自愈机制 (耗时较长)...")
                start_year = 2000  
                for y in range(now.year, start_year - 1, -1):
                    for p in ["1231", "0930", "0630", "0331"]:
                        if y == now.year and int(p[:2]) > now.month: continue
                        scan_periods.append(f"{y}{p}")
            else:
                logging.info("✅ [分红] 历史底座健康，启动极速追踪模式...")
                for y in [now.year, now.year - 1]:
                    for p in ["1231", "0930", "0630", "0331"]:
                        if y == now.year and int(p[:2]) > now.month: continue
                        scan_periods.append(f"{y}{p}")
            
            total_inserted = 0
            for period in scan_periods:
                logging.info(f"🌐 [分红] 请求 AkShare: 探明 {period} 实施计划...")
                df_div = None
                for attempt in range(3):
                    try:
                        df_div = ak.stock_fhps_em(date=period)
                        break 
                    except Exception:
                        if attempt < 2: time.sleep(random.uniform(1.5, 3.0))
                
                if df_div is None or df_div.empty: continue
                    
                try:
                    df_div = df_div[df_div['代码'].astype(str).str.startswith(('60', '00', '30', '68'))].copy()
                    if '方案进度' in df_div.columns:
                        df_div = df_div[df_div['方案进度'].astype(str).str.contains('实施', na=False)]
                        
                    if '除权除息日' not in df_div.columns: continue
                    df_div = df_div.dropna(subset=['除权除息日'])
                    df_div = df_div[df_div['除权除息日'].astype(str).str.strip() != '']
                    if df_div.empty: continue
                        
                    cols = df_div.columns.tolist()
                    cash_col = next((c for c in cols if '派息' in c or '派现' in c or '分红' in c), None)
                    song_col = next((c for c in cols if '送股' in c or '送红股' in c), None)
                    zhuan_col = next((c for c in cols if '转增' in c or '转' in c), None)
                    
                    records = []
                    for _, row in df_div.iterrows():
                        ex_date = pd.to_datetime(row['除权除息日']).strftime('%Y-%m-%d')
                        cash = float(row[cash_col]) / 10.0 if cash_col and pd.notna(row[cash_col]) else 0.0
                        song = float(row[song_col]) / 10.0 if song_col and pd.notna(row[song_col]) else 0.0
                        zhuan = float(row[zhuan_col]) / 10.0 if zhuan_col and pd.notna(row[zhuan_col]) else 0.0
                        
                        D = cash; S = song + zhuan
                        if S > 0 or D > 0:
                            records.append((row['代码'], ex_date, round(S, 4), round(D, 4)))
                    
                    if records:
                        # 写入临时表
                        cursor.executemany("INSERT OR REPLACE INTO temp_dividend_rules (代码, ex_date, S, D) VALUES (?, ?, ?, ?)", records)
                        conn.commit()
                        total_inserted += len(records)
                        
                    time.sleep(random.uniform(0.5, 1.5))
                except Exception:
                    continue
            
            # 执行原子化合并
            logging.info("⏳ [分红] 扫描完毕，执行原子化主表合并...")
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("INSERT OR REPLACE INTO dividend_rules SELECT * FROM temp_dividend_rules")
            cursor.execute("DELETE FROM temp_dividend_rules")
            conn.commit()

            conn.close()
            logging.info(f"🎉 [分红] 除权引擎落地！本轮有效补充规则 {total_inserted} 条。")
        except Exception as e:
            logging.error(f"❌ [分红] 网络链路或计算逻辑中断: {e}")

# ==============================================================================
# 程序启动主入口
# ==============================================================================
if __name__ == "__main__":
    logging.info("="*60)
    logging.info("🌞 [系统启动] 云端无人值守抓取中心已启动 (原子事务版)")
    logging.info("="*60)
    
    start_time = time.time()
    
    CloudDataFetcher.update_kline()
    CloudDataFetcher.update_finance()
    CloudDataFetcher.update_dividend()
    
    elapsed = time.time() - start_time
    logging.info("="*60)
    logging.info(f"🏆 [系统休眠] 所有抓取任务圆满结束。总耗时: {elapsed:.2f} 秒。")
    logging.info("="*60)