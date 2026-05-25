# ==============================================================================
# 模块名称: init_calendar.py
# 代码功能: 交易日历本地固化脚本。
# 运行频率: 独立脚本，仅需在系统首次部署，或每年年底手动运行一次。
# ==============================================================================

import os
import sys
import sqlite3
import baostock as bs
from datetime import datetime

# 动态引入项目的配置文件，将日历存在你的历史 K 线数据库中
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from utils.config import DB_HISTORY_PATH

def build_local_calendar():
    print("🌐 正在连接 Baostock 获取全局交易日历...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败: {lg.error_msg}")
        return

    # 获取当前年份，直接往后拉取到今年年底
    current_year = datetime.now().year
    end_date = f"{current_year}-12-31"
    
    # 暴力拉取 2000 年至今的所有日历记录（包含节假日和周末）
    print(f"⏳ 正在下载 2000-01-01 至 {end_date} 的全量日历，请稍候...")
    rs = bs.query_trade_dates(start_date="2000-01-01", end_date=end_date)
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        # row[0] 是日期 (例如 '2026-05-24')
        # row[1] 是是否为交易日的标识 ('1' 为交易日，'0' 为非交易日)
        data_list.append((row[0], int(row[1]))) 
        
    bs.logout()

    if not data_list:
        print("❌ 获取日历失败，请检查网络。")
        return

    print(f"📦 成功获取 {len(data_list)} 天的日历记录，准备写入本地数据库...")

    # 连接到你的 K 线数据库
    conn = sqlite3.connect(DB_HISTORY_PATH)
    cursor = conn.cursor()
    
    # 建立一张极其轻量的字典表，设定日期为主键
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_calendar (
            calendar_date TEXT PRIMARY KEY,
            is_trading_day INTEGER
        )
    ''')
    
    # 批量写入，如果再次运行遇到重复日期则直接覆盖更新
    cursor.executemany('''
        INSERT OR REPLACE INTO trade_calendar (calendar_date, is_trading_day)
        VALUES (?, ?)
    ''', data_list)
    
    conn.commit()
    conn.close()
    print("🎉 本地静态日历库构建完成！从此彻底告别 Baostock 日历查询卡顿。")

if __name__ == '__main__':
    build_local_calendar()