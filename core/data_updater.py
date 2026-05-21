# ==============================================================================
# 模块名称: core/data_updater.py
# 核心功能: 负责所有网络请求（下载 K 线、财报、分红）及本地 SQLite 数据库的写入。
# 架构设计: 继承 QThread，使耗时的网络请求在后台独立运行，避免主界面卡死。
# 优化更新: K线模块完全独立解耦；增加未来时间过滤防卡死；自检环节全面支持动态中止。
# ==============================================================================

# 导入os模块：提供操作系统交互能力，如文件/路径存在性检测、文件属性获取等
import os                                     
# 导入time模块：提供时间相关函数，用于控制网络请求频率（休眠）、时间戳转换等
import time                                   
# 导入random模块：生成随机数，用于生成随机休眠时间，模拟真人请求频率，避免接口限流
import random                                 
# 导入sqlite3模块：Python内置轻量级关系型数据库，用于本地数据的存储与读写
import sqlite3                                
# 导入pandas库：强大的数据分析库，用于二维表格数据的清洗、转换、查询、写入数据库
import pandas as pd                           
# 导入datetime/timedelta：提供日期时间处理、时间间隔计算（如T-1日期、30天前日期）
from datetime import datetime, timedelta       
# 导入PyQt5多线程相关类：QThread实现后台线程，pyqtSignal实现跨线程信号通信（日志、进度、完成）
from PyQt5.QtCore import QThread, pyqtSignal   

# 导入akshare金融数据接口：用于获取A股财报、分红送配等基本面数据
import akshare as ak                           
# 导入baostock接口：专业的证券数据接口，用于获取高质量的A股历史K线数据
import baostock as bs                          

# 从配置模块导入三个数据库的绝对路径：分别存储K线、财报、分红数据
from utils.config import DB_HISTORY_PATH, DB_FINANCE_PATH, DB_DIVIDEND_PATH  

import requests

from database.db_manager import DBManager

# 定义数据更新线程类，继承QThread实现后台异步执行，避免阻塞主界面
class DataUpdateThread(QThread):
    
    # 定义跨线程通信信号（PyQt5线程间通信核心）
    log_signal = pyqtSignal(str)      # 日志信号：向主界面发送运行日志（字符串类型）
    finished_signal = pyqtSignal()    # 完成信号：通知主界面当前任务已执行完毕
    progress_signal = pyqtSignal(int) # 进度信号：向主界面发送任务进度（0-100整数）

    def __init__(self, task_type):
        # 调用父类QThread的初始化方法，确保线程基类正常初始化
        super().__init__()
        # 接收外部传入的任务类型：self_check(自检)/kline(K线更新)/finance(财报更新)/dividend(分红更新)
        self.task_type = task_type
        # 定义线程运行标志位：用于外部安全中止线程（True=运行中，False=中止），核心防卡死设计
        self.is_running = True

    def run(self):
        """线程主执行函数：QThread启动后自动执行此方法"""
        try:
            # 根据任务类型分发到对应业务方法
            if self.task_type == "self_check":
                self.run_self_check()           # 执行系统环境和数据完整性检测
            elif self.task_type == "kline":
                self.update_kline()             # 执行历史 K 线增量下载
            elif self.task_type == "finance":
                self.update_finance()           # 执行最新财务指标数据下载
            elif self.task_type == "dividend":
                self.update_dividend()          # 执行历史分红派息数据下载
            elif self.task_type == "sync_cloud":
                self.sync_from_cloud()          # 云端同步三大数据
        except Exception as e:
            # 捕获所有异常并通过日志信号发送，避免线程崩溃无提示
            self.log_signal.emit(f"❌ 发生严重异常: {str(e)}")
        finally:
            # 无论任务成功/失败/中止，最终发送完成信号通知主界面
            self.finished_signal.emit()

    # =========================================================================
    # 业务方法 1: 系统启动自检 (已加入中止响应机制)
    # =========================================================================
    def run_self_check(self):
        # 发送自检开始日志，用于主界面展示
        self.log_signal.emit("================ 系统数据健康自检 ================")
        # 更新自检进度为5%，主界面进度条同步更新
        self.progress_signal.emit(5)
        try:
            # --- 步骤 1: 计算安全的 T-1 结算日期（A股数据以T-1日为有效基准）---
            # 登录baostock接口，获取接口鉴权（必须登录才能查询交易日历）
            lg = bs.login()
            # 初始化安全结算日期为"未知"，后续根据交易日历更新
            safe_end_date = "未知"
            # 接口登录成功（error_code=0表示成功）
            if lg.error_code == '0':
                # 获取当前系统时间
                now = datetime.now()
                # 转换为"年-月-日"字符串格式（如2024-05-20）
                today_str = now.strftime('%Y-%m-%d')
                # 计算30天前的日期，作为交易日历查询起始点（覆盖足够的交易日范围）
                start_check = (now - timedelta(days=30)).strftime('%Y-%m-%d')
                
                # 查询指定时间段内的交易日历（包含是否交易日标记）
                rs = bs.query_trade_dates(start_date=start_check, end_date=today_str)
                # 初始化空列表，存储交易日历数据
                trade_dates = []
                # 循环读取查询结果（rs.next()表示读取下一条，直到无数据）
                while (rs.error_code == '0') & rs.next():
                    # 将每条交易日历数据添加到列表
                    trade_dates.append(rs.get_row_data()) 
                
                # 若获取到交易日历数据
                if trade_dates:
                    # 将列表转换为DataFrame，方便筛选处理；columns=rs.fields指定列名
                    df_calendar = pd.DataFrame(trade_dates, columns=rs.fields)
                    # 筛选出所有交易日（is_trading_day=1表示交易日）
                    df_calendar = df_calendar[df_calendar['is_trading_day'] == '1']
                    
                    # 判断今天是否为交易日
                    is_today_trading = today_str in df_calendar['calendar_date'].values
                    # 若今天是交易日且当前时间小于18点（A股收盘后数据未完成结算）
                    if is_today_trading and now.hour < 18:
                        # 取今天之前的最后一个交易日作为安全结算日（避免未结算的无效数据）
                        safe_end_date = df_calendar[df_calendar['calendar_date'] < today_str]['calendar_date'].max()
                    else:
                        # 否则取最近的最后一个交易日作为安全结算日
                        safe_end_date = df_calendar['calendar_date'].max()
            # 无论登录是否成功，最终退出baostock接口（释放连接）
            bs.logout()

            # 发送安全结算日期日志，告知用户当前数据基准日
            self.log_signal.emit(f"📅 数据基准日期：当前标准 T-1 结算日应为 -> {safe_end_date}")
            # 更新自检进度为30%
            self.progress_signal.emit(30)

            # ====== 【新增】检查中止指令：在关键耗时节点前判断 ======
            # 若外部设置is_running=False（用户点击中止），则终止自检流程
            if not self.is_running:
                self.log_signal.emit("🛑 收到用户指令，已安全中止系统自检。")
                return
            # =====================================================

            # --- 步骤 2: 检测 K 线数据库的数据完整性 ---
            valid_codes = set()
            if os.path.exists(DB_FINANCE_PATH):
                try:
                    conn_f = sqlite3.connect(DB_FINANCE_PATH)
                    df_codes = pd.read_sql("SELECT DISTINCT 代码 FROM all_financials", conn_f)
                    valid_codes = set([str(c) for c in df_codes['代码'].tolist() if str(c).startswith(('60', '00', '30', '68'))])
                    conn_f.close()
                except: pass
            
            # 记录从财报库拿到的参考基准数量（如果没有财报库，这里就是 0）
            reference_stock_count = len(valid_codes)

            if os.path.exists(DB_HISTORY_PATH):
                # 建立与 K 线数据库的连接
                conn_h = sqlite3.connect(DB_HISTORY_PATH)
                try:
                    # 【核心重构】：彻底废弃全表扫描的 GROUP BY，改为直接读取只有几千行的状态表
                    # 这一句的耗时将从 23 秒直接降至 0.05 秒以内
                    df_watermark = pd.read_sql("SELECT 代码, 最新日期 as max_date FROM kline_status", conn_h)
                    
                    if reference_stock_count > 0:
                        # 如果有财报库作为 A 股名录基准，则用它来过滤有效代码
                        df_valid = df_watermark[df_watermark['代码'].isin(valid_codes)]
                        total_stocks = reference_stock_count
                    else:
                        # 如果没有财报库，就以状态表中记录的股票总数作为分母基准
                        df_valid = df_watermark
                        total_stocks = len(df_watermark)

                    # 统计状态表中，最新日期大于等于“安全 T-1 结算日”的股票数量
                    reached_target_count = len(df_valid[df_valid['max_date'] >= safe_end_date])
                    # 从状态表中获取全市场目前更新到的最晚日期，用于 UI 展示
                    max_db_date = df_valid['max_date'].max() if not df_valid.empty else "无数据"

                    # 发送 UI 日志，汇报全市场最高水位日期
                    self.log_signal.emit(f"📦 历史K线数据：已更新到 {max_db_date}")
                    
                    if total_stocks > 0:
                        # 计算数据完整率覆盖度百分比
                        coverage = (reached_target_count / total_stocks) * 100
                        if coverage >= 90:
                            # 覆盖率健康，发送绿色日志
                            self.log_signal.emit(f"📈 数据完整度：{reached_target_count}/{total_stocks} 只股票已达标 (完整率 {coverage:.1f}%)。状态：健康。")
                        else:
                            # 覆盖率不足，提示用户去点击更新
                            self.log_signal.emit(f"📈 数据完整度：{reached_target_count}/{total_stocks} 只股票已达标 (完整率 {coverage:.1f}%)。存在缺失，建议更新。")
                    else:
                        self.log_signal.emit("📦 历史K线数据：本地库文件存在，但内部数据为空。")
                except Exception as e:
                    # 如果找不到状态表，说明用户还没执行第一步的 SQL 脚本，给出明确提示
                    self.log_signal.emit(f"📦 历史K线数据异常：未找到状态表，请执行数据库重建脚本！({e})")
                finally:
                    # 无论如何，确保游标连接被安全关闭，防止文件锁定
                    conn_h.close()
            else:
                self.log_signal.emit("📦 历史K线数据状态：未找到本地数据库，请执行初始化下载。")

            # 发送分隔符日志，优化界面展示
            self.log_signal.emit("--------------------------------------------------------")
            # 更新自检进度为80%
            self.progress_signal.emit(80)

            # ====== 【新增】检查中止指令 ======
            if not self.is_running:
                self.log_signal.emit("🛑 收到用户指令，已安全中止系统自检。")
                return
            # =====================================================

            # --- 步骤 3: 检查辅助数据库 (财报与分红) 的最后更新时间 ---
            # 定义内部函数：检查指定数据库的健康状态
            def check_db_health(db_path, name, is_finance=True):
                # 若数据库文件存在
                if os.path.exists(db_path):
                    # 获取文件最后修改时间戳（os.path.getmtime返回秒级时间戳）
                    mtime = os.path.getmtime(db_path)
                    # 计算最后更新时间距现在的天数
                    days_ago = (datetime.now() - datetime.fromtimestamp(mtime)).days
                    # 发送最后更新时间日志
                    self.log_signal.emit(f"💰 {name}：最后更新于 {days_ago} 天前。")
                    
                    # 获取当前月份
                    month = datetime.now().month
                    # 区分财报/分红库，给出不同的更新建议
                    if is_finance:
                        # 4/8/10月是财报密集披露期（一季报/中报/三季报）
                        if month in [4, 8, 10]:
                            self.log_signal.emit(f"   👉 [建议] 当前为 {month}月 财报密集披露期，建议提升更新频率。")
                        else:
                            # 其他月份为财报真空期，数据无需频繁更新
                            self.log_signal.emit(f"   👉 [建议] 当前处于财报真空期，历史数据库安全可用。")
                    else:
                        # 5/6/7月是除权实施旺季
                        if month in [5, 6, 7]:
                            self.log_signal.emit(f"   👉 [建议] 当前为除权实施旺季，建议定期更新以确保复权精度。")
                        else:
                            # 其他月份非密集除权期，数据安全
                            self.log_signal.emit(f"   👉 [建议] 非密集除权期，复权规则库当前状态安全。")
                else:
                    # 数据库文件不存在，提示执行下载
                    self.log_signal.emit(f"💰 {name}：未找到本地数据库，请执行下载。")

            # 检查财报数据库健康状态
            check_db_health(DB_FINANCE_PATH, "财报数据库", is_finance=True)
            # 检查分红数据库健康状态
            check_db_health(DB_DIVIDEND_PATH, "分红除权数据库", is_finance=False)

            # 发送自检结束分隔符日志
            self.log_signal.emit("========================================================")
            # 更新自检进度为100%（完成）
            self.progress_signal.emit(100)

        except Exception as e:
            # 捕获自检过程中的所有异常，发送日志
            self.log_signal.emit(f"❌ 自检模块异常: {e}")

    # =========================================================================
    # 业务方法 2: Baostock 历史 K 线增量下载 (完全解耦架构)
    # =========================================================================
    def update_kline(self):
        # 发送K线更新开始日志
        self.log_signal.emit("🚀 开始独立获取全市场通讯录并更新 K 线数据...")
        
        # 登录baostock接口（必须登录才能查询数据）
        lg = bs.login()
        # 接口登录失败（error_code≠0）
        if lg.error_code != '0':
            # 发送登录失败日志，包含错误信息
            self.log_signal.emit(f"❌ 接口鉴权失败: {lg.error_msg}")
            return
            
        try:
            # 1. 测算安全的 T-1 截止日期（同自检逻辑，确保数据基准有效）
            # 获取当前系统时间
            now = datetime.now()
            # 转换为"年-月-日"字符串
            today_str = now.strftime('%Y-%m-%d')
            # 计算30天前的日期，作为交易日历查询起始点
            start_check = (now - timedelta(days=30)).strftime('%Y-%m-%d')
            
            # 查询交易日历
            rs = bs.query_trade_dates(start_date=start_check, end_date=today_str)
            # 初始化交易日历列表
            trade_dates = []
            # 循环读取交易日历数据
            while (rs.error_code == '0') & rs.next(): 
                trade_dates.append(rs.get_row_data())
            # 转换为DataFrame，指定列名
            df_calendar = pd.DataFrame(trade_dates, columns=rs.fields)
            # 筛选出交易日
            df_calendar = df_calendar[df_calendar['is_trading_day'] == '1']
            
            # 判断今天是否为交易日
            is_today_trading = today_str in df_calendar['calendar_date'].values
            # 若今天是交易日且当前时间<18点（数据未结算）
            if is_today_trading and now.hour < 18:
                # 取今天之前的最后一个交易日作为安全截止日
                safe_end_date = df_calendar[df_calendar['calendar_date'] < today_str]['calendar_date'].max()
            else:
                # 否则取最近的最后一个交易日
                safe_end_date = df_calendar['calendar_date'].max()

            # 发送K线更新目标日期日志
            self.log_signal.emit(f"📅 更新目标日期: {safe_end_date}")

            # ====== 【核心解耦重构】直接通过 Baostock 获取 A股通讯录，不依赖财报库 ======
            # 发送获取股票列表日志
            self.log_signal.emit(f"📊 正在向 Baostock 申请 {safe_end_date} 的全市场 A 股名单...")
            # 查询指定日期的全市场股票列表（确保获取的是当日交易的股票）
            rs_stocks = bs.query_all_stock(day=safe_end_date)
            
            # 获取股票列表失败
            if rs_stocks.error_code != '0':
                # 发送失败日志，包含错误信息
                self.log_signal.emit(f"❌ 获取全市场股票列表失败，原因: {rs_stocks.error_msg}")
                return
                
            # 将查询结果转换为DataFrame（get_data()是baostock的便捷方法）
            stock_df = rs_stocks.get_data()
            # 若股票列表为空或无code列（异常情况）
            if stock_df.empty or 'code' not in stock_df.columns:
                # 发送警告日志，提示非交易日或重试
                self.log_signal.emit(f"❌ 警告：未获取到有效股票代码，该日可能非交易日，请换时重试。")
                return

            # 过滤提取原生有效 A 股代码
            valid_codes = []
            # 遍历股票列表中的代码
            for code in stock_df['code']:
                # 筛选沪市60/68开头、深市00/30开头的股票（baostock代码格式：sh.60XXXX/sz.00XXXX）
                if code.startswith(('sh.60', 'sh.68', 'sz.00', 'sz.30')):
                    # 剔除sh./sz.前缀，统一为6位纯数字（对齐数据库存储格式）
                    valid_codes.append(code.replace('sh.', '').replace('sz.', ''))
                    
            # 发送股票列表获取成功日志，包含股票数量
            self.log_signal.emit(f"✅ 通讯录获取成功，共计发现 {len(valid_codes)} 只交易态 A 股。")
            # =========================================================================

            # 2. 连接历史 K 线数据库，准备比对与写入
            conn_h = sqlite3.connect(DB_HISTORY_PATH)
            cursor = conn_h.cursor()
            
            # 【核心修复】：修改 Python 端的建表防护语句，确保新库按照 (代码, 日期) 顺序建立主键
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS history_kline (
                    日期 TEXT, 代码 TEXT, 开盘 REAL, 最高 REAL, 最低 REAL, 
                    收盘 REAL, 昨收 REAL, 成交量 INTEGER, 成交额 REAL, 
                    换手率 REAL, 状态 TEXT,
                    PRIMARY KEY (代码, 日期) 
                )
            ''')
            
            # 【高阶架构】：同步在建表阶段兜底创建状态表，防止初次使用的用户报错
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kline_status (
                    代码 TEXT PRIMARY KEY,
                    最新日期 TEXT
                )
            ''')
            # 提交建表事务
            conn_h.commit()

            # 3. 提取本地高水位标记（断点续传核心）
            # 【重构读取】：下载前的断点检查，也改为从极速的状态表中读取，不再扫描明细表
            df_watermark = pd.read_sql("SELECT 代码, 最新日期 as max_date FROM kline_status", conn_h)
            # 生成字典，格式如 {'600000': '2026-05-08'}，供后续比对
            watermark_dict = dict(zip(df_watermark['代码'], df_watermark['max_date']))

            # 计算总股票数（用于进度计算）
            total_stocks = len(valid_codes)
            # 初始化已更新股票数
            updated_stocks = 0
            # 初始化总插入数据行数
            total_inserted = 0

            # 4. 遍历执行数据拉取与更新
            # 遍历有效股票代码，idx为索引（从1开始），code为股票代码
            for idx, code in enumerate(valid_codes, 1):
                # 随时响应用户的中止操作（核心防卡死设计）
                if not self.is_running: 
                    # 发送中止日志，提示已取消下载
                    self.log_signal.emit("🛑 收到中止指令，已取消后续 K线下载。已下载的数据安全入库。")
                    break
                
                # 计算当前进度（idx/总股票数*100），转换为整数
                progress = int((idx / total_stocks) * 100)
                # 发送进度信号，更新主界面进度条
                self.progress_signal.emit(progress)

                # 将纯数字代码转回baostock识别的格式（加sh./sz.前缀）
                bs_code = f"sh.{code}" if code.startswith(('60', '68')) else f"sz.{code}"

                # 判断全量拉取还是增量补全（断点续传核心）
                if code in watermark_dict:
                    # 若本地已有该股票数据，取最新日期+1天作为起始点（增量下载）
                    last_date = watermark_dict[code]
                    start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    # 若本地无该股票数据，从2000-01-01开始全量下载
                    start_date = "2000-01-01" 

                # 若起始日期>安全截止日，说明该股票已更新到最新，跳过
                if start_date > safe_end_date:
                    continue

                # 每100只股票或第一只股票，发送进度日志（避免日志刷屏）
                if idx % 100 == 0 or idx == 1:
                    self.log_signal.emit(f"🌐 更新进度 [{idx}/{total_stocks}] 股票 {code} (拉取区间: {start_date} -> {safe_end_date})")

                # Baostock 容灾重试请求（最多3次重试，避免网络波动导致失败）
                rs = None  # 初始化查询结果
                # 重试3次
                for attempt in range(3):
                    try:
                        # 查询历史K线数据（核心接口）
                        rs = bs.query_history_k_data_plus(
                            bs_code,  # 股票代码（baostock格式）
                            # 需要获取的字段：日期、代码、开盘、最高、最低、收盘、昨收、成交量、成交额、换手率、交易状态
                            "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus",
                            start_date=start_date,  # 起始日期
                            end_date=safe_end_date, # 截止日期
                            frequency="d",          # 频率：d=日线（可选周/月等）
                            adjustflag="3"          # 复权类型：3=后复权（保证价格连续性）
                        )
                        # 查询成功，跳出重试循环
                        if rs.error_code == '0': break 
                        else: 
                            # 查询失败，休眠1秒后重试
                            time.sleep(1)
                    except Exception:
                        # 捕获异常，非最后一次重试则休眠1秒
                        if attempt < 2: time.sleep(1)
                
                # 若3次重试后仍失败，跳过该股票
                if rs is None or rs.error_code != '0': 
                    continue

                # 初始化K线数据列表
                data_list = []
                # 循环读取K线数据
                while (rs.error_code == '0') & rs.next():
                    # 添加每条K线数据到列表
                    data_list.append(rs.get_row_data())

                # 若无K线数据，跳过
                if not data_list: continue 
                
                # 将列表转换为DataFrame，指定列名
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                # 重命名列（对齐数据库字段名，方便写入）
                df.rename(columns={
                    'date': '日期', 'code': '代码', 'open': '开盘', 'high': '最高', 
                    'low': '最低', 'close': '收盘', 'preclose': '昨收', 
                    'volume': '成交量', 'amount': '成交额', 'turn': '换手率', 'tradestatus': '状态'
                }, inplace=True)

                # 转换数值列类型（字符串转数值，避免数据库存储异常）
                num_cols = ['开盘', '最高', '最低', '收盘', '昨收', '成交额', '换手率']
                # 遍历数值列，转换为数值类型（errors='coerce'将异常值转为NaN，fillna(0.0)填充为0）
                for c in num_cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
                # 成交量转换为整数（同理，异常值填充为0）
                df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce').fillna(0).astype(int)
                
                # 重新剔除前缀存入数据库（恢复为6位纯数字代码）
                df['代码'] = df['代码'].str.replace('sh.', '').str.replace('sz.', '')
                
                # 仅保留真实发生交易的阳性数据（过滤停牌/无成交数据）
                df = df[(df['状态'] == '1') & (df['成交量'] > 0)] 
                # 过滤后无数据，跳过
                if df.empty: continue

                # 将DataFrame转换为元组列表（适配sqlite3的executemany方法）
                data_tuples = list(df.itertuples(index=False, name=None))
                try:
                    # 批量插入数据（INSERT OR IGNORE：主键重复则忽略，避免重复写入）
                    cursor.executemany('''
                        INSERT OR IGNORE INTO history_kline 
                        (日期, 代码, 开盘, 最高, 最低, 收盘, 昨收, 成交量, 成交额, 换手率, 状态) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', data_tuples)
                    # 提取本次下载获取到的最新一天的日期（DataFrame最后一行）
                    latest_date_in_df = df['日期'].max()
                    
                    # 顺手将该股票的最新日期拍入状态表，INSERT OR REPLACE 保证只有一条记录
                    cursor.execute('''
                        INSERT OR REPLACE INTO kline_status (代码, 最新日期)
                        VALUES (?, ?)
                    ''', (code, latest_date_in_df))
                    # 提交事务（sqlite3批量插入后必须commit才生效）
                    conn_h.commit()
                    # 累计插入数据行数
                    total_inserted += len(data_tuples)
                    # 累计已更新股票数
                    updated_stocks += 1
                except Exception as e:
                    # 捕获写入异常，发送日志
                    self.log_signal.emit(f"⚠️ 写入 {code} 异常: {e}")

            # 关闭K线数据库连接（释放资源）
            conn_h.close()
            
            # 若未收到中止指令（正常完成）
            if self.is_running:
                # 发送K线更新完成日志
                self.log_signal.emit(f"🎉 独立 K线数据更新完毕! 成功为 {updated_stocks} 只股票写入数据。")

        finally:
            # 无论成功/失败，最终退出baostock接口
            bs.logout() 

    # =========================================================================
    # 业务方法 3: 财报与基本面同步 (3年滑动窗口 + 防穿越过滤)
    # =========================================================================
    def update_finance(self):
        self.log_signal.emit("📊 开始更新财务报表数据...")
        try:
            conn = sqlite3.connect(DB_FINANCE_PATH)
            cursor = conn.cursor()
            # 纯净 SQL：已彻底删除任何 # 注释
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS all_financials (
                    代码 TEXT, 名称 TEXT, 每股收益 REAL, 每股净资产 REAL,
                    净利润 REAL, 净利润同比增长 REAL, 报告期 TEXT,
                    PRIMARY KEY (代码, 报告期)
                )
            ''')
            conn.commit()

            now = datetime.now()
            current_year = now.year
            current_month = now.month  
            
            # ====== 【核心优化 1】3年滑动窗口，完美保障 210 天极限 TTM 计算所需的前置数据 ======
            years = [current_year, current_year - 1, current_year - 2]
            periods = ["1231", "0930", "0630", "0331"]
            
            valid_scan_list = []
            for y in years:
                for p in periods:
                    # 防穿越拦截：剔除今年尚未发生的季度，防止请求死锁
                    if y == current_year and int(p[:2]) > current_month:
                        continue
                    valid_scan_list.append(f"{y}{p}")
            # =================================================================================
            
            total_steps = len(valid_scan_list)
            current_step = 0
            
            for r_date in valid_scan_list:
                if not self.is_running: break
                
                current_step += 1
                progress = int((current_step / total_steps) * 100)
                self.progress_signal.emit(progress)
                self.log_signal.emit(f"🌐 正在拉取 {r_date} 季度的财务数据...")
                
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
                    cursor.executemany('''
                        INSERT OR REPLACE INTO all_financials 
                        (代码, 名称, 每股收益, 每股净资产, 净利润, 净利润同比增长, 报告期)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', [(r['代码'], r['名称'], r['每股收益'], r['每股净资产'], r['净利润'], r['净利润同比增长'], r['报告期']) for r in records])
                    conn.commit()
                    
                    time.sleep(1.5)
                except Exception:
                    continue
                        
            conn.close()
            if self.is_running:
                self.log_signal.emit("🎉 财务报表数据安全同步完成！")
        except Exception as e:
            self.log_signal.emit(f"❌ 财报网络交互受阻: {e}")

    # =========================================================================
    # 业务方法 4: 分红除权规则同步 (底层深度自愈机制 + 极速增量)
    # =========================================================================
    def update_dividend(self):
        self.log_signal.emit("🎁 开始更新分红送配数据...")
        try:
            conn = sqlite3.connect(DB_DIVIDEND_PATH) 
            cursor = conn.cursor()
            # 纯净 SQL：已彻底删除任何 # 注释
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS dividend_rules (
                    代码 TEXT NOT NULL, ex_date TEXT NOT NULL,
                    S REAL, D REAL,
                    PRIMARY KEY (代码, ex_date)
                )
            ''')
            conn.commit()

            # ====== 【核心优化 2】基于日期的深度自愈机制，而非不可靠的行数 ======
            cursor.execute("SELECT MIN(ex_date) FROM dividend_rules")
            min_date = cursor.fetchone()[0]
            
            now = datetime.now()
            current_year = now.year
            current_month = now.month 
            scan_periods = []
            
            # 判断逻辑：如果库里为空，或者最老的一条记录竟然比 2006 年还晚，说明底层数据存在严重断层
            if not min_date or min_date > "2006-01-01":
                self.log_signal.emit("⚠️ 检测到分红历史底座不完整，启动全量数据回补 (预计耗时较长)...")
                start_year = 2000  # 强制从 2000 年开始全量扫街
                for y in range(current_year, start_year - 1, -1):
                    for p in ["1231", "0930", "0630", "0331"]:
                        if y == current_year and int(p[:2]) > current_month:
                            continue
                        scan_periods.append(f"{y}{p}")
            else:
                self.log_signal.emit("✅ 历史分红底座深厚且健康，启动极速增量更新模式...")
                # 底座完整时，仅拉取去年和今年，确保预案落地的更新不被遗漏
                for y in [current_year, current_year - 1]:
                    for p in ["1231", "0930", "0630", "0331"]:
                        if y == current_year and int(p[:2]) > current_month:
                            continue
                        scan_periods.append(f"{y}{p}")
            # ==============================================================================
            
            total_inserted = 0
            total_periods = len(scan_periods)
            for idx, period in enumerate(scan_periods, 1):
                if not self.is_running: break
                
                progress = int((idx / total_periods) * 100)
                self.progress_signal.emit(progress)
                self.log_signal.emit(f"🌐 正在扫描 {period} 财务窗口的除权实施报告...")
                
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
                        cursor.executemany("INSERT OR REPLACE INTO dividend_rules (代码, ex_date, S, D) VALUES (?, ?, ?, ?)", records)
                        conn.commit()
                        total_inserted += len(records)
                        
                    time.sleep(random.uniform(0.5, 1.5))
                except Exception:
                    continue
            
            conn.close()
            if self.is_running:
                self.log_signal.emit(f"🎉 分红数据更新完成！本次新增 {total_inserted} 条新增除权基准点。")
        except Exception as e:
            self.log_signal.emit(f"❌ 分红网络交互受阻: {e}")
            
    # =========================================================================
    # 业务方法 5: 一键极速云同步 (客户端主动拉取)
    # =========================================================================
    def sync_from_cloud(self):
        self.log_signal.emit("☁️ 正在连接云端数据中心...")
        server_url = "http://39.96.212.178:8000"  # 您的服务器公网 IP
        
        try:
            # =================================================================
            # 轨道 A：冷启动（本地纯空库 -> 触发物理文件流式下载）
            # =================================================================
            # -----------------------------------------------------------------
            # 强化版多维防崩审查网：智能研判走【物理文件流下载】还是【内存字典切片】
            # -----------------------------------------------------------------
            is_cold_start = False
            
            # 维度 1：物理文件不存在，或文件大小严重残缺（例如小于 50MB）
            if not os.path.exists(DB_HISTORY_PATH) or os.path.getsize(DB_HISTORY_PATH) < 50 * 1024 * 1024:
                is_cold_start = True
            else:
                # 维度 2：文件虽在，但探查内部高水位，如果太落后，为保护云端内存，强制判定为冷启动
                try:
                    conn_check = sqlite3.connect(DB_HISTORY_PATH)
                    df_check = pd.read_sql("SELECT 代码, 最新日期 FROM kline_status", conn_check)
                    conn_check.close()
                    
                    if df_check.empty:
                        is_cold_start = True
                    else:
                        # 计算全市场当前保留数据的平均年份
                        df_check['year'] = pd.to_datetime(df_check['最新日期'], errors='coerce').dt.year
                        avg_year = df_check['year'].mean()
                        # 如果平均年份早于 2024 年，说明本地缺失了数年的巨量历史，切片会榨干服务器内存
                        if avg_year < 2024:
                            self.log_signal.emit("⚠️ 检测到本地数据严重过时，走切片会触发云端内存预警，自动切换至冷启动整体覆盖...")
                            is_cold_start = True
                except Exception:
                    # 如果读取状态表报错（说明表结构损坏或有脏数据），安全起见直接全量重建
                    is_cold_start = True

            # =================================================================
            # 轨道 A 执行体
            # =================================================================
            if is_cold_start:
                self.log_signal.emit("📦 已激活【全局底座流式覆盖下载】模式，正在向云端申请完整的物理数据库...")
                
                # 发起 GET 请求，开启 stream=True (防止把本地内存撑爆)
                with requests.get(f"{server_url}/api/kline/download", stream=True, timeout=15) as r:
                    r.raise_for_status()
                    
                    # 获取文件总大小 (单位：字节)
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded_size = 0
                    
                    self.log_signal.emit(f"⬇️ 核心底座大小约为 {total_size / (1024*1024):.1f} MB，开始高速下载...")
                    
                    # 确保本地 data 文件夹存在
                    os.makedirs(os.path.dirname(DB_HISTORY_PATH), exist_ok=True)
                    
                    # 以二进制写入模式打开本地文件，分块接收
                    with open(DB_HISTORY_PATH, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): # 每次接收 8KB
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                                
                                # 计算并发送进度给主界面的进度条
                                if total_size > 0:
                                    progress = int((downloaded_size / total_size) * 100)
                                    self.progress_signal.emit(progress)
                                    
                if self.is_running:
                    self.log_signal.emit("🎉 全量历史底座下载完成！环境初始化成功。")
                return

            # =================================================================
            # 轨道 B：热启动（终极进化版：副表字典精准同步）
            # =================================================================
            self.log_signal.emit("📦 正在扫描本地副表进度，准备向云端请求精准切片...")

            # 【核心升级】：直接读取副表，瞬间生成进度字典
            conn_h = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
            conn_h.execute("PRAGMA journal_mode=WAL;")
            cursor = conn_h.cursor()
            
            try:
                df_watermark = pd.read_sql("SELECT 代码, 最新日期 FROM kline_status", conn_h)
                watermark_dict = dict(zip(df_watermark['代码'], df_watermark['最新日期']))
            except Exception as e:
                self.log_signal.emit(f"❌ 读取本地副表失败，无法同步: {e}")
                conn_h.close()
                return
                
            # 如果本地副表为空，提示用户去初始化
            if not watermark_dict:
                self.log_signal.emit("⚠️ 本地副表为空，请先执行一次极速系统自检或手动下载！")
                self.progress_signal.emit(100)
                conn_h.close()
                return

            # 封装请求负载
            payload = {
                "watermark_dict": watermark_dict
            }

            self.log_signal.emit("☁️ 正在与云端服务器进行 Delta 增量握手计算...")
            self.progress_signal.emit(30) # 假进度，提升交互感
            
            # 发起 POST 请求，将庞大的字典发给服务器让其切片
            response = requests.post(f"{server_url}/api/kline/sync", json=payload, timeout=120)
            
            if response.status_code != 200:
                self.log_signal.emit(f"❌ 云端服务器异常，状态码: {response.status_code}")
                conn_h.close()
                return
                
            res_data = response.json()
            
            # 判断是否已是最新
            if res_data.get("status") == "up_to_date" or not res_data.get("data"):
                self.log_signal.emit("✅ 本地 K 线数据库已是最新，无断层需修补。")
                self.progress_signal.emit(100)
                conn_h.close()
                return
                
            batch_data = res_data.get("data", [])
            self.log_signal.emit(f"⬇️ 云端切片计算完成，准备接收并写入 {len(batch_data)} 条精准增量数据...")
            self.progress_signal.emit(60)

            # 【核心安全机制】：主表与副表的双重原子化写入
            try:
                # 开启事务保护
                cursor.execute("BEGIN TRANSACTION;")
                
                # 1. 将接收到的切片写入 K 线主表
                data_tuples = [
                    (r.get('日期'), r.get('代码'), r.get('开盘'), r.get('最高'), 
                     r.get('最低'), r.get('收盘'), r.get('昨收'), r.get('成交量'), 
                     r.get('成交额'), r.get('换手率'), r.get('状态')) 
                    for r in batch_data
                ]
                cursor.executemany('''
                    INSERT OR REPLACE INTO history_kline 
                    (日期, 代码, 开盘, 最高, 最低, 收盘, 昨收, 成交量, 成交额, 换手率, 状态) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_tuples)
                
                # 2. 从刚才这批新数据中，提取出每只股票的“新最大日期”，顺手刷新副表
                df_new = pd.DataFrame(batch_data)
                latest_dates = df_new.groupby('代码')['日期'].max().reset_index()
                status_tuples = list(latest_dates.itertuples(index=False, name=None))
                
                cursor.executemany('''
                    INSERT OR REPLACE INTO kline_status (代码, 最新日期)
                    VALUES (?, ?)
                ''', status_tuples)
                
                # 一并提交落盘
                conn_h.commit()
                
                self.progress_signal.emit(100)
                if self.is_running:
                    self.log_signal.emit(f"🎉 极速云同步完美收官！成功修复/补齐了 {len(batch_data)} 条 K 线切片。")
                
            except Exception as db_e:
                cursor.execute("ROLLBACK;")
                self.log_signal.emit(f"❌ 本地落盘入库异常，已回滚保护: {db_e}")
            finally:
                conn_h.close()
                
        # =====================================================================
        # 补全最外层的 except：拦截所有断网、超时等网络级或系统级错误
        # =====================================================================
        except requests.exceptions.RequestException as req_e:
            self.log_signal.emit(f"❌ 网络请求失败，请检查服务器是否开启: {req_e}")
        except Exception as e:
            self.log_signal.emit(f"❌ 云同步发生未知异常: {str(e)}")