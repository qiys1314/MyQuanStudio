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
        
    @staticmethod
    def get_safe_end_date():
        """
        [公共方法] 测算安全的 T-1 结算基准日（零网络延迟，纯本地 SQL 毫秒级查询）
        不再依赖 Baostock 登录，直接读取本地 trade_calendar 表。
        """
        now = datetime.now()
        
        # 核心逻辑：以每天下午 18:00 为数据结算分界线
        if now.hour >= 18:
            # 如果过了 18 点，说明今天的 K 线数据已经生成了。
            # 我们就把“今天”作为向回查找的极限锚点。
            target_limit = now.strftime('%Y-%m-%d')
        else:
            # 如果还没到 18 点，就算今天是交易日，数据也是没结算完的残缺品。
            # 所以我们把“昨天”作为向回查找的极限锚点。
            target_limit = (now - timedelta(days=1)).strftime('%Y-%m-%d')

        safe_end_date = "未知"
        
        # 拦截：如果底层数据库文件还不存在（极早期冷启动），直接返回未知
        if not os.path.exists(DB_HISTORY_PATH):
            return safe_end_date

        try:
            # 连接本地历史数据库
            conn = sqlite3.connect(DB_HISTORY_PATH)
            cursor = conn.cursor()
            
            # 【极致 SQL 魔法】：查出所有小于等于 target_limit 的日期中，
            # 且 is_trading_day = 1 (是交易日) 的【最大日期】
            # 无论前面连着多少个周末、国庆、春节，这句 SQL 都能瞬间穿透，精确定位到上一个开盘日
            cursor.execute('''
                SELECT MAX(calendar_date) 
                FROM trade_calendar 
                WHERE calendar_date <= ? AND is_trading_day = 1
            ''', (target_limit,))
            
            result = cursor.fetchone()
            if result and result[0]:
                safe_end_date = result[0]
                
        except Exception as e:
            # 捕获异常（比如用户还没跑 init_calendar.py 导致找不到表）
            print(f"本地日历查询异常，请确保已运行日历初始化脚本: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
                
        return safe_end_date
    
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
            safe_end_date = self.get_safe_end_date()

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

            # ==========================================
            # 替换 run_self_check() 中的 步骤 3: 检查辅助数据库 (财报与分红) 的最后更新时间
            # ==========================================
            def check_db_health(api_name, db_path, name):
                """加入云端秒级嗅探的高级健康检查器"""
                if os.path.exists(db_path):
                    try:
                        # 连上本地辅库
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        
                        # 查本地打卡时间
                        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_success_time'")
                        row_s = cursor.fetchone()
                        # 查本地数据真正变动的日期戳
                        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_mutation_date'")
                        row_m = cursor.fetchone()
                        
                        conn.close()
                        
                        # 拆解时间
                        local_success_str = row_s[0] if row_s else "未知"
                        local_mutation_str = row_m[0] if row_m else "未知"
                        
                        # 如果存在本地打卡记录，算一算几天没跑脚本了
                        if local_success_str != "未知":
                            last_time = datetime.strptime(local_success_str, '%Y-%m-%d %H:%M:%S')
                            days_ago = (datetime.now() - last_time).days
                            self.log_signal.emit(f"💰 {name}：本地跑批最后于 {local_success_str} ({days_ago} 天前)，数据本质变更于 {local_mutation_str}。")
                        else:
                            self.log_signal.emit(f"💰 {name}：发现本地旧版库，无时间戳信息。")
                            days_ago = 999
                    except Exception as e:
                        # 兜底捕获数据库损坏等异常
                        self.log_signal.emit(f"💰 {name}：时间戳读取异常 ({e})，表结构可能陈旧。")
                        local_mutation_str = "未知"
                        days_ago = 999
                        
                    # ========================================================
                    # 引入云端秒级比对，提供极其精准的用户建议
                    # ========================================================
                    try:
                        # 设置 1.5 秒极短超时，断网也不影响开机速度
                        server_url = "http://39.96.212.178:8000"
                        res_status = requests.get(f"{server_url}/api/{api_name}/status", timeout=1.5)
                        
                        if res_status.status_code == 200 and res_status.json().get("status") == "success":
                            # 拿到云端的实质变动日期
                            server_mutation_str = res_status.json().get("last_mutation_date", "1900-01-01")
                            
                            # 核心判断：比较两端 YYYY-MM-DD 的字典序大小
                            if local_mutation_str != "未知" and server_mutation_str > local_mutation_str:
                                # 云端变动戳 > 本地，确凿有新财报漏下
                                self.log_signal.emit(f"   👉 [提示] 云端嗅探到有新的实质性数据更新！请点击上方的【☁️ 极速更新数据】。")
                            elif local_mutation_str != "未知" and server_mutation_str <= local_mutation_str:
                                # 本地由于手动同步，甚至大于等于云端
                                self.log_signal.emit(f"   👉 [状态] 云端嗅探对比完成，您的本地数据水平已是最新，无需更新。")
                            else:
                                # 异常状态
                                self.log_signal.emit(f"   👉 [建议] 本地缺少记录锚点，建议点击上方更新重塑数据基底。")
                        else:
                            # 接口请求失败（没抛出网络异常，但返回码不是 200）
                            self.log_signal.emit(f"   👉 [状态] 云端嗅探异常，基于本地时间判断：{'数据滞后' if days_ago > 3 else '数据健康'}。")
                    except Exception:
                        # 完全断网情况下的降级容灾：只依靠刚才在本地算出的 days_ago 给建议
                        self.log_signal.emit(f"   👉 [断网状态] 无法连接云端嗅探，基于本地时间判断：{'数据滞后' if days_ago > 3 else '数据健康'}。")
                else:
                    # 如果一开始发现连 DB 文件都没有
                    self.log_signal.emit(f"💰 {name}：未找到本地数据库，系统处于裸奔状态，请务必执行初始化下载！")

            # 调用新方法：检查财报库（传入对应云端接口路由的前缀）
            check_db_health("finance", DB_FINANCE_PATH, "财报数据库")
            # 调用新方法：检查分红库（传入对应云端接口路由的前缀）
            check_db_health("dividend", DB_DIVIDEND_PATH, "分红除权数据库")
            

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
            safe_end_date = self.get_safe_end_date()

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
    # =========================================================================
    # 业务方法 3: 财报与基本面同步 (3年滑动窗口 + 临时表防空转比对)
    # =========================================================================
    def update_finance(self):
        self.log_signal.emit("📊 开始更新财务报表数据...")
        try:
            conn = sqlite3.connect(DB_FINANCE_PATH)
            cursor = conn.cursor()
            
            # ====== 【新增】1. 初始化临时表 ======
            table_schema = '''
                (
                    代码 TEXT, 名称 TEXT, 每股收益 REAL, 每股净资产 REAL,
                    净利润 REAL, 净利润同比增长 REAL, 报告期 TEXT,
                    PRIMARY KEY (代码, 报告期)
                )
            '''
            cursor.execute(f"CREATE TABLE IF NOT EXISTS all_financials {table_schema}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS temp_all_financials {table_schema}")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_info (
                    config_key TEXT PRIMARY KEY,
                    config_value TEXT
                )
            ''')
            # 执行前必须清空临时表
            cursor.execute("DELETE FROM temp_all_financials")
            conn.commit()

            now = datetime.now()
            current_year = now.year
            current_month = now.month  
            
            years = [current_year, current_year - 1, current_year - 2]
            periods = ["1231", "0930", "0630", "0331"]
            
            valid_scan_list = []
            for y in years:
                for p in periods:
                    if y == current_year and int(p[:2]) > current_month:
                        continue
                    valid_scan_list.append(f"{y}{p}")
            
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
                    # ====== 【修改】2. 全部写入 temp_all_financials ======
                    cursor.executemany('''
                        INSERT OR REPLACE INTO temp_all_financials 
                        (代码, 名称, 每股收益, 每股净资产, 净利润, 净利润同比增长, 报告期)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', [(r['代码'], r['名称'], r['每股收益'], r['每股净资产'], r['净利润'], r['净利润同比增长'], r['报告期']) for r in records])
                    conn.commit()
                    
                    time.sleep(1.5)
                except Exception:
                    continue

            # ====== 阶段三：比对差异，按需写入主表 ======
            if self.is_running:
                self.log_signal.emit("⏳ [财报] 扫描完毕，正在进行数据变动差异化比对...")
                cursor.execute("BEGIN TRANSACTION;")
                
                # 1. 找茬：计算实质性变动的行数
                cursor.execute('''
                    SELECT COUNT(*) FROM temp_all_financials t
                    LEFT JOIN all_financials a ON t.代码 = a.代码 AND t.报告期 = a.报告期
                    WHERE a.代码 IS NULL
                       OR IFNULL(t.每股收益, 0) != IFNULL(a.每股收益, 0)
                       OR IFNULL(t.净利润, 0) != IFNULL(a.净利润, 0)
                ''')
                real_mutations = cursor.fetchone()[0]
                
                # 2. 按需落盘：只有真正发生了变动，才执行写入主表的动作
                if real_mutations > 0:
                    # 将临时表数据写入主表
                    cursor.execute("INSERT OR REPLACE INTO all_financials SELECT * FROM temp_all_financials")
                    
                    # 记录数据库变动时间戳
                    mutation_date = datetime.now().strftime('%Y-%m-%d')
                    cursor.execute("INSERT OR REPLACE INTO system_info (config_key, config_value) VALUES ('last_mutation_date', ?)", (mutation_date,))
                    self.log_signal.emit(f"💡 检测到 {real_mutations} 行实质性变动，数据已合入主表，变动戳更新至 {mutation_date}")
                else:
                    # 如果等于 0，直接跳过 INSERT 主表的动作，极大地节省了硬盘 I/O
                    self.log_signal.emit("🤷‍♂️ 抓取的数据与本地完全一致，跳过主表覆盖，变动戳保持不变。")
                
                # 3. 必做项：清空临时表（打扫战场）
                cursor.execute("DELETE FROM temp_all_financials")
                
                # 4. 必做项：更新成功跑完的打卡时间
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("INSERT OR REPLACE INTO system_info (config_key, config_value) VALUES ('last_success_time', ?)", (current_time,))
                
                conn.commit()
                        
            conn.close()
            if self.is_running:
                self.log_signal.emit("🎉 财务报表数据安全同步完成！")
        except Exception as e:
            self.log_signal.emit(f"❌ 财报网络交互受阻: {e}")
            
    # =========================================================================
    # 业务方法 4: 分红除权规则同步 (底层深度自愈机制 + 极速增量)
    # =========================================================================
    # =========================================================================
    # 业务方法 4: 分红除权规则同步 (底层深度自愈机制 + 临时表防空转比对)
    # =========================================================================
    def update_dividend(self):
        self.log_signal.emit("🎁 开始更新分红送配数据...")
        try:
            conn = sqlite3.connect(DB_DIVIDEND_PATH) 
            cursor = conn.cursor()
            
            # ====== 【新增】1. 初始化临时表 ======
            table_schema = '''
                (
                    代码 TEXT NOT NULL, ex_date TEXT NOT NULL,
                    S REAL, D REAL,
                    PRIMARY KEY (代码, ex_date)
                )
            '''
            cursor.execute(f"CREATE TABLE IF NOT EXISTS dividend_rules {table_schema}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS temp_dividend_rules {table_schema}")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_info (
                    config_key TEXT PRIMARY KEY,
                    config_value TEXT
                )
            ''')
            # 必须清空临时表
            cursor.execute("DELETE FROM temp_dividend_rules")
            conn.commit()

            cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'dividend_full_init'")
            init_row = cursor.fetchone()
            is_full_init = True if init_row and init_row[0] == '1' else False
            
            now = datetime.now()
            scan_periods = []
            
            if not is_full_init:
                self.log_signal.emit("⚠️ 检测到分红历史底座未完整建立，启动全量数据拉取 (预计耗时较长)...")
                start_year = 2000  
                for y in range(now.year, start_year - 1, -1):
                    for p in ["1231", "0930", "0630", "0331"]:
                        if y == now.year and int(p[:2]) > now.month:
                            continue
                        scan_periods.append(f"{y}{p}")
            else:
                self.log_signal.emit("✅ 历史分红底座标识健康，启动极速增量追踪模式...")
                for y in [now.year, now.year - 1]:
                    for p in ["1231", "0930", "0630", "0331"]:
                        if y == now.year and int(p[:2]) > now.month:
                            continue
                        scan_periods.append(f"{y}{p}")
            
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
                        # ====== 【修改】2. 全部写入 temp_dividend_rules ======
                        cursor.executemany("INSERT OR REPLACE INTO temp_dividend_rules (代码, ex_date, S, D) VALUES (?, ?, ?, ?)", records)
                        conn.commit()
                        total_inserted += len(records)
                        
                    time.sleep(random.uniform(0.5, 1.5))
                except Exception:
                    continue
                
            # ====== 阶段三：比对差异，按需写入主表 ======
            if self.is_running:
                self.log_signal.emit("⏳ [分红] 扫描完毕，正在进行规则变动比对...")
                cursor.execute("BEGIN TRANSACTION;")
                
                # 1. 找茬
                cursor.execute('''
                    SELECT COUNT(*) FROM temp_dividend_rules t
                    LEFT JOIN dividend_rules a ON t.代码 = a.代码 AND t.ex_date = a.ex_date
                    WHERE a.代码 IS NULL
                       OR IFNULL(t.S, 0) != IFNULL(a.S, 0)
                       OR IFNULL(t.D, 0) != IFNULL(a.D, 0)
                ''')
                real_mutations = cursor.fetchone()[0]
                
                # 2. 按需落盘
                if real_mutations > 0:
                    cursor.execute("INSERT OR REPLACE INTO dividend_rules SELECT * FROM temp_dividend_rules")
                    
                    mutation_date = datetime.now().strftime('%Y-%m-%d')
                    cursor.execute("INSERT OR REPLACE INTO system_info (config_key, config_value) VALUES ('last_mutation_date', ?)", (mutation_date,))
                    self.log_signal.emit(f"💡 检测到 {real_mutations} 条新规则，已合入主表，变动戳更新至 {mutation_date}")
                else:
                    self.log_signal.emit("🤷‍♂️ 未检测到新规则，跳过主表覆盖，变动戳保持不变。")
                
                # 3. 必做项：清空临时表
                cursor.execute("DELETE FROM temp_dividend_rules")
                
                # 4. 必做项：更新打卡时间及初始化标识
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("INSERT OR REPLACE INTO system_info (config_key, config_value) VALUES ('last_success_time', ?)", (current_time,))
                
                if not is_full_init and total_inserted > 0:
                    cursor.execute("INSERT OR REPLACE INTO system_info (config_key, config_value) VALUES ('dividend_full_init', '1')")
                
                conn.commit()
            
            conn.close()
            if self.is_running:
                self.log_signal.emit(f"🎉 分红数据更新完成！本次拉取扫描 {total_inserted} 条除权基准点。")
        except Exception as e:
            self.log_signal.emit(f"❌ 分红网络交互受阻: {e}")
            
    # =========================================================================
    # 业务方法 5: 一键极速云同步 (客户端主动拉取 - 原子化替身防毁版)
    # =========================================================================
    def sync_from_cloud(self):
        self.log_signal.emit("☁️ 正在连接云端数据中心...")
        server_url = "http://39.96.212.178:8000"  # 您的服务器公网 IP
        
        # -----------------------------------------------------------------
        # 【阶段 1】：历史 K 线数据同步 (分配进度 0% -> 50%)
        # -----------------------------------------------------------------
        try:
            is_cold_start = False
            if not os.path.exists(DB_HISTORY_PATH) or os.path.getsize(DB_HISTORY_PATH) < 50 * 1024 * 1024:
                is_cold_start = True
            else:
                try:
                    conn_check = sqlite3.connect(DB_HISTORY_PATH)
                    df_check = pd.read_sql("SELECT 代码, 最新日期 FROM kline_status", conn_check)
                    conn_check.close()
                    if df_check.empty or pd.to_datetime(df_check['最新日期'], errors='coerce').dt.year.mean() < 2024:
                        self.log_signal.emit("⚠️ K 线数据需全量重建，切入流式底座下载...")
                        is_cold_start = True
                except Exception:
                    is_cold_start = True

            # --- 轨道 A: K线冷启动 (替身安全下载) ---
            if is_cold_start:
                self.log_signal.emit("📦 已激活【替身安全下载】模式获取 K 线物理数据库...")
                temp_path = DB_HISTORY_PATH + ".tmp"  # 使用临时替身文件
                
                with requests.get(f"{server_url}/api/kline/download", stream=True, timeout=15) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded_size = 0
                    
                    os.makedirs(os.path.dirname(DB_HISTORY_PATH), exist_ok=True)
                    with open(temp_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if not self.is_running: break  # 响应中止指令
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                                if total_size > 0:
                                    # 将下载进度映射到全局的 0% - 50% 区间
                                    progress = int((downloaded_size / total_size) * 50)
                                    self.progress_signal.emit(progress)
                
                # 下载循环结束后的兜底审判
                if not self.is_running:
                    if os.path.exists(temp_path): os.remove(temp_path)  # 清理废弃替身
                    self.log_signal.emit("🛑 已中止 K线下载，原数据安全无损。")
                    return # 用户主动放弃，直接退出整个同步
                else:
                    # 替身原子化转正
                    if os.path.exists(temp_path): os.replace(temp_path, DB_HISTORY_PATH)
                    self.log_signal.emit("🎉 全量历史 K 线底座安全入库！")
                    self.progress_signal.emit(50)
                    
            # --- 轨道 B: K线热启动增量 ---
            else:
                self.progress_signal.emit(10)
                self.log_signal.emit("📦 正在向云端请求精准切片...")
                conn_h = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
                conn_h.execute("PRAGMA journal_mode=WAL;")
                cursor = conn_h.cursor()
                
                try:
                    df_watermark = pd.read_sql("SELECT 代码, 最新日期 FROM kline_status", conn_h)
                    watermark_dict = dict(zip(df_watermark['代码'], df_watermark['最新日期']))
                    
                    if watermark_dict:
                        self.progress_signal.emit(25)
                        response = requests.post(f"{server_url}/api/kline/sync", json={"watermark_dict": watermark_dict}, timeout=120)
                        
                        if response.status_code == 200:
                            res_data = response.json()
                            if res_data.get("status") == "up_to_date" or not res_data.get("data"):
                                self.log_signal.emit("✅ 本地 K 线数据库已是最新。")
                                self.progress_signal.emit(50)
                            else:
                                batch_data = res_data.get("data", [])
                                self.log_signal.emit(f"⬇️ 准备安全合入 {len(batch_data)} 条 K 线增量数据...")
                                self.progress_signal.emit(40)
                                try:
                                    cursor.execute("BEGIN TRANSACTION;")
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
                                    
                                    df_new = pd.DataFrame(batch_data)
                                    status_tuples = list(df_new.groupby('代码')['日期'].max().reset_index().itertuples(index=False, name=None))
                                    cursor.executemany("INSERT OR REPLACE INTO kline_status (代码, 最新日期) VALUES (?, ?)", status_tuples)
                                    
                                    # 写入前最后一次判定是否被中止
                                    if not self.is_running:
                                        cursor.execute("ROLLBACK;")
                                        self.log_signal.emit("🛑 已安全回滚 K 线增量写入。")
                                        return
                                    else:
                                        conn_h.commit()
                                        self.log_signal.emit("🎉 K 线云同步增量合入完毕！")
                                        self.progress_signal.emit(50)
                                except Exception as db_e:
                                    cursor.execute("ROLLBACK;")
                                    self.log_signal.emit(f"❌ K 线入库异常，已回滚: {db_e}")
                finally:
                    conn_h.close()
                    
        except requests.exceptions.RequestException as req_e:
            self.log_signal.emit(f"❌ K 线网络请求失败: {req_e}")
        except Exception as e:
            self.log_signal.emit(f"❌ K 线云同步异常: {str(e)}")

        # 如果中途被掐断，不执行后续辅库校验
        if not self.is_running: return

        # -----------------------------------------------------------------
        # 【阶段 2】：辅助数据库同步流水线 (替身防毁核心 + 变动戳比对)
        # -----------------------------------------------------------------
        def _get_local_timestamps(db_path):
            """内部助手：返回本地库的 (打卡时间, 变动日期) 元组"""
            # 如果文件根本就不存在，返回两个最古老的时间
            if not os.path.exists(db_path): 
                return "1900-01-01 00:00:00", "1900-01-01"
            try:
                # 连上本地辅库
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                
                # 查询并提取打卡时间
                cur.execute("SELECT config_value FROM system_info WHERE config_key = 'last_success_time'")
                row_s = cur.fetchone()
                # 查询并提取实质变动日期
                cur.execute("SELECT config_value FROM system_info WHERE config_key = 'last_mutation_date'")
                row_m = cur.fetchone()
                
                # 安全关闭连接
                conn.close()
                
                # 分离出结果，若无记录则用远古时间兜底
                ts_success = row_s[0] if row_s else "1900-01-01 00:00:00"
                ts_mutation = row_m[0] if row_m else "1900-01-01"
                return ts_success, ts_mutation
            except: 
                # 万一表结构坏了或不存在该字段，无脑降级为老时间触发重新下载
                return "1900-01-01 00:00:00", "1900-01-01"

        def _sync_aux_db(api_name, db_name, db_path, progress_start, progress_end):
            # 若触发了中止，立即撤出
            if not self.is_running: return
            
            # 向主界面发信号汇报进度
            self.log_signal.emit(f"☁️ 正在比对【{db_name}】云端版本状态...")
            # 记录物理文件是否真的存在
            file_exists = os.path.exists(db_path)
            
            try:
                # 仅仅耗时 0.05 秒：向云端请求一个极小的 JSON 状态包
                res_status = requests.get(f"{server_url}/api/{api_name}/status", timeout=10)
                
                # 确保 HTTP 请求通畅且服务端给出了成功响应
                if res_status.status_code == 200 and res_status.json().get("status") == "success":
                    
                    # 剥离出云端返回的打卡时间与变动日期
                    t_server_success = res_status.json().get("last_update")
                    t_server_mutation = res_status.json().get("last_mutation_date")
                    
                    # 剥离出本地存着的打卡时间与变动日期
                    t_local_success, t_local_mutation = _get_local_timestamps(db_path)
                    
                    # ==========================================================
                    # 【核心判断网】：基于变动时间戳 (YYYY-MM-DD) 的绝不空转机制
                    # ==========================================================
                    # 条件 1: 如果本地根本没文件，直接下载
                    # 条件 2: 如果云端的变动日期 > 本地的变动日期，说明云端确实多了新货，必须下载
                    if not file_exists or t_server_mutation > t_local_mutation:
                        
                        # 【安全过滤】：验证云端自己有没有长时间宕机（距离上次打卡超过3天报警）
                        days_diff = (datetime.now() - datetime.strptime(t_server_success, '%Y-%m-%d %H:%M:%S')).days
                        if days_diff < 3:
                            self.log_signal.emit(f"⬇️ 发现云端有实质性数据更新！启动替身安全下载【{db_name}】...")
                            os.makedirs(os.path.dirname(db_path), exist_ok=True)
                            
                            temp_path = db_path + ".tmp" 
                            # 开启数据流模式进行大文件下载
                            with requests.get(f"{server_url}/api/{api_name}/download", stream=True, timeout=15) as r:
                                r.raise_for_status()
                                # 提取 Header 中的文件总大小，用于算进度条
                                total_size = int(r.headers.get('content-length', 0))
                                downloaded_size = 0
                                
                                # 以二进制写入模式打开本地的 .tmp 替身文件
                                with open(temp_path, 'wb') as f:
                                    # 每次读取 8KB 数据块
                                    for chunk in r.iter_content(chunk_size=8192):
                                        if not self.is_running: break # 若用户中途点击中止，立刻跳出循环
                                        if chunk: 
                                            f.write(chunk)
                                            downloaded_size += len(chunk)
                                            if total_size > 0:
                                                # 按比例换算当前界面的进度条并发送
                                                fraction = downloaded_size / total_size
                                                current_prog = int(progress_start + (progress_end - progress_start) * fraction)
                                                self.progress_signal.emit(current_prog)
                            
                            # 下载循环出来后的兜底裁判：是被强行中止的，还是自然下完的？
                            if not self.is_running:
                                # 中止状态：抹除未下完的垃圾碎片
                                if os.path.exists(temp_path): os.remove(temp_path)
                                self.log_signal.emit(f"🛑 已中止【{db_name}】同步，丢弃临时碎片。")
                                return
                            else:
                                # 完工状态：将 .tmp 替身文件瞬间重命名，抹除老的 .db 文件
                                if os.path.exists(temp_path): os.replace(temp_path, db_path)
                                self.log_signal.emit(f"🎉 【{db_name}】替身转正，本地底座已焕新！")
                                self.progress_signal.emit(progress_end)
                        else:
                            # 如果云端已经宕机超过 3 天未曾打卡更新，拒绝下载老旧垃圾，并向用户告警
                            self.log_signal.emit(f"⚠️ 云端【{db_name}】自身已滞后超过3天未打卡，拒绝同步，请通知站长排查。")
                            self.progress_signal.emit(progress_end)
                    else:
                        # 【灵魂分支】：只要变动戳一样，管你脚本跑了多少遍，就是不下载！直接跳过！
                        self.log_signal.emit(f"✅ 本地【{db_name}】已是最新版 (数据变动戳一致)，智能跳过下载。")
                        self.progress_signal.emit(progress_end)
            except Exception as e:
                # 捕获网络断线、解析报错等异常，防止主线程崩溃
                self.log_signal.emit(f"❌ 同步【{db_name}】时发生网络或文件系统异常: {e}")
                self.progress_signal.emit(progress_end)
        # 挂载执行：分配进度配额
        _sync_aux_db("finance", "财报数据库", DB_FINANCE_PATH, progress_start=50, progress_end=75)
        _sync_aux_db("dividend", "分红数据库", DB_DIVIDEND_PATH, progress_start=75, progress_end=100)
        
        if self.is_running:
            self.progress_signal.emit(100)