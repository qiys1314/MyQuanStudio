# ==============================================================================
# 模块名称: core/data_fetcher.py
# 代码功能: 本地数据查询接口封装模块。
#           负责向业务层（如画图、独立数据分析等预留功能）提供格式化好的 Pandas 或字典数据。
# ==============================================================================

import sqlite3
import pandas as pd
from utils.config import DB_HISTORY_PATH, DB_FINANCE_PATH

class DataFetcher:
    """
    数据提取工具类，采用全静态方法设计。
    """

    @staticmethod
    def get_all_active_stocks():
        """
        提取当前市场所有处于有效披露期的股票代码及名称字典。
        """
        conn = sqlite3.connect(DB_FINANCE_PATH)
        try:
            # 使用 DISTINCT 过滤掉因多季度财报导致的重复股票代码。
            # 仅提取代码与名称两列。
            query = "SELECT DISTINCT 代码, 名称 FROM all_financials"
            df = pd.read_sql(query, conn)
            # 生成键值对映射，方便前台展示时将 6 位的数字代码转换为易读的中文名称。
            stock_dict = dict(zip(df['代码'], df['名称']))
            return stock_dict
        except Exception as e:
            # 捕获异常，输出基础报错信息（通常仅出现在开发环境控制台中）
            print(f"读取活跃股票名录失败: {e}")
            return {}
        finally:
            conn.close()

    @staticmethod
    def get_kline_data(code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        提取单只指定股票在一个时间区间内的完整历史 K 线数据切片。
        """
        conn = sqlite3.connect(DB_HISTORY_PATH)
        try:
            # SQL 查询语句。
            # WHERE ... BETWEEN ? AND ? 圈定目标时间窗口。
            # ORDER BY 日期 ASC：极度关键。保证返回结果按时间序列从早到晚严格递增，
            # 否则上层如果对返回结果进行均线计算或复权处理，时序错乱将导致结果完全失真。
            query = """
                SELECT 日期, 开盘, 最高, 最低, 收盘, 昨收, 成交量, 成交额, 换手率
                FROM history_kline 
                WHERE 代码=? AND 日期 BETWEEN ? AND ? 
                ORDER BY 日期 ASC
            """
            # 通过 params 参数传入条件值，交由 pandas 执行底层数据库映射
            df = pd.read_sql(query, conn, params=(code, start_date, end_date))
            return df
        except Exception as e:
            print(f"提取 {code} 历史切片失败: {e}")
            # 返回空的 DataFrame 以便外部调用者仍能使用 pandas API 进行安全判定（如 df.empty）
            return pd.DataFrame()
        finally:
            conn.close()

    @staticmethod
    def get_latest_finance(code: str) -> dict:
        """
        提取单只指定股票最新一期的财务基本面指标数据。
        """
        conn = sqlite3.connect(DB_FINANCE_PATH)
        try:
            # SQL 查询语句。
            # ORDER BY 报告期 DESC：按季度日期降序排列，保证最新的季报/年报在第一行。
            # LIMIT 1：限制结果集大小，数据库引擎找到第一条记录即刻停止，极大提升查询性能。
            query = """
                SELECT 报告期, 每股收益, 每股净资产, 净利润, 净利润同比增长 
                FROM all_financials 
                WHERE 代码=? 
                ORDER BY 报告期 DESC LIMIT 1
            """
            df = pd.read_sql(query, conn, params=(code,))
            # 判断结果集是否非空
            if not df.empty:
                # 提取数据框的第 0 行，并将其转换为标准的 Python 字典格式进行返回
                return df.iloc[0].to_dict()
            return {}
        except Exception as e:
            print(f"读取 {code} 财报失败: {e}")
            return {}
        finally:
            conn.close()