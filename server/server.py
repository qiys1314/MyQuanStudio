# ==============================================================================
# 模块名称: server/server.py
# 代码功能: 云端数据中心 API 服务端。
# 架构设计: 基于 FastAPI 框架构建异步 HTTP 接口，提供数据库高水位线查询、精准增量切片下发以及物理文件流式下载。
# ==============================================================================

import os
import sys
import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import FileResponse
from typing import Dict

# 实例化 FastAPI 核心应用对象
app = FastAPI(title="量化数据同步服务端", version="2.0")

# 设定服务端数据库文件的相对路径，并将项目根目录加入系统路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)  

# 导入所有相关数据库的物理路径配置
from utils.config import DB_HISTORY_PATH, DB_FINANCE_PATH, DB_DIVIDEND_PATH

# ------------------------------------------------------------------------------
# Pydantic 数据模型定义
# ------------------------------------------------------------------------------
class SyncRequest(BaseModel):
    # 接收客户端发来的进度字典，格式为 {"股票代码": "最新日期"}
    watermark_dict: Dict[str, str]  

# ------------------------------------------------------------------------------
# K 线数据 API 路由接口
# ------------------------------------------------------------------------------

@app.get("/api/kline/watermark")
def get_server_latest_date():
    """
    查询服务端 K 线数据库的绝对最大日期（高水位线）。
    开启 WAL 模式以防止读取时发生数据库锁死异常。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")
    
    conn = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(日期) FROM history_kline")
        max_date = cursor.fetchone()[0]
        
        if max_date is None:
            return {"latest_date": "1900-01-01", "message": "服务端数据库为空。"}
        return {"latest_date": max_date}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据库查询异常: {str(e)}")
    finally:
        conn.close()

@app.post("/api/kline/sync")
def get_incremental_data(request: SyncRequest):
    """
    基于客户端字典的高效增量切片下发接口。(副表对决优化版)
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")

    conn = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    try:
        client_watermarks = request.watermark_dict
        
        if not client_watermarks:
            return {"status": "success", "data": [], "message": "客户端副表为空，请走冷启动全量下载。"}

        # =====================================================================
        # 步骤 1: 提取服务端的副表 (耗时不到 0.05 秒)
        # =====================================================================
        df_server_status = pd.read_sql("SELECT 代码, 最新日期 FROM kline_status", conn)
        server_watermarks = dict(zip(df_server_status['代码'], df_server_status['最新日期']))

        # =====================================================================
        # 步骤 2: 纯内存字典精准比对，找出真正落后的股票
        # =====================================================================
        needs_update = {}
        for code, client_date in client_watermarks.items():
            server_date = server_watermarks.get(code)
            # 只有当服务端有这只股票，且服务端的日期确实大于客户端时，才需要提取增量
            if server_date and server_date > client_date:
                needs_update[code] = client_date

        # 如果比对后发现没有任何股票需要更新，直接极速返回！(这就是解决 13 秒空转的核心)
        if not needs_update:
            return {"status": "up_to_date", "data": []}

        # =====================================================================
        # 步骤 3: 按需精准切片 (日期分组 + 分块查询，防止 SQL 语句过长爆表)
        # =====================================================================
        results = []
        
        # 将相同起始日期的股票分在一组，极大减少 SQL 查询次数
        # 比如：4000 只股票都是差昨天一天的数据，那就组合成一条 SQL 查出来
        from collections import defaultdict
        date_groups = defaultdict(list)
        for code, date in needs_update.items():
            date_groups[date].append(code)

        for c_date, codes in date_groups.items():
            # SQLite 的 IN (...) 语法有 999 个变量的上限限制
            # 我们按照 800 个一批进行安全切块 (Chunking)
            chunk_size = 800
            for i in range(0, len(codes), chunk_size):
                chunk_codes = codes[i : i + chunk_size]
                
                # 动态拼接占位符: (?, ?, ?, ...)
                placeholders = ','.join(['?'] * len(chunk_codes))
                query = f"SELECT * FROM history_kline WHERE 日期 > ? AND 代码 IN ({placeholders})"
                
                # 参数组装: [日期, 代码1, 代码2, ...]
                params = [c_date] + chunk_codes
                
                df_chunk = pd.read_sql(query, conn, params=params)
                if not df_chunk.empty:
                    results.extend(df_chunk.to_dict(orient="records"))

        if not results:
             return {"status": "up_to_date", "data": []}

        return {
            "status": "success",
            "data": results,
            "total_synced": len(results)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务端精准切片异常: {str(e)}")
    finally:
        conn.close()
        
@app.get("/api/kline/download")
def download_full_db():
    """
    K线数据冷启动下载接口。
    直接以二进制流形式下发整个 SQLite 数据库文件。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=404, detail="服务端底层数据库不存在！")
    
    return FileResponse(
        path=DB_HISTORY_PATH, 
        filename="history_kline.db",
        media_type="application/octet-stream"
    )

# ------------------------------------------------------------------------------
# 财务报表数据 API 路由接口
# ------------------------------------------------------------------------------

@app.get("/api/finance/status")
def get_finance_status():
    """
    财报库状态探活接口。
    同时读取【最后成功打卡时间】和【最后数据变动日期】。
    """
    if not os.path.exists(DB_FINANCE_PATH):
        # 如果底层数据库不存在，返回错误信息
        return {"status": "error", "message": "云端财报库未建立"}
        
    try:
        # 连接财报数据库，设置10秒超时防止锁死
        conn = sqlite3.connect(DB_FINANCE_PATH, timeout=10.0)
        cursor = conn.cursor()
        
        # 查询原有的系统打卡时间戳
        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_success_time'")
        # 获取查询结果的第一行
        row_success = cursor.fetchone()
        
        # 查询新增的【数据库变动时间戳】（精确到天）
        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_mutation_date'")
        # 获取查询结果的第一行
        row_mutation = cursor.fetchone()
        
        # 安全关闭数据库连接
        conn.close()
        
        # 组装返回的 JSON 数据包
        return {
            "status": "success", 
            # 如果查到打卡时间则返回，否则返回默认的远古时间
            "last_update": row_success[0] if row_success else "1900-01-01 00:00:00",
            # 如果查到实质变动日期则返回，否则返回默认的远古日期
            "last_mutation_date": row_mutation[0] if row_mutation else "1900-01-01"
        }
            
    except Exception as e:
        # 捕获任何异常，并打包在 JSON 中返回给客户端
        return {"status": "error", "message": f"财报状态查询异常: {str(e)}"}
    
@app.get("/api/finance/download")
def download_finance_db():
    """
    财报库物理下载接口。
    """
    if not os.path.exists(DB_FINANCE_PATH):
        raise HTTPException(status_code=404, detail="财报库不存在")
    return FileResponse(DB_FINANCE_PATH, filename="stock_finance.db", media_type="application/octet-stream")

# ------------------------------------------------------------------------------
# 分红送配数据 API 路由接口
# ------------------------------------------------------------------------------

@app.get("/api/dividend/status")
def get_dividend_status():
    """
    分红库状态探活接口。
    逻辑同上，同时读取打卡时间与实质变动日期。
    """
    if not os.path.exists(DB_DIVIDEND_PATH):
        # 如果底层数据库不存在，返回错误状态
        return {"status": "error", "message": "云端分红库未建立"}
        
    try:
        # 连接分红数据库
        conn = sqlite3.connect(DB_DIVIDEND_PATH, timeout=10.0)
        cursor = conn.cursor()
        
        # 提取系统正常跑完的打卡时间
        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_success_time'")
        row_success = cursor.fetchone()
        
        # 提取数据真正发生物理变动的日期戳
        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_mutation_date'")
        row_mutation = cursor.fetchone()
        
        # 安全断开连接
        conn.close()
        
        # 组装并向客户端返回包含双时间戳的 JSON 数据包
        return {
            "status": "success", 
            "last_update": row_success[0] if row_success else "1900-01-01 00:00:00",
            "last_mutation_date": row_mutation[0] if row_mutation else "1900-01-01"
        }
            
    except Exception as e:
        # 容灾处理：将异常原因装入标准 JSON 格式返回
        return {"status": "error", "message": f"分红状态查询异常: {str(e)}"}
    
@app.get("/api/dividend/download")
def download_dividend_db():
    """
    分红库物理下载接口。
    """
    if not os.path.exists(DB_DIVIDEND_PATH):
        raise HTTPException(status_code=404, detail="分红库不存在")
    return FileResponse(DB_DIVIDEND_PATH, filename="stock_dividend.db", media_type="application/octet-stream")

# ------------------------------------------------------------------------------
# 服务入口
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    # 启动 Uvicorn 服务器，监听所有公网请求，端口配置为 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)