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
    基于客户端字典的高效增量切片下发接口。
    对比客户端与服务端的日期差异，返回缺失的 K 线数据记录集。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")

    conn = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    try:
        watermarks = request.watermark_dict
        
        if not watermarks:
            return {"status": "success", "data": [], "message": "客户端副表为空，请走冷启动全量下载。"}
            
        # 计算客户端全市场中最旧的日期，以减少 SQL 初次查询的数据量
        min_date = min(watermarks.values())
        
        query = "SELECT * FROM history_kline WHERE 日期 > ?"
        df = pd.read_sql(query, conn, params=(min_date,))
        
        if df.empty:
            return {"status": "up_to_date", "data": []}
            
        # 映射客户端每只股票的最大日期，并过滤出服务端多出的新数据
        df['client_max_date'] = df['代码'].map(watermarks).fillna('2000-01-01')
        df_sliced = df[df['日期'] > df['client_max_date']]
        
        if df_sliced.empty:
            return {"status": "up_to_date", "data": []}
            
        df_sliced = df_sliced.drop(columns=['client_max_date'])
        records = df_sliced.to_dict(orient="records")
        
        return {
            "status": "success",
            "data": records,
            "total_synced": len(records)
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
    读取系统元数据表（system_info）中绑定的最后一次成功写入时间戳。
    """
    if not os.path.exists(DB_FINANCE_PATH):
        return {"status": "error", "message": "云端财报库未建立"}
        
    try:
        conn = sqlite3.connect(DB_FINANCE_PATH, timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_success_time'")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {"status": "success", "last_update": row[0]}
        else:
            return {"status": "error", "message": "元数据表中无时间戳记录，判定为未完整初始化"}
            
    except Exception as e:
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
    读取系统元数据表（system_info）中绑定的最后一次成功写入时间戳。
    """
    if not os.path.exists(DB_DIVIDEND_PATH):
        return {"status": "error", "message": "云端分红库未建立"}
        
    try:
        conn = sqlite3.connect(DB_DIVIDEND_PATH, timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("SELECT config_value FROM system_info WHERE config_key = 'last_success_time'")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {"status": "success", "last_update": row[0]}
        else:
            return {"status": "error", "message": "元数据表中无时间戳记录，判定为未完整初始化"}
            
    except Exception as e:
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