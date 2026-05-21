# ==============================================================================
# 模块名称: server/server.py
# 代码功能: 云端数据中心 API 服务端。
# 架构设计: 基于 FastAPI 框架构建异步 HTTP 接口，提供数据库高水位线查询及精准增量切片下发。
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

# 设定服务端数据库文件的相对路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)  

from utils.config import DB_HISTORY_PATH

# ------------------------------------------------------------------------------
# Pydantic 数据模型定义
# ------------------------------------------------------------------------------
class SyncRequest(BaseModel):
    # 终极进化：接收客户端发来的完整字典 {"股票代码": "最新日期"}
    watermark_dict: Dict[str, str]  

# ------------------------------------------------------------------------------
# API 路由接口定义
# ------------------------------------------------------------------------------

@app.get("/api/kline/watermark")
def get_server_latest_date():
    """
    查询服务端 K 线数据库的绝对最大日期（高水位线），开启 WAL 模式防锁死。
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
    终极进化版：基于字典的精确增量切片下发
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")

    conn = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    try:
        watermarks = request.watermark_dict
        
        if not watermarks:
            return {"status": "success", "data": [], "message": "客户端副表为空，请走冷启动全量下载。"}
            
        min_date = min(watermarks.values())
        
        query = "SELECT * FROM history_kline WHERE 日期 > ?"
        df = pd.read_sql(query, conn, params=(min_date,))
        
        if df.empty:
            return {"status": "up_to_date", "data": []}
            
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
    冷启动专用接口：直接以二进制流形式下发整个 SQLite 数据库文件。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=404, detail="服务端底层数据库不存在！")
    
    return FileResponse(
        path=DB_HISTORY_PATH, 
        filename="history_kline.db",
        media_type="application/octet-stream"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)