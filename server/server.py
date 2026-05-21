# ==============================================================================
# 模块名称: server/server.py
# 代码功能: 云端数据中心 API 服务端。
# 架构设计: 基于 FastAPI 框架构建异步 HTTP 接口，提供数据库高水位线查询及增量数据下发。
# ==============================================================================

import os
import sys
import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import FileResponse

# 实例化 FastAPI 核心应用对象
app = FastAPI(title="量化数据同步服务端", version="1.0")

# 设定服务端数据库文件的相对路径
# 部署时，需确保 server 文件夹同级存在 data 文件夹及对应的 .db 文件
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)  # 把项目根目录加入 Python 搜索路径

from utils.config import DB_HISTORY_PATH

# ------------------------------------------------------------------------------
# Pydantic 数据模型定义
# 用于严格校验客户端通过 POST 请求发送的 JSON 数据格式
# ------------------------------------------------------------------------------
class SyncRequest(BaseModel):
    client_latest_date: str  # 客户端本地数据库的最新日期，格式要求为 YYYY-MM-DD
    limit: int = 100000  # 强行限制单次最大下发条数，防止撑爆内存
    offset: int = 0      # 分页偏移量
    
@app.post("/api/kline/sync")
def get_incremental_data(request: SyncRequest):
    """
    接口功能：根据客户端提供的基准日期，分页提取增量 K 线数据。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")

    # 【修复 1】服务端必须同样开启 WAL 模式和超时等待，否则会被 auto_fetcher 锁死
    conn = sqlite3.connect(DB_HISTORY_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    try:
        cursor = conn.cursor()
        # 【修复 2】先查出总数，发给客户端用于计算进度条
        cursor.execute("SELECT COUNT(*) FROM history_kline WHERE 日期 > ?", (request.client_latest_date,))
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            return {"status": "up_to_date", "data": [], "has_more": False, "total": 0}
        
        # 【修复 3】SQL 语句必须加上 LIMIT 和 OFFSET，实现真正的内存保护流控！
        query = """
            SELECT * FROM history_kline 
            WHERE 日期 > ? 
            ORDER BY 日期 ASC, 代码 ASC 
            LIMIT ? OFFSET ?
        """
        df_incremental = pd.read_sql(query, conn, params=(
            request.client_latest_date, request.limit, request.offset
        ))
        
        records = df_incremental.to_dict(orient="records")
        
        # 判断是否还有剩余数据未发送
        has_more = (request.offset + len(records)) < total_count
        
        return {
            "status": "success",
            "data": records,
            "has_more": has_more,
            "next_offset": request.offset + len(records),
            "total": total_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务端数据提取异常: {str(e)}")
    finally:
        conn.close()

# 顺手把上面的 watermark 接口也加上 WAL，防止查询水位时被锁死
@app.get("/api/kline/watermark")
def get_server_latest_date():
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
# ------------------------------------------------------------------------------
# API 路由接口定义
# ------------------------------------------------------------------------------

@app.get("/api/kline/watermark")
def get_server_latest_date():
    """
    接口功能：查询服务端 K 线数据库的绝对最大日期（高水位线）。
    调用方式：GET /api/kline/watermark
    返回值：包含最新日期字符串的 JSON 对象。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        # 若服务端物理文件缺失，返回 500 内部服务器错误状态码
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")
    
    conn = sqlite3.connect(DB_HISTORY_PATH)
    try:
        # 执行聚合查询提取最大日期
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
    接口功能：根据客户端提供的基准日期，提取大于该日期的所有增量 K 线数据。
    调用方式：POST /api/kline/sync
    参数：JSON 格式的 SyncRequest 对象。
    返回值：包含增量数据记录的 JSON 数组。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=500, detail="服务端数据库文件未找到。")

    target_date = request.client_latest_date
    conn = sqlite3.connect(DB_HISTORY_PATH)
    
    try:
        # 使用参数化查询防止 SQL 注入，提取大于客户端日期的所有记录
        query = "SELECT * FROM history_kline WHERE 日期 > ? ORDER BY 日期 ASC"
        df_incremental = pd.read_sql(query, conn, params=(target_date,))
        
        # 判断是否有增量数据
        if df_incremental.empty:
            return {"status": "up_to_date", "data": [], "count": 0}
        
        # 将 DataFrame 序列化为字典列表格式，以便 FastAPI 自动转换为标准 JSON 响应
        records = df_incremental.to_dict(orient="records")
        
        return {
            "status": "success", 
            "data": records, 
            "count": len(records)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据读取序列化异常: {str(e)}")
    finally:
        conn.close()
        
# 2.全量文件下载接口
@app.get("/api/kline/download")
def download_full_db():
    """
    冷启动专用接口：直接以二进制流形式下发整个 SQLite 数据库文件。
    极度节省服务器内存，支持超大文件传输。
    """
    if not os.path.exists(DB_HISTORY_PATH):
        raise HTTPException(status_code=404, detail="服务端底层数据库不存在！")
    
    # FileResponse 会自动处理大文件的分块读取，绝不会撑爆 2G 内存
    return FileResponse(
        path=DB_HISTORY_PATH, 
        filename="history_kline.db",
        media_type="application/octet-stream"
    )

# 若直接通过 python server.py 启动脚本时的执行入口
if __name__ == "__main__":
    import uvicorn
    # 启动 ASGI 服务器，监听所有网络接口的 8000 端口
    uvicorn.run(app, host="0.0.0.0", port=8000)