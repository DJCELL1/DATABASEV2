from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from cin7_sync import Cin7Sync
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Cin7 Sync Service")


class SyncResponse(BaseModel):
    status: str
    message: str


class SyncStatus(BaseModel):
    last_sync: str | None
    total_records: int


@app.get("/")
def health_check():
    return {"status": "healthy", "service": "cin7-sync"}


@app.post("/sync", response_model=SyncResponse)
def trigger_sync(background_tasks: BackgroundTasks, full: bool = False, limit: int | None = None):
    """
    Trigger a sync from Cin7 to PostgreSQL.
    
    - **full=false** (default): Only sync products modified since last sync
    - **full=true**: Sync all products
    - **limit**: Optional limit on number of SKUs to sync (for testing, e.g. limit=100)
    """
    def run_sync():
        try:
            syncer = Cin7Sync()
            syncer.sync(full_sync=full, limit=limit)
        except Exception as e:
            logger.error(f"Background sync failed: {e}")
    
    background_tasks.add_task(run_sync)
    
    sync_type = "full" if full else "incremental"
    limit_msg = f" (limit: {limit})" if limit else ""
    return SyncResponse(
        status="started",
        message=f"Started {sync_type} sync in background{limit_msg}"
    )


@app.post("/sync/blocking", response_model=SyncResponse)
def trigger_sync_blocking(full: bool = False, limit: int | None = None):
    """
    Trigger a sync and wait for completion (blocking).
    Use /sync for non-blocking background sync.
    
    - **limit**: Optional limit on number of SKUs to sync (for testing, e.g. limit=100)
    """
    try:
        syncer = Cin7Sync()
        syncer.sync(full_sync=full, limit=limit)
        return SyncResponse(
            status="completed",
            message="Sync completed successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status", response_model=SyncStatus)
def get_sync_status():
    """Get the last sync date and total records."""
    try:
        syncer = Cin7Sync()
        syncer.init_database()
        
        conn = syncer.get_db_connection()
        cur = conn.cursor()
        
        # Get last sync date
        cur.execute('''
            SELECT last_sync_date FROM cin7_sync_log 
            WHERE sync_type = 'product_options' AND status = 'success'
            ORDER BY created_at DESC LIMIT 1
        ''')
        last_sync_result = cur.fetchone()
        
        # Get total records
        cur.execute('SELECT COUNT(*) FROM cin7_sku_suppliers')
        count_result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return SyncStatus(
            last_sync=last_sync_result[0].isoformat() if last_sync_result and last_sync_result[0] else None,
            total_records=count_result[0] if count_result else 0
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/skus")
def get_skus(
    supplier_id: int | None = None,
    limit: int = 100,
    offset: int = 0
):
    """
    Query SKUs from the database.
    
    - **supplier_id**: Filter by supplier ID
    - **limit**: Max records to return (default 100)
    - **offset**: Pagination offset
    """
    try:
        syncer = Cin7Sync()
        conn = syncer.get_db_connection()
        cur = conn.cursor()
        
        query = '''
            SELECT sku, sku_id, supplier_code, supplier_name, supplier_id,
                   product_id, product_name, stock_available, stock_on_hand
            FROM cin7_sku_suppliers
        '''
        params = []
        
        if supplier_id:
            query += ' WHERE supplier_id = %s'
            params.append(supplier_id)
        
        query += ' ORDER BY sku LIMIT %s OFFSET %s'
        params.extend([limit, offset])
        
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {"data": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
