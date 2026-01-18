import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from typing import Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Cin7Sync:
    def __init__(self):
        # Cin7 API credentials from environment variables
        self.cin7_username = os.environ.get('CIN7_API_USERNAME')
        self.cin7_api_key = os.environ.get('CIN7_API_KEY')
        self.cin7_base_url = os.environ.get('CIN7_BASE_URL', 'https://api.cin7.com/api/v1')
        
        # Branch IDs (optional)
        self.branch_hamilton_id = os.environ.get('CIN7_BRANCH_HAMILTON_ID')
        self.branch_avondale_id = os.environ.get('CIN7_BRANCH_AVONDALE_ID')
        
        # PostgreSQL connection - Railway provides DATABASE_URL automatically
        # Or build from individual components
        self.database_url = os.environ.get('DATABASE_URL')
        
        if not self.database_url:
            # Build from individual env vars if DATABASE_URL not provided
            db_host = os.environ.get('DB_HOST')
            db_port = os.environ.get('DB_PORT')
            db_name = os.environ.get('DB_NAME')
            db_user = os.environ.get('DB_USER')
            db_password = os.environ.get('DB_PASSWORD')
            db_sslmode = os.environ.get('DB_SSLMODE', 'require')
            
            if all([db_host, db_port, db_name, db_user, db_password]):
                self.database_url = (
                    f"postgresql://{db_user}:{db_password}"
                    f"@{db_host}:{db_port}/{db_name}"
                    f"?sslmode={db_sslmode}"
                )
        
        if not self.database_url:
            raise ValueError("DATABASE_URL or individual DB_* environment variables required")
        
        if not self.cin7_username or not self.cin7_api_key:
            raise ValueError("CIN7_API_USERNAME and CIN7_API_KEY environment variables required")
        
        # Setup session for Cin7 API
        self.session = requests.Session()
        self.session.auth = (self.cin7_username, self.cin7_api_key)
        self.session.headers.update({'Content-Type': 'application/json'})
        
        # Cache for suppliers to reduce API calls
        self.supplier_cache = {}
    
    def get_db_connection(self):
        """Create a database connection."""
        return psycopg2.connect(self.database_url)
    
    def init_database(self):
        """Create the table and tracking table if they don't exist."""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        # Main SKU-Supplier table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS cin7_sku_suppliers (
                id SERIAL PRIMARY KEY,
                sku VARCHAR(50),
                sku_id INTEGER UNIQUE,
                supplier_code VARCHAR(50),
                supplier_name VARCHAR(250),
                supplier_id INTEGER,
                product_id INTEGER,
                product_name VARCHAR(250),
                barcode VARCHAR(50),
                option1 VARCHAR(50),
                option2 VARCHAR(50),
                option3 VARCHAR(50),
                stock_available DECIMAL,
                stock_on_hand DECIMAL,
                retail_price DECIMAL,
                wholesale_price DECIMAL,
                cin7_created_date TIMESTAMP,
                cin7_modified_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Sync tracking table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS cin7_sync_log (
                id SERIAL PRIMARY KEY,
                sync_type VARCHAR(50),
                last_sync_date TIMESTAMP,
                records_synced INTEGER,
                status VARCHAR(20),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sku ON cin7_sku_suppliers(sku)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_supplier_id ON cin7_sku_suppliers(supplier_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_product_id ON cin7_sku_suppliers(product_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cin7_modified ON cin7_sku_suppliers(cin7_modified_date)')
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized successfully")
    
    def get_last_sync_date(self) -> Optional[datetime]:
        """Get the last successful sync date."""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT last_sync_date FROM cin7_sync_log 
            WHERE sync_type = 'product_options' AND status = 'success'
            ORDER BY created_at DESC LIMIT 1
        ''')
        
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result and result[0]:
            return result[0]
        return None
    
    def log_sync(self, sync_type: str, records_synced: int, status: str, error_message: str = None):
        """Log sync activity."""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        cur.execute('''
            INSERT INTO cin7_sync_log (sync_type, last_sync_date, records_synced, status, error_message)
            VALUES (%s, %s, %s, %s, %s)
        ''', (sync_type, datetime.now(timezone.utc), records_synced, status, error_message))
        
        conn.commit()
        cur.close()
        conn.close()
    
    def fetch_product_options(self, modified_since: Optional[datetime] = None, limit: Optional[int] = None) -> list:
        """Fetch product options from Cin7, optionally filtered by modified date."""
        all_options = []
        page = 1
        rows_per_page = 250  # Max allowed by Cin7
        
        while True:
            params = {
                'page': page,
                'rows': rows_per_page
            }
            
            # Add where clause for incremental sync
            if modified_since:
                where_clause = f"modifiedDate > '{modified_since.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
                params['where'] = where_clause
            
            logger.info(f"Fetching ProductOptions page {page}...")
            
            response = self.session.get(
                f'{self.cin7_base_url}/ProductOptions',
                params=params
            )
            response.raise_for_status()
            
            options = response.json()
            
            if not options:
                break
            
            all_options.extend(options)
            logger.info(f"Fetched {len(options)} product options from page {page}")
            
            # Check if we've hit the limit
            if limit and len(all_options) >= limit:
                all_options = all_options[:limit]
                logger.info(f"Reached limit of {limit} product options")
                break
            
            if len(options) < rows_per_page:
                break
            
            page += 1
        
        logger.info(f"Total product options fetched: {len(all_options)}")
        return all_options
    
    def fetch_product(self, product_id: int) -> Optional[dict]:
        """Fetch a single product to get supplier ID."""
        try:
            response = self.session.get(f'{self.cin7_base_url}/Products/{product_id}')
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching product {product_id}: {e}")
            return None
    
    def fetch_supplier(self, supplier_id: int) -> Optional[dict]:
        """Fetch supplier details from Contacts, with caching."""
        if supplier_id in self.supplier_cache:
            return self.supplier_cache[supplier_id]
        
        try:
            response = self.session.get(f'{self.cin7_base_url}/Contacts/{supplier_id}')
            response.raise_for_status()
            supplier = response.json()
            self.supplier_cache[supplier_id] = supplier
            return supplier
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching supplier {supplier_id}: {e}")
            return None
    
    def fetch_all_suppliers(self):
        """Pre-fetch all suppliers to minimize API calls."""
        logger.info("Pre-fetching all suppliers...")
        page = 1
        rows_per_page = 250
        
        while True:
            params = {
                'page': page,
                'rows': rows_per_page,
                'where': "type = 'Supplier'"
            }
            
            response = self.session.get(
                f'{self.cin7_base_url}/Contacts',
                params=params
            )
            response.raise_for_status()
            
            suppliers = response.json()
            
            if not suppliers:
                break
            
            for supplier in suppliers:
                self.supplier_cache[supplier['id']] = supplier
            
            logger.info(f"Cached {len(suppliers)} suppliers from page {page}")
            
            if len(suppliers) < rows_per_page:
                break
            
            page += 1
        
        logger.info(f"Total suppliers cached: {len(self.supplier_cache)}")
    
    def sync(self, full_sync: bool = False, limit: Optional[int] = None):
        """
        Main sync method.
        
        Args:
            full_sync: If True, sync all records. If False, only sync changes since last sync.
            limit: Optional limit on number of SKUs to sync (for testing)
        """
        logger.info(f"Starting {'full' if full_sync else 'incremental'} sync..." + (f" (limit: {limit})" if limit else ""))
        
        try:
            # Initialize database tables
            self.init_database()
            
            # Get last sync date for incremental sync
            modified_since = None
            if not full_sync:
                modified_since = self.get_last_sync_date()
                if modified_since:
                    logger.info(f"Incremental sync since: {modified_since}")
                else:
                    logger.info("No previous sync found, performing full sync")
            
            # Pre-fetch all suppliers to reduce API calls
            self.fetch_all_suppliers()
            
            # Fetch product options (with optional date filter and limit)
            product_options = self.fetch_product_options(modified_since, limit=limit)
            
            if not product_options:
                logger.info("No new or updated products to sync")
                self.log_sync('product_options', 0, 'success')
                return
            
            # Get unique product IDs to fetch product details
            product_ids = set(opt['productId'] for opt in product_options)
            logger.info(f"Fetching details for {len(product_ids)} unique products...")
            
            # Fetch product details (for supplier ID)
            products_cache = {}
            for product_id in product_ids:
                product = self.fetch_product(product_id)
                if product:
                    products_cache[product_id] = product
            
            # Prepare records for upsert
            records = []
            for opt in product_options:
                product = products_cache.get(opt['productId'], {})
                supplier_id = product.get('supplierId')
                supplier = self.supplier_cache.get(supplier_id, {}) if supplier_id else {}
                
                record = (
                    opt.get('code') or opt.get('productOptionCode'),  # sku
                    opt.get('id'),  # sku_id
                    opt.get('supplierCode'),  # supplier_code
                    supplier.get('company'),  # supplier_name
                    supplier_id,  # supplier_id
                    opt.get('productId'),  # product_id
                    product.get('name'),  # product_name
                    opt.get('barcode') or opt.get('productOptionBarcode'),  # barcode
                    opt.get('option1'),  # option1
                    opt.get('option2'),  # option2
                    opt.get('option3'),  # option3
                    opt.get('stockAvailable'),  # stock_available
                    opt.get('stockOnHand'),  # stock_on_hand
                    opt.get('retailPrice'),  # retail_price
                    opt.get('wholesalePrice'),  # wholesale_price
                    opt.get('createdDate'),  # cin7_created_date
                    opt.get('modifiedDate'),  # cin7_modified_date
                )
                records.append(record)
            
            # Upsert records
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            upsert_query = '''
                INSERT INTO cin7_sku_suppliers (
                    sku, sku_id, supplier_code, supplier_name, supplier_id,
                    product_id, product_name, barcode, option1, option2, option3,
                    stock_available, stock_on_hand, retail_price, wholesale_price,
                    cin7_created_date, cin7_modified_date, updated_at
                ) VALUES %s
                ON CONFLICT (sku_id) DO UPDATE SET
                    sku = EXCLUDED.sku,
                    supplier_code = EXCLUDED.supplier_code,
                    supplier_name = EXCLUDED.supplier_name,
                    supplier_id = EXCLUDED.supplier_id,
                    product_id = EXCLUDED.product_id,
                    product_name = EXCLUDED.product_name,
                    barcode = EXCLUDED.barcode,
                    option1 = EXCLUDED.option1,
                    option2 = EXCLUDED.option2,
                    option3 = EXCLUDED.option3,
                    stock_available = EXCLUDED.stock_available,
                    stock_on_hand = EXCLUDED.stock_on_hand,
                    retail_price = EXCLUDED.retail_price,
                    wholesale_price = EXCLUDED.wholesale_price,
                    cin7_modified_date = EXCLUDED.cin7_modified_date,
                    updated_at = CURRENT_TIMESTAMP
            '''
            
            execute_values(
                cur, 
                upsert_query, 
                records,
                template='(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)'
            )
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Successfully synced {len(records)} records")
            self.log_sync('product_options', len(records), 'success')
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            self.log_sync('product_options', 0, 'failed', str(e))
            raise


# Entry point
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync Cin7 data to PostgreSQL')
    parser.add_argument('--full', action='store_true', help='Perform full sync instead of incremental')
    args = parser.parse_args()
    
    syncer = Cin7Sync()
    syncer.sync(full_sync=args.full)
