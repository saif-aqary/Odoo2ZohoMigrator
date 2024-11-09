# # core/odoo_client.py

# import xmlrpc.client
# from typing import List, Dict, Any
# from tqdm import tqdm
# from utils.logger import setup_logger
# import time

# class OdooClient:
#     def __init__(self, config: Dict[str, str]):
#         self.logger = setup_logger(__name__)
#         self.config = config
#         self.uid = None
#         self.models = None
#         self.connect()

#     def connect(self):
#         """Connect to Odoo using XML-RPC"""
#         try:
#             common = xmlrpc.client.ServerProxy(f'{self.config["url"]}/xmlrpc/2/common')
#             self.uid = common.authenticate(
#                 self.config['db'],
#                 self.config['username'],
#                 self.config['password'],
#                 {}
#             )
#             self.models = xmlrpc.client.ServerProxy(f'{self.config["url"]}/xmlrpc/2/object')
#             self.logger.info("Successfully connected to Odoo")
#         except Exception as e:
#             self.logger.error(f"Failed to connect to Odoo: {str(e)}")
#             raise

#     def fetch_records(self, model: str, fields: List[str] = None, domain: List = None, batch_size: int = 100) -> List[Dict[str, Any]]:
#         """Fetch records from Odoo with improved batching and progress tracking"""
#         try:
#             if domain is None:
#                 domain = []
#             if fields is None:
#                 fields = []

#             # Get total count first
#             total_count = self.models.execute_kw(
#                 self.config['db'], self.uid, self.config['password'],
#                 model, 'search_count', [domain]
#             )
            
#             self.logger.info(f"Total records to fetch: {total_count}")
            
#             # Calculate optimal batch size based on total records
#             optimal_batch = min(batch_size, 500)  # Cap at 500 to prevent timeout
            
#             all_records = []
#             offset = 0
            
#             with tqdm(total=total_count, desc=f"Fetching {model} records", unit="records") as pbar:
#                 while offset < total_count:
#                     try:
#                         # Fetch record IDs first (faster than direct search_read)
#                         record_ids = self.models.execute_kw(
#                             self.config['db'], self.uid, self.config['password'],
#                             model, 'search',
#                             [domain],
#                             {
#                                 'offset': offset,
#                                 'limit': optimal_batch,
#                                 'order': 'id'
#                             }
#                         )
                        
#                         if not record_ids:
#                             break
                        
#                         # Then fetch actual records using those IDs
#                         records = self.models.execute_kw(
#                             self.config['db'], self.uid, self.config['password'],
#                             model, 'read',
#                             [record_ids],
#                             {'fields': fields}
#                         )
                        
#                         if records:
#                             all_records.extend(records)
#                             current_batch = len(records)
#                             offset += current_batch
#                             pbar.update(current_batch)
                            
#                             # Log progress periodically
#                             if offset % 1000 == 0:
#                                 self.logger.info(f"Fetched {offset}/{total_count} records")
                                
#                             # Add a small delay to prevent overwhelming the server
#                             time.sleep(0.1)
#                         else:
#                             break
                            
#                     except Exception as batch_error:
#                         self.logger.error(f"Error fetching batch at offset {offset}: {str(batch_error)}")
#                         # Reduce batch size on error and retry
#                         optimal_batch = max(int(optimal_batch/2), 50)
#                         self.logger.info(f"Reduced batch size to {optimal_batch}")
#                         continue

#             self.logger.info(f"Successfully fetched {len(all_records)} records from Odoo")
#             return all_records

#         except Exception as e:
#             self.logger.error(f"Error in fetch_records: {str(e)}")
#             raise








import xmlrpc.client
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import time
from tqdm import tqdm
import logging
from collections import deque
import threading

class OdooClient:
    def __init__(self, config: Dict[str, str], max_workers: int = 4, retry_limit: int = 3):
        self.logger = self._setup_logger()
        self.config = config
        self.uid = None
        self.models = None
        self.max_workers = max_workers
        self.retry_limit = retry_limit
        self._connection_lock = threading.Lock()
        self._connection_pool = deque(maxlen=max_workers)
        self.connect()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _create_connection(self):
        """Create a new XML-RPC connection"""
        models = xmlrpc.client.ServerProxy(
            f'{self.config["url"]}/xmlrpc/2/object',
            allow_none=True,
            use_datetime=True,
            use_builtin_types=True
        )
        return models

    def connect(self):
        """Connect to Odoo using XML-RPC with connection pooling"""
        try:
            common = xmlrpc.client.ServerProxy(f'{self.config["url"]}/xmlrpc/2/common')
            self.uid = common.authenticate(
                self.config['db'],
                self.config['username'],
                self.config['password'],
                {}
            )
            
            # Initialize connection pool
            for _ in range(self.max_workers):
                self._connection_pool.append(self._create_connection())
            
            self.models = self._connection_pool[0]  # Keep one connection as default
            self.logger.info("Successfully connected to Odoo and initialized connection pool")
        except Exception as e:
            self.logger.error(f"Failed to connect to Odoo: {str(e)}")
            raise

    def _get_connection(self):
        """Get a connection from the pool"""
        with self._connection_lock:
            if not self._connection_pool:
                return self._create_connection()
            return self._connection_pool.popleft()

    def _return_connection(self, connection):
        """Return a connection to the pool"""
        with self._connection_lock:
            self._connection_pool.append(connection)

    def _fetch_batch(self, model: str, fields: List[str], domain: List, batch_info: dict) -> List[Dict[str, Any]]:
        """Fetch a single batch of records with retry logic"""
        connection = self._get_connection()
        try:
            offset, limit = batch_info['offset'], batch_info['limit']
            
            # Fetch record IDs
            record_ids = connection.execute_kw(
                self.config['db'], self.uid, self.config['password'],
                model, 'search',
                [domain],
                {
                    'offset': offset,
                    'limit': limit,
                    'order': 'id'
                }
            )
            
            if not record_ids:
                return []
            
            # Fetch actual records
            records = connection.execute_kw(
                self.config['db'], self.uid, self.config['password'],
                model, 'read',
                [record_ids],
                {'fields': fields}
            )
            
            return records
        except Exception as e:
            self.logger.warning(f"Error in batch {batch_info['offset']}: {str(e)}")
            raise
        finally:
            self._return_connection(connection)

    def fetch_records(self, model: str, fields: List[str] = None, domain: List = None, 
                     batch_size: int = 200) -> List[Dict[str, Any]]:
        """Fetch records from Odoo using parallel processing"""
        try:
            if domain is None:
                domain = []
            if fields is None:
                fields = []

            # Get total count
            total_count = self.models.execute_kw(
                self.config['db'], self.uid, self.config['password'],
                model, 'search_count', [domain]
            )
            
            if total_count == 0:
                return []

            self.logger.info(f"Total records to fetch: {total_count}")
            
            # Prepare batches
            batches = [
                {'offset': i, 'limit': batch_size}
                for i in range(0, total_count, batch_size)
            ]
            
            all_records = []
            fetch_fn = partial(self._fetch_batch, model, fields, domain)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(fetch_fn, batch) for batch in batches]
                
                with tqdm(total=total_count, desc=f"Fetching {model} records", unit="records") as pbar:
                    for future in as_completed(futures):
                        try:
                            batch_records = future.result()
                            if batch_records:
                                all_records.extend(batch_records)
                                pbar.update(len(batch_records))
                        except Exception as e:
                            self.logger.error(f"Batch processing error: {str(e)}")
                            continue

            self.logger.info(f"Successfully fetched {len(all_records)} records from Odoo")
            return all_records

        except Exception as e:
            self.logger.error(f"Error in fetch_records: {str(e)}")
            raise