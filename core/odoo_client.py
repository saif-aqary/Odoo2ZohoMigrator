# core/odoo_client.py

import xmlrpc.client
from typing import List, Dict, Any
from tqdm import tqdm
from utils.logger import setup_logger
import time

class OdooClient:
    def __init__(self, config: Dict[str, str]):
        self.logger = setup_logger(__name__)
        self.config = config
        self.uid = None
        self.models = None
        self.connect()

    def connect(self):
        """Connect to Odoo using XML-RPC"""
        try:
            common = xmlrpc.client.ServerProxy(f'{self.config["url"]}/xmlrpc/2/common')
            self.uid = common.authenticate(
                self.config['db'],
                self.config['username'],
                self.config['password'],
                {}
            )
            self.models = xmlrpc.client.ServerProxy(f'{self.config["url"]}/xmlrpc/2/object')
            self.logger.info("Successfully connected to Odoo")
        except Exception as e:
            self.logger.error(f"Failed to connect to Odoo: {str(e)}")
            raise

    def fetch_records(self, model: str, fields: List[str] = None, domain: List = None, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch records from Odoo with improved batching and progress tracking"""
        try:
            if domain is None:
                domain = []
            if fields is None:
                fields = []

            # Get total count first
            total_count = self.models.execute_kw(
                self.config['db'], self.uid, self.config['password'],
                model, 'search_count', [domain]
            )
            
            self.logger.info(f"Total records to fetch: {total_count}")
            
            # Calculate optimal batch size based on total records
            optimal_batch = min(batch_size, 500)  # Cap at 500 to prevent timeout
            
            all_records = []
            offset = 0
            
            with tqdm(total=total_count, desc=f"Fetching {model} records", unit="records") as pbar:
                while offset < total_count:
                    try:
                        # Fetch record IDs first (faster than direct search_read)
                        record_ids = self.models.execute_kw(
                            self.config['db'], self.uid, self.config['password'],
                            model, 'search',
                            [domain],
                            {
                                'offset': offset,
                                'limit': optimal_batch,
                                'order': 'id'
                            }
                        )
                        
                        if not record_ids:
                            break
                        
                        # Then fetch actual records using those IDs
                        records = self.models.execute_kw(
                            self.config['db'], self.uid, self.config['password'],
                            model, 'read',
                            [record_ids],
                            {'fields': fields}
                        )
                        
                        if records:
                            all_records.extend(records)
                            current_batch = len(records)
                            offset += current_batch
                            pbar.update(current_batch)
                            
                            # Log progress periodically
                            if offset % 1000 == 0:
                                self.logger.info(f"Fetched {offset}/{total_count} records")
                                
                            # Add a small delay to prevent overwhelming the server
                            time.sleep(0.1)
                        else:
                            break
                            
                    except Exception as batch_error:
                        self.logger.error(f"Error fetching batch at offset {offset}: {str(batch_error)}")
                        # Reduce batch size on error and retry
                        optimal_batch = max(int(optimal_batch/2), 50)
                        self.logger.info(f"Reduced batch size to {optimal_batch}")
                        continue

            self.logger.info(f"Successfully fetched {len(all_records)} records from Odoo")
            return all_records

        except Exception as e:
            self.logger.error(f"Error in fetch_records: {str(e)}")
            raise