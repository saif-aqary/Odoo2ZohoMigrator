# core/odoo_client.py

import xmlrpc.client
from typing import List, Dict, Any
from tqdm import tqdm
from utils.logger import setup_logger

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
        """Fetch records from Odoo with progress bar"""
        try:
            if domain is None:
                domain = []
            if fields is None:
                fields = []

            total_count = self.models.execute_kw(
                self.config['db'], self.uid, self.config['password'],
                model, 'search_count', [domain]
            )

            self.logger.info(f"Total records to fetch: {total_count}")

            all_records = []
            with tqdm(total=total_count, desc="Fetching Odoo records") as pbar:
                offset = 0
                while offset < total_count:
                    records = self.models.execute_kw(
                        self.config['db'], self.uid, self.config['password'],
                        model, 'search_read',
                        [domain],
                        {
                            'fields': fields,
                            'offset': offset,
                            'limit': batch_size
                        }
                    )
                    
                    if not records:
                        break
                        
                    all_records.extend(records)
                    offset += len(records)
                    pbar.update(len(records))

            self.logger.info(f"Successfully fetched {len(all_records)} records from Odoo")
            return all_records

        except Exception as e:
            self.logger.error(f"Error fetching Odoo records: {str(e)}")
            raise