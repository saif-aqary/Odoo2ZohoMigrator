# main.py

import logging
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from typing import List, Dict, Any, Optional
from tqdm import tqdm

from config.settings import ODOO_CONFIG, ZOHO_CONFIG, BATCH_SIZE, RATE_LIMIT_DELAY
from core.odoo_client import OdooClient
from core.zoho_client import ZohoClient
from core.data_mapper import ContactMapper, LeadMapper
from utils.logger import setup_logger

class MigrationManager:
    def __init__(self, max_workers: int = 4):
        self.logger = setup_logger(__name__)
        self.odoo_client = OdooClient(ODOO_CONFIG)
        
        try:
            self.zoho_client = ZohoClient(ZOHO_CONFIG)
        except Exception as e:
            self.logger.error(f"Failed to initialize Zoho client: {str(e)}")
            raise
            
        self.contact_mapper = ContactMapper()
        self.lead_mapper = LeadMapper()
        self.max_workers = max_workers
        self.stop_event = Event()
        
        # Statistics
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.start_time = None
        self.total_leads = 0

    def log_progress(self):
        """Log current progress statistics"""
        if not self.start_time:
            return
            
        elapsed_time = time.time() - self.start_time
        rate = self.success_count / elapsed_time if elapsed_time > 0 else 0
        
        self.logger.info(
            f"\nProgress Report:\n"
            f"Processed: {self.processed_count}/{self.total_leads} "
            f"({(self.processed_count/self.total_leads*100):.2f}%)\n"
            f"Successful: {self.success_count}\n"
            f"Errors: {self.error_count}\n"
            f"Skipped: {self.skipped_count}\n"
            f"Rate: {rate:.2f} records/second"
        )

    def find_contact_id(self, lead: Dict[str, Any], contact_map: Dict[str, str]) -> Optional[str]:
        """Find corresponding Zoho contact ID for a lead"""
        # Try to match by mobile number
        if lead.get('mobile') and lead['mobile'] in contact_map:
            return contact_map[lead['mobile']]
            
        # Try to match by email
        if lead.get('email_from') and lead['email_from'] in contact_map:
            return contact_map[lead['email_from']]
            
        return None

    def process_lead_batch(self, batch: List[Dict[str, Any]], contact_map: Dict[str, str]) -> List[Dict[str, Any]]:
        results = []
        for lead in batch:
            if self.stop_event.is_set():
                break
                
            try:
                # Find corresponding contact
                contact_id = self.find_contact_id(lead, contact_map)
                
                # Map lead data
                zoho_lead = self.lead_mapper.map_lead(lead, contact_id)
                if not zoho_lead:
                    self.logger.debug(f"Lead mapping failed, skipping lead: {lead.get('name')}")
                    self.skipped_count += 1
                    continue
                
                self.logger.debug(f"Attempting to create lead in Zoho: {zoho_lead}")
                result = self.zoho_client.create_record('Leads', zoho_lead)
                
                if result and result.get('data', [{}])[0].get('status') == 'success':
                    self.success_count += 1
                    self.logger.info(f"Successfully migrated lead: {lead.get('name')}")
                else:
                    self.error_count += 1
                    error_msg = f"Failed to migrate lead {lead.get('name')}: {result}"
                    self.logger.error(error_msg)
                    results.append({
                        'success': False,
                        'name': lead.get('name'),
                        'error': result
                    })
                
                self.processed_count += 1
                
                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.error_count += 1
                error_msg = f"Error processing lead {lead.get('name')}: {str(e)}"
                self.logger.error(error_msg)
                self.logger.debug(f"Lead data: {lead}")
                results.append({
                    'success': False,
                    'name': lead.get('name'),
                    'error': str(e)
                })
                self.processed_count += 1

    def migrate_leads(self):
        """Main lead migration process"""
        self.logger.info("Starting lead migration")
        self.start_time = time.time()
        
        try:
            # Get contact mapping (mobile/email to Zoho ID)
            contact_map = self.zoho_client.get_contact_map()
            
            # Fetch all leads
            lead_fields = [
                'name', 'partner_name', 'contact_name', 'email_from',
                'phone', 'mobile', 'description', 'stage_id', 'source_id',
                'expected_revenue', 'probability', 'partner_id'
            ]
            
            leads = self.odoo_client.fetch_records(
                'crm.lead', 
                fields=lead_fields, 
                batch_size=BATCH_SIZE
            )
            
            self.total_leads = len(leads)
            self.logger.info(f"Found {self.total_leads} leads to process")
            
            # Create batches
            batches = [leads[i:i + BATCH_SIZE] 
                      for i in range(0, len(leads), BATCH_SIZE)]
            
            # Process batches with progress bar
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                self.logger.info(f"Starting migration with {self.max_workers} workers")
                futures = []
                
                for batch in batches:
                    future = executor.submit(
                        self.process_lead_batch, 
                        batch,
                        contact_map
                    )
                    futures.append(future)
                
                with tqdm(total=len(futures), desc="Processing lead batches") as pbar:
                    for future in as_completed(futures):
                        if self.stop_event.is_set():
                            break
                        try:
                            future.result()
                            pbar.update(1)
                            
                            if pbar.n % 10 == 0:
                                self.log_progress()
                                
                        except Exception as e:
                            self.logger.error(f"Batch processing failed: {str(e)}")

            # Final summary
            end_time = time.time()
            duration = end_time - self.start_time
            
            self.logger.info("\nLead Migration Summary:")
            self.logger.info(f"Total leads processed: {self.processed_count}")
            self.logger.info(f"Successfully migrated: {self.success_count}")
            self.logger.info(f"Failed migrations: {self.error_count}")
            self.logger.info(f"Skipped leads: {self.skipped_count}")
            self.logger.info(f"Total time: {duration:.2f} seconds")
            self.logger.info(f"Average rate: {self.success_count/duration:.2f} leads/second")
            
        except Exception as e:
            self.logger.error(f"Lead migration failed: {str(e)}")
            raise

def main():
    logger = setup_logger(__name__)
    
    try:
        max_workers = max(1, multiprocessing.cpu_count() // 2)
        logger.info(f"Starting migration with {max_workers} workers")
        
        migration_manager = MigrationManager(max_workers=max_workers)
        migration_manager.migrate_leads()
        
    except KeyboardInterrupt:
        logger.info("Migration stopped by user")
        if 'migration_manager' in locals():
            migration_manager.stop_event.set()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        logger.info("Migration process completed")

if __name__ == "__main__":
    main()