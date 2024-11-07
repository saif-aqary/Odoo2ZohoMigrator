# main.py

import logging
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from typing import List, Dict, Any, Optional
from tqdm import tqdm
import csv
from datetime import datetime

from config.settings import ODOO_CONFIG, ZOHO_CONFIG, BATCH_SIZE, RATE_LIMIT_DELAY
from core.odoo_client import OdooClient
from core.zoho_client import ZohoClient
from core.data_mapper import ContactMapper, LeadMapper, PropertyMapper
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
        self.property_mapper = PropertyMapper()
        self.max_workers = max_workers
        self.stop_event = Event()
        
        # Statistics
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.start_time = None
        self.total_records = 0

    def log_progress(self, record_type: str):
        """Log current progress statistics"""
        if not self.start_time:
            return
            
        elapsed_time = time.time() - self.start_time
        rate = self.success_count / elapsed_time if elapsed_time > 0 else 0
        
        self.logger.info(
            f"\nProgress Report ({record_type}):\n"
            f"Processed: {self.processed_count}/{self.total_records} "
            f"({(self.processed_count/self.total_records*100):.2f}%)\n"
            f"Successful: {self.success_count}\n"
            f"Errors: {self.error_count}\n"
            f"Skipped: {self.skipped_count}\n"
            f"Rate: {rate:.2f} records/second"
        )

    def reset_statistics(self):
        """Reset migration statistics"""
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.start_time = None
        self.total_records = 0

    def find_contact_id(self, lead: Dict[str, Any], contact_map: Dict[str, str]) -> Optional[str]:
        """Find corresponding Zoho contact ID for a lead"""
        if lead.get('mobile') and lead['mobile'] in contact_map:
            return contact_map[lead['mobile']]
        if lead.get('email_from') and lead['email_from'] in contact_map:
            return contact_map[lead['email_from']]
        return None

    def process_lead_batch(self, batch: List[Dict[str, Any]], contact_map: Dict[str, str]) -> List[Dict[str, Any]]:
        results = []
        for lead in batch:
            if self.stop_event.is_set():
                break
                
            try:
                contact_id = self.find_contact_id(lead, contact_map)
                zoho_lead = self.lead_mapper.map_lead(lead, contact_id)
                
                if not zoho_lead:
                    self.logger.debug(f"Lead mapping failed, skipping lead: {lead.get('name')}")
                    self.skipped_count += 1
                    continue
                
                result = self.zoho_client.create_record('Leads', zoho_lead)
                
                if result and result.get('data', [{}])[0].get('status') == 'success':
                    self.success_count += 1
                    self.logger.info(f"Successfully migrated lead: {lead.get('name')}")
                else:
                    self.error_count += 1
                    self.logger.error(f"Failed to migrate lead {lead.get('name')}: {result}")
                    results.append({'success': False, 'name': lead.get('name'), 'error': result})
                
                self.processed_count += 1
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.error_count += 1
                self.logger.error(f"Error processing lead {lead.get('name')}: {str(e)}")
                results.append({'success': False, 'name': lead.get('name'), 'error': str(e)})
                self.processed_count += 1
        
        return results

    def export_properties_to_csv(self, properties: List[Dict[str, Any]]) -> str:
        """Export mapped properties to CSV file"""
        if not properties:
            self.logger.warning("No properties to export")
            return ""

        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'property_export_{timestamp}.csv'
            
            fieldnames = list(properties[0].keys())
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for prop in tqdm(properties, desc="Writing to CSV"):
                    writer.writerow(prop)
                    
            self.logger.info(f"Successfully exported {len(properties)} properties to {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {str(e)}")
            raise

    def migrate_properties(self):
        """Property migration process"""
        self.logger.info("Starting property migration")
        self.reset_statistics()
        self.start_time = time.time()
        
        try:
            # Define all fields we need from Odoo
            property_fields = [
                'id', 'name', 'property_code', 'owner_id', 'create_date', 'write_date',
                'ownership_type', 'type_id', 'property_community_id', 'property_sub_community_id',
                'property_type', 'property_overview', 'ref_no', 'country_id', 'state_id',
                'city_id', 'builtup_area', 'plot_area', 'handover_date', 'off_plan_property',
                'maintanence_fee_per_sq_ft', 'facilities_ids', 'latitude', 'longitude',
                'gym', 'beach', 'medical_center', 'schools', 'shopping_malls', 'restaurants',
                'marina', 'golf_course'
            ]
            
            # Add ownership_type to domain to filter only freehold/leashold properties
            domain = [('ownership_type', 'in', ['freehold', 'leashold'])]
            
            properties = self.odoo_client.fetch_records(
                'property.master',  # Updated model name
                fields=property_fields,
                domain=domain,
                batch_size=BATCH_SIZE
            )
            
            self.total_records = len(properties)
            self.logger.info(f"Found {self.total_records} freehold/leasehold properties to process")
            
            mapped_properties = []
            with tqdm(total=self.total_records, desc="Mapping properties") as pbar:
                for prop in properties:
                    if self.stop_event.is_set():
                        break
                        
                    try:
                        mapped_property = self.property_mapper.map_property(prop)
                        if mapped_property:
                            mapped_properties.append(mapped_property)
                            self.success_count += 1
                        else:
                            self.skipped_count += 1
                            
                        self.processed_count += 1
                        
                        if self.processed_count % 100 == 0:
                            self.log_progress("Properties")
                            
                    except Exception as e:
                        self.error_count += 1
                        self.logger.error(f"Error mapping property {prop.get('name')}: {str(e)}")
                        
                    pbar.update(1)
            
            if mapped_properties:
                filename = self.export_properties_to_csv(mapped_properties)
                self.logger.info(f"Property migration completed. CSV file created: {filename}")
                self.logger.info(f"Total freehold/leasehold properties exported: {len(mapped_properties)}")
            else:
                self.logger.warning("No properties were successfully mapped")
                
        except Exception as e:
            self.logger.error(f"Property migration failed: {str(e)}")
            raise

    def migrate_leads(self):
        """Lead migration process"""
        self.logger.info("Starting lead migration")
        self.reset_statistics()
        self.start_time = time.time()
        
        try:
            contact_map = self.zoho_client.get_contact_map()
            
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
            
            self.total_records = len(leads)
            self.logger.info(f"Found {self.total_records} leads to process")
            
            batches = [leads[i:i + BATCH_SIZE] 
                      for i in range(0, len(leads), BATCH_SIZE)]
            
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
                                self.log_progress("Leads")
                                
                        except Exception as e:
                            self.logger.error(f"Batch processing failed: {str(e)}")

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
        
        # Migrate properties first
        migration_manager.migrate_properties()
        
        # Then migrate leads
       # migration_manager.migrate_leads()
        
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