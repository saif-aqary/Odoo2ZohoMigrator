# main.py

import logging
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from typing import List, Dict, Any, Optional
import requests
from tqdm import tqdm
import csv
from datetime import datetime

from config.settings import ODOO_CONFIG, ZOHO_CONFIG, BATCH_SIZE, RATE_LIMIT_DELAY
from core.odoo_client import OdooClient
from core.zoho_client import ZohoClient
from core.data_mapper import ContactMapper, LeadMapper, PropertyMapper , UnitMapper
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
        self.unit_mapper = UnitMapper()
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
    def create_owner_contact(self, owner_data: Dict[str, Any]) -> Optional[str]:
        """Create a new contact in Zoho for the property owner"""
        try:
            # Extract owner name
            full_name = owner_data.get('name', '').strip()
            if not full_name:
                self.logger.warning("Cannot create contact without name")
                return None
                
            # Split name into first and last name
            name_parts = full_name.split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else 'Unknown'
            
            # Prepare contact data
            contact_data = {
                'First_Name': first_name,
                'Last_Name': last_name,
                'Email': owner_data.get('email', ''),
                'Phone': owner_data.get('phone', ''),
                'Mobile': owner_data.get('mobile', ''),
                'Contact_Type': 'Property Owner',
                'Description': 'Automatically created during property migration',
                'Source': 'Odoo Migration'
            }
            
            # Add address if available
            if owner_data.get('address'):
                contact_data.update({
                    'Mailing_Street': owner_data.get('address', ''),
                    'Mailing_City': owner_data.get('city', ''),
                    'Mailing_State': owner_data.get('state', ''),
                    'Mailing_Country': owner_data.get('country', '')
                })
            
            # Create contact in Zoho
            result = self.zoho_client.create_record('Contacts', contact_data)
            
            if result and result.get('data', [{}])[0].get('status') == 'success':
                contact_id = result['data'][0]['details']['id']
                self.logger.info(f"Successfully created contact for owner: {full_name}")
                return contact_id
            else:
                self.logger.error(f"Failed to create contact for owner: {full_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating contact: {str(e)}")
            return None

    def get_or_create_owner_contact(self, owner_data: Dict[str, Any], contact_map: Dict[str, str]) -> Optional[str]:
        """Get existing contact ID or create new contact for owner"""
        try:
            # Try to find existing contact by email
            if owner_data.get('email') and owner_data['email'] in contact_map:
                return contact_map[owner_data['email']]
                
            # Try to find existing contact by phone
            if owner_data.get('phone') and owner_data['phone'] in contact_map:
                return contact_map[owner_data['phone']]
                
            # Try to find existing contact by mobile
            if owner_data.get('mobile') and owner_data['mobile'] in contact_map:
                return contact_map[owner_data['mobile']]
                
            # If no existing contact found, create new one
            return self.create_owner_contact(owner_data)
            
        except Exception as e:
            self.logger.error(f"Error getting/creating owner contact: {str(e)}")
            return None

    def process_unit_batch(self, batch: List[Dict[str, Any]], contact_map: Dict[str, str]) -> List[Dict[str, Any]]:
        """Process a batch of units for migration"""
        results = []
        MODULE_NAME = "Properties_Units"  # Confirmed module name
        
        for unit in batch:
            if self.stop_event.is_set():
                break
                
            try:
                # Check if unit already exists
                existing_unit = self.zoho_client.get_existing_unit(unit.get('property_code'))
                is_update = existing_unit is not None
                
                # Extract owner information
                owner_id = unit.get('owner_id', [False, False])
                owner_data = {}
                owner_contact_id = None
                
                if isinstance(owner_id, (list, tuple)) and len(owner_id) > 1:
                    # First check if owner exists in Zoho by Odoo_ID
                    owner_contact = self.zoho_client.get_contact_by_odoo_id(str(owner_id[0]))
                    
                    if owner_contact:
                        owner_contact_id = owner_contact['id']
                    else:
                        # Fetch and create owner if not exists
                        owner_data = self.odoo_client.fetch_records(
                            'res.partner',
                            fields=[
                                'name', 'email', 'phone', 'mobile',
                                'street', 'city', 'state_id', 'country_id'
                            ],
                            domain=[('id', '=', owner_id[0])],
                            batch_size=1
                        )
                        if owner_data:
                            owner_contact_id = self.get_or_create_owner_contact(owner_data[0], contact_map)
                
                # Map unit data based on whether it's an update or new record
                zoho_unit = self.unit_mapper.map_unit(unit, is_update)
                if not zoho_unit:
                    self.logger.debug(f"Unit mapping failed, skipping unit: {unit.get('name')}")
                    self.skipped_count += 1
                    continue
                
                # Add owner reference if available
                if owner_contact_id:
                    zoho_unit['Unit_Owner_Name'] = owner_contact_id
                
                # Add amenities
                amenities = self.unit_mapper.map_amenities(unit)
                if amenities:
                    zoho_unit['Private_Amenities'] = amenities
                
                self.logger.debug(f"Payload for {'update' if is_update else 'create'}: {zoho_unit}")
                
                if is_update:
                    # Update existing record
                    result = self.zoho_client.update_record(
                        MODULE_NAME, 
                        existing_unit['id'], 
                        zoho_unit
                    )
                    operation = "updated"
                else:
                    # Create new record
                    result = self.zoho_client.create_record(MODULE_NAME, zoho_unit)
                    operation = "created"
                
                if result and result.get('data', [{}])[0].get('status') == 'success':
                    self.success_count += 1
                    self.logger.info(f"Successfully {operation} unit: {unit.get('name')}")
                else:
                    self.error_count += 1
                    self.logger.error(f"Failed to {operation} unit {unit.get('name')}: {result}")
                    results.append({
                        'success': False,
                        'name': unit.get('name'),
                        'error': result,
                        'operation': operation
                    })
                
                self.processed_count += 1
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.error_count += 1
                self.logger.error(f"Error processing unit {unit.get('name')}: {str(e)}")
                results.append({
                    'success': False,
                    'name': unit.get('name'),
                    'error': str(e),
                    'operation': 'processing'
                })
                self.processed_count += 1
        
        return results
    
    def process_unit(self, unit: Dict[str, Any]) -> bool:
        """Process a single unit, handling both creation and updates"""
        try:
            # Check if unit already exists
            existing_unit = self.zoho_client.get_existing_unit(unit.get('property_code'))
            
            # Handle owner contact first
            owner_id = unit.get('owner_id', [False, False])
            owner_zoho_id = None
            
            if isinstance(owner_id, (list, tuple)) and len(owner_id) > 1:
                # Check if owner contact exists in Zoho
                owner_contact = self.zoho_client.get_contact_by_odoo_id(str(owner_id[0]))
                if owner_contact:
                    owner_zoho_id = owner_contact['id']
                else:
                    # Create owner contact if needed
                    owner_data = self.odoo_client.fetch_records(
                        'res.partner',
                        fields=['name', 'email', 'phone', 'mobile'],
                        domain=[('id', '=', owner_id[0])],
                        batch_size=1
                    )
                    if owner_data:
                        contact_result = self.create_owner_contact(owner_data[0])
                        if contact_result:
                            owner_zoho_id = contact_result['id']

            if existing_unit:
                # Update existing unit
                unit_data = self.unit_mapper.map_unit(unit, is_update=True)
                if owner_zoho_id:
                    unit_data['Unit_Owner_Name'] = owner_zoho_id
                
                result = self.zoho_client.update_record('CustomModule1', existing_unit['id'], unit_data)
                if result and result.get('data', [{}])[0].get('status') == 'success':
                    self.success_count += 1
                    self.logger.info(f"Successfully updated unit: {unit.get('name')}")
                    return True
            else:
                # Create new unit
                unit_data = self.unit_mapper.map_unit(unit)
                if owner_zoho_id:
                    unit_data['Unit_Owner_Name'] = owner_zoho_id
                
                result = self.zoho_client.create_record('CustomModule1', unit_data)
                if result and result.get('data', [{}])[0].get('status') == 'success':
                    self.success_count += 1
                    self.logger.info(f"Successfully created unit: {unit.get('name')}")
                    return True

            self.error_count += 1
            return False

        except Exception as e:
            self.error_count += 1
            self.logger.error(f"Error processing unit {unit.get('name')}: {str(e)}")
            return False
   

    def migrate_units(self):
        """Unit migration process with automatic contact creation"""
        self.logger.info("Starting unit migration")
        self.reset_statistics()
        self.start_time = time.time()
        
        try:
                # First check available modules
            modules = self.zoho_client.check_available_modules()
            if modules:
                self.logger.info("Found available modules in Zoho CRM")
            else:
                self.logger.warning("Could not verify modules, proceeding with default module name")
            # Get initial contact mapping from Zoho
            self.logger.info("Fetching contact mapping from Zoho...")
            contact_map = self.zoho_client.get_contact_map()
            
            # Define required fields from Odoo
            unit_fields = [
                # Basic fields
                'name', 'property_code', 'unit_number', 'ref_no',
                'state', 'unit_type_id', 'property_community_id',
                'property_sub_community_id',
                
                # Property features
                'bedroom', 'bathroom', 'floor_number', 'balconies',
                'builtup_area', 'plot_area', 'total_area',
                'furnished', 'property_status',
                
                # Location details
                'street', 'city', 'state_id', 'country_id',
                'locality', 'sub_locality', 'latitude', 'longitude',
                
                # Listing details
                'listing_date', 'permit_number', 'property_title',
                'property_title_arabic', 'property_description',
                'property_description_arabic',
                
                # Financial details
                'rent_amount', 'service_charge', 'no_of_cheques',
                'payment_allocated', 'price_per_sqt_foot',
                'maintenance_fee', 'discount_amount',
                
                # Owner/Agent details
                'owner_id', 'agent_name', 'agent_id', 'agent_email', 'agent_phone',
                
                # Additional features
                'amenities_ids', 'features_ids', 'commercial_amenities_ids',
                'web_portal_ids', 'view360_url', 'floor_plan_url'
            ]
            
            # Fetch units from Odoo
            units = self.odoo_client.fetch_records(
                'account.asset.asset',  # The model name for units
                fields=unit_fields,
                batch_size=BATCH_SIZE
            )
            
            self.total_records = len(units)
            self.logger.info(f"Found {self.total_records} units to process")
            
            # Create batches
            batches = [units[i:i + BATCH_SIZE] 
                    for i in range(0, len(units), BATCH_SIZE)]
            
            # Process batches with thread pool
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                self.logger.info(f"Starting migration with {self.max_workers} workers")
                futures = []
                
                for batch in batches:
                    future = executor.submit(
                        self.process_unit_batch, 
                        batch,
                        contact_map
                    )
                    futures.append(future)
                
                # Monitor progress
                with tqdm(total=len(futures), desc="Processing unit batches") as pbar:
                    for future in as_completed(futures):
                        if self.stop_event.is_set():
                            break
                        try:
                            future.result()
                            pbar.update(1)
                            
                            if pbar.n % 10 == 0:
                                self.log_progress("Units")
                                
                        except Exception as e:
                            self.logger.error(f"Batch processing failed: {str(e)}")

            # Final summary
            end_time = time.time()
            duration = end_time - self.start_time
            
            self.logger.info("\nUnit Migration Summary:")
            self.logger.info(f"Total units processed: {self.processed_count}")
            self.logger.info(f"Successfully migrated: {self.success_count}")
            self.logger.info(f"Failed migrations: {self.error_count}")
            self.logger.info(f"Skipped units: {self.skipped_count}")
            self.logger.info(f"Total time: {duration:.2f} seconds")
            self.logger.info(f"Average rate: {self.success_count/duration:.2f} units/second")
            
        except Exception as e:
            self.logger.error(f"Unit migration failed: {str(e)}")
            raise

def main():
    logger = setup_logger(__name__)
    
    try:
        max_workers = max(1, multiprocessing.cpu_count() // 2)
        logger.info(f"Starting migration with {max_workers} workers")
        
        migration_manager = MigrationManager(max_workers=max_workers)
        
        # Migrate properties first
        # migration_manager.migrate_properties()

        # Migrate units
        migration_manager.migrate_units()

        
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