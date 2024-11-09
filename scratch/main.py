from datetime import datetime
import xmlrpc.client
from typing import Dict, List, Any
import time
from tqdm import tqdm
import requests
import time
from typing import Dict, Any, Set, Optional
from threading import Lock
from tqdm import tqdm
import re
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
# from core.data_mapper import ContactMapper




ODOO_CONFIG = {
    'url': 'https://aqarycrm.com',
    'db': 'Finehome_live_07Sep',
    'username': 'admin',
    'password': 'CRMAdmin@AqaryAuh146'
}

ZOHO_CONFIG = {
    'client_id': '1000.KEDXHYCSO6K21ARJSZ31PXM36OFWRT',
    'client_secret': 'e66887d4d8e657152aafba4ae2f1c7af74d6ee90fc',
    'refresh_token': '1000.eeb4c9c93f25ce86f3a006cb9c402e7f.53c451ef13672d796dbadde011319a0d',
    'organization_id': '862006792'
}

# Migration settings
BATCH_SIZE = 200
MAX_WORKERS = 7  # Will be overridden by CPU count in main.py
RATE_LIMIT_DELAY = 0.3  # seconds between API calls
UPDATE_INTERVAL = 5  # seconds between progress updates
MAX_RETRIES = 3



class OdooClient:
    def __init__(self, config: Dict[str, str]):
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
        except Exception as e:
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
                            
                            time.sleep(0.1)
                        else:
                            break
                            
                    except Exception as batch_error:
                        optimal_batch = max(int(optimal_batch/2), 50)
                        continue

            return all_records

        except Exception as e:
            raise






class ZohoClient:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.access_token = None
        self.token_lock = Lock()
        
        # Try different Zoho domains
        self.domains = [
            {
                'base_url': "https://www.zohoapis.com/crm/v2",
                'auth_url': "https://accounts.zoho.com/oauth/v2/token"
            },
            {
                'base_url': "https://www.zohoapis.eu/crm/v2",
                'auth_url': "https://accounts.zoho.eu/oauth/v2/token"
            },
            {
                'base_url': "https://www.zohoapis.com.au/crm/v2",
                'auth_url': "https://accounts.zoho.com.au/oauth/v2/token"
            },
            {
                'base_url': "https://www.zohoapis.in/crm/v2",
                'auth_url': "https://accounts.zoho.in/oauth/v2/token"
            }
        ]
        
        self.current_domain = None
        self.refresh_token()


    def try_refresh_token(self, domain: Dict[str, str]) -> bool:
        """Try to refresh token with a specific domain"""
        try:
            data = {
                'refresh_token': self.config['refresh_token'],
                'client_id': self.config['client_id'],
                'client_secret': self.config['client_secret'],
                'grant_type': 'refresh_token'
            }
            
            
            response = requests.post(domain['auth_url'], data=data)
            
            if response.status_code == 200 and 'access_token' in response.json():
                self.access_token = response.json()['access_token']
                self.current_domain = domain
                return True
                
            return False
            
        except Exception as e:
            return False


    def refresh_token(self):
        """Try to refresh token with all available domains"""
        with self.token_lock:
            for domain in self.domains:
                if self.try_refresh_token(domain):
                    return
                    
            error_msg = "Failed to refresh token with all available domains"
            raise Exception(error_msg)



    def create_record(self, module: str, data: Dict[str, Any], retry_count: int = 0) -> Optional[Dict[str, Any]]:
        """Create a record in Zoho CRM"""
        # try:
        if not self.current_domain:
            raise Exception("No valid Zoho domain found")
            
        url = f"{self.current_domain['base_url']}/{module}"
        headers = {
            'Authorization': f'Zoho-oauthtoken {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {'data': [data]}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 401 and retry_count < 3:
            self.refresh_token()
            return self.create_record(module, data, retry_count + 1)
            
        response_data = response.json()
        return response_data
            


    def get_contact_map(self) -> Dict[str, str]:
        """Fetch all contacts and create a mapping of mobile/email to Zoho contact ID"""
        contact_map = {}
        page = 1
        
        with tqdm(desc="Fetching contacts for mapping", unit="page") as pbar:
            while True:
                try:
                    if not self.current_domain:
                        raise Exception("No valid Zoho domain found")
                        
                    url = f"{self.current_domain['base_url']}/Contacts"
                    headers = {
                        'Authorization': f'Zoho-oauthtoken {self.access_token}',
                    }
                    params = {
                        'page': page,
                        'per_page': 200,
                        'fields': 'id,Mobile,Email'
                    }
                    
                    response = requests.get(url, headers=headers, params=params)
                    if response.status_code == 401:
                        self.refresh_token()
                        continue
                        
                    data = response.json()
                    
                    if not data.get('data'):
                        break
                        
                    for contact in data['data']:
                        if contact.get('Mobile'):
                            contact_map[contact['Mobile']] = contact['id']
                        if contact.get('Email'):
                            contact_map[contact['Email']] = contact['id']
                    
                    page += 1
                    pbar.update(1)
                    
                except Exception as e:
                    break

        return contact_map


    def get_existing_contacts(self) -> Set[str]:
        """Fetch existing contacts from Zoho"""
        existing_contacts = set()
        page = 1
        
        with tqdm(desc="Fetching existing contacts", unit="page") as pbar:
            while True:
                try:
                    if not self.current_domain:
                        raise Exception("No valid Zoho domain found")
                        
                    url = f"{self.current_domain['base_url']}/Contacts"
                    headers = {
                        'Authorization': f'Zoho-oauthtoken {self.access_token}',
                    }
                    params = {
                        'page': page,
                        'per_page': 200,
                        'fields': 'First_Name,Last_Name,Mobile'
                    }
                    
                    response = requests.get(url, headers=headers, params=params)
                    if response.status_code == 401:
                        self.refresh_token()
                        continue
                        
                    data = response.json()
                    
                    if not data.get('data'):
                        break
                        
                    for contact in data['data']:
                        if contact.get('Mobile'):
                            existing_contacts.add(contact['Mobile'])
                    
                    page += 1
                    pbar.update(1)
                    
                except Exception as e:
                    break
                
        return existing_contacts
    
        
    def get_contact_by_odoo_id(self, odoo_id: str) -> Optional[Dict[str, Any]]:
        """Find contact using Odoo_ID field"""
        if not self.current_domain:
            raise Exception("No valid Zoho domain found")
            
        url = f"{self.current_domain['base_url']}/Contacts/search"
        headers = {
            'Authorization': f'Zoho-oauthtoken {self.access_token}',
            'Content-Type': 'application/json'  # Add content type header
        }
        
        params = {
            'criteria': f'(Odoo_ID:equals:{odoo_id})'
        }
        
        # try:
        response = requests.get(url, headers=headers, params=params)
        
        # Log response details for debugging
        
        # Handle different response status codes
        if response.status_code == 401:
            self.refresh_token()
            return self.get_contact_by_odoo_id(odoo_id)
        elif response.status_code == 204:  # No content
            return None
        elif response.status_code != 200:
            return None
            
        # Only try to parse JSON if we have content
        if response.text.strip():
            data = response.json()
            if data.get('data'):
                return data['data']
        
        return None
            
        # except requests.RequestException as e:
        #     return None


    def update_record(self, module: str, record_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update existing record in Zoho CRM"""
        try:
            if not self.current_domain:
                raise Exception("No valid Zoho domain found")

            url = f"{self.current_domain['base_url']}/{module}/{record_id}"
            headers = {
                'Authorization': f'Zoho-oauthtoken {self.access_token}',
                'Content-Type': 'application/json'
            }

            payload = {'data': [data]}
            response = requests.put(url, headers=headers, json=payload)

            if response.status_code == 401:
                self.refresh_token()
                return self.update_record(module, record_id, data)
                
            return response.json()
            
        except Exception as e:
            return None
        
        


class MigrationManager:
    def __init__(self, max_workers: int = 7):
        self.odoo_client = OdooClient(ODOO_CONFIG)
        
        try:
            self.zoho_client = ZohoClient(ZOHO_CONFIG)
        except Exception as e:
            raise
            
        self.max_workers = max_workers
        # self.contact_mapper = ContactMapper()
        self.stop_event = Event()
            
        # Statistics
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.start_time = None
        self.total_records = 0


    def find_contact_id(self, contact: Dict[str, Any], contact_map: Dict[str, str]) -> Optional[str]:
        """Find corresponding Zoho contact ID for a contact"""
        if contact.get('mobile') and contact['mobile'] in contact_map:
            return contact_map[contact['mobile']]
        if contact.get('email_from') and contact['email_from'] in contact_map:
            return contact_map[contact['email_from']]
        return None


    def process_contact_batch(self, batch: List[Dict[str, Any]], contact_map: Dict[str, str]) -> List[Dict[str, Any]]:
        results = []
        for contact in batch:
            if self.stop_event.is_set():
                break
                
            try:
                contact_id = self.find_contact_id(contact, contact_map)
                contact
                # zoho_contact = self.contact_mapper.map_contact(contact, contact_id)
                first_name = contact['name'].strip().split()[0]
                last_name = contact['name'].strip().split()[1]
                
                if contact['phone'] == False:
                    contact['phone'] = ''
                    
                if contact['email'] == False:
                    contact['email'] = ''
                    
                if contact['mobile'] == False:
                    contact['mobile'] = ''
                    
                zoho_contact = {
                    'First_Name': first_name,
                    'Last_Name': last_name,
                    'Phone': contact['phone'],
                    "Email": contact['email'],
                    'Mobile': contact['mobile'],
                    'Odoo_ID': contact['id'],
                    'Lead_Source': 'Odoo Migration',
                    'Contact_Type': 'Imported Contact'
                }
                
                if not zoho_contact:
                    self.skipped_count += 1
                    continue
                
                result = self.zoho_client.create_record('Contacts', zoho_contact)
                print('----------------------')
                print(result)
                print('----------------------')
                
                if result and result.get('data', [{}])[0].get('status') == 'success':
                    self.success_count += 1
                else:
                    self.error_count += 1
                    results.append({'success': False, 'name': contact.get('name'), 'error': result})
                
                self.processed_count += 1
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.error_count += 1
                results.append({'success': False, 'name': contact.get('name'), 'error': str(e)})
                self.processed_count += 1
        
        return results



    def migrate_contact(self):
        """Contact migration process"""
        self.start_time = time.time()
        
        contact_map = self.zoho_client.get_contact_map()
        contacts_fields = [
            'phone', 'title', 'mobile', 'mobile2', 'unit_contact_ids', 'contact_id',
            'user_id', 'name', 'email', 'email2', 'agent', 'id', 'main_contact',
            'street', 'street2', 'country_id', 'contract_ids', 'city', 'employee'
        ]
        
        contact = self.odoo_client.fetch_records(
            'res.partner', 
            fields=contacts_fields, 
            batch_size=BATCH_SIZE
        )
        
        self.total_records = len(contact)
        batches = [contact[i:i + BATCH_SIZE] 
                    for i in range(0, len(contact), BATCH_SIZE)]
        
        print(batches)
        
        
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for batch in batches:
                future = executor.submit(
                    self.process_contact_batch, 
                    batch,
                    contact_map
                )
                futures.append(future)
            
        #     with tqdm(total=len(futures), desc="Processing contact batches") as pbar:
        #         for future in as_completed(futures):
        #             if self.stop_event.is_set():
        #                 break
                    
        #             future.result()
        #             pbar.update(1)
                    
                            




# client = OdooClient(ODOO_CONFIG)

# contacts = client.fetch_records('res.partner', fields=[
#     'phone', 'title', 'mobile', 'mobile2', 'unit_contact_ids', 'contact_id',
#     'user_id', 'name', 'email', 'email2', 'agent', 'id', 'main_contact',
#     'street', 'street2', 'country_id', 'contract_ids', 'city', 'employee'
# ])



# zoho_client = ZohoClient(ZOHO_CONFIG)


migrate_client = MigrationManager()
migrate_client.migrate_contact()

# for contact in contacts[:30]:
#     print('-----------------------------')
#     print(contact)
#     try:
        # id = contact['id']
        # name = contact['name'].strip()
        # mobile = contact['mobile']
        # email = contact['email']
        # country = contact['country_id']
#         contact_by_odoo_id = zoho_client.get_contact_by_odoo_id(id)
#         if contact_by_odoo_id is None:
#             migrate_client
        
        
#     except:
#         pass
    
