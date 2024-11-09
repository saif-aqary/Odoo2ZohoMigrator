# core/zoho_client.py

import requests
import time
from typing import Dict, Any, Set, Optional
from threading import Lock
from tqdm import tqdm
from utils.logger import setup_logger

class ZohoClient:
    def __init__(self, config: Dict[str, str]):
        self.logger = setup_logger(__name__)
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
            
            self.logger.debug(f"Attempting to refresh token with domain {domain['auth_url']}")
            self.logger.debug(f"Request data: {data}")
            
            response = requests.post(domain['auth_url'], data=data)
            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response content: {response.text}")
            
            if response.status_code == 200 and 'access_token' in response.json():
                self.access_token = response.json()['access_token']
                self.current_domain = domain
                self.logger.info(f"Successfully refreshed token with domain {domain['auth_url']}")
                return True
                
            return False
            
        except Exception as e:
            self.logger.debug(f"Failed to refresh token with domain {domain['auth_url']}: {str(e)}")
            return False

    def refresh_token(self):
        """Try to refresh token with all available domains"""
        with self.token_lock:
            for domain in self.domains:
                if self.try_refresh_token(domain):
                    return
                    
            error_msg = "Failed to refresh token with all available domains"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def create_record(self, module: str, data: Dict[str, Any], retry_count: int = 0) -> Optional[Dict[str, Any]]:
        """Create a record in Zoho CRM"""
        try:
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
            
        except Exception as e:
            self.logger.error(f"Error creating Zoho record: {str(e)}")
            if retry_count < 3:
                time.sleep(1)
                return self.create_record(module, data, retry_count + 1)
            return None

    def get_contact_map(self) -> Dict[str, str]:
        """Fetch all contacts and create a mapping of mobile/email to Zoho contact ID"""
        self.logger.info("Building contact mapping...")
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
                    self.logger.error(f"Error fetching contacts page {page}: {str(e)}")
                    break

        self.logger.info(f"Built mapping for {len(contact_map)} contacts")
        return contact_map

    def get_existing_contacts(self) -> Set[str]:
        """Fetch existing contacts from Zoho"""
        self.logger.info("Fetching existing contacts from Zoho...")
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
                    self.logger.error(f"Error fetching existing contacts page {page}: {str(e)}")
                    break

        self.logger.info(f"Found {len(existing_contacts)} existing contacts in Zoho")
        return existing_contacts
    def check_available_modules(self):
        """Check available modules in Zoho CRM"""
        try:
            # Make a request to list all modules
            url = f"{self.current_domain['base_url']}/settings/modules"
            headers = {
                'Authorization': f'Zoho-oauthtoken {self.access_token}'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                modules = response.json()
                self.logger.info("Available Zoho CRM modules:")
                for module in modules.get('modules', []):
                    self.logger.info(f"API Name: {module.get('api_name')} - Display Name: {module.get('module_name')}")
                return modules
            else:
                self.logger.error(f"Failed to fetch modules: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error checking modules: {str(e)}")
            return None
    def get_existing_unit(self, unit_code: str) -> Optional[Dict[str, Any]]:
        """Find existing unit by Unit_Code"""
        try:
            if not self.current_domain:
                raise Exception("No valid Zoho domain found")
                
            url = f"{self.current_domain['base_url']}/CustomModule1/search"
            headers = {
                'Authorization': f'Zoho-oauthtoken {self.access_token}',
            }
            
            # Search criteria
            params = {
                'criteria': f'(Unit_Code:equals:{unit_code})'
            }
            
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 401:
                self.refresh_token()
                return self.get_existing_unit(unit_code)
                
            data = response.json()
            if data.get('data'):
                return data['data'][0]
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding existing unit: {str(e)}")
            return None

    def get_contact_by_odoo_id(self, odoo_id: str) -> Optional[Dict[str, Any]]:
        """Find contact using Odoo_ID field"""
        try:
            if not self.current_domain:
                raise Exception("No valid Zoho domain found")
                
            url = f"{self.current_domain['base_url']}/Contacts/search"
            headers = {
                'Authorization': f'Zoho-oauthtoken {self.access_token}',
            }
            
            params = {
                'criteria': f'(Odoo_ID:equals:{odoo_id})'
            }
            
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 401:
                self.refresh_token()
                return self.get_contact_by_odoo_id(odoo_id)
                
            data = response.json()
            if data.get('data'):
                return data['data'][0]
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding contact by Odoo ID: {str(e)}")
            return None

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
            self.logger.error(f"Error updating record: {str(e)}")
            return None