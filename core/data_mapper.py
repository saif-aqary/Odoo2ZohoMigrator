# core/data_mapper.py

from typing import Dict, Any, Optional
from utils.validators import DataValidator

class ContactMapper:
    @staticmethod
    def map_contact(odoo_contact: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Map Odoo contact fields to Zoho contact fields with validation"""
        try:
            # Clean and validate name
            full_name = DataValidator.validate_name(odoo_contact.get('name', ''))
            if not full_name:
                return None

            # Split name
            name_parts = full_name.split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else 'N/A'

            # Clean and validate contact data
            email = DataValidator.validate_email(odoo_contact.get('email'))
            phone = DataValidator.validate_phone(odoo_contact.get('phone'))
            mobile = DataValidator.validate_phone(odoo_contact.get('mobile'))

            zoho_contact = {
                'First_Name': first_name,
                'Last_Name': last_name,
                'Phone': phone,
                'Mobile': mobile,
                'Description': odoo_contact.get('comment', ''),
                'Lead_Source': 'Odoo Migration',
                'Contact_Type': 'Imported Contact'
            }

            # Only add email if valid
            if email:
                zoho_contact['Email'] = email

            # Remove empty fields
            return {k: v for k, v in zoho_contact.items() if v}

        except Exception as e:
            return None
        
class LeadMapper:
    @staticmethod
    def map_lead(odoo_lead: Dict[str, Any], contact_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Map Odoo lead fields to Zoho lead fields with validation"""
        try:
            # Clean and validate data
            company = DataValidator.validate_name(odoo_lead.get('partner_name', ''))
            contact_name = DataValidator.validate_name(odoo_lead.get('contact_name', ''))
            email = DataValidator.validate_email(odoo_lead.get('email_from'))
            phone = DataValidator.validate_phone(odoo_lead.get('phone'))
            mobile = DataValidator.validate_phone(odoo_lead.get('mobile'))

            # Map lead stage
            stage_mapping = {
                'new': 'New',
                'qualified': 'Qualified',
                'proposition': 'Proposition',
                'won': 'Closed Won',
                'lost': 'Closed Lost',
            }
            
            stage = stage_mapping.get(odoo_lead.get('stage_id', [False, ''])[1].lower(), 'New')

            zoho_lead = {
                'Company': company or 'N/A',
                'Phone': phone,
                'Mobile': mobile,
                'Description': odoo_lead.get('description', ''),
                'Lead_Status': stage,
                'Lead_Source': odoo_lead.get('source_id', [False, 'Odoo Migration'])[1],
                'Expected_Revenue': float(odoo_lead.get('expected_revenue', 0)),
                'Probability': float(odoo_lead.get('probability', 0)),
            }

            # Add contact information if no existing contact is linked
            if contact_name:
                zoho_lead['Last_Name'] = contact_name

            if email:
                zoho_lead['Email'] = email

            # Link to existing contact if provided
            if contact_id:
                zoho_lead['Contact_Id'] = contact_id

            # Remove empty fields
            return {k: v for k, v in zoho_lead.items() if v}

        except Exception as e:
            return None