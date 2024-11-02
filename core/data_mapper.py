# core/data_mapper.py

from typing import Dict, Any, Optional
from utils.validators import DataValidator
import logging

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
        logger = logging.getLogger(__name__)
        try:
            logger.debug(f"Mapping lead: {odoo_lead}")

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
            
            # Extract stage name safely
            stage_id = odoo_lead.get('stage_id')
            stage_name = stage_id[1].lower() if isinstance(stage_id, (list, tuple)) and len(stage_id) > 1 else 'new'
            stage = stage_mapping.get(stage_name, 'New')

            # Extract source safely
            source_id = odoo_lead.get('source_id')
            source = source_id[1] if isinstance(source_id, (list, tuple)) and len(source_id) > 1 else 'Odoo Migration'

            zoho_lead = {
                'Lead_Source': source,
                'Lead_Status': stage
            }

            # Add company name if available
            if company:
                zoho_lead['Company'] = company
            elif odoo_lead.get('name'):  # Use lead name as company if no company name
                zoho_lead['Company'] = odoo_lead['name']
            else:
                zoho_lead['Company'] = 'Unknown Company'

            # Add contact name if available
            if contact_name:
                name_parts = contact_name.split(' ', 1)
                zoho_lead['First_Name'] = name_parts[0]
                if len(name_parts) > 1:
                    zoho_lead['Last_Name'] = name_parts[1]
                else:
                    zoho_lead['Last_Name'] = 'Unknown'
            else:
                zoho_lead['Last_Name'] = 'Unknown'

            # Add contact information
            if phone:
                zoho_lead['Phone'] = phone
            if mobile:
                zoho_lead['Mobile'] = mobile
            if email:
                zoho_lead['Email'] = email

            # Add description if available
            if odoo_lead.get('description'):
                zoho_lead['Description'] = odoo_lead['description']

            # Add revenue and probability if available
            if odoo_lead.get('expected_revenue'):
                try:
                    zoho_lead['Expected_Revenue'] = float(odoo_lead['expected_revenue'])
                except (ValueError, TypeError):
                    pass

            if odoo_lead.get('probability'):
                try:
                    zoho_lead['Probability'] = float(odoo_lead['probability'])
                except (ValueError, TypeError):
                    pass

            # Link to existing contact if provided
            if contact_id:
                zoho_lead['Contact_Id'] = contact_id

            logger.debug(f"Mapped lead data: {zoho_lead}")
            return zoho_lead

        except Exception as e:
            logger.error(f"Error mapping lead: {str(e)}")
            logger.debug(f"Original lead data: {odoo_lead}")
            return None