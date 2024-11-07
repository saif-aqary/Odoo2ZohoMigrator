# core/data_mapper.py

from datetime import datetime
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
                'Odoo_ID': odoo_contact.get('contact_id'),
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
            # Assign the Lead to Specific User
            zoho_lead['Owner'] = '6421814000003834001'

            logger.debug(f"Mapped lead data: {zoho_lead}")
            return zoho_lead

        except Exception as e:
            logger.error(f"Error mapping lead: {str(e)}")
            logger.debug(f"Original lead data: {odoo_lead}")
            return None
class PropertyMapper:
    # @staticmethod
    def map_property(self , odoo_property: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Map Odoo property fields to Zoho property fields with validation"""
        try:
            # Check ownership type - only proceed if freehold or leashold
            ownership_type = odoo_property.get('ownership_type')
            if ownership_type not in ['freehold', 'leashold']:
                return None

            # Get related field values safely
            type_id = odoo_property.get('type_id', [False, ''])
            property_type = type_id[1] if isinstance(type_id, (list, tuple)) and len(type_id) > 1 else ''
            
            community_id = odoo_property.get('property_community_id', [False, ''])
            community = community_id[1] if isinstance(community_id, (list, tuple)) and len(community_id) > 1 else ''
            
            sub_community_id = odoo_property.get('property_sub_community_id', [False, ''])
            sub_community = sub_community_id[1] if isinstance(sub_community_id, (list, tuple)) and len(sub_community_id) > 1 else ''

            # Map the property to Zoho format
            property_data = {
                'Properties / Units Id': f"odoo_{odoo_property.get('id', '')}",
                'Unit Code': odoo_property.get('property_code', ''),
                'Properties / Units Owner': odoo_property.get('owner_id', [False, ''])[1] if isinstance(odoo_property.get('owner_id'), (list, tuple)) else '',
                'Status': 'false',  # Default status as per sample
                'Created Time': odoo_property.get('create_date', ''),
                'Modified Time': odoo_property.get('write_date', ''),
                'Tag': 'Freehold' if ownership_type == 'freehold' else 'Leasehold',
                
                # Property specific fields
                'Unit Name': odoo_property.get('name', ''),
                'Unit Types': property_type,
                'Property Details': 'For Sale' if odoo_property.get('property_type') == 'sale' else 'For Lease',
                'Building Name': odoo_property.get('name', ''),
                'Property Description': odoo_property.get('property_overview', ''),
                'Property Description (AR)': '',  # No Arabic description in Odoo
                'Ref.No': odoo_property.get('ref_no', ''),
                
                # Location details
                'Community': community,
                'Sub Community': sub_community,
                'Country': odoo_property.get('country_id', [False, ''])[1] if isinstance(odoo_property.get('country_id'), (list, tuple)) else '',
                'State': odoo_property.get('state_id', [False, ''])[1] if isinstance(odoo_property.get('state_id'), (list, tuple)) else '',
                'City': odoo_property.get('city_id', [False, ''])[1] if isinstance(odoo_property.get('city_id'), (list, tuple)) else '',
                
                # Additional details
                'Covered Area': str(odoo_property.get('builtup_area', '')),
                'Other Area': str(odoo_property.get('plot_area', '')),
                'Handover Date': odoo_property.get('handover_date', ''),
                'Possession Status': 'Under Construction' if odoo_property.get('off_plan_property') else 'Ready',
                'Maintenance fee': odoo_property.get('maintanence_fee_per_sq_ft', ''),
                
                # Get amenities
                'Private Amenities': self._get_amenities_string(odoo_property),
                
                # Location coordinates
                'Geopoints': f"{odoo_property.get('latitude', '')},{odoo_property.get('longitude', '')}" if odoo_property.get('latitude') and odoo_property.get('longitude') else ''
            }

            # Remove empty fields
            return {k: v for k, v in property_data.items() if v}

        except Exception as e:
            print(f"Error mapping property: {str(e)}")
            return None

    @staticmethod
    def _get_amenities_string(odoo_property: Dict[str, Any]) -> str:
        """Combine all amenities into a semicolon-separated string"""
        amenities = []
        
        # Check boolean amenities
        amenity_fields = [
            'gym', 'swimming_pool', 'beach', 'medical_center', 'schools',
            'shopping_malls', 'restaurants', 'marina', 'golf_course'
        ]
        
        for field in amenity_fields:
            if odoo_property.get(field):
                amenities.append(field.replace('_', ' ').title())
        
        # Add facilities from many2many fields if available
        if isinstance(odoo_property.get('facilities_ids'), (list, tuple)):
            amenities.extend([str(f) for f in odoo_property['facilities_ids']])
            
        return ';'.join(amenities) if amenities else ''

class DataMapper:
    contact_mapper = ContactMapper()
    property_mapper = PropertyMapper()

    @staticmethod
    def map_record(odoo_record: Dict[str, Any], record_type: str) -> Optional[Dict[str, Any]]:
        """Map Odoo records based on type"""
        if record_type == 'contact':
            return ContactMapper.map_contact(odoo_record)
        elif record_type == 'property':
            return PropertyMapper.map_property(odoo_record)
        return None