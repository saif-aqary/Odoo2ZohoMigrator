# core/data_mapper.py

from datetime import datetime
from typing import Dict, Any, Optional
from utils.validators import DataValidator
import logging

class ContactMapper:
    @staticmethod
    def map_contact(odoo_contact: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Map Odoo contact fields to Zoho contact fields with validation"""
        
        print('odoo_contact:         ', odoo_contact)
        print('-------------------------------------------')
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
        print('odoo_lead:         ', odoo_lead)
        print('-------------------------------------------')
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
        print('odoo_property:         ', odoo_property)
        print('-------------------------------------------')
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
    
class UnitMapper:
    """Maps Odoo property/unit fields to Zoho CRM fields based on exact Zoho field specifications"""

    # Property type mapping based on account_asset.csv
    PROPERTY_TYPE_MAPPING = {
        'apartment': 'Apartment',
        'villa': 'Villa',
        'warehouse': 'Warehouse',
        'commercial_villa': 'Commercial Villa',
        'mixed_used_building': 'Mixed Used Building',
        'commercial_building': 'Commercial Building',
        'townhouse': 'Townhouse',
        'plot': 'Residential Land',
        'commercial_plot': 'Commercial Land',
        'residential_building': 'Residential Building',
        'retail': 'Retail',
        'office': 'Office',
        'labour_camp': 'Labour Camp',
        'multiple_units': 'Bulk Units'
    }
    
    # Bedroom mapping based on unit.bedrooms_unit_types.csv
    BEDROOM_MAPPING = {
        'Studio': 'Studio',
        '0': '0',
        '1': '1',
        '2': '2',
        '3': '3',
        '4': '4',
        '5': '5',
        '6': '6',
        '7': '7',
        '8': '8',
        'NULL': 'N/A',
        'n/a': 'N/A'
    }
    
    # Mapping for property status
    PROPERTY_STATUS_MAPPING = {
        'draft': 'Available',
        'book': 'Reserved',
        'normal': 'Leased',
        'close': 'Sold',
        'sold': 'Sold',
        'cancel': 'Cancelled',
        'block': 'Blocked',
        'upcoming': 'Upcoming'
    }
    
    # Mapping for property details (sale/rent)
    PROPERTY_DETAILS_MAPPING = {
        'sale': 'For Sale',
        'rent': 'For Rent',
        'both': 'None'
    }

    # Mapping for furnishing status
    FURNISHING_MAPPING = {
        'none': 'NO',
        'semi_furnished': 'YES',
        'full_furnished': 'YES'
    }

    @staticmethod
    def extract_relation_name(relation: Any) -> str:
        """Extract name from a many2one relation safely"""
        if isinstance(relation, (list, tuple)) and len(relation) > 1:
            return relation[1]
        return ''

    @staticmethod
    def map_unit(odoo_unit: Dict[str, Any],  is_update: bool = False) -> Optional[Dict[str, Any]]:
        """Map Odoo unit/property fields to Zoho CRM fields"""
        try:
            # Basic validation
            if not odoo_unit.get('name'):
                return None
             # Check ownership type - only proceed if freehold or leashold
            ownership_type = odoo_unit.get('ownership_type')
            if ownership_type not in ['freehold', 'leashold']:
                return None
            
            print(odoo_unit)
            print('------------------------------------------------------------------------')
            # print(odoo_unit.get('ref_no'))

            # Get community and sub-community as text
            community = UnitMapper.get_relation_name(odoo_unit.get('property_community_id'))
            sub_community = UnitMapper.get_relation_name(odoo_unit.get('property_sub_community_id'))

            zoho_unit = {
                # Basic Unit Information
                'Unit_Code': odoo_unit.get('property_code'),
                'Property_Title': odoo_unit.get('name'),
                'Unit_No': odoo_unit.get('unit_number'),
                'Ref_No': odoo_unit.get('ref_no'),
                
                # Location Information - Converting dropdowns to text
                'Locality': community,  # Using community as locality
                'Sub_Locality': sub_community,  # Using sub-community as sub-locality
                'City': UnitMapper.get_relation_name(odoo_unit.get('city_id')),
                'State': UnitMapper.get_relation_name(odoo_unit.get('state_id')),
                'Country': UnitMapper.get_relation_name(odoo_unit.get('country_id')),
                
                # Property Type and Status
                'Unit_Types': UnitMapper.PROPERTY_TYPE_MAPPING.get(
                    odoo_unit.get('type', '').lower(), 
                    UnitMapper.get_relation_name(odoo_unit.get('unit_type_id'))
                ),
                'Status': odoo_unit.get('state'),
                'Possession_Status': 'Under Construction' if odoo_unit.get('off_plan_property') else 'Ready',
                
                # Property Features
                'Bedrooms': UnitMapper.BEDROOM_MAPPING.get(
                    str(odoo_unit.get('bedroom', '')), 
                    str(odoo_unit.get('bedroom', ''))
                ),
                'Bathrooms': str(odoo_unit.get('bathroom', '')),
                'Floor_No': odoo_unit.get('floor_number'),
                'Total_Area': odoo_unit.get('total_area'),
                'Internal_Area_UOM': odoo_unit.get('builtup_area'),
                'External_Area_UOM': odoo_unit.get('plot_area'),
                
                # Financial Information
                'Unit_Sale_Price': UnitMapper.clean_currency(odoo_unit.get('selling_price')),
                'Rent_Amount': UnitMapper.clean_currency(odoo_unit.get('rent_per_year')),
                'Price_Per_UOM': UnitMapper.clean_currency(odoo_unit.get('price_per_sqt_foot')),
                'Maintenance_fee': UnitMapper.clean_currency(odoo_unit.get('service_charge')),
                'No_of_Cheques': odoo_unit.get('no_of_cheques'),
                'Payment_Allocated': UnitMapper.clean_currency(odoo_unit.get('payment_allocated')),
                'Total_Value': UnitMapper.clean_currency(odoo_unit.get('total_price')),
                'Discount_Amount': UnitMapper.clean_currency(odoo_unit.get('discount')),
                
                # Dates and Timestamps
                'Created_On': odoo_unit.get('create_date'),
                'Listing_Date': odoo_unit.get('listing_date'),
                
                # Additional Details
                'Property_Description': odoo_unit.get('marketing_desc'),
                'Property_Description_AR': odoo_unit.get('marketing_desc_arabic'),
                'Property_Title_AR': odoo_unit.get('name_arabic'),
                'Permit_Number': odoo_unit.get('permit_number'),
                'Geopoints': f"{odoo_unit.get('latitude', '')},{odoo_unit.get('longitude', '')}" if odoo_unit.get('latitude') and odoo_unit.get('longitude') else '',
                
                # Agent Information
                'Agent_Name': odoo_unit.get('agent_name'),
                'Agent_Email': odoo_unit.get('agent_email'),
                'Agent_Phone': odoo_unit.get('agent_phone'),
                'Agent_ID': odoo_unit.get('agent_id'),
                
                # Tracking Information
                'Properties_Units_Id': f"odoo_{odoo_unit.get('id')}",
                'Exchange_Rate': UnitMapper.clean_currency(odoo_unit.get('exchange_rate')),
                'Currency': odoo_unit.get('currency', 'AED')
            }

            # Handle amenities and features
            if odoo_unit.get('amenities_ids'):
                amenities = [am[1] for am in odoo_unit['amenities_ids'] if isinstance(am, (list, tuple))]
                if amenities:
                    zoho_unit['Private_Amenities'] = ';'.join(amenities)

            if odoo_unit.get('commercial_amenities_ids'):
                comm_amenities = [am[1] for am in odoo_unit['commercial_amenities_ids'] if isinstance(am, (list, tuple))]
                if comm_amenities:
                    zoho_unit['Commercial_Amenities'] = ';'.join(comm_amenities)

            print(zoho_unit)

            
            # Only include Unit_Code for new records
            if not is_update:
                zoho_unit['Unit_Code'] = odoo_unit.get('property_code')

            # Clean up empty values
            return {k: v for k, v in zoho_unit.items() if v not in (None, '', False)}

        except Exception as e:
            print(f"Error mapping unit {odoo_unit.get('name', 'Unknown')}: {str(e)}")
            return None

    @staticmethod
    def clean_currency(value: Any) -> Optional[float]:
        """Clean and validate currency values"""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                cleaned = ''.join(c for c in value if c.isdigit() or c == '.')
                return float(cleaned) if cleaned else None
            return None
        except (ValueError, TypeError):
            return None
        
    @staticmethod
    def get_relation_name(relation: Any) -> str:
        """Extract name from a many2one relation tuple"""
        if isinstance(relation, (list, tuple)) and len(relation) > 1:
            return relation[1]
        return ''

    @staticmethod
    def format_geopoints(latitude: Any, longitude: Any) -> str:
        """Format latitude and longitude into geopoints string"""
        try:
            if latitude and longitude:
                return f"{float(latitude)},{float(longitude)}"
            return ''
        except (ValueError, TypeError):
            return ''
        
    @staticmethod
    def generate_reference(odoo_unit: Dict[str, Any]) -> str:
        """Generate a unique reference for the property"""
        ref_parts = []
        
        if odoo_unit.get('ref_no'):
            ref_parts.append(odoo_unit['ref_no'])
        elif odoo_unit.get('property_code'):
            ref_parts.append(odoo_unit['property_code'])
            
        if odoo_unit.get('property_community_id'):
            ref_parts.append(odoo_unit['property_community_id'][1][:3].upper())
            
        if not ref_parts:
            ref_parts.append(f"PROP{datetime.now().strftime('%Y%m%d%H%M%S')}")
            
        return '_'.join(ref_parts)

    @staticmethod
    def clean_currency(value: Any) -> Optional[float]:
        """Clean and validate currency values"""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                # Remove currency symbols and commas
                cleaned = ''.join(c for c in value if c.isdigit() or c == '.')
                return float(cleaned) if cleaned else None
            return None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def map_amenities(odoo_unit: Dict[str, Any]) -> list:
        """Extract and map amenities from Odoo unit"""
        amenities = []
        
        # Get amenities from many2many fields
        if odoo_unit.get('amen_ids'):
            amenities.extend([amen[1] for amen in odoo_unit['amen_ids']])
        
        # Add facilities
        if odoo_unit.get('faci_ids'):
            amenities.extend([fac[1] for fac in odoo_unit['faci_ids']])
            
        # Add features based on boolean fields
        feature_mappings = {
            'parking': 'Parking',
            'full_floor': 'Full Floor',
            'vacant': 'Vacant',
            'multiple_owners': 'Multiple Owners',
            'car_park_allowed': 'Car Parking'
        }
        
        for field, feature in feature_mappings.items():
            if odoo_unit.get(field):
                amenities.append(feature)
                
        return list(set(amenities))  # Remove duplicates

    @staticmethod
    def generate_reference(odoo_unit: Dict[str, Any]) -> str:
        """Generate a unique reference for the property"""
        ref_parts = []
        
        if odoo_unit.get('property_code'):
            ref_parts.append(odoo_unit['property_code'])
        elif odoo_unit.get('unit_number'):
            ref_parts.append(odoo_unit['unit_number'])
            
        if odoo_unit.get('property_community_id'):
            ref_parts.append(odoo_unit['property_community_id'][1][:3].upper())
            
        if not ref_parts:
            ref_parts.append(f"PROP{datetime.now().strftime('%Y%m%d%H%M%S')}")
            
        return '_'.join(ref_parts)

class DataMapper:
    contact_mapper = ContactMapper()
    property_mapper = PropertyMapper()

    @staticmethod
    def map_record(odoo_record: Dict[str, Any], record_type: str) -> Optional[Dict[str, Any]]:
        """Map Odoo records based on type"""
        print('odoo_record:             ', odoo_record)
        print('---------------------------------------')
        if record_type == 'contact':
            return ContactMapper.map_contact(odoo_record)
        elif record_type == 'property':
            return PropertyMapper.map_property(odoo_record)
        return None