from typing import List, Dict, Any
from core.odoo_client import OdooClient
from config.settings import ODOO_CONFIG
from utils.logger import setup_logger

class OdooInspector:
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.client = OdooClient(ODOO_CONFIG)

    def list_models(self, pattern: str = '') -> List[str]:
        """List all available models in Odoo that match the pattern"""
        try:
            # Execute ir.model search to get all models
            model_ids = self.client.models.execute_kw(
                self.client.config['db'],
                self.client.uid,
                self.client.config['password'],
                'ir.model',
                'search_read',
                [[('model', 'ilike', pattern)]],
                {'fields': ['model', 'name']}
            )
            
            self.logger.info(f"Found {len(model_ids)} models matching pattern '{pattern}'")
            
            # Print models in a formatted way
            print("\nAvailable Models:")
            print("-" * 60)
            print(f"{'Model Name':<30} | {'Technical Name':<30}")
            print("-" * 60)
            
            for model in sorted(model_ids, key=lambda x: x['model']):
                print(f"{model['name'][:30]:<30} | {model['model']:<30}")
                
            return [model['model'] for model in model_ids]
            
        except Exception as e:
            self.logger.error(f"Error listing models: {str(e)}")
            raise

    def get_fields(self, model_name: str) -> Dict[str, Any]:
        """Get all fields and their details for a specific model"""
        try:
            fields_info = self.client.models.execute_kw(
                self.client.config['db'],
                self.client.uid,
                self.client.config['password'],
                model_name,
                'fields_get',
                [],
                {'attributes': ['string', 'help', 'type', 'required', 'selection']}
            )
            
            self.logger.info(f"Retrieved {len(fields_info)} fields for model {model_name}")
            
            # Print fields in a formatted way
            print(f"\nFields for model '{model_name}':")
            print("-" * 100)
            print(f"{'Field Name':<30} | {'Field Label':<30} | {'Type':<15} | {'Required':<8}")
            print("-" * 100)
            
            for field_name, field_info in sorted(fields_info.items()):
                print(f"{field_name[:30]:<30} | "
                      f"{field_info['string'][:30]:<30} | "
                      f"{field_info['type']:<15} | "
                      f"{'Yes' if field_info.get('required') else 'No':<8}")
                
                # If it's a selection field, show possible values
                if field_info['type'] == 'selection' and field_info.get('selection'):
                    print(f"    Selection values: {dict(field_info['selection'])}")
                
            return fields_info
            
        except Exception as e:
            self.logger.error(f"Error getting fields for model {model_name}: {str(e)}")
            raise

    def inspect_record(self, model_name: str, limit: int = 1) -> None:
        """Inspect actual records from a model to see data structure"""
        try:
            # Get all fields first
            fields_info = self.get_fields(model_name)
            field_names = list(fields_info.keys())
            
            # Fetch sample records
            records = self.client.models.execute_kw(
                self.client.config['db'],
                self.client.uid,
                self.client.config['password'],
                model_name,
                'search_read',
                [[]],
                {'fields': field_names, 'limit': limit}
            )
            
            if not records:
                print(f"\nNo records found in model {model_name}")
                return
                
            print(f"\nSample record from '{model_name}':")
            print("-" * 100)
            
            for record in records:
                for field, value in record.items():
                    print(f"{field:<30}: {value}")
                print("-" * 100)
                
        except Exception as e:
            self.logger.error(f"Error inspecting records for model {model_name}: {str(e)}")
            raise

def main():
    inspector = OdooInspector()
    
    # Example usage:
    
    # 1. List all property-related models
    print("\nSearching for property-related models...")
    inspector.list_models('property')
    
    # 2. List all CRM-related models
    print("\nSearching for CRM-related models...")
    inspector.list_models('crm')

    # 3. List all contact-related models
    print("\nSearching for contact-related models...")
    inspector.list_models('contact')
    # Let user specify a model to inspect
    model_name = input("\nEnter model name to inspect (e.g., crm.lead): ")
    if model_name:
        # Get fields for the model
        inspector.get_fields(model_name)
        
        # Show sample record
        # inspector.inspect_record(model_name)

if __name__ == "__main__":
    main()