# utils/validators.py

import re
from typing import Optional

class DataValidator:
    @staticmethod
    def validate_email(email: Optional[str]) -> Optional[str]:
        """Validate and clean email address"""
        if not email:
            return None
            
        email = email.strip().lower()
        
        # Basic email validation pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if re.match(pattern, email):
            return email
        return None

    @staticmethod
    def validate_phone(phone: Optional[str]) -> Optional[str]:
        """Validate and clean phone number"""
        if not phone:
            return None
            
        # Remove all non-digit characters
        phone = re.sub(r'\D', '', phone)
        
        # Ensure minimum length (adjust as needed)
        if len(phone) >= 8:
            return phone
        return None

    @staticmethod
    def validate_name(name: Optional[str]) -> Optional[str]:
        """Validate and clean name"""
        if not name:
            return None
            
        # Remove extra whitespace and special characters
        name = re.sub(r'[^\w\s-]', '', name)
        name = ' '.join(name.split())
        
        if name:
            return name
        return None