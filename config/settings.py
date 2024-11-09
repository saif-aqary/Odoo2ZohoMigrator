# config/settings.py

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