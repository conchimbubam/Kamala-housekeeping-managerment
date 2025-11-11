import os

class Config:
    """Cấu hình ứng dụng"""
    # Google Sheets API - Ưu tiên lấy từ environment variables
    API_KEY = os.environ.get('API_KEY', 'AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A')
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '14-m1Wg2g2J75YYwZnqe_KV7nxLn1c_zVVT-uMxz-uJo')
    RANGE_NAME = os.environ.get('RANGE_NAME', 'A2:J63')
    
    # Flask Config
    SECRET_KEY = os.environ.get('SECRET_KEY', 'hotel-management-secret-key-2024')
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    # Data File Config
    DATA_DIR = os.environ.get('DATA_DIR', 'data')
    ROOMS_JSON = os.path.join(DATA_DIR, 'rooms.json')
    BACKUP_DIR = os.path.join(DATA_DIR, 'backups')