# config.py
import os

class Config:
    """Cấu hình ứng dụng với SQLite Database"""
    
    # ==================== GOOGLE SHEETS CONFIG ====================
    API_KEY = os.environ.get('API_KEY', 'AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A')
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '14-m1Wg2g2J75YYwZnqe_KV7nxLn1c_zVVT-uMxz-uJo')
    RANGE_NAME = os.environ.get('RANGE_NAME', 'A2:J63')
    
    # ==================== FLASK CONFIG ====================
    SECRET_KEY = os.environ.get('SECRET_KEY', 'hotel-management-secret-key-2024')
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    # ==================== DATABASE CONFIG (MỚI) ====================
    DATA_DIR = os.environ.get('DATA_DIR', 'data')
    
    # Database SQLite path
    DATABASE_PATH = os.path.join(DATA_DIR, 'hotel.db')
    
    # Backup configuration
    BACKUP_DIR = os.path.join(DATA_DIR, 'backups')
    BACKUP_RETENTION_DAYS = 7  # Giữ backup trong 7 ngày
    
    # ==================== APPLICATION CONFIG ====================
    # Session timeout (minutes)
    SESSION_TIMEOUT = 480  # 8 hours
    
    # Logging configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.path.join(DATA_DIR, 'app.log')
    
    # HK Report configuration
    HK_REPORT_START_HOUR = 8
    HK_REPORT_START_MINUTE = 15
    
    # ==================== SECURITY CONFIG ====================
    # Department code for login (có thể chuyển sang database sau này)
    DEPARTMENT_CODE = '123'
    
    # Rate limiting (có thể triển khai sau)
    RATE_LIMIT_ENABLED = False
    
    # ==================== INITIALIZATION ====================
    # Đảm bảo các thư mục cần thiết tồn tại
    @classmethod
    def ensure_directories_exist(cls):
        """Đảm bảo các thư mục data và backups tồn tại"""
        directories = [cls.DATA_DIR, cls.BACKUP_DIR]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f"✅ Đã tạo thư mục: {directory}")

# Khởi tạo thư mục khi import config
Config.ensure_directories_exist()