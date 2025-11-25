# config.py
import os
from datetime import timedelta
import re

class Config:
    """Cấu hình ứng dụng với PostgreSQL Database cho Render"""
    
    # ==================== RENDER POSTGRESQL DATABASE CONFIG ====================
    # Sử dụng DATABASE_URL từ environment variable (Render cung cấp)
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    # Fallback cho development nếu không có DATABASE_URL
    if not DATABASE_URL:
        # Thông tin database từ Render PostgreSQL bạn cung cấp
        DB_HOST = 'dpg-d4hu220gjchc73dh9ogg-a'
        DB_PORT = '5432'
        DB_NAME = 'hotel_management_zga5'
        DB_USER = 'hotel_user'
        DB_PASSWORD = 'h2348SIpwIDUk0Uv7xNu4VRATOJNHYzb'
        
        # Xây dựng DATABASE_URL từ các thành phần
        DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    else:
        # Parse DATABASE_URL để lấy các thành phần
        parsed_url = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', DATABASE_URL)
        if parsed_url:
            DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = parsed_url.groups()
        else:
            # Fallback values nếu parse không thành công
            DB_HOST = 'unknown'
            DB_PORT = '5432'
            DB_NAME = 'unknown'
            DB_USER = 'unknown'
            DB_PASSWORD = 'unknown'
    
    # ==================== GOOGLE SHEETS CONFIG ====================
    API_KEY = os.environ.get('API_KEY', 'AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A')
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '14-m1Wg2g2J75YYwZnqe_KV7nxLn1c_zVVT-uMxz-uJo')
    RANGE_NAME = os.environ.get('RANGE_NAME', 'A2:J63')
    
    # ==================== FLASK CONFIG ====================
    SECRET_KEY = os.environ.get('SECRET_KEY', 'hotel-management-render-secret-key-2024')
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    # Session configuration
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)
    
    # ==================== APPLICATION SETTINGS ====================
    DEPARTMENT_CODE = os.environ.get('DEPARTMENT_CODE', '123')
    HK_REPORT_START_HOUR = 8
    HK_REPORT_START_MINUTE = 15
    
    # Backup configuration
    BACKUP_RETENTION_DAYS = 30
    
    # Logging configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    
    # ==================== RENDER SPECIFIC SETTINGS ====================
    @classmethod
    def is_render(cls):
        """Kiểm tra có đang chạy trên Render không"""
        return 'RENDER' in os.environ
    
    @classmethod
    def is_production(cls):
        """Kiểm tra môi trường production"""
        return cls.is_render() or os.environ.get('ENVIRONMENT') == 'production'
    
    @classmethod
    def get_database_config(cls):
        """Lấy cấu hình database dạng dict để debug"""
        return {
            'host': cls.DB_HOST,
            'port': cls.DB_PORT,
            'database': cls.DB_NAME,
            'user': cls.DB_USER,
            'password': '***' + cls.DB_PASSWORD[-4:] if cls.DB_PASSWORD else 'None',
            'has_database_url': bool(os.environ.get('DATABASE_URL'))
        }
    
    @classmethod
    def print_config_summary(cls):
        """In summary cấu hình - an toàn (không hiển thị password đầy đủ)"""
        print("=" * 60)
        print("🏨 Hotel Management System - PostgreSQL Render Edition")
        print("=" * 60)
        print(f"🌐 Environment: {'Render' if cls.is_render() else 'Local Development'}")
        print(f"🔧 Mode: {'Production' if cls.is_production() else 'Development'}")
        print(f"🐛 Debug: {cls.DEBUG}")
        
        # Database info (an toàn)
        db_config = cls.get_database_config()
        print(f"🗃️  Database: {db_config['database']}@{db_config['host']}:{db_config['port']}")
        print(f"👤 DB User: {db_config['user']}")
        print(f"🔐 DB Auth: {db_config['password']}")
        print(f"📡 Using DATABASE_URL: {db_config['has_database_url']}")
        
        # App info
        print(f"📊 Google Sheets: {cls.SPREADSHEET_ID}")
        print(f"🔑 Department Code: {cls.DEPARTMENT_CODE}")
        print(f"📈 HK Report Start: {cls.HK_REPORT_START_HOUR:02d}:{cls.HK_REPORT_START_MINUTE:02d}")
        print(f"📝 Log Level: {cls.LOG_LEVEL}")
        
        if cls.is_render():
            print("✅ Optimized for Render Cloud Deployment")
            print("💡 Features: Persistent Data, Auto Backup, SSL Enabled")
        else:
            print("💻 Local Development Mode")
            print("💡 Features: SQLite Fallback, Debug Tools")
        
        print("=" * 60)

    @classmethod
    def validate_config(cls):
        """Validate cấu hình và trả về các cảnh báo"""
        warnings = []
        
        # Kiểm tra database configuration
        if not cls.DATABASE_URL:
            warnings.append("⚠️  DATABASE_URL không được tìm thấy, sử dụng fallback configuration")
        
        if cls.DB_PASSWORD == 'unknown':
            warnings.append("⚠️  Không thể parse DATABASE_URL, kiểm tra định dạng")
        
        # Kiểm tra Google Sheets configuration
        if cls.API_KEY == 'AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A':
            warnings.append("⚠️  Đang sử dụng API Key mặc định, xem xét thiết lập environment variable")
        
        if cls.DEPARTMENT_CODE == '123':
            warnings.append("⚠️  Đang sử dụng Department Code mặc định, xem xét thay đổi")
        
        # Kiểm tra security trong production
        if cls.is_production() and cls.DEBUG:
            warnings.append("🚨 DEBUG mode đang bật trong production - TẮT NGAY LẬP TỨC")
        
        if cls.is_production() and cls.SECRET_KEY == 'hotel-management-render-secret-key-2024':
            warnings.append("🚨 Đang sử dụng SECRET_KEY mặc định trong production - THAY ĐỔI NGAY")
        
        return warnings


# Khởi tạo và validate config
if __name__ == '__main__':
    Config.print_config_summary()
    
    warnings = Config.validate_config()
    if warnings:
        print("\n🔔 CONFIG WARNINGS:")
        for warning in warnings:
            print(f"   {warning}")
    
    # Test database connection (chỉ khi chạy trực tiếp)
    try:
        from models.database import DatabaseManager
        db = DatabaseManager(Config.DATABASE_URL)
        health = db.health_check()
        print(f"\n🏥 Database Health: {health['status']}")
        if health['status'] == 'healthy':
            print("✅ Database connection successful!")
        else:
            print(f"❌ Database issues: {health.get('error', 'Unknown error')}")
    except Exception as e:
        print(f"\n❌ Cannot test database connection: {e}")
else:
    # Khi import, chỉ in summary nếu debug mode
    if Config.DEBUG:
        Config.print_config_summary()
        
        warnings = Config.validate_config()
        if warnings:
            print("\n🔔 CONFIG WARNINGS:")
            for warning in warnings:
                print(f"   {warning}")