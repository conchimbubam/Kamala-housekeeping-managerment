import os
from datetime import timedelta
from urllib.parse import urlparse

class Config:
    """Cấu hình ứng dụng với Supabase PostgreSQL"""
    
    # ==================== SUPABASE CONFIG ====================
    # Sử dụng DATABASE_URL từ environment variable
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    # Nếu không có DATABASE_URL, sử dụng thông tin Supabase
    if not DATABASE_URL:
        # Thông tin Supabase
        DB_HOST = 'aws-1-ap-south-1.pooler.supabase.com'
        DB_PORT = '5432'
        DB_NAME = 'postgres'
        DB_USER = 'postgres.cbrscaaoifhtkktjpmiq'
        DB_PASSWORD = 'Y1tDWxcbelREH0X1'  # MẬT KHẨU MỚI - không có ký tự đặc biệt
        
        # Xây dựng DATABASE_URL với password đã encode (nếu cần)
        # Lưu ý: @ trong password cần được encode thành %40
        if '@' in DB_PASSWORD:
            encoded_password = DB_PASSWORD.replace('@', '%40')
        else:
            encoded_password = DB_PASSWORD
        DATABASE_URL = f'postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    else:
        # Parse DATABASE_URL từ environment variable
        try:
            parsed = urlparse(DATABASE_URL)
            
            # Decode password (nếu có %40 chuyển lại thành @)
            password = parsed.password.replace('%40', '@') if parsed.password else ''
            
            DB_USER = parsed.username or 'unknown'
            DB_PASSWORD = password
            DB_HOST = parsed.hostname or 'unknown'
            DB_PORT = str(parsed.port) if parsed.port else '5432'
            DB_NAME = parsed.path[1:] if parsed.path else 'postgres'  # Bỏ '/' đầu tiên
            
        except Exception:
            # Fallback values nếu parse không thành công
            DB_HOST = 'unknown'
            DB_PORT = '5432'
            DB_NAME = 'unknown'
            DB_USER = 'unknown'
            DB_PASSWORD = 'unknown'
    
    # ==================== GOOGLE SHEETS CONFIG ====================
    API_KEY = os.environ.get('API_KEY', 'AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A')
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '14-m1Wg2g2J75YYwZnqe_KV7nxLn1c_zVVT-uMxz-uJo')
    RANGE_NAME = os.environ.get('RANGE_NAME', 'A2:K63')
    
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
            'password': '***' + cls.DB_PASSWORD[-4:] if cls.DB_PASSWORD and cls.DB_PASSWORD != 'unknown' else 'None',
            'has_database_url': bool(os.environ.get('DATABASE_URL')),
            'using_hardcoded_password': not bool(os.environ.get('DATABASE_URL'))  # Cảnh báo dùng mật khẩu cứng
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
        print(f"📡 Using DATABASE_URL from env: {db_config['has_database_url']}")
        if db_config['using_hardcoded_password']:
            print("⚠️  WARNING: Using hardcoded password in code!")
        
        # App info
        print(f"📊 Google Sheets: {cls.SPREADSHEET_ID}")
        print(f"📈 Google Sheets Range: {cls.RANGE_NAME}")
        print(f"🔑 Department Code: {cls.DEPARTMENT_CODE}")
        print(f"📈 HK Report Start: {cls.HK_REPORT_START_HOUR:02d}:{cls.HK_REPORT_START_MINute:02d}")
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
        
        # CẢNH BÁO BẢO MẬT QUAN TRỌNG: mật khẩu cứng trong production
        if cls.is_production() and not os.environ.get('DATABASE_URL'):
            warnings.append("🚨 SECURITY RISK: Using hardcoded database password in production code!")
            warnings.append("🚨 ACTION REQUIRED: Set DATABASE_URL environment variable on Render Dashboard")
        
        # Kiểm tra Google Sheets configuration
        if cls.API_KEY == 'AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A':
            warnings.append("⚠️  Đang sử dụng API Key mặc định, xem xét thiết lập environment variable")
        
        if cls.DEPARTMENT_CODE == '123':
            warnings.append("⚠️  Đang sử dụng Department Code mặc định, xem xét thay đổi")
        
        # Kiểm tra range configuration
        if 'K' not in cls.RANGE_NAME.upper():
            warnings.append("⚠️  RANGE_NAME có thể không đầy đủ 11 cột (A-K). Đã cập nhật chưa?")
        
        # Kiểm tra security trong production
        if cls.is_production() and cls.DEBUG:
            warnings.append("🚨 DEBUG mode đang bật trong production - TẮT NGAY LẬP TỨC")
        
        if cls.is_production() and cls.SECRET_KEY == 'hotel-management-render-secret-key-2024':
            warnings.append("🚨 Đang sử dụng SECRET_KEY mặc định trong production - THAY ĐỔI NGAY")
        
        return warnings

    @classmethod
    def get_room_status_options(cls):
        """Trả về danh sách các trạng thái phòng hợp lệ (cập nhật theo cấu trúc mới)"""
        return [
            'vc', 'vd', 'od', 'oc', 'dnd', 'nn', 'lock', 'ip', 'do',
            'vd/arr', 'vc/arr', 'do/arr'  # Thêm các trạng thái kết hợp với ARR
        ]
    
    @classmethod
    def get_room_status_labels(cls):
        """Trả về nhãn hiển thị cho các trạng thái phòng"""
        return {
            'vc': 'Vacant Clean',
            'vd': 'Vacant Dirty',
            'od': 'Occupied Dirty',
            'oc': 'Occupied Clean',
            'dnd': 'Do Not Disturb',
            'nn': 'No Need Service',
            'lock': 'Lock',
            'ip': 'In Progress',
            'do': 'Due Out',
            'vd/arr': 'Vacant Dirty (Arrival)',
            'vc/arr': 'Vacant Clean (Arrival)',
            'do/arr': 'Due Out (Arrival)'
        }


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