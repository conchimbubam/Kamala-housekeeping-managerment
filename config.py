# config.py
import os
from datetime import timedelta
import re

class Config:
    """Cấu hình ứng dụng với PostgreSQL Database cho Render"""
    
    # ==================== RENDER POSTGRESQL DATABASE CONFIG ====================
    # Sử dụng thông tin bạn cung cấp
    DB_HOST = 'dpg-d4hu220gjchc73dh9ogg-a'  # Host từ Render
    DB_PORT = '5432'  # Port mặc định PostgreSQL
    DB_NAME = 'hotel_management'  # Tên database (có thể cần điều chỉnh)
    DB_USER = 'hotel_user'  # Username (có thể cần điều chỉnh)
    DB_PASSWORD = 'dpg-d4hu220gjchc73dh9ogg-a'  # Password từ Render
    
    # PostgreSQL connection string
    SQLALCHEMY_DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
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
    BACKUP_RETENTION_COUNT = 5
    
    # ==================== RENDER SPECIFIC ====================
    @classmethod
    def is_render(cls):
        """Kiểm tra có đang chạy trên Render không"""
        return 'RENDER' in os.environ
    
    @classmethod
    def print_config_summary(cls):
        """In summary cấu hình"""
        print("=" * 50)
        print("🏨 Hotel Management System - Render Deployment")
        print("=" * 50)
        print(f"🌐 Environment: {'Render' if cls.is_render() else 'Local'}")
        print(f"📊 Database: {cls.DB_NAME}@{cls.DB_HOST}:{cls.DB_PORT}")
        print(f"👤 Database User: {cls.DB_USER}")
        print(f"🔐 Authentication: Department Code Required")
        print(f"🐛 Debug Mode: {cls.DEBUG}")
        
        if cls.is_render():
            print("✅ Optimized for Render Cloud")
        
        print("=" * 50)

# In config summary
Config.print_config_summary()