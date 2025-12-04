# models/database.py
import psycopg2
import logging
from contextlib import contextmanager
import os
from urllib.parse import urlparse
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_url=None):
        """
        Khởi tạo PostgreSQL Database Manager
        
        Args:
            db_url: PostgreSQL connection string (DATABASE_URL từ environment variable)
                   Hoặc có thể là None để sử dụng giá trị từ config
        """
        # Sử dụng DATABASE_URL từ tham số hoặc environment variable
        self.db_url = db_url or os.getenv('DATABASE_URL')
        
        if not self.db_url:
            # Nếu không có db_url, thử import config để lấy thông tin
            try:
                from config import Config
                self.db_url = Config.DATABASE_URL
                logger.info("✅ Sử dụng DATABASE_URL từ Config")
            except ImportError:
                logger.error("❌ Không thể import Config và không có DATABASE_URL")
                raise ValueError("DATABASE_URL là bắt buộc")
        
        if not self.db_url:
            raise ValueError("DATABASE_URL là bắt buộc. Kiểm tra config.py hoặc environment variables.")
        
        logger.info(f"🔗 Database URL: {self._mask_db_url(self.db_url)}")
        
        # Khởi tạo database ngay khi tạo instance
        self.initialize_database()
    
    def _mask_db_url(self, db_url):
        """Ẩn password trong database URL để log an toàn"""
        if not db_url:
            return "None"
        try:
            # Mask password trong connection string
            parsed = urlparse(db_url)
            if parsed.password:
                masked_url = db_url.replace(parsed.password, "***" + parsed.password[-4:])
                return masked_url
            return db_url
        except:
            return "***masked***"
    
    def get_connection_params(self):
        """Trích xuất thông tin kết nối từ URL"""
        try:
            parsed_url = urlparse(self.db_url)
            
            params = {
                'host': parsed_url.hostname,
                'database': parsed_url.path[1:],  # Bỏ qua '/' đầu tiên
                'user': parsed_url.username,
                'password': parsed_url.password,
            }
            
            # Thêm port nếu có
            if parsed_url.port:
                params['port'] = parsed_url.port
            else:
                params['port'] = 5432  # PostgreSQL default port
            
            # Thêm SSL mode cho production (Render PostgreSQL)
            if parsed_url.hostname and ('render.com' in parsed_url.hostname or 'amazonaws.com' in parsed_url.hostname):
                params['sslmode'] = 'require'
            
            return params
        except Exception as e:
            logger.error(f"❌ Lỗi parse database URL: {e}")
            raise ValueError(f"Database URL không hợp lệ: {e}")
    
    def initialize_database(self):
        """Khởi tạo database với schema hoàn chỉnh cho PostgreSQL - ĐÃ CẬP NHẬT"""
        logger.info("🔄 Đang khởi tạo database schema...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Bảng rooms - ĐÃ CẬP NHẬT với các trường newGuest
                        cur.execute('''
                            CREATE TABLE IF NOT EXISTS rooms (
                                room_no VARCHAR(10) PRIMARY KEY,
                                room_type VARCHAR(50) NOT NULL,
                                room_status VARCHAR(20) NOT NULL DEFAULT 'vc',
                                guest_name TEXT DEFAULT '',
                                check_in DATE,                     -- ✅ ĐỔI THÀNH DATE
                                check_out DATE,                    -- ✅ ĐỔI THÀNH DATE
                                new_guest_name TEXT DEFAULT '',    -- ✅ THÊM MỚI
                                new_check_in DATE,                 -- ✅ THÊM MỚI
                                new_check_out DATE,                -- ✅ THÊM MỚI
                                notes TEXT DEFAULT '',
                                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        ''')
                        
                        # Bảng activity_logs
                        cur.execute('''
                            CREATE TABLE IF NOT EXISTS activity_logs (
                                id SERIAL PRIMARY KEY,
                                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                user_name VARCHAR(100) NOT NULL,
                                user_department VARCHAR(20) NOT NULL,
                                room_no VARCHAR(10) NOT NULL,
                                action_type VARCHAR(50) NOT NULL,
                                old_status VARCHAR(20),
                                new_status VARCHAR(20),
                                action_detail TEXT,
                                ip_address VARCHAR(45)
                            )
                        ''')
                        
                        # Bảng sync_history
                        cur.execute('''
                            CREATE TABLE IF NOT EXISTS sync_history (
                                id SERIAL PRIMARY KEY,
                                sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                synced_by VARCHAR(100) NOT NULL,
                                total_rooms INTEGER,
                                success BOOLEAN DEFAULT TRUE,
                                error_message TEXT
                            )
                        ''')
                        
                        # Tạo indexes cho hiệu suất
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_rooms_status 
                            ON rooms(room_status)
                        ''')
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_activity_timestamp 
                            ON activity_logs(timestamp)
                        ''')
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_activity_room 
                            ON activity_logs(room_no)
                        ''')
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_activity_user 
                            ON activity_logs(user_name)
                        ''')
                        
                        # Tạo composite index cho tìm kiếm hiệu quả
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_activity_room_timestamp 
                            ON activity_logs(room_no, timestamp DESC)
                        ''')
                        
                        # THÊM: Tạo index cho các cột mới
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_rooms_check_in 
                            ON rooms(check_in)
                        ''')
                        cur.execute('''
                            CREATE INDEX IF NOT EXISTS idx_rooms_new_check_in 
                            ON rooms(new_check_in)
                        ''')
                    
                    conn.commit()
                    logger.info("✅ PostgreSQL database schema đã được khởi tạo với cột mới cho newGuest")
                    return True
                    
            except Exception as e:
                logger.error(f"❌ Lỗi khởi tạo PostgreSQL database (lần {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.error("❌ Không thể khởi tạo database sau nhiều lần thử")
                    return False
                import time
                time.sleep(2)  # Chờ 2 giây trước khi thử lại
        return False
    
    def migrate_to_new_schema(self):
        """Migration script để thêm các cột mới và chuyển đổi kiểu dữ liệu"""
        logger.info("🔄 Đang chạy migration để thêm cột mới...")
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # 1. Kiểm tra xem các cột mới đã tồn tại chưa
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'rooms' 
                        AND column_name = 'new_guest_name'
                    """)
                    
                    if cur.fetchone():
                        logger.info("✅ Các cột newGuest đã tồn tại, bỏ qua migration")
                        return True
                    
                    logger.info("🔄 Bắt đầu migration...")
                    
                    # 2. Tạo bảng tạm thời với cấu trúc mới
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS rooms_new (
                            room_no VARCHAR(10) PRIMARY KEY,
                            room_type VARCHAR(50) NOT NULL,
                            room_status VARCHAR(20) NOT NULL DEFAULT 'vc',
                            guest_name TEXT DEFAULT '',
                            check_in DATE,
                            check_out DATE,
                            new_guest_name TEXT DEFAULT '',
                            new_check_in DATE,
                            new_check_out DATE,
                            notes TEXT DEFAULT '',
                            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    # 3. Copy dữ liệu từ bảng cũ sang bảng mới
                    cur.execute('''
                        INSERT INTO rooms_new 
                        (room_no, room_type, room_status, guest_name, notes, last_updated, created_at)
                        SELECT 
                            room_no, room_type, room_status, guest_name, notes, last_updated, created_at
                        FROM rooms
                    ''')
                    
                    # 4. Xóa bảng cũ và đổi tên bảng mới
                    cur.execute('DROP TABLE rooms')
                    cur.execute('ALTER TABLE rooms_new RENAME TO rooms')
                    
                    # 5. Tạo lại indexes
                    cur.execute('CREATE INDEX idx_rooms_status ON rooms(room_status)')
                    cur.execute('CREATE INDEX idx_rooms_check_in ON rooms(check_in)')
                    cur.execute('CREATE INDEX idx_rooms_new_check_in ON rooms(new_check_in)')
                    
                    conn.commit()
                    logger.info("✅ Migration thành công! Đã thêm cột mới cho newGuest")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Lỗi migration: {e}")
            return False

    def safe_initialize_database(self):
        """Khởi tạo database an toàn với migration nếu cần"""
        # Trước tiên khởi tạo database
        success = self.initialize_database()
        
        # Sau đó kiểm tra và chạy migration nếu cần
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Kiểm tra kiểu dữ liệu của check_in
                    cur.execute("""
                        SELECT data_type 
                        FROM information_schema.columns 
                        WHERE table_name = 'rooms' 
                        AND column_name = 'check_in'
                    """)
                    
                    result = cur.fetchone()
                    if result and result[0] == 'character varying':
                        logger.warning("⚠️  Database đang dùng VARCHAR cho check_in, cần migration")
                        # Chạy migration
                        return self.migrate_to_new_schema()
        
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra cấu trúc database: {e}")
        
        return success

    # Giữ nguyên các phương thức khác...
    # ... (phần còn lại của class giữ nguyên)