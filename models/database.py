# models/database.py
import sqlite3
import logging
from contextlib import contextmanager
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path='data/hotel.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Khởi tạo database với schema hoàn chỉnh"""
        # Đảm bảo thư mục data tồn tại
        data_dir = os.path.dirname(self.db_path)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            logger.info(f"✅ Đã tạo thư mục {data_dir}")
        
        with self.get_connection() as conn:
            # Bảng rooms - thay thế rooms.json
            conn.execute('''
                CREATE TABLE IF NOT EXISTS rooms (
                    room_no TEXT PRIMARY KEY,
                    room_type TEXT NOT NULL,
                    room_status TEXT NOT NULL DEFAULT 'vc',
                    guest_name TEXT DEFAULT '',
                    check_in TEXT DEFAULT '',
                    check_out TEXT DEFAULT '',
                    notes TEXT DEFAULT '',
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Bảng activity_logs - thay thế hk_activity_log.json
            conn.execute('''
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    user_name TEXT NOT NULL,
                    user_department TEXT NOT NULL,
                    room_no TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    old_status TEXT,
                    new_status TEXT,
                    action_detail TEXT,
                    ip_address TEXT
                )
            ''')
            
            # Bảng sync_history - theo dõi đồng bộ Google Sheets
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    synced_by TEXT NOT NULL,
                    total_rooms INTEGER,
                    success BOOLEAN DEFAULT 1,
                    error_message TEXT
                )
            ''')
            
            # Tạo indexes cho hiệu suất
            conn.execute('CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(room_status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON activity_logs(timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_activity_room ON activity_logs(room_no)')
            
            conn.commit()
            print("✅ Database schema đã được khởi tạo")

    def is_database_empty(self):
        """Kiểm tra database có dữ liệu không"""
        with self.get_connection() as conn:
            result = conn.execute('SELECT COUNT(*) as count FROM rooms').fetchone()
            return result[0] == 0

    @contextmanager 
    def get_connection(self):
        """Context manager cho database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Trả về dict-like rows
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def test_connection(self):
        """Test kết nối database"""
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT sqlite_version()").fetchone()
                print(f"✅ Kết nối SQLite thành công - Version: {result[0]}")
                return True
        except Exception as e:
            print(f"❌ Lỗi kết nối database: {e}")
            return False