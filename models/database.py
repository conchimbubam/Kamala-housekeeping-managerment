# models/database.py
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from contextlib import contextmanager
import os
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_url=None):
        # Sử dụng DATABASE_URL từ environment variable (Render PostgreSQL)
        self.db_url = db_url or os.getenv('DATABASE_URL')
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Parse database URL
        self.parsed_url = urlparse(self.db_url)
        self.init_database()
    
    def get_connection_params(self):
        """Trích xuất thông tin kết nối từ URL"""
        params = {
            'host': self.parsed_url.hostname,
            'port': self.parsed_url.port or 5432,
            'database': self.parsed_url.path[1:],  # Bỏ qua '/' đầu tiên
            'user': self.parsed_url.username,
            'password': self.parsed_url.password,
        }
        
        # Thêm SSL cho production (Render PostgreSQL)
        if self.parsed_url.hostname and 'render.com' in self.parsed_url.hostname:
            params['sslmode'] = 'require'
        
        return params
    
    def init_database(self):
        """Khởi tạo database với schema hoàn chỉnh cho PostgreSQL"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Bảng rooms - thay thế rooms.json
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS rooms (
                            room_no TEXT PRIMARY KEY,
                            room_type TEXT NOT NULL,
                            room_status TEXT NOT NULL DEFAULT 'vc',
                            guest_name TEXT DEFAULT '',
                            check_in TEXT DEFAULT '',
                            check_out TEXT DEFAULT '',
                            notes TEXT DEFAULT '',
                            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    # Bảng activity_logs - thay thế hk_activity_log.json
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS activity_logs (
                            id SERIAL PRIMARY KEY,
                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS sync_history (
                            id SERIAL PRIMARY KEY,
                            sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            synced_by TEXT NOT NULL,
                            total_rooms INTEGER,
                            success BOOLEAN DEFAULT TRUE,
                            error_message TEXT
                        )
                    ''')
                    
                    # Tạo indexes cho hiệu suất
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(room_status)')
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON activity_logs(timestamp)')
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_activity_room ON activity_logs(room_no)')
                    cur.execute('CREATE INDEX IF NOT EXISTS idx_activity_type ON activity_logs(action_type)')
                
                conn.commit()
                logger.info("✅ PostgreSQL database schema đã được khởi tạo")
                
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo PostgreSQL database: {e}")
            raise

    def is_database_empty(self):
        """Kiểm tra database có dữ liệu không"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT COUNT(*) as count FROM rooms')
                    result = cur.fetchone()
                    return result[0] == 0
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra database: {e}")
            return True

    @contextmanager 
    def get_connection(self):
        """Context manager cho PostgreSQL connection với DictCursor"""
        conn = None
        try:
            conn = psycopg2.connect(
                **self.get_connection_params(),
                cursor_factory=RealDictCursor  # Trả về dict-like rows
            )
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def test_connection(self):
        """Test kết nối PostgreSQL database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    result = cur.fetchone()
                    logger.info(f"✅ Kết nối PostgreSQL thành công - Version: {result['version']}")
                    return True
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối PostgreSQL: {e}")
            return False

    def initialize_database(self):
        """Tương thích với app.py - gọi init_database"""
        return self.init_database()

    def backup_database(self):
        """Sao lưu database (placeholder cho tính năng future)"""
        try:
            # Đây là placeholder - trong thực tế cần cài đặt backup phù hợp
            logger.info("✅ Database backup initiated (placeholder)")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi sao lưu database: {e}")
            return False

    def get_database_size(self):
        """Lấy thông tin kích thước database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT 
                            pg_size_pretty(pg_database_size(current_database())) as size,
                            current_database() as database_name
                    ''')
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy kích thước database: {e}")
            return {'size': 'Unknown', 'database_name': 'Unknown'}

    def get_table_stats(self):
        """Lấy thống kê các bảng"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT 
                            table_name,
                            pg_size_pretty(pg_total_relation_size('"' || table_name || '"')) as size,
                            (SELECT COUNT(*) FROM "' || table_name || '"') as row_count
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    ''')
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy thống kê bảng: {e}")
            return []

    def vacuum_database(self):
        """Dọn dẹp và tối ưu database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('VACUUM ANALYZE')
                conn.commit()
            logger.info("✅ Đã hoàn thành VACUUM ANALYZE database")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi VACUUM database: {e}")
            return False

    def check_connection_health(self):
        """Kiểm tra sức khỏe kết nối database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Kiểm tra các bảng có tồn tại không
                    cur.execute('''
                        SELECT 
                            COUNT(*) as tables_count
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name IN ('rooms', 'activity_logs', 'sync_history')
                    ''')
                    tables_result = cur.fetchone()
                    
                    # Kiểm tra số lượng bản ghi
                    cur.execute('SELECT COUNT(*) as rooms_count FROM rooms')
                    rooms_count = cur.fetchone()
                    
                    cur.execute('SELECT COUNT(*) as logs_count FROM activity_logs')
                    logs_count = cur.fetchone()
                    
                    return {
                        'status': 'healthy',
                        'tables_count': tables_result['tables_count'],
                        'rooms_count': rooms_count['rooms_count'],
                        'logs_count': logs_count['logs_count'],
                        'database': self.parsed_url.database,
                        'host': self.parsed_url.hostname
                    }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'database': self.parsed_url.database if hasattr(self, 'parsed_url') else 'unknown'
            }

    def execute_raw_query(self, query, params=None):
        """Thực thi query raw (chỉ dùng cho admin)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    if query.strip().upper().startswith('SELECT'):
                        result = cur.fetchall()
                        conn.commit()
                        return {'success': True, 'data': result}
                    else:
                        conn.commit()
                        return {'success': True, 'rows_affected': cur.rowcount}
        except Exception as e:
            logger.error(f"❌ Lỗi execute raw query: {e}")
            return {'success': False, 'error': str(e)}

    def get_connection_pool(self):
        """Tạo connection pool (placeholder cho scaling future)"""
        # Placeholder cho connection pool implementation
        # Trong production có thể sử dụng psycopg2.pool.SimpleConnectionPool
        logger.info("📍 Connection pool placeholder - using single connection")
        return self

    def close_all_connections(self):
        """Đóng tất cả connections (placeholder)"""
        logger.info("📍 Close connections placeholder - no pool implemented")
        return True


# Test function để kiểm tra database
def test_database_connection():
    """Test kết nối database độc lập"""
    try:
        db = DatabaseManager()
        if db.test_connection():
            print("✅ Database connection test: PASSED")
            
            # Kiểm tra schema
            health = db.check_connection_health()
            print(f"✅ Database health: {health}")
            
            # Kiểm tra kích thước
            size_info = db.get_database_size()
            print(f"✅ Database size: {size_info}")
            
            return True
        else:
            print("❌ Database connection test: FAILED")
            return False
    except Exception as e:
        print(f"❌ Database test error: {e}")
        return False


if __name__ == '__main__':
    print("🧪 Testing PostgreSQL Database Connection...")
    test_database_connection()