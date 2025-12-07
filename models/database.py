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
        """Khởi tạo database với schema hoàn chỉnh cho PostgreSQL - Tương thích với app.py"""
        logger.info("🔄 Đang khởi tạo database schema...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Bảng rooms - thay thế rooms.json với đầy đủ thông tin khách mới
                        cur.execute('''
                            CREATE TABLE IF NOT EXISTS rooms (
                                room_no VARCHAR(10) PRIMARY KEY,
                                room_type VARCHAR(50) NOT NULL,
                                room_status VARCHAR(20) NOT NULL DEFAULT 'vc',
                                
                                -- Thông tin khách hiện tại (current guest)
                                guest_name TEXT DEFAULT '',
                                check_in VARCHAR(20) DEFAULT '',
                                check_out VARCHAR(20) DEFAULT '',
                                current_guest_pax INTEGER DEFAULT 0,
                                
                                -- Thông tin khách mới (new guest)
                                new_guest_name TEXT DEFAULT '',
                                new_check_in VARCHAR(20) DEFAULT '',
                                new_check_out VARCHAR(20) DEFAULT '',
                                new_guest_pax INTEGER DEFAULT 0,
                                
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
                        
                        # Bảng sync_history - theo dõi đồng bộ Google Sheets
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
                    
                    conn.commit()
                    logger.info("✅ PostgreSQL database schema đã được khởi tạo")
                    return True
                    
            except Exception as e:
                logger.error(f"❌ Lỗi khởi tạo PostgreSQL database (lần {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.error("❌ Không thể khởi tạo database sau nhiều lần thử")
                    return False
                import time
                time.sleep(2)  # Chờ 2 giây trước khi thử lại
        return False

    # Giữ nguyên phương thức cũ để tương thích
    def init_database(self):
        """Phương thức cũ để tương thích - gọi initialize_database()"""
        return self.initialize_database()

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
        """
        Context manager cho PostgreSQL connection
        
        Usage:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM rooms")
                    result = cur.fetchall()
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.get_connection_params())
            conn.autocommit = False
            yield conn
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Lỗi kết nối PostgreSQL: {e}")
            raise ConnectionError(f"Không thể kết nối đến database: {e}")
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ PostgreSQL error: {e}")
            raise
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Unexpected error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def test_connection(self):
        """Test kết nối PostgreSQL database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version(), NOW() as current_time")
                    result = cur.fetchone()
                    version, current_time = result
                    
                    # Kiểm tra số lượng bảng
                    cur.execute('''
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    ''')
                    table_count = cur.fetchone()[0]
                    
                    logger.info(f"✅ Kết nối PostgreSQL thành công")
                    logger.info(f"📊 Database Version: {version.split(',')[0]}")
                    logger.info(f"🕒 Server Time: {current_time}")
                    logger.info(f"🗃️  Table Count: {table_count}")
                    
                    return {
                        'status': 'connected',
                        'version': version,
                        'server_time': current_time,
                        'table_count': table_count
                    }
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối PostgreSQL: {e}")
            return {
                'status': 'disconnected',
                'error': str(e)
            }

    def get_database_info(self):
        """Lấy thông tin chi tiết về database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Thông tin tổng quan
                    cur.execute('''
                        SELECT 
                            (SELECT COUNT(*) FROM rooms) as room_count,
                            (SELECT COUNT(*) FROM activity_logs) as log_count,
                            (SELECT COUNT(*) FROM sync_history) as sync_count,
                            (SELECT MAX(timestamp) FROM activity_logs) as latest_activity,
                            (SELECT MAX(sync_time) FROM sync_history) as latest_sync
                    ''')
                    info = cur.fetchone()
                    
                    # Thống kê trạng thái phòng
                    cur.execute('''
                        SELECT room_status, COUNT(*) as count
                        FROM rooms 
                        GROUP BY room_status 
                        ORDER BY count DESC
                    ''')
                    status_stats = cur.fetchall()
                    
                    return {
                        'room_count': info[0],
                        'log_count': info[1],
                        'sync_count': info[2],
                        'latest_activity': info[3],
                        'latest_sync': info[4],
                        'status_stats': dict(status_stats)
                    }
                    
        except Exception as e:
            logger.error(f"❌ Lỗi lấy thông tin database: {e}")
            return {}

    def execute_query(self, query, params=None, return_result=True):
        """
        Thực thi query một cách an toàn
        
        Args:
            query: SQL query string
            params: Parameters cho query
            return_result: Có trả về kết quả không
            
        Returns:
            List of dicts nếu return_result=True, None nếu không
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    
                    if return_result and cur.description:
                        columns = [desc[0] for desc in cur.description]
                        rows = cur.fetchall()
                        return [dict(zip(columns, row)) for row in rows]
                    else:
                        conn.commit()
                        return None
                        
        except Exception as e:
            logger.error(f"❌ Lỗi thực thi query: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

    def health_check(self):
        """Health check chi tiết cho database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Kiểm tra connection cơ bản
                    cur.execute("SELECT 1 as test")
                    basic_test = cur.fetchone()[0]
                    
                    # Kiểm tra các bảng quan trọng
                    cur.execute('''
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('rooms', 'activity_logs', 'sync_history')
                    ''')
                    required_tables = {row[0] for row in cur.fetchall()}
                    
                    # Kiểm tra số lượng bản ghi
                    cur.execute("SELECT COUNT(*) FROM rooms")
                    room_count = cur.fetchone()[0]
                    
                    cur.execute("SELECT COUNT(*) FROM activity_logs")
                    log_count = cur.fetchone()[0]
                    
                    health_status = {
                        'status': 'healthy' if basic_test == 1 else 'unhealthy',
                        'database': 'PostgreSQL',
                        'required_tables': list(required_tables),
                        'missing_tables': list(set(['rooms', 'activity_logs', 'sync_history']) - required_tables),
                        'room_count': room_count,
                        'log_count': log_count,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    if health_status['missing_tables']:
                        health_status['status'] = 'degraded'
                        logger.warning(f"⚠️ Missing tables: {health_status['missing_tables']}")
                    
                    return health_status
                    
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def cleanup_old_logs(self, days_to_keep=30):
        """
        Dọn dẹp logs cũ để giữ database gọn gàng
        
        Args:
            days_to_keep: Số ngày giữ lại logs
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        DELETE FROM activity_logs 
                        WHERE timestamp < CURRENT_DATE - INTERVAL '%s days'
                    ''', (days_to_keep,))
                    
                    deleted_count = cur.rowcount
                    conn.commit()
                    
                    logger.info(f"✅ Đã xóa {deleted_count} logs cũ (trước {days_to_keep} ngày)")
                    return deleted_count
                    
        except Exception as e:
            logger.error(f"❌ Lỗi cleanup logs: {e}")
            return 0


# Helper function để tạo database manager instance
def create_db_manager():
    """Factory function để tạo DatabaseManager instance"""
    return DatabaseManager()


if __name__ == '__main__':
    # Test database connection
    try:
        db = DatabaseManager()
        result = db.test_connection()
        
        if result['status'] == 'connected':
            print("🎉 PostgreSQL Database Manager đã sẵn sàng!")
            print(f"📊 Database Info:")
            info = db.get_database_info()
            print(f"   • Rooms: {info.get('room_count', 0)}")
            print(f"   • Activity Logs: {info.get('log_count', 0)}")
            print(f"   • Status Stats: {info.get('status_stats', {})}")
            
            # Test initialize_database method
            print(f"🔄 Testing initialize_database...")
            success = db.initialize_database()
            print(f"   • initialize_database: {'✅ Success' if success else '❌ Failed'}")
            
        else:
            print(f"❌ Lỗi kết nối database: {result.get('error')}")
    except Exception as e:
        print(f"❌ Lỗi khởi tạo DatabaseManager: {e}")