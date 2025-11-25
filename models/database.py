# models/database.py
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Quản lý kết nối và thao tác với PostgreSQL database trên Render"""
    
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_config = {
            'host': db_host,
            'port': db_port,
            'database': db_name,
            'user': db_user,
            'password': db_password
        }
        self._initialize_database()
    
    def _get_connection(self):
        """Tạo kết nối mới đến PostgreSQL database"""
        try:
            conn = psycopg2.connect(
                **self.db_config,
                cursor_factory=RealDictCursor,
                connect_timeout=10
            )
            logger.info(f"✅ Kết nối PostgreSQL thành công: {self.db_config['host']}")
            return conn
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối PostgreSQL: {e}")
            logger.error(f"📌 Connection details: host={self.db_config['host']}, db={self.db_config['database']}, user={self.db_config['user']}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager để quản lý kết nối database"""
        conn = None
        try:
            conn = self._get_connection()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Lỗi database transaction: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self, dict_cursor=True):
        """Context manager để quản lý cursor"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if dict_cursor:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
            else:
                cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Lỗi database cursor: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def _initialize_database(self):
        """Khởi tạo database và các bảng nếu chưa tồn tại"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Tạo bảng rooms
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS rooms (
                            id SERIAL PRIMARY KEY,
                            room_no VARCHAR(10) UNIQUE NOT NULL,
                            room_type VARCHAR(50),
                            room_status VARCHAR(20) DEFAULT 'vd',
                            guest_name TEXT,
                            check_in DATE,
                            check_out DATE,
                            notes TEXT,
                            floor INTEGER DEFAULT 1,
                            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_by VARCHAR(100),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Tạo bảng hk_logs cho House Keeping
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS hk_logs (
                            id SERIAL PRIMARY KEY,
                            room_no VARCHAR(10) NOT NULL,
                            old_status VARCHAR(20),
                            new_status VARCHAR(20),
                            changed_by VARCHAR(100),
                            department VARCHAR(10),
                            change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            notes TEXT
                        )
                    """)
                    
                    # Tạo bảng file_info để lưu thông tin file Google Sheets
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS file_info (
                            id SERIAL PRIMARY KEY,
                            file_name VARCHAR(255),
                            last_modified TIMESTAMP,
                            total_rows INTEGER,
                            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            sync_by VARCHAR(100)
                        )
                    """)
                    
                    # Tạo indexes để tối ưu hiệu suất
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rooms_room_no ON rooms(room_no)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(room_status)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rooms_floor ON rooms(floor)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hk_logs_room_no ON hk_logs(room_no)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hk_logs_time ON hk_logs(change_time)")
                    
                    logger.info("✅ Đã khởi tạo PostgreSQL database và các bảng")
                    
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo database: {e}")
            raise
    
    def execute_query(self, query, params=None, fetch=True):
        """Thực thi query và trả về kết quả"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                if fetch and cursor.description:
                    return cursor.fetchall()
                return None
        except Exception as e:
            logger.error(f"❌ Lỗi execute query: {e}")
            logger.error(f"📌 Query: {query}")
            logger.error(f"📌 Params: {params}")
            raise
    
    def execute_insert(self, query, params=None, return_id=True):
        """Thực thi INSERT query và trả về ID nếu cần"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                if return_id and cursor.description:
                    result = cursor.fetchone()
                    return result['id'] if result else None
                return None
        except Exception as e:
            logger.error(f"❌ Lỗi execute insert: {e}")
            raise
    
    def execute_update(self, query, params=None):
        """Thực thi UPDATE query"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.rowcount
        except Exception as e:
            logger.error(f"❌ Lỗi execute update: {e}")
            raise
    
    def execute_delete(self, query, params=None):
        """Thực thi DELETE query"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.rowcount
        except Exception as e:
            logger.error(f"❌ Lỗi execute delete: {e}")
            raise
    
    def get_one(self, query, params=None):
        """Lấy một bản ghi duy nhất"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"❌ Lỗi get one: {e}")
            raise
    
    def get_all(self, query, params=None):
        """Lấy tất cả bản ghi"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi get all: {e}")
            raise
    
    def table_exists(self, table_name):
        """Kiểm tra xem bảng có tồn tại không"""
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """
            result = self.get_one(query, (table_name,))
            return result['exists'] if result else False
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra bảng: {e}")
            return False
    
    def is_database_empty(self):
        """Kiểm tra database có dữ liệu không"""
        try:
            # Kiểm tra xem bảng rooms có dữ liệu không
            query = "SELECT COUNT(*) as count FROM rooms"
            result = self.get_one(query)
            return result['count'] == 0 if result else True
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra database empty: {e}")
            return True
    
    def upsert_room(self, room_data):
        """Insert hoặc update phòng nếu đã tồn tại"""
        try:
            query = """
                INSERT INTO rooms (room_no, room_type, room_status, guest_name, check_in, check_out, notes, floor, last_updated, updated_by)
                VALUES (%(room_no)s, %(room_type)s, %(room_status)s, %(guest_name)s, %(check_in)s, %(check_out)s, %(notes)s, %(floor)s, %(last_updated)s, %(updated_by)s)
                ON CONFLICT (room_no) 
                DO UPDATE SET
                    room_type = EXCLUDED.room_type,
                    room_status = EXCLUDED.room_status,
                    guest_name = EXCLUDED.guest_name,
                    check_in = EXCLUDED.check_in,
                    check_out = EXCLUDED.check_out,
                    notes = EXCLUDED.notes,
                    floor = EXCLUDED.floor,
                    last_updated = EXCLUDED.last_updated,
                    updated_by = EXCLUDED.updated_by
            """
            return self.execute_update(query, room_data)
        except Exception as e:
            logger.error(f"❌ Lỗi upsert room: {e}")
            raise
    
    def get_database_stats(self):
        """Lấy thống kê database"""
        try:
            stats = {}
            
            # Số lượng bản ghi trong mỗi bảng
            tables = ['rooms', 'hk_logs', 'file_info']
            for table in tables:
                query = f"SELECT COUNT(*) as count FROM {table}"
                result = self.get_one(query)
                stats[f'{table}_count'] = result['count'] if result else 0
            
            # Kích thước database
            query = """
                SELECT 
                    pg_size_pretty(pg_database_size(current_database())) as db_size,
                    pg_database_size(current_database()) as db_size_bytes
            """
            result = self.get_one(query)
            if result:
                stats['database_size'] = result['db_size']
                stats['database_size_bytes'] = result['db_size_bytes']
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy database stats: {e}")
            return {}
    
    def test_connection(self):
        """Kiểm tra kết nối database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 as test, version() as version")
                    result = cursor.fetchone()
                    logger.info(f"✅ Database connection test: {result['test']}")
                    logger.info(f"📊 Database version: {result['version']}")
                    return result['test'] == 1 if result else False
        except Exception as e:
            logger.error(f"❌ Lỗi test connection: {e}")
            return False
    
    def close_all_connections(self):
        """Đóng tất cả kết nối (chủ yếu cho cleanup)"""
        logger.info("✅ Database connection pool closed")


# Utility functions cho ứng dụng
def create_database_manager_from_config(config):
    """Tạo DatabaseManager từ config object"""
    return DatabaseManager(
        db_host=config.DB_HOST,
        db_port=config.DB_PORT,
        db_name=config.DB_NAME,
        db_user=config.DB_USER,
        db_password=config.DB_PASSWORD
    )