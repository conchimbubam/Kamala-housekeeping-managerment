# test_connection.py
import psycopg2
from config import Config

def test_supabase_connection():
    """Kiểm tra kết nối Supabase"""
    print("🧪 Testing Supabase Connection...")
    print(f"📡 Host: {Config.DB_HOST}")
    print(f"👤 User: {Config.DB_USER}")
    
    try:
        # Kết nối database
        conn = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Test 1: Kiểm tra version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ PostgreSQL Version: {version[0]}")
        
        # Test 2: Kiểm tra bảng
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        if tables:
            print(f"📋 Found {len(tables)} tables:")
            for table in tables:
                print(f"   - {table[0]}")
        else:
            print("📋 No tables found. Need to create tables.")
            
        # Test 3: Kiểm tra connection pool
        cursor.execute("SELECT pg_database_size(current_database());")
        db_size = cursor.fetchone()[0]
        print(f"💾 Database size: {db_size / 1024 / 1024:.2f} MB")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 SUPABASE CONNECTION SUCCESSFUL!")
        return True
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection Error: {e}")
        print("\n🔧 Troubleshooting Steps:")
        print("1. Kiểm tra password có ký tự @ - cần encode thành %40")
        print("2. Vào Supabase Dashboard → Settings → Database")
        print("3. Kiểm tra phần 'Connection Pooling'")
        print("4. Kiểm tra IP Restrictions")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        return False

if __name__ == "__main__":
    Config.print_config_summary()
    test_supabase_connection()