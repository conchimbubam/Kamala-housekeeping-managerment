# models/data_processor.py
import requests
import logging
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, db_manager, api_key=None, spreadsheet_id=None, range_name=None):
        self.db = db_manager
        self.api_key = api_key or Config.API_KEY
        self.spreadsheet_id = spreadsheet_id or Config.SPREADSHEET_ID
        self.range_name = range_name or Config.RANGE_NAME
    
    def initialize_rooms_from_google_sheets(self, user_info="System"):
        """Khởi tạo dữ liệu phòng từ Google Sheets lần đầu tiên"""
        try:
            # Lấy dữ liệu từ Google Sheets
            raw_data = self.fetch_data_from_sheets()
            if not raw_data:
                logger.warning("Không có dữ liệu từ Google Sheets")
                return False
            
            rooms_data = self.process_room_data(raw_data)
            
            if not rooms_data:
                logger.warning("Không có dữ liệu phòng sau khi xử lý")
                return False
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Xóa dữ liệu cũ (nếu có) và insert mới
                    cur.execute('DELETE FROM rooms')
                    
                    for room in rooms_data:
                        # Xử lý thông tin khách
                        current_guest = room.get('currentGuest', {})
                        new_guest = room.get('newGuest', {})
                        
                        cur.execute('''
                            INSERT INTO rooms 
                            (room_no, room_type, room_status, guest_name, check_in, check_out, notes)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            room.get('roomNo', ''),
                            room.get('roomType', ''),
                            room.get('roomStatus', 'vc'),
                            current_guest.get('name', ''),
                            current_guest.get('checkIn', ''),
                            current_guest.get('checkOut', ''),
                            f"Pax: {current_guest.get('pax', 0)}" if current_guest.get('pax', 0) else ''
                        ))
                    
                    # Ghi log sync
                    cur.execute('''
                        INSERT INTO sync_history (synced_by, total_rooms, success)
                        VALUES (%s, %s, %s)
                    ''', (user_info, len(rooms_data), True))
                
                conn.commit()
            
            logger.info(f"✅ Đã khởi tạo {len(rooms_data)} phòng từ Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo từ Google Sheets: {e}")
            
            # Ghi log lỗi
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO sync_history (synced_by, total_rooms, success, error_message)
                        VALUES (%s, %s, %s, %s)
                    ''', (user_info, 0, False, str(e)))
                conn.commit()
            
            return False

    # ==================== GOOGLE SHEETS METHODS ====================
    
    def fetch_data_from_sheets(self):
        """Lấy dữ liệu từ Google Sheets"""
        if not all([self.api_key, self.spreadsheet_id, self.range_name]):
            logger.error("Thiếu cấu hình Google Sheets API")
            return None
            
        url = f'https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{self.range_name}?key={self.api_key}'
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy dữ liệu từ Google Sheets: {e}")
            return None
        except Exception as e:
            logger.error(f"Lỗi không xác định khi fetch Google Sheets: {e}")
            return None

    def clean_room_status(self, status):
        """Làm sạch và chuẩn hóa trạng thái phòng"""
        if not status:
            return 'vc'  # Mặc định là Vacant Clean
            
        status = str(status).strip().upper()
        
        status_mapping = {
            'VD': 'vd', 'OD': 'od', 'VC': 'vc', 'OC': 'oc',
            'DND': 'dnd', 'NN': 'nn', 'LOCK': 'lock', 'IP': 'ip', 'DO': 'do'
        }
        
        for key, value in status_mapping.items():
            if key == status:
                return value
        
        # Xử lý các trạng thái kết hợp
        if 'VD' in status and 'ARR' in status:
            return 'vd/arr'
        elif 'VC' in status and 'ARR' in status:
            return 'vc/arr'
        elif 'DO' in status and 'ARR' in status:
            return 'do/arr'
        elif 'VD' in status:
            return 'vd'
        elif 'VC' in status:
            return 'vc'
        elif 'DO' in status:
            return 'do'
        elif 'OD' in status:
            return 'od'
        elif 'OC' in status:
            return 'oc'
        elif 'IP' in status:
            return 'ip'
        elif 'DND' in status:
            return 'dnd'
        elif 'NN' in status:
            return 'nn'
        else:
            logger.warning(f"Trạng thái không xác định: {status}, mặc định về 'vc'")
            return 'vc'

    def parse_date(self, date_str):
        """Chuyển đổi định dạng ngày"""
        if not date_str or date_str in ['00-01-00', '01-01-00', '01-00-00']:
            return ''
        
        date_str = str(date_str).strip()
        
        try:
            import re
            patterns = [
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',
                r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY-MM-DD
            ]
            
            for pattern in patterns:
                match = re.search(pattern, date_str)
                if match:
                    groups = match.groups()
                    
                    if len(groups) == 3:
                        if len(groups[2]) == 4:  # YYYY format
                            day, month, year = groups[0], groups[1], groups[2]
                        else:  # DD-MM-YY format
                            day, month, year = groups[0], groups[1], groups[2]
                        
                        # Đảm bảo year có 2 chữ số
                        if len(year) == 4:
                            year = year[2:]
                        
                        day = day.zfill(2)
                        month = month.zfill(2)
                        year = year.zfill(2)
                        
                        return f"{day}-{month}-{year}"
            
            # Nếu không match pattern nào, trả về chuỗi gốc (đã được làm sạch)
            return date_str.replace('/', '-')
        except Exception as e:
            logger.warning(f"Lỗi phân tích ngày tháng: {date_str}, Error: {e}")
            return ''

    def parse_pax(self, pax_str):
        """Chuyển đổi số lượng khách sang integer"""
        if not pax_str:
            return 0
        
        try:
            import re
            pax_clean = re.sub(r'[^\d]', '', str(pax_str))
            if pax_clean:
                return int(pax_clean)
            return 0
        except (ValueError, TypeError) as e:
            logger.warning(f"Lỗi parse pax: {pax_str}, Error: {e}")
            return 0

    def clean_guest_name(self, name_str):
        """Làm sạch tên khách"""
        if not name_str:
            return ''
        
        name_clean = str(name_str).strip()
        # Loại bỏ các ký tự đặc biệt không cần thiết, giữ lại dấu cách và chữ cái
        import re
        name_clean = re.sub(r'[^\w\s]', '', name_clean)
        return name_clean

    def process_room_data(self, raw_data):
        """Xử lý dữ liệu thô từ Google Sheets"""
        if not raw_data or 'values' not in raw_data:
            return []
        
        values = raw_data['values']
        if len(values) < 2:
            return []
        
        rooms_data = []
        
        for row_index, row in enumerate(values[1:], start=2):
            try:
                # Đảm bảo row có đủ 10 cột
                while len(row) < 10:
                    row.append('')
                
                room_no = str(row[0]).strip() if row[0] else ''
                if not room_no:
                    continue
                
                # Xác định room type dựa trên room number
                room_type = self.determine_room_type(room_no)
                room_status = self.clean_room_status(row[1])
                
                current_guest = {
                    'name': self.clean_guest_name(row[2]),
                    'checkIn': self.parse_date(row[3]),
                    'checkOut': self.parse_date(row[4]),
                    'pax': self.parse_pax(row[5])
                }
                
                new_guest = {
                    'name': self.clean_guest_name(row[6]),
                    'checkIn': self.parse_date(row[7]),
                    'checkOut': self.parse_date(row[8]),
                    'pax': self.parse_pax(row[9])
                }
                
                room_data = {
                    'roomNo': room_no,
                    'roomType': room_type,
                    'roomStatus': room_status,
                    'currentGuest': current_guest,
                    'newGuest': new_guest
                }
                
                rooms_data.append(room_data)
                
            except Exception as e:
                logger.warning(f"Lỗi xử lý dòng {row_index}: {row}. Error: {e}")
                continue
        
        logger.info(f"✅ Đã xử lý {len(rooms_data)} phòng từ Google Sheets")
        return rooms_data

    def determine_room_type(self, room_no):
        """Xác định loại phòng dựa trên số phòng"""
        if not room_no:
            return 'Standard'
        
        room_no = str(room_no).upper()
        
        # Phòng suite
        if any(suite in room_no for suite in ['S', 'SUITE', 'P']):
            return 'Suite'
        
        # Phòng deluxe
        elif any(dlx in room_no for dlx in ['D', 'DLX', 'DELUXE']):
            return 'Deluxe'
        
        # Phòng family
        elif any(fam in room_no for fam in ['F', 'FAM', 'FAMILY']):
            return 'Family'
        
        # Mặc định là Standard
        else:
            return 'Standard'

    # ==================== DATABASE METHODS ====================

    def get_all_rooms(self):
        """Lấy tất cả phòng từ database"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, guest_name, 
                               check_in, check_out, notes, last_updated
                        FROM rooms 
                        ORDER BY room_no
                    ''')
                    
                    rooms = []
                    for row in cur.fetchall():
                        # Parse notes để lấy thông tin pax (nếu có)
                        notes = row['notes'] or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except (ValueError, IndexError):
                                pax = 0
                        
                        rooms.append({
                            'roomNo': row['room_no'],
                            'roomType': row['room_type'],
                            'roomStatus': row['room_status'],
                            'currentGuest': {
                                'name': row['guest_name'] or '',
                                'checkIn': row['check_in'] or '',
                                'checkOut': row['check_out'] or '',
                                'pax': pax
                            },
                            'newGuest': {
                                'name': '',
                                'checkIn': '',
                                'checkOut': '',
                                'pax': 0
                            }
                        })
                    
                    return {'success': True, 'data': rooms}
                    
        except Exception as e:
            logger.error(f"Lỗi get_all_rooms: {e}")
            return {'success': False, 'error': str(e)}

    def get_room_by_number(self, room_no):
        """Lấy thông tin chi tiết một phòng"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        'SELECT * FROM rooms WHERE room_no = %s', 
                        (room_no,)
                    )
                    row = cur.fetchone()
                    
                    if row:
                        # Parse notes để lấy thông tin pax
                        notes = row['notes'] or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except (ValueError, IndexError):
                                pax = 0
                        
                        return {
                            'roomNo': row['room_no'],
                            'roomType': row['room_type'],
                            'roomStatus': row['room_status'],
                            'currentGuest': {
                                'name': row['guest_name'] or '',
                                'checkIn': row['check_in'] or '',
                                'checkOut': row['check_out'] or '',
                                'pax': pax
                            },
                            'newGuest': {
                                'name': '',
                                'checkIn': '',
                                'checkOut': '',
                                'pax': 0
                            }
                        }
                    return None
                    
        except Exception as e:
            logger.error(f"Lỗi get_room_by_number {room_no}: {e}")
            return None

    def update_room_data(self, room_no, updated_data, user_info):
        """Cập nhật thông tin phòng trong database"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Lấy thông tin phòng hiện tại
                    cur.execute(
                        'SELECT * FROM rooms WHERE room_no = %s', 
                        (room_no,)
                    )
                    current_room = cur.fetchone()
                    
                    if not current_room:
                        logger.warning(f"Không tìm thấy phòng {room_no} để cập nhật")
                        return False
                    
                    # Build dynamic update query
                    set_clause = []
                    params = []
                    
                    # Xử lý các trường cập nhật
                    if 'roomStatus' in updated_data:
                        set_clause.append('room_status = %s')
                        params.append(updated_data['roomStatus'])
                    
                    if 'currentGuest' in updated_data:
                        guest_data = updated_data['currentGuest']
                        set_clause.append('guest_name = %s')
                        params.append(guest_data.get('name', ''))
                        
                        set_clause.append('check_in = %s')
                        params.append(guest_data.get('checkIn', ''))
                        
                        set_clause.append('check_out = %s')
                        params.append(guest_data.get('checkOut', ''))
                        
                        # Lưu pax vào notes
                        pax = guest_data.get('pax', 0)
                        notes = f"Pax: {pax}" if pax else ''
                        set_clause.append('notes = %s')
                        params.append(notes)
                    
                    if 'roomType' in updated_data:
                        set_clause.append('room_type = %s')
                        params.append(updated_data['roomType'])
                    
                    if not set_clause:
                        logger.warning(f"Không có trường nào để cập nhật cho phòng {room_no}")
                        return False
                    
                    # Thêm room_no cho WHERE clause
                    params.append(room_no)
                    
                    query = f'''
                        UPDATE rooms 
                        SET {', '.join(set_clause)}, last_updated = CURRENT_TIMESTAMP
                        WHERE room_no = %s
                    '''
                    
                    cur.execute(query, params)
                    conn.commit()
                    
                    logger.info(f"✅ Đã cập nhật phòng {room_no} bởi {user_info}")
                    return True
                    
        except Exception as e:
            logger.error(f"Lỗi update_room_data {room_no}: {e}")
            return False

    def get_statistics(self):
        """Thống kê trạng thái phòng từ database"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_status, COUNT(*) as count 
                        FROM rooms 
                        GROUP BY room_status
                        ORDER BY count DESC
                    ''')
                    
                    stats = {}
                    for row in cur.fetchall():
                        stats[row['room_status']] = row['count']
                    
                    return stats
                    
        except Exception as e:
            logger.error(f"Lỗi get_statistics: {e}")
            return {}

    def get_rooms_by_floor(self):
        """Nhóm phòng theo tầng"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM rooms ORDER BY room_no
                    ''')
                    
                    floors = {}
                    for row in cur.fetchall():
                        room_data = dict(row)
                        floor = room_data['room_no'][0] if room_data['room_no'] and room_data['room_no'][0].isdigit() else '0'
                        
                        if floor not in floors:
                            floors[floor] = []
                        
                        # Parse notes để lấy pax
                        notes = room_data['notes'] or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except (ValueError, IndexError):
                                pax = 0
                        
                        floors[floor].append({
                            'roomNo': room_data['room_no'],
                            'roomType': room_data['room_type'],
                            'roomStatus': room_data['room_status'],
                            'currentGuest': {
                                'name': room_data['guest_name'] or '',
                                'checkIn': room_data['check_in'] or '',
                                'checkOut': room_data['check_out'] or '',
                                'pax': pax
                            },
                            'newGuest': {
                                'name': '',
                                'checkIn': '',
                                'checkOut': '',
                                'pax': 0
                            }
                        })
                    
                    return floors
                    
        except Exception as e:
            logger.error(f"Lỗi get_rooms_by_floor: {e}")
            return {}

    def get_room_info(self):
        """Lấy thông tin file/data từ database"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Lấy thông tin sync cuối cùng
                    cur.execute('''
                        SELECT sync_time, synced_by, total_rooms 
                        FROM sync_history 
                        WHERE success = true 
                        ORDER BY sync_time DESC 
                        LIMIT 1
                    ''')
                    last_sync = cur.fetchone()
                    
                    # Lấy tổng số phòng hiện tại
                    cur.execute('SELECT COUNT(*) as count FROM rooms')
                    total_rooms = cur.fetchone()['count']
                    
                    if last_sync:
                        return {
                            'last_updated': last_sync['sync_time'].isoformat() if last_sync['sync_time'] else None,
                            'last_updated_by': last_sync['synced_by'],
                            'total_rooms': total_rooms,
                            'last_sync_rooms': last_sync['total_rooms']
                        }
                    else:
                        return {
                            'last_updated': None,
                            'last_updated_by': None,
                            'total_rooms': total_rooms
                        }
                        
        except Exception as e:
            logger.error(f"Lỗi get_room_info: {e}")
            return {}

    def search_rooms(self, search_term):
        """Tìm kiếm phòng theo số phòng, tên khách, hoặc trạng thái"""
        try:
            search_term = f"%{search_term}%"
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, guest_name, 
                               check_in, check_out, notes
                        FROM rooms 
                        WHERE room_no ILIKE %s 
                           OR guest_name ILIKE %s 
                           OR room_status ILIKE %s
                        ORDER BY room_no
                    ''', (search_term, search_term, search_term))
                    
                    rooms = []
                    for row in cur.fetchall():
                        # Parse notes để lấy thông tin pax
                        notes = row['notes'] or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except (ValueError, IndexError):
                                pax = 0
                        
                        rooms.append({
                            'roomNo': row['room_no'],
                            'roomType': row['room_type'],
                            'roomStatus': row['room_status'],
                            'currentGuest': {
                                'name': row['guest_name'] or '',
                                'checkIn': row['check_in'] or '',
                                'checkOut': row['check_out'] or '',
                                'pax': pax
                            }
                        })
                    
                    return {'success': True, 'data': rooms, 'total': len(rooms)}
                    
        except Exception as e:
            logger.error(f"Lỗi search_rooms: {e}")
            return {'success': False, 'error': str(e)}

    def get_rooms_by_status(self, status):
        """Lấy danh sách phòng theo trạng thái cụ thể"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, guest_name, 
                               check_in, check_out, notes
                        FROM rooms 
                        WHERE room_status = %s
                        ORDER BY room_no
                    ''', (status,))
                    
                    rooms = []
                    for row in cur.fetchall():
                        notes = row['notes'] or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except (ValueError, IndexError):
                                pax = 0
                        
                        rooms.append({
                            'roomNo': row['room_no'],
                            'roomType': row['room_type'],
                            'roomStatus': row['room_status'],
                            'currentGuest': {
                                'name': row['guest_name'] or '',
                                'checkIn': row['check_in'] or '',
                                'checkOut': row['check_out'] or '',
                                'pax': pax
                            }
                        })
                    
                    return {'success': True, 'data': rooms, 'total': len(rooms)}
                    
        except Exception as e:
            logger.error(f"Lỗi get_rooms_by_status {status}: {e}")
            return {'success': False, 'error': str(e)}

    # ==================== COMPATIBILITY METHODS ====================

    def load_rooms_data(self):
        """Tương thích với code cũ - trả về danh sách phòng"""
        result = self.get_all_rooms()
        return result.get('data', []) if result['success'] else []

    def update_from_google_sheets(self, user_info=None):
        """Tương thích với code cũ - cập nhật từ Google Sheets"""
        success = self.initialize_rooms_from_google_sheets(user_info)
        if success:
            result = self.get_all_rooms()
            return result.get('data', []) if result['success'] else []
        else:
            raise Exception("Không thể cập nhật từ Google Sheets")

    def test_google_sheets_connection(self):
        """Test kết nối Google Sheets"""
        try:
            raw_data = self.fetch_data_from_sheets()
            if raw_data and 'values' in raw_data:
                return {
                    'success': True,
                    'total_rows': len(raw_data['values']) - 1 if len(raw_data['values']) > 1 else 0,
                    'message': 'Kết nối Google Sheets thành công'
                }
            else:
                return {
                    'success': False,
                    'error': 'Không thể kết nối đến Google Sheets hoặc không có dữ liệu'
                }
        except Exception as e:
            return {
                'success': False,
                'error': f'Lỗi kết nối Google Sheets: {str(e)}'
            }


# Test function
def test_data_processor():
    """Test DataProcessor độc lập"""
    try:
        from database import DatabaseManager
        
        db = DatabaseManager()
        processor = DataProcessor(db)
        
        # Test database connection
        rooms_result = processor.get_all_rooms()
        if rooms_result['success']:
            print(f"✅ Database test: PASSED - {len(rooms_result['data'])} rooms")
        else:
            print(f"❌ Database test: FAILED - {rooms_result.get('error')}")
        
        # Test Google Sheets connection
        sheets_test = processor.test_google_sheets_connection()
        if sheets_test['success']:
            print(f"✅ Google Sheets test: PASSED - {sheets_test['total_rows']} rows")
        else:
            print(f"❌ Google Sheets test: FAILED - {sheets_test.get('error')}")
        
        # Test statistics
        stats = processor.get_statistics()
        print(f"📊 Room statistics: {stats}")
        
        return True
        
    except Exception as e:
        print(f"❌ DataProcessor test error: {e}")
        return False


if __name__ == '__main__':
    print("🧪 Testing DataProcessor...")
    test_data_processor()