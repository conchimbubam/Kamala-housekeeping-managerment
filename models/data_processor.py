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

    def fetch_data_from_sheets(self):
        """Lấy dữ liệu từ Google Sheets"""
        url = f'https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{self.range_name}?key={self.api_key}'
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu từ Google Sheets: {e}")
            return None

    def clean_room_status(self, status):
        """Làm sạch và chuẩn hóa trạng thái phòng"""
        if not status:
            return ''
        
        status = str(status).strip().upper()
        
        status_mapping = {
            'VD': 'vd', 'OD': 'od', 'VC': 'vc', 'OC': 'oc',
            'DND': 'dnd', 'NN': 'nn', 'LOCK': 'lock', 'IP': 'ip', 'DO': 'do'
        }
        
        for key, value in status_mapping.items():
            if key == status:
                return value
        
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
        else:
            return status.lower()

    def parse_date(self, date_str):
        """Chuyển đổi định dạng ngày"""
        if not date_str or date_str == '00-01-00':
            return '00-01-00'
        
        date_str = str(date_str).strip()
        
        try:
            import re
            patterns = [
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, date_str)
                if match:
                    day, month, year = match.groups()
                    
                    if len(year) == 4:
                        year = year[2:]
                    
                    day = day.zfill(2)
                    month = month.zfill(2)
                    year = year.zfill(2)
                    
                    return f"{day}-{month}-{year}"
            
            return '00-01-00'
        except Exception as e:
            logger.warning(f"Lỗi phân tích ngày tháng: {date_str}, Error: {e}")
            return '00-01-00'

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
        except (ValueError, TypeError):
            return 0

    def clean_guest_name(self, name_str):
        """Làm sạch tên khách"""
        if not name_str:
            return ''
        
        name_clean = str(name_str).strip()
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
                while len(row) < 10:
                    row.append('')
                
                room_no = str(row[0]).strip() if row[0] else ''
                if not room_no:
                    continue
                
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
                    'roomStatus': room_status,
                    'currentGuest': current_guest,
                    'newGuest': new_guest
                }
                
                rooms_data.append(room_data)
                
            except Exception as e:
                logger.warning(f"Lỗi xử lý dòng {row_index}: {row}. Error: {e}")
                continue
        
        return rooms_data

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
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    rooms = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        notes = row_dict.get('notes', '') or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except:
                                pax = 0
                        
                        rooms.append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': row_dict.get('check_in', '') or '',
                                'checkOut': row_dict.get('check_out', '') or '',
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
                    
                    columns = [desc[0] for desc in cur.description]
                    row = cur.fetchone()
                    
                    if row:
                        row_dict = dict(zip(columns, row))
                        
                        notes = row_dict.get('notes', '') or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except:
                                pax = 0
                        
                        return {
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': row_dict.get('check_in', '') or '',
                                'checkOut': row_dict.get('check_out', '') or '',
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
                    
                    columns = [desc[0] for desc in cur.description]
                    current_row = cur.fetchone()
                    
                    if not current_row:
                        return False
                    
                    # Build dynamic update query
                    set_clause = []
                    params = []
                    
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
                        
                        pax = guest_data.get('pax', 0)
                        notes = f"Pax: {pax}" if pax else ''
                        set_clause.append('notes = %s')
                        params.append(notes)
                    
                    if 'roomType' in updated_data:
                        set_clause.append('room_type = %s')
                        params.append(updated_data['roomType'])
                    
                    if not set_clause:
                        return False
                    
                    params.append(room_no)
                    
                    query = f'''
                        UPDATE rooms 
                        SET {', '.join(set_clause)}, last_updated = CURRENT_TIMESTAMP
                        WHERE room_no = %s
                    '''
                    
                    cur.execute(query, params)
                    conn.commit()
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
                    ''')
                    
                    rows = cur.fetchall()
                    statistics = {}
                    for row in rows:
                        statistics[row[0]] = row[1]
                    
                    return statistics
                    
        except Exception as e:
            logger.error(f"Lỗi get_statistics: {e}")
            return {}

    def get_rooms_by_floor(self):
        """Nhóm phòng theo tầng"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT * FROM rooms ORDER BY room_no')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    floors = {}
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        notes = row_dict.get('notes', '') or ''
                        pax = 0
                        if 'Pax:' in notes:
                            try:
                                pax_str = notes.split('Pax:')[1].strip().split()[0]
                                pax = int(pax_str)
                            except:
                                pax = 0
                        
                        floor = row_dict.get('room_no', '0')[0] if row_dict.get('room_no') else '0'
                        
                        if floor not in floors:
                            floors[floor] = []
                        
                        floors[floor].append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': row_dict.get('check_in', '') or '',
                                'checkOut': row_dict.get('check_out', '') or '',
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
                    cur.execute('''
                        SELECT sync_time, synced_by, total_rooms 
                        FROM sync_history 
                        WHERE success = true
                        ORDER BY sync_time DESC 
                        LIMIT 1
                    ''')
                    
                    last_sync_row = cur.fetchone()
                    
                    cur.execute('SELECT COUNT(*) as count FROM rooms')
                    total_rooms_row = cur.fetchone()
                    total_rooms = total_rooms_row[0] if total_rooms_row else 0
                    
                    if last_sync_row:
                        return {
                            'last_updated': last_sync_row[0],
                            'last_updated_by': last_sync_row[1],
                            'total_rooms': total_rooms,
                            'last_sync_rooms': last_sync_row[2]
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