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
                        
                        # Xử lý ngày tháng cho current guest
                        check_in = self.parse_date_for_postgresql(current_guest.get('checkIn', ''))
                        check_out = self.parse_date_for_postgresql(current_guest.get('checkOut', ''))
                        
                        # Xử lý ngày tháng cho new guest
                        new_check_in = self.parse_date_for_postgresql(new_guest.get('checkIn', ''))
                        new_check_out = self.parse_date_for_postgresql(new_guest.get('checkOut', ''))
                        
                        cur.execute('''
                            INSERT INTO rooms 
                            (room_no, room_type, room_status, 
                             guest_name, check_in, check_out, current_guest_pax,
                             new_guest_name, new_check_in, new_check_out, new_guest_pax)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            room.get('roomNo', ''),
                            room.get('roomType', ''),
                            room.get('roomStatus', 'vc'),
                            current_guest.get('name', ''),
                            check_in,
                            check_out,
                            current_guest.get('pax', 0),
                            new_guest.get('name', ''),
                            new_check_in,
                            new_check_out,
                            new_guest.get('pax', 0)
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

    def parse_date_for_postgresql(self, date_str):
        """Chuyển đổi định dạng ngày cho PostgreSQL - trả về NULL nếu không hợp lệ"""
        if not date_str or date_str.strip() == '' or date_str == '00-01-00':
            return None
        
        date_str = str(date_str).strip()
        
        try:
            import re
            patterns = [
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{1,2})',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, date_str)
                if match:
                    day, month, year = match.groups()
                    
                    # Xử lý năm
                    if len(year) == 2:
                        year = f"20{year}"  # Giả định là năm 2000+
                    elif len(year) == 4:
                        pass  # Giữ nguyên
                    else:
                        return None
                    
                    # Chuyển đổi thành định dạng PostgreSQL DATE
                    try:
                        # Tạo đối tượng datetime để validate
                        date_obj = datetime(int(year), int(month), int(day))
                        return date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        return None
            
            return None
        except Exception as e:
            logger.warning(f"Lỗi phân tích ngày tháng: {date_str}, Error: {e}")
            return None

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
        """Chuyển đổi định dạng ngày - giữ nguyên cho tương thích"""
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
                
                room_type = str(row[1]).strip() if len(row) > 1 and row[1] else ''
                room_status = self.clean_room_status(row[2]) if len(row) > 2 and row[2] else 'vc'
                
                current_guest = {
                    'name': self.clean_guest_name(row[3]) if len(row) > 3 and row[3] else '',
                    'checkIn': self.parse_date(row[4]) if len(row) > 4 and row[4] else '',
                    'checkOut': self.parse_date(row[5]) if len(row) > 5 and row[5] else '',
                    'pax': self.parse_pax(row[6]) if len(row) > 6 and row[6] else 0
                }
                
                new_guest = {
                    'name': self.clean_guest_name(row[7]) if len(row) > 7 and row[7] else '',
                    'checkIn': self.parse_date(row[8]) if len(row) > 8 and row[8] else '',
                    'checkOut': self.parse_date(row[9]) if len(row) > 9 and row[9] else '',
                    'pax': self.parse_pax(row[10]) if len(row) > 10 and row[10] else 0
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
        
        return rooms_data

    def get_all_rooms(self):
        """Lấy tất cả phòng từ database với đầy đủ thông tin khách mới"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, 
                               guest_name, check_in, check_out, current_guest_pax,
                               new_guest_name, new_check_in, new_check_out, new_guest_pax,
                               last_updated
                        FROM rooms 
                        ORDER BY room_no
                    ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    rooms = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        rooms.append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('check_out')),
                                'pax': row_dict.get('current_guest_pax', 0) or 0
                            },
                            'newGuest': {
                                'name': row_dict.get('new_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('new_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('new_check_out')),
                                'pax': row_dict.get('new_guest_pax', 0) or 0
                            }
                        })
                    
                    return {'success': True, 'data': rooms}
                    
        except Exception as e:
            logger.error(f"Lỗi get_all_rooms: {e}")
            return {'success': False, 'error': str(e)}

    def format_date_for_display(self, date_value):
        """Định dạng ngày tháng cho hiển thị (dd-mm-yy)"""
        if not date_value:
            return ''
        
        try:
            # Nếu là string và đã ở định dạng PostgreSQL DATE (YYYY-MM-DD)
            if isinstance(date_value, str) and re.match(r'\d{4}-\d{2}-\d{2}', date_value):
                date_obj = datetime.strptime(date_value, '%Y-%m-%d')
                return date_obj.strftime('%d-%m-%y')
            
            # Nếu là datetime object
            if isinstance(date_value, datetime):
                return date_value.strftime('%d-%m-%y')
            
            # Nếu đã ở định dạng dd-mm-yy
            if isinstance(date_value, str) and re.match(r'\d{2}-\d{2}-\d{2}', date_value):
                return date_value
            
            return str(date_value)
        except Exception as e:
            logger.warning(f"Lỗi định dạng ngày tháng: {date_value}, Error: {e}")
            return ''

    def get_room_by_number(self, room_no):
        """Lấy thông tin chi tiết một phòng với đầy đủ thông tin khách mới"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, 
                               guest_name, check_in, check_out, current_guest_pax,
                               new_guest_name, new_check_in, new_check_out, new_guest_pax,
                               last_updated
                        FROM rooms 
                        WHERE room_no = %s
                    ''', (room_no,))
                    
                    columns = [desc[0] for desc in cur.description]
                    row = cur.fetchone()
                    
                    if row:
                        row_dict = dict(zip(columns, row))
                        
                        return {
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('check_out')),
                                'pax': row_dict.get('current_guest_pax', 0) or 0
                            },
                            'newGuest': {
                                'name': row_dict.get('new_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('new_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('new_check_out')),
                                'pax': row_dict.get('new_guest_pax', 0) or 0
                            },
                            'lastUpdated': row_dict.get('last_updated')
                        }
                    return None
                    
        except Exception as e:
            logger.error(f"Lỗi get_room_by_number {room_no}: {e}")
            return None

    def update_room_data(self, room_no, updated_data, user_info):
        """Cập nhật thông tin phòng trong database với đầy đủ thông tin khách mới"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build dynamic update query
                    set_clause = []
                    params = []
                    
                    if 'roomStatus' in updated_data:
                        set_clause.append('room_status = %s')
                        params.append(updated_data['roomStatus'])
                    
                    if 'roomType' in updated_data:
                        set_clause.append('room_type = %s')
                        params.append(updated_data['roomType'])
                    
                    if 'currentGuest' in updated_data:
                        guest_data = updated_data['currentGuest']
                        set_clause.append('guest_name = %s')
                        params.append(guest_data.get('name', ''))
                        
                        check_in = self.parse_date_for_postgresql(guest_data.get('checkIn', ''))
                        check_out = self.parse_date_for_postgresql(guest_data.get('checkOut', ''))
                        
                        set_clause.append('check_in = %s')
                        params.append(check_in)
                        
                        set_clause.append('check_out = %s')
                        params.append(check_out)
                        
                        set_clause.append('current_guest_pax = %s')
                        params.append(guest_data.get('pax', 0))
                    
                    if 'newGuest' in updated_data:
                        new_guest_data = updated_data['newGuest']
                        set_clause.append('new_guest_name = %s')
                        params.append(new_guest_data.get('name', ''))
                        
                        new_check_in = self.parse_date_for_postgresql(new_guest_data.get('checkIn', ''))
                        new_check_out = self.parse_date_for_postgresql(new_guest_data.get('checkOut', ''))
                        
                        set_clause.append('new_check_in = %s')
                        params.append(new_check_in)
                        
                        set_clause.append('new_check_out = %s')
                        params.append(new_check_out)
                        
                        set_clause.append('new_guest_pax = %s')
                        params.append(new_guest_data.get('pax', 0))
                    
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
        """Nhóm phòng theo tầng với đầy đủ thông tin khách mới"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, 
                               guest_name, check_in, check_out, current_guest_pax,
                               new_guest_name, new_check_in, new_check_out, new_guest_pax
                        FROM rooms 
                        ORDER BY room_no
                    ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    floors = {}
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        floor = row_dict.get('room_no', '0')[0] if row_dict.get('room_no') else '0'
                        
                        if floor not in floors:
                            floors[floor] = []
                        
                        floors[floor].append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('check_out')),
                                'pax': row_dict.get('current_guest_pax', 0) or 0
                            },
                            'newGuest': {
                                'name': row_dict.get('new_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('new_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('new_check_out')),
                                'pax': row_dict.get('new_guest_pax', 0) or 0
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