# models/data_processor.py
import requests
import logging
import re
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
        """Khởi tạo dữ liệu phòng từ Google Sheets lần đầu tiên - ĐÃ SỬA ĐỂ LƯU CẢ NEWGUEST"""
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
                        # Xử lý thông tin khách hiện tại
                        current_guest = room.get('currentGuest', {})
                        new_guest = room.get('newGuest', {})
                        
                        # Xử lý ngày tháng cho PostgreSQL
                        current_check_in = self.parse_date_for_postgresql(current_guest.get('checkIn', ''))
                        current_check_out = self.parse_date_for_postgresql(current_guest.get('checkOut', ''))
                        new_check_in = self.parse_date_for_postgresql(new_guest.get('checkIn', ''))
                        new_check_out = self.parse_date_for_postgresql(new_guest.get('checkOut', ''))
                        
                        # Tạo notes cho cả 2 khách
                        notes_parts = []
                        if current_guest.get('pax', 0):
                            notes_parts.append(f"Current Pax: {current_guest.get('pax', 0)}")
                        if new_guest.get('pax', 0):
                            notes_parts.append(f"New Pax: {new_guest.get('pax', 0)}")
                        notes = "; ".join(notes_parts)
                        
                        # INSERT với cả currentGuest và newGuest
                        cur.execute('''
                            INSERT INTO rooms 
                            (room_no, room_type, room_status, 
                             guest_name, check_in, check_out,
                             new_guest_name, new_check_in, new_check_out,
                             notes, last_updated)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ''', (
                            room.get('roomNo', ''),
                            room.get('roomType', ''),
                            room.get('roomStatus', 'vc'),
                            current_guest.get('name', ''),
                            current_check_in,
                            current_check_out,
                            new_guest.get('name', ''),
                            new_check_in,
                            new_check_out,
                            notes
                        ))
                    
                    # Ghi log sync
                    cur.execute('''
                        INSERT INTO sync_history (synced_by, total_rooms, success)
                        VALUES (%s, %s, %s)
                    ''', (user_info, len(rooms_data), True))
                
                conn.commit()
            
            logger.info(f"✅ Đã khởi tạo {len(rooms_data)} phòng từ Google Sheets (bao gồm cả newGuest)")
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
        """Chuyển đổi định dạng ngày cho hiển thị - giữ nguyên cho tương thích"""
        if not date_str or date_str == '00-01-00':
            return ''
        
        date_str = str(date_str).strip()
        
        try:
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
                    elif len(year) == 2:
                        pass  # Giữ nguyên
                    
                    day = day.zfill(2)
                    month = month.zfill(2)
                    year = year.zfill(2)
                    
                    return f"{day}-{month}-{year}"
            
            return ''
        except Exception as e:
            logger.warning(f"Lỗi phân tích ngày tháng: {date_str}, Error: {e}")
            return ''

    def parse_pax(self, pax_str):
        """Chuyển đổi số lượng khách sang integer"""
        if not pax_str:
            return 0
        
        try:
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
        
        return str(name_str).strip()

    def process_room_data(self, raw_data):
        """Xử lý dữ liệu thô từ Google Sheets - ĐÃ SỬA ĐỂ XỬ LÝ CẢ NEWGUEST"""
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
                
                room_status = self.clean_room_status(row[1])
                room_type = str(row[10]).strip() if len(row) > 10 else ''  # Thêm room_type nếu có
                
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
        
        return rooms_data

    def get_all_rooms(self):
        """Lấy tất cả phòng từ database - ĐÃ SỬA ĐỂ TRẢ VỀ CẢ NEWGUEST"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, 
                               guest_name, check_in, check_out,
                               new_guest_name, new_check_in, new_check_out,
                               notes, last_updated
                        FROM rooms 
                        ORDER BY room_no
                    ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    rooms = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        # Xử lý notes để lấy thông tin pax
                        notes = row_dict.get('notes', '') or ''
                        current_pax, new_pax = self.extract_pax_from_notes(notes)
                        
                        rooms.append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('check_out')),
                                'pax': current_pax
                            },
                            'newGuest': {
                                'name': row_dict.get('new_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('new_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('new_check_out')),
                                'pax': new_pax
                            }
                        })
                    
                    return {'success': True, 'data': rooms}
                    
        except Exception as e:
            logger.error(f"Lỗi get_all_rooms: {e}")
            return {'success': False, 'error': str(e)}

    def extract_pax_from_notes(self, notes):
        """Trích xuất thông tin pax từ notes"""
        current_pax = 0
        new_pax = 0
        
        if not notes:
            return current_pax, new_pax
        
        # Tìm Current Pax
        if 'Current Pax:' in notes:
            try:
                current_parts = notes.split('Current Pax:')[1].split(';')[0].strip()
                current_pax = int(re.sub(r'[^\d]', '', current_parts))
            except:
                current_pax = 0
        
        # Tìm New Pax
        if 'New Pax:' in notes:
            try:
                new_parts = notes.split('New Pax:')[1].split(';')[0].strip()
                new_pax = int(re.sub(r'[^\d]', '', new_parts))
            except:
                new_pax = 0
        elif 'Pax:' in notes and 'Current Pax:' not in notes:  # Tương thích cũ
            try:
                pax_str = notes.split('Pax:')[1].strip().split()[0]
                current_pax = int(pax_str)
            except:
                current_pax = 0
        
        return current_pax, new_pax

    def format_date_for_display(self, date_value):
        """Định dạng ngày tháng cho hiển thị"""
        if not date_value:
            return ''
        
        try:
            # Nếu là string, trả về trực tiếp
            if isinstance(date_value, str):
                # Nếu đã là định dạng dd-mm-yy thì giữ nguyên
                if re.match(r'\d{2}-\d{2}-\d{2}', date_value):
                    return date_value
                # Nếu là định dạng YYYY-MM-DD thì chuyển đổi
                elif re.match(r'\d{4}-\d{2}-\d{2}', date_value):
                    date_obj = datetime.strptime(date_value, '%Y-%m-%d')
                    return date_obj.strftime('%d-%m-%y')
                else:
                    return str(date_value)
            
            # Nếu là datetime object, format lại
            if isinstance(date_value, datetime):
                return date_value.strftime('%d-%m-%y')
            
            return str(date_value)
        except:
            return ''

    def get_room_by_number(self, room_no):
        """Lấy thông tin chi tiết một phòng - ĐÃ SỬA ĐỂ TRẢ VỀ CẢ NEWGUEST"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, 
                               guest_name, check_in, check_out,
                               new_guest_name, new_check_in, new_check_out,
                               notes, last_updated
                        FROM rooms WHERE room_no = %s
                    ''', (room_no,))
                    
                    columns = [desc[0] for desc in cur.description]
                    row = cur.fetchone()
                    
                    if row:
                        row_dict = dict(zip(columns, row))
                        
                        # Xử lý notes để lấy thông tin pax
                        notes = row_dict.get('notes', '') or ''
                        current_pax, new_pax = self.extract_pax_from_notes(notes)
                        
                        return {
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': row_dict.get('room_status', 'vc'),
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('check_out')),
                                'pax': current_pax
                            },
                            'newGuest': {
                                'name': row_dict.get('new_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('new_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('new_check_out')),
                                'pax': new_pax
                            }
                        }
                    return None
                    
        except Exception as e:
            logger.error(f"Lỗi get_room_by_number {room_no}: {e}")
            return None

    def update_room_data(self, room_no, updated_data, user_info):
        """Cập nhật thông tin phòng trong database - ĐÃ SỬA ĐỂ CẬP NHẬT CẢ NEWGUEST"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build dynamic update query
                    set_clause = []
                    params = []
                    
                    # Cập nhật roomStatus
                    if 'roomStatus' in updated_data:
                        set_clause.append('room_status = %s')
                        params.append(updated_data['roomStatus'])
                    
                    # Cập nhật roomType
                    if 'roomType' in updated_data:
                        set_clause.append('room_type = %s')
                        params.append(updated_data['roomType'])
                    
                    # Cập nhật currentGuest
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
                    
                    # Cập nhật newGuest
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
                    
                    # Cập nhật notes dựa trên pax
                    if 'currentGuest' in updated_data or 'newGuest' in updated_data:
                        current_pax = 0
                        new_pax = 0
                        
                        if 'currentGuest' in updated_data:
                            current_pax = updated_data['currentGuest'].get('pax', 0)
                        if 'newGuest' in updated_data:
                            new_pax = updated_data['newGuest'].get('pax', 0)
                        
                        notes_parts = []
                        if current_pax:
                            notes_parts.append(f"Current Pax: {current_pax}")
                        if new_pax:
                            notes_parts.append(f"New Pax: {new_pax}")
                        notes = "; ".join(notes_parts)
                        
                        set_clause.append('notes = %s')
                        params.append(notes)
                    
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
        """Nhóm phòng theo tầng - ĐÃ SỬA ĐỂ TRẢ VỀ CẢ NEWGUEST"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, 
                               guest_name, check_in, check_out,
                               new_guest_name, new_check_in, new_check_out,
                               notes
                        FROM rooms 
                        ORDER BY room_no
                    ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    floors = {}
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        # Xử lý notes để lấy thông tin pax
                        notes = row_dict.get('notes', '') or ''
                        current_pax, new_pax = self.extract_pax_from_notes(notes)
                        
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
                                'pax': current_pax
                            },
                            'newGuest': {
                                'name': row_dict.get('new_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(row_dict.get('new_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('new_check_out')),
                                'pax': new_pax
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

    def clear_new_guest(self, room_no, user_info):
        """Xóa thông tin khách sắp đến (newGuest) của phòng"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        UPDATE rooms 
                        SET new_guest_name = '',
                            new_check_in = NULL,
                            new_check_out = NULL,
                            notes = TRIM(BOTH '; ' FROM REPLACE(notes, 'New Pax: ' || 
                                    (CASE WHEN notes LIKE '%New Pax:%' THEN 
                                        SUBSTRING(notes FROM 'New Pax: ([0-9]+)') 
                                        ELSE '0' END), '')),
                            last_updated = CURRENT_TIMESTAMP
                        WHERE room_no = %s
                    ''', (room_no,))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logger.error(f"Lỗi clear_new_guest {room_no}: {e}")
            return False

    def transfer_new_to_current(self, room_no, user_info):
        """Chuyển thông tin khách sắp đến thành khách hiện tại"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Lấy thông tin hiện tại
                    cur.execute('''
                        SELECT guest_name, check_in, check_out, 
                               new_guest_name, new_check_in, new_check_out,
                               notes
                        FROM rooms WHERE room_no = %s
                    ''', (room_no,))
                    
                    row = cur.fetchone()
                    if not row:
                        return False
                    
                    # Cập nhật: newGuest -> currentGuest, clear newGuest
                    notes = row[6] or ''
                    # Giữ lại Current Pax nếu có, xóa New Pax
                    if 'Current Pax:' in notes:
                        current_pax_part = notes.split('Current Pax:')[1].split(';')[0].strip()
                        notes = f"Current Pax: {current_pax_part}"
                    else:
                        notes = ''
                    
                    cur.execute('''
                        UPDATE rooms 
                        SET guest_name = new_guest_name,
                            check_in = new_check_in,
                            check_out = new_check_out,
                            new_guest_name = '',
                            new_check_in = NULL,
                            new_check_out = NULL,
                            notes = %s,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE room_no = %s
                    ''', (notes, room_no))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logger.error(f"Lỗi transfer_new_to_current {room_no}: {e}")
            return False