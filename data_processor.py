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
        """Khởi tạo dữ liệu phòng từ Google Sheets lần đầu tiên - HỖ TRỢ ĐẦY ĐỦ NEW GUEST"""
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
                        # Xử lý thông tin khách HIỆN TẠI
                        current_guest = room.get('currentGuest', {})
                        
                        # Xử lý thông tin khách SẮP ĐẾN
                        new_guest = room.get('newGuest', {})
                        
                        # Xử lý ngày tháng - chuyển đổi thành NULL nếu không hợp lệ
                        check_in = self.parse_date_for_postgresql(current_guest.get('checkIn', ''))
                        check_out = self.parse_date_for_postgresql(current_guest.get('checkOut', ''))
                        
                        next_check_in = self.parse_date_for_postgresql(new_guest.get('checkIn', ''))
                        next_check_out = self.parse_date_for_postgresql(new_guest.get('checkOut', ''))
                        
                        # Xác định arr_status từ roomStatus
                        room_status = room.get('roomStatus', 'vc')
                        arr_status = 'arr' if '/arr' in room_status else ''
                        
                        # Xác định base status (loại bỏ /arr)
                        base_status = room_status.replace('/arr', '')
                        
                        # LOẠI BỎ: Không tạo notes từ pax
                        
                        cur.execute('''
                            INSERT INTO rooms 
                            (room_no, room_type, room_status, arr_status, 
                             guest_name, check_in, check_out, current_pax,
                             next_guest_name, next_check_in, next_check_out, next_pax)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            room.get('roomNo', ''),
                            room.get('roomType', ''),  # Lấy từ Google Sheets
                            base_status,  # Base status không có /arr
                            arr_status,   # arr_status riêng
                            current_guest.get('name', ''),
                            check_in,
                            check_out,
                            current_guest.get('pax', 0),
                            new_guest.get('name', ''),
                            next_check_in,
                            next_check_out,
                            new_guest.get('pax', 0)
                        ))
                    
                    # Ghi log sync
                    cur.execute('''
                        INSERT INTO sync_history (synced_by, total_rooms, success)
                        VALUES (%s, %s, %s)
                    ''', (user_info, len(rooms_data), True))
                
                conn.commit()
            
            logger.info(f"✅ Đã khởi tạo {len(rooms_data)} phòng từ Google Sheets (hỗ trợ đầy đủ newGuest)")
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

    def clean_room_status(self, status, arr_flag):
        """Làm sạch và chuẩn hóa trạng thái phòng với ARR flag"""
        if not status:
            status = ''
        
        status = str(status).strip().upper()
        arr_flag = str(arr_flag).strip().upper() if arr_flag else ''
        
        # Xử lý trạng thái cơ bản
        status_mapping = {
            'VD': 'vd', 'OD': 'od', 'VC': 'vc', 'OC': 'oc',
            'DND': 'dnd', 'NN': 'nn', 'LOCK': 'lock', 'IP': 'ip', 'DO': 'do'
        }
        
        # Lấy trạng thái cơ bản
        base_status = status_mapping.get(status, status.lower())
        
        # Xử lý ARR flag
        if arr_flag == 'ARR' and base_status in ['vd', 'vc', 'do']:
            return f'{base_status}/arr'
        
        return base_status

    def parse_date(self, date_str):
        """Chuyển đổi định dạng ngày - giữ nguyên cho tương thích"""
        if not date_str or date_str == '00-01-00':
            return '00-01-00'
        
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
        """Xử lý dữ liệu thô từ Google Sheets - CẬP NHẬT theo cấu trúc mới"""
        if not raw_data or 'values' not in raw_data:
            return []
        
        values = raw_data['values']
        if len(values) < 2:
            return []
        
        rooms_data = []
        
        for row_index, row in enumerate(values[1:], start=2):
            try:
                # Đảm bảo hàng có đủ 11 cột (A-K)
                while len(row) < 11:
                    row.append('')
                
                room_no = str(row[0]).strip() if row[0] else ''
                if not room_no:
                    continue
                
                # Cột B (index 1): trạng thái phòng, Cột C (index 2): ARR flag
                room_status = self.clean_room_status(row[1], row[2])
                
                # Lấy room_type từ cột nào đó (giả sử cột thứ 12 nếu có)
                room_type = row[11].strip() if len(row) > 11 and row[11] else ''
                
                # Khách hiện tại: cột D-G (index 3-6)
                current_guest = {
                    'name': self.clean_guest_name(row[3]),
                    'checkIn': self.parse_date(row[4]),
                    'checkOut': self.parse_date(row[5]),
                    'pax': self.parse_pax(row[6])
                }
                
                # Khách mới: cột H-K (index 7-10)
                new_guest = {
                    'name': self.clean_guest_name(row[7]),
                    'checkIn': self.parse_date(row[8]),
                    'checkOut': self.parse_date(row[9]),
                    'pax': self.parse_pax(row[10])
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
        """Lấy tất cả phòng từ database - HỖ TRỢ ĐẦY ĐỦ NEW GUEST"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, arr_status,
                               guest_name, check_in, check_out, current_pax,
                               next_guest_name, next_check_in, next_check_out, next_pax,
                               last_updated
                        FROM rooms 
                        ORDER BY room_no
                    ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    rooms = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        # Xử lý room_status kết hợp với arr_status
                        room_status = row_dict.get('room_status', 'vc')
                        arr_status = row_dict.get('arr_status', '')
                        
                        if arr_status == 'arr' and room_status in ['vd', 'vc', 'do']:
                            final_status = f'{room_status}/arr'
                        else:
                            final_status = room_status
                        
                        # Lấy pax từ current_pax
                        pax = row_dict.get('current_pax', 0)
                        
                        # Xử lý ngày tháng từ PostgreSQL
                        check_in = row_dict.get('check_in')
                        check_out = row_dict.get('check_out')
                        next_check_in = row_dict.get('next_check_in')
                        next_check_out = row_dict.get('next_check_out')
                        
                        rooms.append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': final_status,
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(check_in),
                                'checkOut': self.format_date_for_display(check_out),
                                'pax': pax
                            },
                            'newGuest': {
                                'name': row_dict.get('next_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(next_check_in),
                                'checkOut': self.format_date_for_display(next_check_out),
                                'pax': row_dict.get('next_pax', 0) or 0
                            }
                        })
                    
                    return {'success': True, 'data': rooms}
                    
        except Exception as e:
            logger.error(f"Lỗi get_all_rooms: {e}")
            return {'success': False, 'error': str(e)}

    def format_date_for_display(self, date_value):
        """Định dạng ngày tháng cho hiển thị"""
        if not date_value:
            return ''
        
        try:
            # Nếu là string, có thể đã là định dạng dd-mm-yy
            if isinstance(date_value, str):
                # Kiểm tra xem có phải định dạng PostgreSQL DATE không (YYYY-MM-DD)
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_value):
                    date_obj = datetime.strptime(date_value, '%Y-%m-%d')
                    return date_obj.strftime('%d-%m-%y')
                # Nếu đã là dd-mm-yy hoặc dd-mm-yyyy
                elif re.match(r'^\d{2}-\d{2}-\d{2,4}$', date_value):
                    return date_value
                else:
                    return date_value
            
            # Nếu là datetime object, format lại
            if isinstance(date_value, datetime):
                return date_value.strftime('%d-%m-%y')
            
            return str(date_value)
        except Exception:
            return ''

    def get_room_by_number(self, room_no):
        """Lấy thông tin chi tiết một phòng - HỖ TRỢ ĐẦY ĐỦ NEW GUEST"""
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
                        
                        # Xử lý room_status kết hợp với arr_status
                        room_status = row_dict.get('room_status', 'vc')
                        arr_status = row_dict.get('arr_status', '')
                        
                        if arr_status == 'arr' and room_status in ['vd', 'vc', 'do']:
                            final_status = f'{room_status}/arr'
                        else:
                            final_status = room_status
                        
                        # Lấy pax từ current_pax
                        pax = row_dict.get('current_pax', 0)
                        
                        # Xử lý ngày tháng từ PostgreSQL
                        check_in = row_dict.get('check_in')
                        check_out = row_dict.get('check_out')
                        next_check_in = row_dict.get('next_check_in')
                        next_check_out = row_dict.get('next_check_out')
                        
                        return {
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': final_status,
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(check_in),
                                'checkOut': self.format_date_for_display(check_out),
                                'pax': pax
                            },
                            'newGuest': {
                                'name': row_dict.get('next_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(next_check_in),
                                'checkOut': self.format_date_for_display(next_check_out),
                                'pax': row_dict.get('next_pax', 0) or 0
                            },
                            'last_updated': row_dict.get('last_updated')
                        }
                    return None
                    
        except Exception as e:
            logger.error(f"Lỗi get_room_by_number {room_no}: {e}")
            return None

    def update_room_data(self, room_no, updated_data, user_info):
        """Cập nhật thông tin phòng trong database - HỖ TRỢ ĐẦY ĐỦ NEW GUEST"""
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
                    
                    # Xử lý trạng thái phòng
                    if 'roomStatus' in updated_data:
                        room_status = updated_data['roomStatus']
                        
                        # Tách base status và arr status
                        if '/arr' in room_status:
                            base_status = room_status.replace('/arr', '')
                            arr_status = 'arr'
                        else:
                            base_status = room_status
                            arr_status = ''
                        
                        set_clause.append('room_status = %s')
                        params.append(base_status)
                        
                        set_clause.append('arr_status = %s')
                        params.append(arr_status)
                    
                    # Xử lý khách hiện tại
                    if 'currentGuest' in updated_data:
                        guest_data = updated_data['currentGuest']
                        set_clause.append('guest_name = %s')
                        params.append(guest_data.get('name', ''))
                        
                        # Xử lý ngày tháng cho PostgreSQL
                        check_in = self.parse_date_for_postgresql(guest_data.get('checkIn', ''))
                        check_out = self.parse_date_for_postgresql(guest_data.get('checkOut', ''))
                        
                        set_clause.append('check_in = %s')
                        params.append(check_in)
                        
                        set_clause.append('check_out = %s')
                        params.append(check_out)
                        
                        # Lưu pax vào current_pax
                        pax = guest_data.get('pax', 0)
                        set_clause.append('current_pax = %s')
                        params.append(pax)
                    
                    # Xử lý khách sắp đến (NEW GUEST) - QUAN TRỌNG!
                    if 'newGuest' in updated_data:
                        new_guest_data = updated_data['newGuest']
                        set_clause.append('next_guest_name = %s')
                        params.append(new_guest_data.get('name', ''))
                        
                        # Xử lý ngày tháng cho PostgreSQL
                        next_check_in = self.parse_date_for_postgresql(new_guest_data.get('checkIn', ''))
                        next_check_out = self.parse_date_for_postgresql(new_guest_data.get('checkOut', ''))
                        
                        set_clause.append('next_check_in = %s')
                        params.append(next_check_in)
                        
                        set_clause.append('next_check_out = %s')
                        params.append(next_check_out)
                        
                        # Lưu pax vào next_pax
                        next_pax = new_guest_data.get('pax', 0)
                        set_clause.append('next_pax = %s')
                        params.append(next_pax)
                    
                    # Xử lý room type
                    if 'roomType' in updated_data:
                        set_clause.append('room_type = %s')
                        params.append(updated_data['roomType'])
                    
                    # LOẠI BỎ: Không xử lý notes dưới bất kỳ hình thức nào
                    
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
                    
                    logger.info(f"✅ Đã cập nhật phòng {room_no} với newGuest support")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Lỗi update_room_data {room_no}: {e}")
            return False

    def get_statistics(self):
        """Thống kê trạng thái phòng từ database - Tính cả ARR status"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Lấy tất cả phòng để tính toán thủ công
                    cur.execute('SELECT room_no, room_status, arr_status FROM rooms')
                    rows = cur.fetchall()
                    
                    statistics = {}
                    for row in rows:
                        room_status = row[1]  # base status
                        arr_status = row[2]   # arr status
                        
                        # Kết hợp room_status và arr_status
                        if arr_status == 'arr' and room_status in ['vd', 'vc', 'do']:
                            final_status = f'{room_status}/arr'
                        else:
                            final_status = room_status
                        
                        if final_status in statistics:
                            statistics[final_status] += 1
                        else:
                            statistics[final_status] = 1
                    
                    return statistics
                    
        except Exception as e:
            logger.error(f"Lỗi get_statistics: {e}")
            return {}

    def get_rooms_by_floor(self):
        """Nhóm phòng theo tầng - HỖ TRỢ ĐẦY ĐỦ NEW GUEST"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT * FROM rooms ORDER BY room_no')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    floors = {}
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        # Xử lý room_status kết hợp với arr_status
                        room_status = row_dict.get('room_status', 'vc')
                        arr_status = row_dict.get('arr_status', '')
                        
                        if arr_status == 'arr' and room_status in ['vd', 'vc', 'do']:
                            final_status = f'{room_status}/arr'
                        else:
                            final_status = room_status
                        
                        # Lấy pax từ current_pax
                        pax = row_dict.get('current_pax', 0)
                        
                        floor = row_dict.get('room_no', '0')[0] if row_dict.get('room_no') else '0'
                        
                        if floor not in floors:
                            floors[floor] = []
                        
                        # Xử lý ngày tháng từ PostgreSQL
                        check_in = row_dict.get('check_in')
                        check_out = row_dict.get('check_out')
                        next_check_in = row_dict.get('next_check_in')
                        next_check_out = row_dict.get('next_check_out')
                        
                        floors[floor].append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': final_status,
                            'currentGuest': {
                                'name': row_dict.get('guest_name', '') or '',
                                'checkIn': self.format_date_for_display(check_in),
                                'checkOut': self.format_date_for_display(check_out),
                                'pax': pax
                            },
                            'newGuest': {
                                'name': row_dict.get('next_guest_name', '') or '',
                                'checkIn': self.format_date_for_display(next_check_in),
                                'checkOut': self.format_date_for_display(next_check_out),
                                'pax': row_dict.get('next_pax', 0) or 0
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

    def clear_all_rooms(self):
        """Xóa tất cả dữ liệu phòng (chỉ dùng cho testing)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('DELETE FROM rooms')
                    conn.commit()
                    logger.info("✅ Đã xóa tất cả dữ liệu phòng")
                    return True
        except Exception as e:
            logger.error(f"Lỗi clear_all_rooms: {e}")
            return False

    def get_rooms_with_new_guests(self):
        """Lấy danh sách phòng có khách sắp đến (newGuest)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT room_no, room_type, room_status, arr_status,
                               next_guest_name, next_check_in, next_check_out, next_pax
                        FROM rooms 
                        WHERE next_guest_name IS NOT NULL 
                        AND next_guest_name != ''
                        ORDER BY room_no
                    ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    rooms = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        # Xử lý room_status kết hợp với arr_status
                        room_status = row_dict.get('room_status', 'vc')
                        arr_status = row_dict.get('arr_status', '')
                        
                        if arr_status == 'arr' and room_status in ['vd', 'vc', 'do']:
                            final_status = f'{room_status}/arr'
                        else:
                            final_status = room_status
                        
                        rooms.append({
                            'roomNo': row_dict.get('room_no', ''),
                            'roomType': row_dict.get('room_type', ''),
                            'roomStatus': final_status,
                            'newGuest': {
                                'name': row_dict.get('next_guest_name', ''),
                                'checkIn': self.format_date_for_display(row_dict.get('next_check_in')),
                                'checkOut': self.format_date_for_display(row_dict.get('next_check_out')),
                                'pax': row_dict.get('next_pax', 0)
                            }
                        })
                    
                    return {'success': True, 'data': rooms, 'total': len(rooms)}
                    
        except Exception as e:
            logger.error(f"Lỗi get_rooms_with_new_guests: {e}")
            return {'success': False, 'error': str(e)}