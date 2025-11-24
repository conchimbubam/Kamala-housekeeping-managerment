# models/data_processor.py
import requests
import logging
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, db_manager, api_key=None, spreadsheet_id=None, range_name=None):
        self.db = db_manager
        self.api_key = api_key
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
    
    def initialize_rooms_from_google_sheets(self, user_info="System"):
        """Khởi tạo dữ liệu phòng từ Google Sheets"""
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
            
            # Xóa dữ liệu cũ và insert mới
            self.db.execute_query("DELETE FROM rooms", fetch=False)
            
            for room in rooms_data:
                # Xử lý thông tin khách
                current_guest = room.get('currentGuest', {})
                
                # Xác định floor từ room_no (lấy ký tự đầu)
                room_no = room.get('roomNo', '')
                floor = int(room_no[0]) if room_no and room_no[0].isdigit() else 1
                
                room_data = {
                    'room_no': room_no,
                    'room_type': room.get('roomType', ''),
                    'room_status': room.get('roomStatus', 'vd'),
                    'guest_name': current_guest.get('name', ''),
                    'check_in': current_guest.get('checkIn', ''),
                    'check_out': current_guest.get('checkOut', ''),
                    'notes': f"Pax: {current_guest.get('pax', 0)}" if current_guest.get('pax', 0) else '',
                    'floor': floor,
                    'last_updated': datetime.now(),
                    'updated_by': user_info
                }
                
                self.db.upsert_room(room_data)
            
            # Ghi log sync vào file_info table
            sync_data = {
                'file_name': f'Google Sheets - {self.spreadsheet_id}',
                'last_modified': datetime.now(),
                'total_rows': len(rooms_data),
                'last_sync': datetime.now(),
                'sync_by': user_info
            }
            
            self.db.execute_query("DELETE FROM file_info", fetch=False)
            self.db.execute_insert("""
                INSERT INTO file_info (file_name, last_modified, total_rows, last_sync, sync_by)
                VALUES (%(file_name)s, %(last_modified)s, %(total_rows)s, %(last_sync)s, %(sync_by)s)
            """, sync_data)
            
            logger.info(f"✅ Đã khởi tạo {len(rooms_data)} phòng từ Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo từ Google Sheets: {e}")
            return False

    # ==================== GOOGLE SHEETS METHODS ====================
    
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
            return 'vd'  # Mặc định là Vacant Dirty
        
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
            return None
        
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
            
            return None
        except Exception as e:
            logger.warning(f"Lỗi phân tích ngày tháng: {date_str}, Error: {e}")
            return None

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
                
                # Xác định room_type từ room_no
                room_type = 'Standard'
                if room_no.startswith('1'):
                    room_type = 'Standard'
                elif room_no.startswith('2'):
                    room_type = 'Superior'
                elif room_no.startswith('3'):
                    room_type = 'Deluxe'
                elif room_no.startswith('4'):
                    room_type = 'Suite'
                
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

    # ==================== POSTGRESQL DATABASE METHODS ====================

    def get_all_rooms(self):
        """Lấy tất cả phòng từ PostgreSQL database"""
        try:
            rows = self.db.get_all("""
                SELECT room_no, room_type, room_status, guest_name, 
                       check_in, check_out, notes, floor, last_updated, updated_by
                FROM rooms 
                ORDER BY room_no
            """)
            
            rooms = []
            for row in rows:
                # Parse notes để lấy thông tin pax
                notes = row['notes'] or ''
                pax = 0
                if 'Pax:' in notes:
                    try:
                        pax_str = notes.split('Pax:')[1].strip().split()[0]
                        pax = int(pax_str)
                    except:
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
                    },
                    'floor': row['floor'],
                    'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None,
                    'updatedBy': row['updated_by']
                })
            
            return {'success': True, 'data': rooms}
            
        except Exception as e:
            logger.error(f"Lỗi get_all_rooms: {e}")
            return {'success': False, 'error': str(e)}

    def get_room_by_number(self, room_no):
        """Lấy thông tin chi tiết một phòng từ PostgreSQL"""
        try:
            row = self.db.get_one(
                "SELECT * FROM rooms WHERE room_no = %s", 
                (room_no,)
            )
            
            if row:
                # Parse notes để lấy thông tin pax
                notes = row['notes'] or ''
                pax = 0
                if 'Pax:' in notes:
                    try:
                        pax_str = notes.split('Pax:')[1].strip().split()[0]
                        pax = int(pax_str)
                    except:
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
                    },
                    'floor': row['floor'],
                    'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None,
                    'updatedBy': row['updated_by']
                }
            return None
            
        except Exception as e:
            logger.error(f"Lỗi get_room_by_number {room_no}: {e}")
            return None

    def update_room_data(self, room_no, updated_data, user_info):
        """Cập nhật thông tin phòng trong PostgreSQL database"""
        try:
            # Lấy thông tin phòng hiện tại
            current_room = self.db.get_one(
                "SELECT * FROM rooms WHERE room_no = %s", 
                (room_no,)
            )
            
            if not current_room:
                return False
            
            # Build dynamic update query
            set_clause = []
            params = {}
            
            # Thêm các trường cập nhật
            if 'roomStatus' in updated_data:
                set_clause.append("room_status = %(room_status)s")
                params['room_status'] = updated_data['roomStatus']
            
            if 'currentGuest' in updated_data:
                guest_data = updated_data['currentGuest']
                set_clause.append("guest_name = %(guest_name)s")
                params['guest_name'] = guest_data.get('name', '')
                
                set_clause.append("check_in = %(check_in)s")
                params['check_in'] = guest_data.get('checkIn', '')
                
                set_clause.append("check_out = %(check_out)s")
                params['check_out'] = guest_data.get('checkOut', '')
                
                # Lưu pax vào notes
                pax = guest_data.get('pax', 0)
                notes = f"Pax: {pax}" if pax else ''
                set_clause.append("notes = %(notes)s")
                params['notes'] = notes
            
            if 'roomType' in updated_data:
                set_clause.append("room_type = %(room_type)s")
                params['room_type'] = updated_data['roomType']
            
            if not set_clause:
                return False
            
            # Thêm các trường cố định
            set_clause.append("last_updated = %(last_updated)s")
            params['last_updated'] = datetime.now()
            
            set_clause.append("updated_by = %(updated_by)s")
            params['updated_by'] = user_info
            
            # Thêm room_no cho WHERE clause
            params['room_no'] = room_no
            
            query = f"""
                UPDATE rooms 
                SET {', '.join(set_clause)}
                WHERE room_no = %(room_no)s
            """
            
            row_count = self.db.execute_update(query, params)
            return row_count > 0
            
        except Exception as e:
            logger.error(f"Lỗi update_room_data {room_no}: {e}")
            return False

    def get_statistics(self):
        """Thống kê trạng thái phòng từ PostgreSQL database"""
        try:
            stats = self.db.get_all("""
                SELECT room_status, COUNT(*) as count 
                FROM rooms 
                GROUP BY room_status
            """)
            
            statistics = {}
            for row in stats:
                statistics[row['room_status']] = row['count']
            
            return statistics
            
        except Exception as e:
            logger.error(f"Lỗi get_statistics: {e}")
            return {}

    def get_rooms_by_floor(self):
        """Nhóm phòng theo tầng từ PostgreSQL"""
        try:
            rows = self.db.get_all("""
                SELECT * FROM rooms ORDER BY room_no
            """)
            
            floors = {}
            for row in rows:
                room_no = row['room_no']
                floor = str(room_no[0]) if room_no and room_no[0].isdigit() else '0'
                
                if floor not in floors:
                    floors[floor] = []
                
                # Parse notes để lấy pax
                notes = row['notes'] or ''
                pax = 0
                if 'Pax:' in notes:
                    try:
                        pax_str = notes.split('Pax:')[1].strip().split()[0]
                        pax = int(pax_str)
                    except:
                        pax = 0
                
                floors[floor].append({
                    'roomNo': room_no,
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
                    },
                    'floor': row['floor'],
                    'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None,
                    'updatedBy': row['updated_by']
                })
            
            return floors
            
        except Exception as e:
            logger.error(f"Lỗi get_rooms_by_floor: {e}")
            return {}

    def get_room_info(self):
        """Lấy thông tin file/data từ PostgreSQL database"""
        try:
            # Lấy thông tin sync cuối cùng từ file_info
            last_sync = self.db.get_one("""
                SELECT last_sync, sync_by, total_rows 
                FROM file_info 
                ORDER BY last_sync DESC 
                LIMIT 1
            """)
            
            # Lấy tổng số phòng hiện tại
            total_result = self.db.get_one("SELECT COUNT(*) as count FROM rooms")
            total_rooms = total_result['count'] if total_result else 0
            
            if last_sync:
                return {
                    'last_updated': last_sync['last_sync'].isoformat() if last_sync['last_sync'] else None,
                    'last_updated_by': last_sync['sync_by'],
                    'total_rooms': total_rooms,
                    'last_sync_rooms': last_sync['total_rows']
                }
            else:
                return {
                    'last_updated': None,
                    'last_updated_by': None,
                    'total_rooms': total_rooms
                }
                
        except Exception as e:
            logger.error(f"Lỗi get_room_info: {e}")
            return {
                'last_updated': None,
                'last_updated_by': None,
                'total_rooms': 0
            }

    def search_rooms(self, search_term):
        """Tìm kiếm phòng theo số phòng, tên khách, hoặc trạng thái"""
        try:
            search_pattern = f"%{search_term}%"
            rows = self.db.get_all("""
                SELECT * FROM rooms 
                WHERE room_no ILIKE %s 
                   OR guest_name ILIKE %s 
                   OR room_status ILIKE %s
                ORDER BY room_no
            """, (search_pattern, search_pattern, search_pattern))
            
            rooms = []
            for row in rows:
                notes = row['notes'] or ''
                pax = 0
                if 'Pax:' in notes:
                    try:
                        pax_str = notes.split('Pax:')[1].strip().split()[0]
                        pax = int(pax_str)
                    except:
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
                    },
                    'floor': row['floor'],
                    'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None,
                    'updatedBy': row['updated_by']
                })
            
            return {'success': True, 'data': rooms, 'total': len(rooms)}
            
        except Exception as e:
            logger.error(f"Lỗi search_rooms: {e}")
            return {'success': False, 'error': str(e), 'data': [], 'total': 0}

    def get_rooms_by_status(self, status):
        """Lấy danh sách phòng theo trạng thái cụ thể"""
        try:
            rows = self.db.get_all("""
                SELECT * FROM rooms 
                WHERE room_status = %s 
                ORDER BY room_no
            """, (status,))
            
            rooms = []
            for row in rows:
                notes = row['notes'] or ''
                pax = 0
                if 'Pax:' in notes:
                    try:
                        pax_str = notes.split('Pax:')[1].strip().split()[0]
                        pax = int(pax_str)
                    except:
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
                    },
                    'floor': row['floor'],
                    'lastUpdated': row['last_updated'].isoformat() if row['last_updated'] else None,
                    'updatedBy': row['updated_by']
                })
            
            return {'success': True, 'data': rooms, 'total': len(rooms)}
            
        except Exception as e:
            logger.error(f"Lỗi get_rooms_by_status: {e}")
            return {'success': False, 'error': str(e), 'data': [], 'total': 0}

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