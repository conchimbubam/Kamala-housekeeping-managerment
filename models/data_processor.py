import requests
import json
import os
import shutil
from datetime import datetime
import logging
from config import Config

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, api_key, spreadsheet_id, range_name):
        self.api_key = api_key
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
        self._ensure_data_directory()
        
    def _ensure_data_directory(self):
        """Đảm bảo thư mục data và backups tồn tại"""
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.BACKUP_DIR, exist_ok=True)
        
    def _create_backup(self):
        """Tạo backup file trước khi cập nhật"""
        if os.path.exists(Config.ROOMS_JSON):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(Config.BACKUP_DIR, f"rooms_backup_{timestamp}.json")
            shutil.copy2(Config.ROOMS_JSON, backup_file)
            logger.info(f"Backup created: {backup_file}")
            
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
        
        # Xử lý các trạng thái phổ biến
        status_mapping = {
            'VD': 'vd',
            'OD': 'od', 
            'VC': 'vc',
            'OC': 'oc',
            'DND': 'dnd',
            'NN': 'nn',
            'LOCK': 'lock',
            'IP': 'ip',
            'DO': 'do'
        }
        
        # Kiểm tra các trạng thái đơn
        for key, value in status_mapping.items():
            if key == status:
                return value
        
        # Xử lý trạng thái kết hợp như VD/ARR, VC/ARR, DO/ARR
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
            # Các pattern ngày tháng phổ biến
            patterns = [
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',
            ]
            
            import re
            for pattern in patterns:
                match = re.search(pattern, date_str)
                if match:
                    day, month, year = match.groups()
                    
                    # Chuẩn hóa năm
                    if len(year) == 4:
                        year = year[2:]
                    
                    # Chuẩn hóa ngày và tháng
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
        
        # Bỏ qua dòng tiêu đề, xử lý từ dòng thứ 2
        for row_index, row in enumerate(values[1:], start=2):
            try:
                # Đảm bảo row có đủ 10 cột (A-J)
                while len(row) < 10:
                    row.append('')
                
                # Cột A: Số phòng
                room_no = str(row[0]).strip() if row[0] else ''
                
                # Bỏ qua nếu không có số phòng
                if not room_no:
                    continue
                
                # Cột B: Trạng thái phòng
                room_status = self.clean_room_status(row[1])
                
                # Cột C-F: Thông tin khách hiện tại
                current_guest = {
                    'name': self.clean_guest_name(row[2]),
                    'checkIn': self.parse_date(row[3]),
                    'checkOut': self.parse_date(row[4]),
                    'pax': self.parse_pax(row[5])
                }
                
                # Cột G-J: Thông tin khách mới (sắp đến)
                new_guest = {
                    'name': self.clean_guest_name(row[6]),
                    'checkIn': self.parse_date(row[7]),
                    'checkOut': self.parse_date(row[8]),
                    'pax': self.parse_pax(row[9])
                }
                
                # Tạo đối tượng phòng
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
    
    def load_rooms_data(self):
        """Đọc dữ liệu từ file JSON"""
        try:
            if os.path.exists(Config.ROOMS_JSON):
                with open(Config.ROOMS_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('data', [])
            else:
                # Nếu file chưa tồn tại, tạo từ Google Sheets
                return self.update_from_google_sheets()
        except Exception as e:
            logger.error(f"Lỗi đọc file JSON: {e}")
            return []
    
    def update_from_google_sheets(self, user_info=None):
        """Cập nhật dữ liệu từ Google Sheets và lưu vào JSON"""
        try:
            # Tạo backup trước khi cập nhật
            self._create_backup()
            
            raw_data = self.fetch_data_from_sheets()
            if not raw_data:
                raise Exception("Không thể lấy dữ liệu từ Google Sheets")
            
            rooms_data = self.process_room_data(raw_data)
            
            # Tạo structure cho file JSON
            data_structure = {
                'last_updated': datetime.now().isoformat(),
                'updated_by': user_info or 'system',
                'source': 'google_sheets',
                'total_rooms': len(rooms_data),
                'data': rooms_data
            }
            
            # Ghi vào file JSON
            with open(Config.ROOMS_JSON, 'w', encoding='utf-8') as f:
                json.dump(data_structure, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Dữ liệu đã được cập nhật từ Google Sheets. Tổng phòng: {len(rooms_data)}")
            return rooms_data
            
        except Exception as e:
            logger.error(f"Lỗi cập nhật từ Google Sheets: {e}")
            raise
    
    def get_room_by_number(self, room_no):
        """Lấy thông tin phòng theo số phòng"""
        try:
            rooms = self.load_rooms_data()
            for room in rooms:
                if room.get('roomNo') == room_no:
                    return room
            return None
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin phòng {room_no}: {e}")
            return None
    
    def update_room_data(self, room_no, updated_data, user_info):
        """Cập nhật thông tin một phòng cụ thể - XÓA THÔNG TIN KHÁCH KHI CẦN"""
        try:
            # Đọc dữ liệu hiện tại
            if not os.path.exists(Config.ROOMS_JSON):
                raise Exception("File dữ liệu chưa được khởi tạo")
            
            with open(Config.ROOMS_JSON, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
            
            # Tìm và cập nhật phòng
            room_updated = False
            for room in current_data['data']:
                if room['roomNo'] == room_no:
                    # Cập nhật từng trường trong updated_data
                    for key, value in updated_data.items():
                        if key == 'currentGuest':
                            # XỬ LÝ ĐẶC BIỆT: Kiểm tra xem có phải đang xóa thông tin khách hiện tại không
                            is_empty_current_guest = (
                                value.get('name', '') == '' and
                                value.get('checkIn', '') in ['', '00-01-00'] and
                                value.get('checkOut', '') in ['', '00-01-00'] and
                                value.get('pax', 0) == 0
                            )
                            
                            if is_empty_current_guest:
                                # XÓA HOÀN TOÀN thông tin khách hiện tại
                                room['currentGuest'] = {
                                    'name': '',
                                    'checkIn': '',
                                    'checkOut': '',
                                    'pax': 0
                                }
                                logger.info(f"Đã xóa thông tin khách hiện tại cho phòng {room_no}")
                            else:
                                # Đảm bảo currentGuest object tồn tại
                                if 'currentGuest' not in room:
                                    room['currentGuest'] = {}
                                
                                # Cập nhật thông tin khách hiện tại
                                for guest_key, guest_value in value.items():
                                    room['currentGuest'][guest_key] = guest_value
                                    
                        elif key == 'newGuest':
                            # XỬ LÝ ĐẶC BIỆT: Kiểm tra xem có phải đang xóa thông tin khách sắp đến không
                            is_empty_new_guest = (
                                value.get('name', '') == '' and
                                value.get('checkIn', '') in ['', '00-01-00'] and
                                value.get('checkOut', '') in ['', '00-01-00'] and
                                value.get('pax', 0) == 0
                            )
                            
                            if is_empty_new_guest:
                                # XÓA HOÀN TOÀN thông tin khách sắp đến
                                room['newGuest'] = {
                                    'name': '',
                                    'checkIn': '',
                                    'checkOut': '',
                                    'pax': 0
                                }
                                logger.info(f"Đã xóa thông tin khách sắp đến cho phòng {room_no}")
                            else:
                                # Đảm bảo newGuest object tồn tại
                                if 'newGuest' not in room:
                                    room['newGuest'] = {}
                                
                                # Cập nhật thông tin khách sắp đến
                                for guest_key, guest_value in value.items():
                                    room['newGuest'][guest_key] = guest_value
                        else:
                            # Cập nhật các trường thông thường (roomStatus, notes, etc.)
                            room[key] = value
                    
                    room_updated = True
                    break
            
            if not room_updated:
                raise Exception(f"Không tìm thấy phòng {room_no}")
            
            # KIỂM TRA VÀ XỬ LÝ LOGIC DỰA TRÊN roomStatus
            for room in current_data['data']:
                if room['roomNo'] == room_no:
                    room_status = room.get('roomStatus', '')
                    
                    # XỬ LÝ ĐẶC BIỆT: Khi chuyển sang trạng thái IP (In Progress)
                    if room_status == 'ip':
                        # Kiểm tra xem có thông tin khách sắp đến không
                        has_new_guest_data = (
                            room.get('newGuest', {}).get('name', '') != '' or
                            room.get('newGuest', {}).get('checkIn', '') not in ['', '00-01-00'] or
                            room.get('newGuest', {}).get('checkOut', '') not in ['', '00-01-00'] or
                            room.get('newGuest', {}).get('pax', 0) != 0
                        )
                        
                        if has_new_guest_data:
                            # CHUYỂN thông tin khách sắp đến thành khách hiện tại
                            room['currentGuest'] = room['newGuest'].copy()
                            logger.info(f"Đã chuyển thông tin khách sắp đến thành khách hiện tại cho phòng {room_no} khi chuyển trạng thái IP")
                            
                            # Sau khi chuyển, xóa thông tin khách sắp đến
                            room['newGuest'] = {
                                'name': '',
                                'checkIn': '',
                                'checkOut': '',
                                'pax': 0
                            }
                            logger.info(f"Đã xóa thông tin khách sắp đến sau khi chuyển thành khách hiện tại cho phòng {room_no}")
                    
                    # Nếu trạng thái phòng là trống (VD, VC) thì đảm bảo thông tin khách hiện tại bị xóa
                    if room_status in ['vd', 'vc', 'vd/arr', 'vc/arr']:
                        # Kiểm tra xem currentGuest có dữ liệu không
                        has_current_guest_data = (
                            room.get('currentGuest', {}).get('name', '') != '' or
                            room.get('currentGuest', {}).get('checkIn', '') not in ['', '00-01-00'] or
                            room.get('currentGuest', {}).get('checkOut', '') not in ['', '00-01-00'] or
                            room.get('currentGuest', {}).get('pax', 0) != 0
                        )
                        
                        if has_current_guest_data:
                            logger.warning(f"Phòng {room_no} có trạng thái {room_status} nhưng vẫn có thông tin khách hiện tại - Đang xóa...")
                            # XÓA HOÀN TOÀN thông tin khách hiện tại
                            room['currentGuest'] = {
                                'name': '',
                                'checkIn': '',
                                'checkOut': '',
                                'pax': 0
                            }
                    
                    # Nếu trạng thái phòng không phải VD/ARR, VC/ARR, DO/ARR thì đảm bảo thông tin khách sắp đến bị xóa
                    if not room_status.endswith('/arr'):
                        # Kiểm tra xem newGuest có dữ liệu không
                        has_new_guest_data = (
                            room.get('newGuest', {}).get('name', '') != '' or
                            room.get('newGuest', {}).get('checkIn', '') not in ['', '00-01-00'] or
                            room.get('newGuest', {}).get('checkOut', '') not in ['', '00-01-00'] or
                            room.get('newGuest', {}).get('pax', 0) != 0
                        )
                        
                        if has_new_guest_data:
                            logger.warning(f"Phòng {room_no} có trạng thái {room_status} nhưng vẫn có thông tin khách sắp đến - Đang xóa...")
                            # XÓA HOÀN TOÀN thông tin khách sắp đến
                            room['newGuest'] = {
                                'name': '',
                                'checkIn': '',
                                'checkOut': '',
                                'pax': 0
                            }
                    
                    break
            
            # Cập nhật metadata
            current_data['last_updated'] = datetime.now().isoformat()
            current_data['updated_by'] = user_info
            current_data['source'] = 'manual_update'
            
            # Ghi lại file
            with open(Config.ROOMS_JSON, 'w', encoding='utf-8') as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Phòng {room_no} đã được cập nhật bởi {user_info}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi cập nhật phòng {room_no}: {e}")
            raise
    
    def get_statistics(self):
        """Lấy thống kê tổng quan về trạng thái phòng"""
        rooms = self.load_rooms_data()
        
        stats = {
            'total': len(rooms),
            'vd': 0, 'od': 0, 'vc': 0, 'oc': 0, 
            'vd/arr': 0, 'vc/arr': 0, 'dnd': 0, 'nn': 0, 
            'lock': 0, 'ip': 0, 'do': 0, 'do/arr': 0, 'empty': 0
        }
        
        for room in rooms:
            status = room.get('roomStatus', '')
            if status in stats:
                stats[status] += 1
            elif not status:
                stats['empty'] += 1
        
        return stats
    
    def get_rooms_by_floor(self):
        """Nhóm phòng theo tầng"""
        rooms = self.load_rooms_data()
        floors = {}
        
        for room in rooms:
            room_no = room.get('roomNo', '')
            if room_no and len(room_no) >= 1:
                # Lấy ký tự đầu tiên làm tầng (giả sử số phòng có format "TầngSố", ví dụ "101")
                floor = room_no[0]
                if floor.isdigit():  # Chỉ xử lý nếu là số
                    if floor not in floors:
                        floors[floor] = []
                    floors[floor].append(room)
        
        # Sắp xếp các tầng
        return {k: floors[k] for k in sorted(floors.keys())}
    
    def get_room_info(self):
        """Lấy thông tin về file dữ liệu"""
        try:
            if os.path.exists(Config.ROOMS_JSON):
                with open(Config.ROOMS_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return {
                    'last_updated': data.get('last_updated'),
                    'updated_by': data.get('updated_by'),
                    'source': data.get('source'),
                    'total_rooms': data.get('total_rooms', 0),
                    'success': True
                }
            else:
                return {'error': 'File dữ liệu chưa được khởi tạo', 'success': False}
        except Exception as e:
            return {'error': str(e), 'success': False}
    
    def get_all_rooms(self):
        """Lấy toàn bộ danh sách phòng với structure đầy đủ"""
        try:
            if os.path.exists(Config.ROOMS_JSON):
                with open(Config.ROOMS_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return {
                    'success': True,
                    'data': data.get('data', []),
                    'last_updated': data.get('last_updated'),
                    'total_rooms': data.get('total_rooms', 0)
                }
            else:
                return {'success': False, 'error': 'File dữ liệu chưa tồn tại'}
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách phòng: {e}")
            return {'success': False, 'error': str(e)}