import json
import os
from datetime import datetime, time
from config import Config
import logging

logger = logging.getLogger(__name__)

class HKLogger:
    def __init__(self):
        self.log_file = os.path.join(Config.DATA_DIR, 'hk_activity_log.json')
        self.notes_log_file = os.path.join(Config.DATA_DIR, 'notes_history_log.json')
        self._ensure_log_files()
    
    def _ensure_log_files(self):
        """Đảm bảo file log tồn tại"""
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
        
        if not os.path.exists(self.notes_log_file):
            with open(self.notes_log_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
    
    def _is_within_report_period(self, timestamp_str):
        """Kiểm tra xem thời gian có nằm trong khoảng báo cáo (từ 8h15 hôm nay) không"""
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            today_8_15 = datetime.now().replace(hour=8, minute=15, second=0, microsecond=0)
            
            # Nếu bây giờ chưa đến 8h15, lấy 8h15 của hôm qua
            if datetime.now() < today_8_15:
                today_8_15 = today_8_15.replace(day=today_8_15.day - 1)
            
            return timestamp >= today_8_15
        except Exception as e:
            logger.error(f"Lỗi kiểm tra thời gian: {e}")
            return False
    
    def log_room_status_change(self, room_no, old_status, new_status, user_name):
        """Ghi log thay đổi trạng thái phòng"""
        try:
            # Chỉ log các chuyển đổi quan trọng
            important_transitions = [
                ('vd', 'vc'), ('vd/arr', 'vc/arr'),  # Dọn phòng trống
                ('od', 'oc'), ('od', 'dnd'), ('od', 'nn')  # Dọn phòng ở
            ]
            
            if (old_status, new_status) not in important_transitions:
                return
            
            # Xác định loại thao tác
            if (old_status, new_status) in [('vd', 'vc'), ('vd/arr', 'vc/arr')]:
                action_type = "dọn phòng trống"
                action_detail = f"{old_status.upper()} → {new_status.upper()}"
            else:
                action_type = "dọn phòng ở"
                action_detail = f"{old_status.upper()} → {new_status.upper()}"
            
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'user_name': user_name,
                'room_no': room_no,
                'activity_type': 'room_status',
                'action_type': action_type,
                'action_detail': action_detail,
                'old_status': old_status,
                'new_status': new_status
            }
            
            # Đọc log hiện tại
            with open(self.log_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)
            
            # Thêm log mới
            logs.append(log_entry)
            
            # Giới hạn số lượng log (giữ 1000 bản ghi gần nhất)
            if len(logs) > 1000:
                logs = logs[-1000:]
            
            # Ghi lại file
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Đã ghi log HK: {room_no} - {action_type} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"Lỗi ghi log HK: {e}")
    
    def log_note_change(self, room_no, old_note, new_note, user_name):
        """Ghi log thay đổi ghi chú"""
        try:
            # Chỉ log nếu có thay đổi thực sự
            if old_note == new_note:
                return
            
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'user_name': user_name,
                'room_no': room_no,
                'activity_type': 'note_change',
                'old_note': old_note or '',
                'new_note': new_note or '',
                'action_type': 'cập nhật ghi chú',
                'action_detail': f'Ghi chú: "{old_note or "Trống"}" → "{new_note or "Trống"}"'
            }
            
            # Đọc log ghi chú hiện tại
            with open(self.notes_log_file, 'r', encoding='utf-8') as f:
                notes_logs = json.load(f)
            
            # Thêm log mới
            notes_logs.append(log_entry)
            
            # Giới hạn số lượng log (giữ 500 bản ghi gần nhất)
            if len(notes_logs) > 500:
                notes_logs = notes_logs[-500:]
            
            # Ghi lại file
            with open(self.notes_log_file, 'w', encoding='utf-8') as f:
                json.dump(notes_logs, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Đã ghi log ghi chú: {room_no} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"Lỗi ghi log ghi chú: {e}")
    
    def get_today_report(self):
        """Lấy báo cáo từ 8h15 đến hiện tại (cả trạng thái và ghi chú)"""
        try:
            # Đọc log trạng thái
            with open(self.log_file, 'r', encoding='utf-8') as f:
                status_logs = json.load(f)
            
            # Đọc log ghi chú
            with open(self.notes_log_file, 'r', encoding='utf-8') as f:
                notes_logs = json.load(f)
            
            # Kết hợp cả hai loại log
            all_logs = status_logs + notes_logs
            
            # Lọc log trong khoảng thời gian báo cáo
            today_logs = [log for log in all_logs if self._is_within_report_period(log['timestamp'])]
            
            # Sắp xếp theo thời gian mới nhất
            today_logs.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return today_logs
            
        except Exception as e:
            logger.error(f"Lỗi đọc báo cáo HK: {e}")
            return []
    
    def get_report_statistics(self, report_data):
        """Tính toán thống kê từ dữ liệu báo cáo"""
        stats = {
            'total_actions': len(report_data),
            'staff_stats': {},
            'activity_types': {
                'room_status': 0,
                'note_change': 0
            },
            'action_types': {
                'dọn phòng trống': 0,
                'dọn phòng ở': 0,
                'cập nhật ghi chú': 0
            }
        }
        
        for log in report_data:
            # Thống kê theo nhân viên
            staff_name = log['user_name']
            if staff_name not in stats['staff_stats']:
                stats['staff_stats'][staff_name] = {
                    'total': 0,
                    'dọn phòng trống': 0,
                    'dọn phòng ở': 0,
                    'cập nhật ghi chú': 0
                }
            
            stats['staff_stats'][staff_name]['total'] += 1
            
            # Thống kê theo loại hoạt động
            activity_type = log.get('activity_type', '')
            if activity_type in stats['activity_types']:
                stats['activity_types'][activity_type] += 1
            
            # Thống kê theo loại thao tác
            action_type = log.get('action_type', '')
            if action_type in stats['action_types']:
                stats['action_types'][action_type] += 1
                stats['staff_stats'][staff_name][action_type] += 1
        
        return stats
    
    def get_notes_history(self, room_no=None):
        """Lấy lịch sử ghi chú (có thể lọc theo phòng)"""
        try:
            with open(self.notes_log_file, 'r', encoding='utf-8') as f:
                notes_logs = json.load(f)
            
            if room_no:
                notes_logs = [log for log in notes_logs if log['room_no'] == room_no]
            
            # Sắp xếp theo thời gian mới nhất
            notes_logs.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return notes_logs
            
        except Exception as e:
            logger.error(f"Lỗi đọc lịch sử ghi chú: {e}")
            return []