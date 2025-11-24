# models/hk_logger.py
import logging
from datetime import datetime, timedelta
from config import Config

logger = logging.getLogger(__name__)

class HKLogger:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def log_room_status_change(self, room_no, old_status, new_status, user_name, user_department="HK"):
        """Ghi log thay đổi trạng thái phòng vào PostgreSQL"""
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
            
            # Insert vào PostgreSQL
            log_data = {
                'room_no': room_no,
                'old_status': old_status,
                'new_status': new_status,
                'changed_by': user_name,
                'department': user_department,
                'change_time': datetime.now(),
                'notes': action_detail
            }
            
            self.db.execute_insert("""
                INSERT INTO hk_logs (room_no, old_status, new_status, changed_by, department, change_time, notes)
                VALUES (%(room_no)s, %(old_status)s, %(new_status)s, %(changed_by)s, %(department)s, %(change_time)s, %(notes)s)
            """, log_data)
            
            logger.info(f"✅ Đã ghi log HK: {room_no} - {action_type} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi ghi log HK: {e}")
    
    def log_note_change(self, room_no, old_note, new_note, user_name, user_department="HK"):
        """Ghi log thay đổi ghi chú vào PostgreSQL"""
        try:
            # Chỉ log nếu có thay đổi thực sự
            if old_note == new_note:
                return
            
            action_detail = f'Ghi chú: "{old_note or "Trống"}" → "{new_note or "Trống"}"'
            
            # Insert vào PostgreSQL
            log_data = {
                'room_no': room_no,
                'old_status': '',  # Không có thay đổi trạng thái
                'new_status': '',  # Không có thay đổi trạng thái
                'changed_by': user_name,
                'department': user_department,
                'change_time': datetime.now(),
                'notes': action_detail
            }
            
            self.db.execute_insert("""
                INSERT INTO hk_logs (room_no, old_status, new_status, changed_by, department, change_time, notes)
                VALUES (%(room_no)s, %(old_status)s, %(new_status)s, %(changed_by)s, %(department)s, %(change_time)s, %(notes)s)
            """, log_data)
            
            logger.info(f"✅ Đã ghi log ghi chú: {room_no} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi ghi log ghi chú: {e}")
    
    def log_room_cleaning(self, room_no, user_name, user_department="HK", notes=""):
        """Ghi log dọn phòng vào PostgreSQL"""
        try:
            log_data = {
                'room_no': room_no,
                'old_status': '',  # Không có thay đổi trạng thái cụ thể
                'new_status': '',  # Không có thay đổi trạng thái cụ thể
                'changed_by': user_name,
                'department': user_department,
                'change_time': datetime.now(),
                'notes': notes or 'Đã hoàn thành dọn phòng'
            }
            
            self.db.execute_insert("""
                INSERT INTO hk_logs (room_no, old_status, new_status, changed_by, department, change_time, notes)
                VALUES (%(room_no)s, %(old_status)s, %(new_status)s, %(changed_by)s, %(department)s, %(change_time)s, %(notes)s)
            """, log_data)
                
            logger.info(f"✅ Đã ghi log dọn phòng: {room_no} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi log_room_cleaning: {e}")
    
    def get_today_report(self):
        """Lấy báo cáo từ 8h15 đến hiện tại từ PostgreSQL"""
        try:
            now = datetime.now()
            start_time = now.replace(
                hour=Config.HK_REPORT_START_HOUR, 
                minute=Config.HK_REPORT_START_MINUTE, 
                second=0, 
                microsecond=0
            )
            
            # Nếu bây giờ là trước 8h15, thì lấy từ 8h15 ngày hôm trước
            if now < start_time:
                start_time = start_time - timedelta(days=1)
            
            rows = self.db.get_all("""
                SELECT * FROM hk_logs 
                WHERE change_time >= %s
                ORDER BY change_time DESC
            """, (start_time,))
            
            # Convert để tương thích với format frontend
            report_data = []
            for row in rows:
                # Xác định action_type dựa trên nội dung
                action_type = self._determine_action_type(row)
                
                report_data.append({
                    'timestamp': row['change_time'],
                    'user_name': row['changed_by'],
                    'room_no': row['room_no'],
                    'action_type': action_type,
                    'action_detail': row['notes'],
                    'old_status': row['old_status'] or '',
                    'new_status': row['new_status'] or '',
                    'department': row['department'],
                    'activity_type': 'room_status' if row['old_status'] and row['new_status'] else 'note_change'
                })
            
            return report_data
            
        except Exception as e:
            logger.error(f"❌ Lỗi get_today_report: {e}")
            return []
    
    def _determine_action_type(self, log_row):
        """Xác định loại hành động từ dữ liệu log"""
        notes = log_row['notes'] or ''
        old_status = log_row['old_status'] or ''
        new_status = log_row['new_status'] or ''
        
        if 'dọn phòng trống' in notes.lower():
            return 'dọn phòng trống'
        elif 'dọn phòng ở' in notes.lower():
            return 'dọn phòng ở'
        elif 'ghi chú' in notes.lower():
            return 'cập nhật ghi chú'
        elif 'dọn phòng' in notes.lower():
            return 'dọn phòng'
        elif old_status and new_status:
            if (old_status, new_status) in [('vd', 'vc'), ('vd/arr', 'vc/arr')]:
                return 'dọn phòng trống'
            else:
                return 'dọn phòng ở'
        else:
            return 'hoạt động khác'
    
    def get_report_statistics(self, report_data):
        """Tính toán thống kê từ dữ liệu báo cáo"""
        stats = {
            'total_actions': len(report_data),
            'staff_stats': {},
            'activity_types': {
                'room_status': 0,
                'note_change': 0,
                'other': 0
            },
            'action_types': {
                'dọn phòng trống': 0,
                'dọn phòng ở': 0,
                'cập nhật ghi chú': 0,
                'dọn phòng': 0,
                'hoạt động khác': 0
            },
            'department_stats': {
                'HK': 0,
                'FO': 0,
                'Other': 0
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
                    'cập nhật ghi chú': 0,
                    'dọn phòng': 0,
                    'hoạt động khác': 0
                }
            
            stats['staff_stats'][staff_name]['total'] += 1
            
            # Thống kê theo loại hoạt động
            activity_type = log.get('activity_type', 'other')
            if activity_type in stats['activity_types']:
                stats['activity_types'][activity_type] += 1
            else:
                stats['activity_types']['other'] += 1
            
            # Thống kê theo loại thao tác
            action_type = log.get('action_type', 'hoạt động khác')
            if action_type in stats['action_types']:
                stats['action_types'][action_type] += 1
                if action_type in stats['staff_stats'][staff_name]:
                    stats['staff_stats'][staff_name][action_type] += 1
            
            # Thống kê theo department
            department = log.get('department', 'Other')
            if department in stats['department_stats']:
                stats['department_stats'][department] += 1
            else:
                stats['department_stats']['Other'] += 1
        
        return stats
    
    def get_notes_history(self, room_no=None):
        """Lấy lịch sử ghi chú từ PostgreSQL (có thể lọc theo phòng)"""
        try:
            if room_no:
                rows = self.db.get_all("""
                    SELECT * FROM hk_logs 
                    WHERE notes ILIKE %s AND room_no = %s
                    ORDER BY change_time DESC
                """, ('%ghi chú%', room_no))
            else:
                rows = self.db.get_all("""
                    SELECT * FROM hk_logs 
                    WHERE notes ILIKE %s
                    ORDER BY change_time DESC
                """, ('%ghi chú%',))
            
            notes_history = []
            for row in rows:
                # Parse action_detail để lấy thông tin ghi chú cũ và mới
                old_note, new_note = self._parse_note_change(row['notes'])
                
                notes_history.append({
                    'timestamp': row['change_time'],
                    'user_name': row['changed_by'],
                    'room_no': row['room_no'],
                    'action_detail': row['notes'],
                    'old_note': old_note,
                    'new_note': new_note
                })
            
            return notes_history
            
        except Exception as e:
            logger.error(f"❌ Lỗi get_notes_history: {e}")
            return []
    
    def _parse_note_change(self, action_detail):
        """Parse thông tin thay đổi ghi chú từ action_detail"""
        try:
            if '→' in action_detail:
                parts = action_detail.split('→')
                if len(parts) == 2:
                    old_note = parts[0].split('"')[1] if '"' in parts[0] else parts[0].split(':')[1].strip()
                    new_note = parts[1].split('"')[1] if '"' in parts[1] else parts[1].strip()
                    return old_note, new_note
            return '', ''
        except:
            return '', ''
    
    def clear_all_logs(self):
        """Xóa toàn bộ logs từ PostgreSQL (chỉ FO)"""
        try:
            row_count = self.db.execute_delete("DELETE FROM hk_logs")
            logger.info(f"✅ Đã xóa {row_count} logs HK từ PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi clear_all_logs: {e}")
            return False
    
    def get_activity_by_user(self, user_name, days=7):
        """Lấy hoạt động của một nhân viên trong khoảng thời gian từ PostgreSQL"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            
            rows = self.db.get_all("""
                SELECT * FROM hk_logs 
                WHERE changed_by = %s AND change_time >= %s
                ORDER BY change_time DESC
            """, (user_name, start_date))
            
            return rows
            
        except Exception as e:
            logger.error(f"❌ Lỗi get_activity_by_user: {e}")
            return []
    
    def get_room_activity_history(self, room_no, limit=50):
        """Lấy lịch sử hoạt động của một phòng từ PostgreSQL"""
        try:
            rows = self.db.get_all("""
                SELECT * FROM hk_logs 
                WHERE room_no = %s
                ORDER BY change_time DESC
                LIMIT %s
            """, (room_no, limit))
            
            return rows
            
        except Exception as e:
            logger.error(f"❌ Lỗi get_room_activity_history: {e}")
            return []
    
    def get_department_activity(self, department, days=1):
        """Lấy hoạt động của một department trong khoảng thời gian"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            
            rows = self.db.get_all("""
                SELECT * FROM hk_logs 
                WHERE department = %s AND change_time >= %s
                ORDER BY change_time DESC
            """, (department, start_date))
            
            return rows
            
        except Exception as e:
            logger.error(f"❌ Lỗi get_department_activity: {e}")
            return []
    
    def get_performance_stats(self, days=7):
        """Lấy thống kê hiệu suất trong khoảng thời gian"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            
            # Thống kê theo user
            user_stats = self.db.get_all("""
                SELECT changed_by, department, COUNT(*) as action_count
                FROM hk_logs 
                WHERE change_time >= %s
                GROUP BY changed_by, department
                ORDER BY action_count DESC
            """, (start_date,))
            
            # Thống kê theo loại hành động
            action_stats = self.db.get_all("""
                SELECT 
                    CASE 
                        WHEN old_status = 'vd' AND new_status = 'vc' THEN 'dọn phòng trống'
                        WHEN old_status = 'od' AND new_status = 'oc' THEN 'dọn phòng ở'
                        WHEN notes ILIKE '%ghi chú%' THEN 'cập nhật ghi chú'
                        ELSE 'hoạt động khác'
                    END as action_type,
                    COUNT(*) as count
                FROM hk_logs 
                WHERE change_time >= %s
                GROUP BY action_type
            """, (start_date,))
            
            # Thống kê theo phòng
            room_stats = self.db.get_all("""
                SELECT room_no, COUNT(*) as activity_count
                FROM hk_logs 
                WHERE change_time >= %s
                GROUP BY room_no
                ORDER BY activity_count DESC
                LIMIT 10
            """, (start_date,))
            
            return {
                'user_stats': user_stats,
                'action_stats': action_stats,
                'room_stats': room_stats,
                'period_days': days,
                'total_activities': sum(stat['action_count'] for stat in user_stats) if user_stats else 0
            }
            
        except Exception as e:
            logger.error(f"❌ Lỗi get_performance_stats: {e}")
            return {
                'user_stats': [],
                'action_stats': [],
                'room_stats': [],
                'period_days': days,
                'total_activities': 0
            }