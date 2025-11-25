# models/hk_logger.py
import logging
from datetime import datetime, timedelta
from config import Config

logger = logging.getLogger(__name__)

class HKLogger:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def log_room_status_change(self, room_no, old_status, new_status, user_name, user_department="HK"):
        """Ghi log thay đổi trạng thái phòng"""
        try:
            important_transitions = [
                ('vd', 'vc'), ('vd/arr', 'vc/arr'),
                ('od', 'oc'), ('od', 'dnd'), ('od', 'nn')
            ]
            
            if (old_status, new_status) not in important_transitions:
                return
            
            if (old_status, new_status) in [('vd', 'vc'), ('vd/arr', 'vc/arr')]:
                action_type = "dọn phòng trống"
                action_detail = f"{old_status.upper()} → {new_status.upper()}"
            else:
                action_type = "dọn phòng ở"
                action_detail = f"{old_status.upper()} → {new_status.upper()}"
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO activity_logs 
                        (user_name, user_department, room_no, action_type, old_status, new_status, action_detail)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        user_name,
                        user_department,
                        room_no,
                        action_type,
                        old_status,
                        new_status,
                        action_detail
                    ))
                conn.commit()
            
            logger.info(f"✅ Đã ghi log HK: {room_no} - {action_type} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi ghi log HK: {e}")
    
    def log_note_change(self, room_no, old_note, new_note, user_name, user_department="HK"):
        """Ghi log thay đổi ghi chú"""
        try:
            if old_note == new_note:
                return
            
            action_detail = f'Ghi chú: "{old_note or "Trống"}" → "{new_note or "Trống"}"'
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO activity_logs 
                        (user_name, user_department, room_no, action_type, action_detail)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        user_name,
                        user_department,
                        room_no,
                        'cập nhật ghi chú',
                        action_detail
                    ))
                conn.commit()
            
            logger.info(f"✅ Đã ghi log ghi chú: {room_no} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi ghi log ghi chú: {e}")
    
    def log_room_cleaning(self, room_no, user_name, user_department="HK", notes=""):
        """Ghi log dọn phòng"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO activity_logs 
                        (user_name, user_department, room_no, action_type, action_detail)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        user_name,
                        user_department,
                        room_no,
                        'dọn phòng',
                        notes or 'Đã hoàn thành dọn phòng'
                    ))
                conn.commit()
                
            logger.info(f"✅ Đã ghi log dọn phòng: {room_no} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi log_room_cleaning: {e}")
    
    def get_today_report(self):
        """Lấy báo cáo từ 8h15 đến hiện tại"""
        try:
            now = datetime.now()
            start_time = now.replace(
                hour=Config.HK_REPORT_START_HOUR, 
                minute=Config.HK_REPORT_START_MINUTE, 
                second=0, 
                microsecond=0
            )
            
            if now < start_time:
                start_time = start_time - timedelta(days=1)
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM activity_logs 
                        WHERE timestamp >= %s
                        ORDER BY timestamp DESC
                    ''', (start_time,))
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    report_data = []
                    for row in rows:
                        log_entry = dict(zip(columns, row))
                        
                        report_data.append({
                            'timestamp': log_entry.get('timestamp'),
                            'user_name': log_entry.get('user_name', ''),
                            'room_no': log_entry.get('room_no', ''),
                            'action_type': log_entry.get('action_type', ''),
                            'action_detail': log_entry.get('action_detail', ''),
                            'old_status': log_entry.get('old_status', ''),
                            'new_status': log_entry.get('new_status', ''),
                            'activity_type': 'room_status' if log_entry.get('old_status') else 'note_change'
                        })
                    
                    return report_data
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_today_report: {e}")
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
                'cập nhật ghi chú': 0,
                'dọn phòng': 0
            }
        }
        
        for log in report_data:
            staff_name = log['user_name']
            if staff_name not in stats['staff_stats']:
                stats['staff_stats'][staff_name] = {
                    'total': 0,
                    'dọn phòng trống': 0,
                    'dọn phòng ở': 0,
                    'cập nhật ghi chú': 0,
                    'dọn phòng': 0
                }
            
            stats['staff_stats'][staff_name]['total'] += 1
            
            activity_type = log.get('activity_type', '')
            if activity_type in stats['activity_types']:
                stats['activity_types'][activity_type] += 1
            
            action_type = log.get('action_type', '')
            if action_type in stats['action_types']:
                stats['action_types'][action_type] += 1
                if action_type in stats['staff_stats'][staff_name]:
                    stats['staff_stats'][staff_name][action_type] += 1
        
        return stats
    
    def get_notes_history(self, room_no=None):
        """Lấy lịch sử ghi chú (có thể lọc theo phòng)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    if room_no:
                        cur.execute('''
                            SELECT * FROM activity_logs 
                            WHERE action_type = 'cập nhật ghi chú' AND room_no = %s
                            ORDER BY timestamp DESC
                        ''', (room_no,))
                    else:
                        cur.execute('''
                            SELECT * FROM activity_logs 
                            WHERE action_type = 'cập nhật ghi chú'
                            ORDER BY timestamp DESC
                        ''')
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    notes_history = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        notes_history.append({
                            'timestamp': row_dict.get('timestamp'),
                            'user_name': row_dict.get('user_name', ''),
                            'room_no': row_dict.get('room_no', ''),
                            'action_detail': row_dict.get('action_detail', ''),
                            'old_note': '',
                            'new_note': ''
                        })
                    
                    return notes_history
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_notes_history: {e}")
            return []
    
    def clear_all_logs(self):
        """Xóa toàn bộ logs (chỉ FO)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('DELETE FROM activity_logs')
                    conn.commit()
                    logger.info("✅ Đã xóa toàn bộ logs HK")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Lỗi clear_all_logs: {e}")
            return False
    
    def get_activity_by_user(self, user_name, days=7):
        """Lấy hoạt động của một nhân viên trong khoảng thời gian"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM activity_logs 
                        WHERE user_name = %s AND timestamp >= %s
                        ORDER BY timestamp DESC
                    ''', (user_name, start_date))
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    return [dict(zip(columns, row)) for row in rows]
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_activity_by_user: {e}")
            return []
    
    def get_room_activity_history(self, room_no, limit=50):
        """Lấy lịch sử hoạt động của một phòng"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM activity_logs 
                        WHERE room_no = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    ''', (room_no, limit))
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    return [dict(zip(columns, row)) for row in rows]
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_room_activity_history: {e}")
            return []