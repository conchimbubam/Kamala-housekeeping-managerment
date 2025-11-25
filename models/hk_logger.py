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
            # Chỉ log các chuyển đổi quan trọng
            important_transitions = [
                ('vd', 'vc'), ('vd/arr', 'vc/arr'),  # Dọn phòng trống
                ('od', 'oc'), ('od', 'dnd'), ('od', 'nn'),  # Dọn phòng ở
                ('vc', 'vd'), ('vc/arr', 'vd/arr'),  # Chuyển từ clean sang dirty
                ('oc', 'od')  # Chuyển từ clean sang dirty (occupied)
            ]
            
            if (old_status, new_status) not in important_transitions:
                logger.debug(f"Bỏ qua log không quan trọng: {room_no} {old_status}→{new_status}")
                return
            
            # Xác định loại thao tác
            if (old_status, new_status) in [('vd', 'vc'), ('vd/arr', 'vc/arr')]:
                action_type = "dọn phòng trống"
                action_detail = f"{old_status.upper()} → {new_status.upper()}"
            elif (old_status, new_status) in [('vc', 'vd'), ('vc/arr', 'vd/arr')]:
                action_type = "đánh dấu phòng bẩn"
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
            
            logger.info(f"✅ Đã ghi log HK: {room_no} - {action_type} ({old_status}→{new_status}) bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi ghi log HK: {e}")
    
    def log_note_change(self, room_no, old_note, new_note, user_name, user_department="HK"):
        """Ghi log thay đổi ghi chú"""
        try:
            # Chỉ log nếu có thay đổi thực sự
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
        """Ghi log dọn phòng (bổ sung thêm)"""
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
    
    def log_guest_checkin(self, room_no, guest_name, user_name, user_department="FO"):
        """Ghi log check-in khách"""
        try:
            action_detail = f'Check-in khách: {guest_name}'
            
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
                        'check-in',
                        action_detail
                    ))
                conn.commit()
            
            logger.info(f"✅ Đã ghi log check-in: {room_no} - {guest_name} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi log_guest_checkin: {e}")
    
    def log_guest_checkout(self, room_no, guest_name, user_name, user_department="FO"):
        """Ghi log check-out khách"""
        try:
            action_detail = f'Check-out khách: {guest_name}'
            
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
                        'check-out',
                        action_detail
                    ))
                conn.commit()
            
            logger.info(f"✅ Đã ghi log check-out: {room_no} - {guest_name} bởi {user_name}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi log_guest_checkout: {e}")
    
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
            
            # Nếu bây giờ là trước 8h15, thì lấy từ 8h15 ngày hôm trước
            if now < start_time:
                start_time = start_time - timedelta(days=1)
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM activity_logs 
                        WHERE timestamp >= %s
                        ORDER BY timestamp DESC
                    ''', (start_time,))
                    
                    rows = cur.fetchall()
                    
                    # Convert để tương thích với format cũ
                    report_data = []
                    for row in rows:
                        log_entry = dict(row)
                        
                        # Xác định activity_type dựa trên dữ liệu
                        if log_entry.get('old_status'):
                            activity_type = 'room_status'
                        elif log_entry['action_type'] == 'cập nhật ghi chú':
                            activity_type = 'note_change'
                        elif log_entry['action_type'] in ['check-in', 'check-out']:
                            activity_type = 'guest_activity'
                        else:
                            activity_type = 'cleaning'
                        
                        report_data.append({
                            'timestamp': log_entry['timestamp'],
                            'user_name': log_entry['user_name'],
                            'user_department': log_entry['user_department'],
                            'room_no': log_entry['room_no'],
                            'action_type': log_entry['action_type'],
                            'action_detail': log_entry['action_detail'],
                            'old_status': log_entry.get('old_status', ''),
                            'new_status': log_entry.get('new_status', ''),
                            'activity_type': activity_type,
                            'id': log_entry['id']
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
                'note_change': 0,
                'cleaning': 0,
                'guest_activity': 0
            },
            'action_types': {
                'dọn phòng trống': 0,
                'dọn phòng ở': 0,
                'cập nhật ghi chú': 0,
                'dọn phòng': 0,
                'check-in': 0,
                'check-out': 0,
                'đánh dấu phòng bẩn': 0
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
                    'check-in': 0,
                    'check-out': 0,
                    'đánh dấu phòng bẩn': 0,
                    'department': log.get('user_department', 'Unknown')
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
                if action_type in stats['staff_stats'][staff_name]:
                    stats['staff_stats'][staff_name][action_type] += 1
            
            # Thống kê theo department
            department = log.get('user_department', 'Other')
            if department in stats['department_stats']:
                stats['department_stats'][department] += 1
            else:
                stats['department_stats']['Other'] += 1
        
        return stats
    
    def get_notes_history(self, room_no=None, limit=100):
        """Lấy lịch sử ghi chú (có thể lọc theo phòng)"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    if room_no:
                        cur.execute('''
                            SELECT * FROM activity_logs 
                            WHERE action_type = 'cập nhật ghi chú' AND room_no = %s
                            ORDER BY timestamp DESC
                            LIMIT %s
                        ''', (room_no, limit))
                    else:
                        cur.execute('''
                            SELECT * FROM activity_logs 
                            WHERE action_type = 'cập nhật ghi chú'
                            ORDER BY timestamp DESC
                            LIMIT %s
                        ''', (limit,))
                    
                    rows = cur.fetchall()
                    
                    notes_history = []
                    for row in rows:
                        notes_history.append({
                            'timestamp': row['timestamp'],
                            'user_name': row['user_name'],
                            'user_department': row['user_department'],
                            'room_no': row['room_no'],
                            'action_detail': row['action_detail'],
                            'id': row['id']
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
                    cur.execute('ALTER SEQUENCE activity_logs_id_seq RESTART WITH 1')
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
                    
                    rows = cur.fetchall()
                    return [dict(row) for row in rows]
                    
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
                    
                    rows = cur.fetchall()
                    return [dict(row) for row in rows]
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_room_activity_history: {e}")
            return []
    
    def get_daily_summary(self, date=None):
        """Lấy tổng hợp hoạt động theo ngày"""
        try:
            if date is None:
                date = datetime.now().date()
            
            start_of_day = datetime.combine(date, datetime.min.time())
            end_of_day = datetime.combine(date, datetime.max.time())
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Thống kê theo loại hành động
                    cur.execute('''
                        SELECT action_type, COUNT(*) as count
                        FROM activity_logs 
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY action_type
                        ORDER BY count DESC
                    ''', (start_of_day, end_of_day))
                    
                    action_stats = {row['action_type']: row['count'] for row in cur.fetchall()}
                    
                    # Thống kê theo nhân viên
                    cur.execute('''
                        SELECT user_name, user_department, COUNT(*) as count
                        FROM activity_logs 
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY user_name, user_department
                        ORDER BY count DESC
                    ''', (start_of_day, end_of_day))
                    
                    staff_stats = []
                    for row in cur.fetchall():
                        staff_stats.append({
                            'user_name': row['user_name'],
                            'user_department': row['user_department'],
                            'count': row['count']
                        })
                    
                    # Tổng số hoạt động
                    cur.execute('''
                        SELECT COUNT(*) as total
                        FROM activity_logs 
                        WHERE timestamp BETWEEN %s AND %s
                    ''', (start_of_day, end_of_day))
                    
                    total = cur.fetchone()['total']
                    
                    return {
                        'date': date,
                        'total_activities': total,
                        'action_types': action_stats,
                        'staff_performance': staff_stats,
                        'period': f"{start_of_day.strftime('%H:%M')} - {end_of_day.strftime('%H:%M')}"
                    }
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_daily_summary: {e}")
            return {}
    
    def get_cleaning_performance(self, start_date=None, end_date=None):
        """Thống kê hiệu suất dọn phòng"""
        try:
            if start_date is None:
                start_date = datetime.now() - timedelta(days=30)
            if end_date is None:
                end_date = datetime.now()
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Thống kê theo nhân viên
                    cur.execute('''
                        SELECT 
                            user_name,
                            COUNT(*) as total_cleanings,
                            COUNT(DISTINCT room_no) as unique_rooms,
                            MIN(timestamp) as first_activity,
                            MAX(timestamp) as last_activity
                        FROM activity_logs 
                        WHERE timestamp BETWEEN %s AND %s
                        AND action_type IN ('dọn phòng trống', 'dọn phòng ở', 'dọn phòng')
                        GROUP BY user_name
                        ORDER BY total_cleanings DESC
                    ''', (start_date, end_date))
                    
                    performance_data = []
                    for row in cur.fetchall():
                        performance_data.append({
                            'user_name': row['user_name'],
                            'total_cleanings': row['total_cleanings'],
                            'unique_rooms': row['unique_rooms'],
                            'first_activity': row['first_activity'],
                            'last_activity': row['last_activity'],
                            'avg_per_room': round(row['total_cleanings'] / max(row['unique_rooms'], 1), 2)
                        })
                    
                    return performance_data
                    
        except Exception as e:
            logger.error(f"❌ Lỗi get_cleaning_performance: {e}")
            return []
    
    def export_activity_logs(self, start_date=None, end_date=None, format_type='json'):
        """Xuất logs hoạt động"""
        try:
            if start_date is None:
                start_date = datetime.now() - timedelta(days=30)
            if end_date is None:
                end_date = datetime.now()
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM activity_logs 
                        WHERE timestamp BETWEEN %s AND %s
                        ORDER BY timestamp DESC
                    ''', (start_date, end_date))
                    
                    logs = [dict(row) for row in cur.fetchall()]
                    
                    if format_type == 'json':
                        return {
                            'success': True,
                            'data': logs,
                            'metadata': {
                                'start_date': start_date,
                                'end_date': end_date,
                                'total_records': len(logs),
                                'exported_at': datetime.now()
                            }
                        }
                    else:
                        # Có thể mở rộng cho các format khác (CSV, Excel)
                        return {
                            'success': False,
                            'error': f'Format {format_type} chưa được hỗ trợ'
                        }
                    
        except Exception as e:
            logger.error(f"❌ Lỗi export_activity_logs: {e}")
            return {'success': False, 'error': str(e)}
    
    def search_activity_logs(self, search_term, limit=100):
        """Tìm kiếm trong logs hoạt động"""
        try:
            search_pattern = f"%{search_term}%"
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        SELECT * FROM activity_logs 
                        WHERE user_name ILIKE %s 
                           OR room_no ILIKE %s 
                           OR action_type ILIKE %s
                           OR action_detail ILIKE %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    ''', (search_pattern, search_pattern, search_pattern, search_pattern, limit))
                    
                    rows = cur.fetchall()
                    return [dict(row) for row in rows]
                    
        except Exception as e:
            logger.error(f"❌ Lỗi search_activity_logs: {e}")
            return []


# Test function
def test_hk_logger():
    """Test HKLogger độc lập"""
    try:
        from database import DatabaseManager
        
        db = DatabaseManager()
        hk_logger = HKLogger(db)
        
        # Test các chức năng cơ bản
        today_report = hk_logger.get_today_report()
        print(f"✅ Today's report: {len(today_report)} records")
        
        stats = hk_logger.get_report_statistics(today_report)
        print(f"📊 Report statistics: {stats['total_actions']} total actions")
        
        notes_history = hk_logger.get_notes_history(limit=5)
        print(f"📝 Notes history: {len(notes_history)} records")
        
        daily_summary = hk_logger.get_daily_summary()
        print(f"📈 Daily summary: {daily_summary.get('total_activities', 0)} activities")
        
        return True
        
    except Exception as e:
        print(f"❌ HKLogger test error: {e}")
        return False


if __name__ == '__main__':
    print("🧪 Testing HKLogger...")
    test_hk_logger()