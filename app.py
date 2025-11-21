from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from config import Config
from models.database import DatabaseManager
from models.data_processor import DataProcessor
from models.hk_logger import HKLogger
import logging
from datetime import datetime, timedelta
from functools import wraps
import os
import shutil
import threading
import time
from pathlib import Path

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Factory function để tạo Flask app với SQLite"""
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config['SECRET_KEY'] = Config.SECRET_KEY
    
    # Khởi tạo database manager
    db_manager = DatabaseManager(Config.DATABASE_PATH)
    
    # Khởi tạo data processor với database
    data_processor = DataProcessor(
        db_manager=db_manager,
        api_key=Config.API_KEY,
        spreadsheet_id=Config.SPREADSHEET_ID,
        range_name=Config.RANGE_NAME
    )
    
    # Khởi tạo HK logger với database
    hk_logger = HKLogger(db_manager)
    
    # Lưu các instances vào app context
    app.db_manager = db_manager
    app.data_processor = data_processor
    app.hk_logger = hk_logger

    # ==================== BACKUP SYSTEM (EVENT-BASED) ====================

    def create_backup():
        """Tạo bản sao lưu database - CHỈ GIỮ 5 BẢN GẦN NHẤT"""
        try:
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            
            # Tạo tên file backup với timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"hotel_backup_{timestamp}.db"
            
            # Sao chép file database
            shutil.copy2(Config.DATABASE_PATH, backup_file)
            
            # CHỈ GIỮ LẠI 5 BACKUP GẦN NHẤT (thay vì 24)
            cleanup_old_backups(backup_dir, keep_count=5)
            
            logger.info(f"✅ Đã tạo backup: {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo backup: {e}")
            return False

    def cleanup_old_backups(backup_dir, keep_count=5):
        """Xóa các bản backup cũ, chỉ giữ lại `keep_count` bản mới nhất"""
        try:
            backup_files = list(backup_dir.glob("hotel_backup_*.db"))
            if len(backup_files) > keep_count:
                # Sắp xếp theo thời gian tạo (cũ nhất đầu tiên)
                backup_files.sort(key=os.path.getctime)
                # Xóa các file cũ vượt quá số lượng giữ lại
                for old_file in backup_files[:-keep_count]:
                    os.remove(old_file)
                    logger.info(f"🗑️ Đã xóa backup cũ: {old_file}")
        except Exception as e:
            logger.error(f"Lỗi khi dọn dẹp backup cũ: {e}")

    def restore_latest_backup():
        """Tìm và khôi phục backup gần nhất khi app khởi động"""
        try:
            backup_dir = Path("backups")
            if not backup_dir.exists():
                logger.info("📂 Thư mục backup không tồn tại")
                return False
            
            backup_files = list(backup_dir.glob("hotel_backup_*.db"))
            if not backup_files:
                logger.info("📭 Không tìm thấy file backup nào")
                return False
            
            # Sắp xếp theo thời gian tạo (mới nhất đầu tiên)
            backup_files.sort(key=os.path.getctime, reverse=True)
            latest_backup = backup_files[0]
            
            # Sao chép backup vào database chính
            shutil.copy2(latest_backup, Config.DATABASE_PATH)
            
            backup_time = datetime.fromtimestamp(latest_backup.stat().st_ctime)
            logger.info(f"✅ Đã khôi phục từ backup: {latest_backup.name} (tạo lúc {backup_time})")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi khôi phục backup: {e}")
            return False

    # ==================== DECORATORS PHÂN QUYỀN ====================

    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated_function

    def fo_required(f):
        """Chỉ FO mới được truy cập"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_info = session.get('user_info', {})
            if user_info.get('department') != 'FO':
                return jsonify({
                    'success': False,
                    'error': 'Chỉ Front Office mới được thực hiện chức năng này'
                }), 403
            return f(*args, **kwargs)
        return decorated_function

    def hk_required(f):
        """HK và FO đều được truy cập"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_info = session.get('user_info', {})
            if user_info.get('department') not in ['HK', 'FO']:
                return jsonify({
                    'success': False,
                    'error': 'Chỉ House Keeping và Front Office mới được thực hiện chức năng này'
                }), 403
            return f(*args, **kwargs)
        return decorated_function

    # ==================== ROUTES CHÍNH ====================

    @app.route('/')
    @login_required
    def dashboard():
        """Trang chủ dashboard"""
        user_info = session.get('user_info', {})
        return render_template('dashboard.html', user=user_info)

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        """Trang đăng nhập"""
        if session.get('logged_in'):
            return redirect(url_for('dashboard'))
        
        if request.method == 'POST':
            name = request.form.get('name', '').strip()
            department = request.form.get('department', '')
            department_code = request.form.get('department_code', '')
            
            if not name or not department or not department_code:
                return render_template('login.html', 
                                    error='Vui lòng điền đầy đủ thông tin')
            
            if department_code != Config.DEPARTMENT_CODE:
                return render_template('login.html', 
                                    error='Mã bộ phận không chính xác')
            
            session['logged_in'] = True
            session['user_info'] = {
                'name': name,
                'department': department,
                'login_time': datetime.now().strftime('%H:%M %d/%m/%Y')
            }
            
            logger.info(f"User logged in: {name} - {department}")
            return redirect(url_for('dashboard'))
        
        return render_template('login.html')

    @app.route('/logout')
    def logout():
        """Đăng xuất"""
        user_info = session.get('user_info', {})
        logger.info(f"User logged out: {user_info.get('name', 'Unknown')}")
        session.clear()
        return redirect(url_for('login'))

    @app.route('/print-tasksheet')
    @login_required
    @fo_required
    def print_tasksheet():
        """Route để in tasksheet - chỉ dành cho FO"""
        try:
            # Lấy dữ liệu phòng từ database
            result = app.data_processor.get_all_rooms()
            if not result['success']:
                return render_template('error.html', error="Không thể tải dữ liệu phòng"), 500

            rooms_data = result['data']
            
            # Lấy thông tin file để hiển thị thời gian cập nhật
            file_info = app.data_processor.get_room_info()
            
            # Truyền dữ liệu vào template tasksheet
            return render_template('Tasksheet.html', 
                                 rooms=rooms_data,
                                 file_info=file_info,
                                 current_time=datetime.now())
                                 
        except Exception as e:
            logger.error(f"Lỗi khi tạo tasksheet: {e}")
            return render_template('error.html', error="Lỗi khi tạo tasksheet"), 500

    # ==================== API BACKUP MANAGEMENT ====================

    @app.route('/api/backup/create', methods=['POST'])
    @login_required
    @fo_required
    def manual_backup():
        """API tạo backup thủ công (chỉ FO)"""
        try:
            success = create_backup()
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Đã tạo bản sao lưu thành công',
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Không thể tạo bản sao lưu'
                }), 500
        except Exception as e:
            logger.error(f"Lỗi tạo backup thủ công: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi tạo backup: {str(e)}'
            }), 500

    @app.route('/api/backup/list')
    @login_required
    @fo_required
    def list_backups():
        """API liệt kê các bản backup có sẵn"""
        try:
            backup_dir = Path("backups")
            backup_files = []
            
            if backup_dir.exists():
                for file_path in backup_dir.glob("hotel_backup_*.db"):
                    stat = file_path.stat()
                    backup_files.append({
                        'filename': file_path.name,
                        'size': round(stat.st_size / 1024 / 1024, 2),  # MB
                        'created': datetime.fromtimestamp(stat.st_ctime).strftime('%H:%M %d/%m/%Y'),
                        'filepath': str(file_path)
                    })
                
                # Sắp xếp mới nhất đầu tiên
                backup_files.sort(key=lambda x: x['created'], reverse=True)
            
            return jsonify({
                'success': True,
                'data': backup_files,
                'total': len(backup_files),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Lỗi liệt kê backup: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi liệt kê backup: {str(e)}'
            }), 500

    @app.route('/api/backup/restore', methods=['POST'])
    @login_required
    @fo_required
    def restore_backup():
        """API khôi phục từ bản backup (chỉ FO)"""
        try:
            data = request.get_json()
            filename = data.get('filename')
            
            if not filename:
                return jsonify({
                    'success': False,
                    'error': 'Thiếu tên file backup'
                }), 400
            
            backup_path = Path("backups") / filename
            
            if not backup_path.exists():
                return jsonify({
                    'success': False,
                    'error': 'File backup không tồn tại'
                }), 404
            
            # Tạo backup hiện tại trước khi restore
            create_backup()
            
            # Sao chép file backup vào vị trí database chính
            shutil.copy2(backup_path, Config.DATABASE_PATH)
            
            logger.info(f"✅ Đã khôi phục từ backup: {filename}")
            
            return jsonify({
                'success': True,
                'message': f'Đã khôi phục thành công từ {filename}',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Lỗi khôi phục backup: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi khôi phục: {str(e)}'
            }), 500

    # ==================== API ENDPOINTS ====================

    @app.route('/api/user-info')
    @login_required
    def get_user_info():
        """API endpoint trả về thông tin người dùng"""
        return jsonify({
            'success': True,
            'data': session.get('user_info', {})
        })
    
    @app.route('/api/rooms')
    @login_required
    def get_rooms():
        """API endpoint trả về dữ liệu tất cả phòng"""
        try:
            result = app.data_processor.get_all_rooms()
            file_info = app.data_processor.get_room_info()
            
            if result['success']:
                return jsonify({
                    'success': True,
                    'data': result['data'],
                    'total': len(result['data']),
                    'file_info': file_info,
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                }), 500
                
        except Exception as e:
            logger.error(f"API Error in get_rooms: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/statistics')
    @login_required
    def get_statistics():
        """API endpoint trả về thống kê trạng thái phòng"""
        try:
            stats = app.data_processor.get_statistics()
            return jsonify({
                'success': True,
                'data': stats,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"API Error in get_statistics: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/floors')
    @login_required
    def get_floors():
        """API endpoint trả về phòng được nhóm theo tầng"""
        try:
            floors = app.data_processor.get_rooms_by_floor()
            return jsonify({
                'success': True,
                'data': floors,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"API Error in get_floors: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    # ==================== API BÁO CÁO HK ====================

    @app.route('/api/report/hk')
    @login_required
    @hk_required
    def get_hk_report():
        """API lấy báo cáo hoạt động HK"""
        try:
            report_data = app.hk_logger.get_today_report()
            statistics = app.hk_logger.get_report_statistics(report_data)
            
            return jsonify({
                'success': True,
                'data': report_data,
                'statistics': statistics,
                'report_period': 'Từ 8h15 đến hiện tại',
                'total_records': len(report_data),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Lỗi lấy báo cáo HK: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/api/report/hk/export')
    @login_required
    @hk_required
    def export_hk_report():
        """Xuất báo cáo HK dạng HTML để in"""
        try:
            # Lấy dữ liệu báo cáo
            report_data = app.hk_logger.get_today_report()
            statistics = app.hk_logger.get_report_statistics(report_data)

            # Render template print_report.html và trả về
            return render_template('print_report.html', 
                                 report_data=report_data, 
                                 statistics=statistics,
                                 report_time=datetime.now())
        except Exception as e:
            logger.error(f"Lỗi xuất báo cáo HK: {e}")
            return "Lỗi khi tạo báo cáo", 500

    @app.route('/api/report/hk/clear', methods=['POST'])
    @login_required
    @fo_required
    def clear_hk_report():
        """API xóa toàn bộ lịch sử báo cáo HK (chỉ FO)"""
        try:
            success = app.hk_logger.clear_all_logs()
            
            if success:
                logger.info("Đã xóa toàn bộ lịch sử báo cáo HK")
                return jsonify({
                    'success': True,
                    'message': 'Đã xóa toàn bộ lịch sử báo cáo HK',
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Không thể xóa logs HK'
                }), 500
                
        except Exception as e:
            logger.error(f"Lỗi xóa báo cáo HK: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    # ==================== API PHÂN QUYỀN ====================

    @app.route('/api/refresh', methods=['POST'])
    @login_required
    @fo_required
    def refresh_data():
        """API endpoint để refresh dữ liệu từ Google Sheets (chỉ FO) - CÓ BACKUP"""
        try:
            user_info = f"{session.get('user_info', {}).get('name', 'Unknown')} ({session.get('user_info', {}).get('department', 'Unknown')})"
            
            # ✅ TẠO BACKUP TRƯỚC KHI REFRESH (vì sẽ thay đổi nhiều dữ liệu)
            threading.Thread(target=create_backup, daemon=True).start()
            
            # Sử dụng phương thức mới để khởi tạo từ Google Sheets
            success = app.data_processor.initialize_rooms_from_google_sheets(user_info)
            
            if success:
                result = app.data_processor.get_all_rooms()
                total_rooms = len(result['data']) if result['success'] else 0
                
                logger.info(f"Data refreshed by {user_info}. Total rooms: {total_rooms}")
                
                return jsonify({
                    'success': True,
                    'message': 'Dữ liệu đã được cập nhật thành công từ Google Sheets',
                    'total_rooms': total_rooms,
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Không thể cập nhật dữ liệu từ Google Sheets'
                }), 500
                
        except Exception as e:
            logger.error(f"Error refreshing data: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi cập nhật dữ liệu: {str(e)}'
            }), 500
    
    @app.route('/api/rooms/update', methods=['POST'])
    @login_required
    def update_room():
        """API endpoint để cập nhật thông tin một phòng - CÓ BACKUP"""
        try:
            data = request.get_json()
            room_no = data.get('roomNo')
            updated_data = data.get('updatedData')
            
            if not room_no or not updated_data:
                return jsonify({
                    'success': False,
                    'error': 'Thiếu thông tin roomNo hoặc updatedData'
                }), 400
            
            user_info = session.get('user_info', {})
            user_dept = user_info.get('department')
            
            # LẤY TRẠNG THÁI CŨ TRƯỚC KHI CẬP NHẬT
            current_room = app.data_processor.get_room_by_number(room_no)
            if not current_room:
                return jsonify({
                    'success': False,
                    'error': f'Không tìm thấy phòng {room_no}'
                }), 404
            
            old_status = current_room.get('roomStatus')
            new_status = updated_data.get('roomStatus')
            
            # KIỂM TRA PHÂN QUYỀN THEO DEPARTMENT
            if user_dept == 'HK':
                # HK chỉ được cập nhật một số trạng thái nhất định
                current_status = current_room.get('roomStatus')
                new_status = updated_data.get('roomStatus')
                
                # Loại bỏ phần /arr để kiểm tra trạng thái cơ bản
                current_base_status = current_status.replace('/arr', '')
                new_base_status = new_status.replace('/arr', '') if new_status else None
                
                allowed_transitions = {
                    'vd': ['vc'],
                    'vc': ['vd', 'ip'],
                    'od': ['oc', 'dnd', 'nn'],
                    'oc': ['od'],
                    'dnd': ['nn', 'oc', 'od'],
                    'nn': ['dnd', 'oc', 'od'],
                    'ip': ['vc']
                }
                
                if current_base_status not in allowed_transitions:
                    return jsonify({
                        'success': False,
                        'error': f'Không được phép chuyển từ trạng thái {current_base_status}'
                    }), 403
                
                if new_base_status and new_base_status not in allowed_transitions[current_base_status]:
                    return jsonify({
                        'success': False,
                        'error': f'Không được phép chuyển từ {current_base_status} sang {new_base_status}'
                    }), 403
            
            user_info_str = f"{user_info.get('name', 'Unknown')} ({user_info.get('department', 'Unknown')})"
            
            # Gọi hàm update_room_data
            success = app.data_processor.update_room_data(room_no, updated_data, user_info_str)
            
            if not success:
                return jsonify({
                    'success': False,
                    'error': 'Không thể cập nhật phòng'
                }), 500
            
            # ✅ TẠO BACKUP SAU KHI CẬP NHẬT THÀNH CÔNG
            threading.Thread(target=create_backup, daemon=True).start()
            
            # GHI LOG THAY ĐỔI TRẠNG THÁI PHÒNG
            if old_status and new_status and old_status != new_status:
                app.hk_logger.log_room_status_change(
                    room_no, 
                    old_status, 
                    new_status, 
                    user_info.get('name', 'Unknown'),
                    user_info.get('department', 'Unknown')
                )
            
            logger.info(f"Room {room_no} updated by {user_info_str}")
            
            return jsonify({
                'success': True,
                'message': f'Phòng {room_no} đã được cập nhật thành công',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error updating room: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi cập nhật phòng: {str(e)}'
            }), 500

    @app.route('/api/rooms/<room_no>')
    @login_required
    def get_room_detail(room_no):
        """API endpoint lấy chi tiết thông tin một phòng"""
        try:
            room = app.data_processor.get_room_by_number(room_no)
            if not room:
                return jsonify({
                    'success': False,
                    'error': f'Không tìm thấy phòng {room_no}'
                }), 404
            
            return jsonify({
                'success': True,
                'data': room,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error getting room detail: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi lấy thông tin phòng: {str(e)}'
            }), 500

    @app.route('/api/rooms/hk-quick-update', methods=['POST'])
    @login_required
    @hk_required
    def hk_quick_update():
        """API cho HK cập nhật nhanh trạng thái phòng - CÓ BACKUP"""
        try:
            data = request.get_json()
            room_no = data.get('roomNo')
            new_status = data.get('newStatus')
            
            if not room_no or not new_status:
                return jsonify({
                    'success': False,
                    'error': 'Thiếu thông tin roomNo hoặc newStatus'
                }), 400
            
            current_room = app.data_processor.get_room_by_number(room_no)
            if not current_room:
                return jsonify({
                    'success': False,
                    'error': f'Không tìm thấy phòng {room_no}'
                }), 404
            
            current_status = current_room.get('roomStatus')
            old_status = current_status  # Lưu trạng thái cũ để ghi log
            
            # Loại bỏ phần /arr để kiểm tra trạng thái cơ bản
            current_base_status = current_status.replace('/arr', '')
            new_base_status = new_status.replace('/arr', '')
            
            allowed_transitions = {
                'vd': ['vc'],
                'vc': ['vd', 'ip'],
                'od': ['oc', 'dnd', 'nn'],
                'oc': ['od'],
                'dnd': ['nn', 'oc', 'od'],
                'nn': ['dnd', 'oc', 'od'],
                'ip': ['vc']
            }
            
            if current_base_status not in allowed_transitions:
                return jsonify({
                    'success': False,
                    'error': f'Không được phép chuyển từ trạng thái {current_base_status}'
                }), 403
            
            if new_base_status not in allowed_transitions[current_base_status]:
                return jsonify({
                    'success': False,
                    'error': f'Không được phép chuyển từ {current_base_status} sang {new_base_status}'
                }), 403
            
            user_info = session.get('user_info', {})
            user_info_str = f"{user_info.get('name', 'Unknown')} ({user_info.get('department', 'Unknown')})"
            
            # Giữ nguyên phần ARR nếu có
            if current_status.endswith('/arr') and new_base_status in ['vd', 'vc']:
                new_status = f"{new_base_status}/arr"
            
            updated_data = {'roomStatus': new_status}
            success = app.data_processor.update_room_data(room_no, updated_data, user_info_str)
            
            if not success:
                return jsonify({
                    'success': False,
                    'error': 'Không thể cập nhật phòng'
                }), 500
            
            # ✅ TẠO BACKUP SAU KHI CẬP NHẬT THÀNH CÔNG
            threading.Thread(target=create_backup, daemon=True).start()
            
            # GHI LOG THAY ĐỔI TRẠNG THÁI PHÒNG
            app.hk_logger.log_room_status_change(
                room_no, 
                old_status, 
                new_status, 
                user_info.get('name', 'Unknown'),
                user_info.get('department', 'Unknown')
            )
            
            logger.info(f"HK quick update: {room_no} from {old_status} to {new_status} by {user_info_str}")
            
            return jsonify({
                'success': True,
                'message': f'Đã cập nhật phòng {room_no} từ {old_status} sang {new_status}',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error in HK quick update: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi cập nhật phòng: {str(e)}'
            }), 500

    @app.route('/api/file-info')
    @login_required
    def get_file_info():
        """API endpoint trả về thông tin file dữ liệu"""
        try:
            file_info = app.data_processor.get_room_info()
            return jsonify({
                'success': True,
                'data': file_info
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/health')
    def health_check():
        """Health check endpoint"""
        try:
            # Test database connection
            with app.db_manager.get_connection() as conn:
                conn.execute("SELECT 1")
            
            return jsonify({
                'status': 'healthy',
                'service': 'Hotel Management Dashboard API',
                'database': 'connected',
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            return jsonify({
                'status': 'unhealthy',
                'service': 'Hotel Management Dashboard API',
                'database': 'disconnected',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    # ==================== ERROR HANDLERS ====================

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({
            'success': False,
            'error': 'Endpoint not found'
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

    # ==================== KHỞI TẠO DỮ LIỆU ====================

    def initialize_data():
        """Khởi tạo dữ liệu - Ưu tiên khôi phục từ backup trước"""
        try:
            if app.db_manager.is_database_empty():
                # THỬ KHÔI PHỤC TỪ BACKUP TRƯỚC
                backup_restored = restore_latest_backup()
                
                if backup_restored:
                    logger.info("✅ Đã khôi phục dữ liệu từ backup gần nhất")
                    return
                
                # Nếu không có backup, mới lấy từ Google Sheets
                logger.info("🔄 Khởi tạo dữ liệu lần đầu từ Google Sheets...")
                success = app.data_processor.initialize_rooms_from_google_sheets('system_initialization')
                if success:
                    logger.info("✅ Khởi tạo dữ liệu thành công")
                else:
                    logger.error("❌ Không thể khởi tạo dữ liệu từ Google Sheets")
            else:
                logger.info("✅ Database đã có dữ liệu, bỏ qua khởi tạo")
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo dữ liệu: {e}")

    with app.app_context():
        initialize_data()
        
        # 🗑️ XÓA DÒNG NÀY: start_backup_service()
        # Vì giờ backup sẽ chạy theo event, không cần scheduler
        # start_backup_service()

    return app

app = create_app()

if __name__ == '__main__':
    app = create_app()
    
    print("🚀 Dashboard Quản Lý Khách Sạn - EVENT-BASED BACKUP EDITION")
    print("=" * 50)
    print("🔐 Đăng nhập: http://localhost:5000/login")
    print("🏨 Dashboard: http://localhost:5000/")
    print("🗃️  Database: data/hotel.db")
    print("💾 Backup: Tự động sao lưu KHI CÓ CẬP NHẬT")
    print("🎯 TÍNH NĂNG MỚI:")
    print("   • Event-Based Backup - Sao lưu khi có thay đổi")
    print("   • Chỉ giữ 5 bản backup gần nhất")
    print("   • Tự động khôi phục từ backup khi khởi động")
    print("📊 BACKUP API:")
    print("   • List: GET http://localhost:5000/api/backup/list")
    print("   • Create: POST http://localhost:5000/api/backup/create")
    print("   • Restore: POST http://localhost:5000/api/backup/restore")
    print("=" * 50)
    
    app.run(
        host='0.0.0.0', 
        port=5000, 
        debug=app.config['DEBUG']
    )