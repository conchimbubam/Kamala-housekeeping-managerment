from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from config import Config
from models.data_processor import DataProcessor
from models.hk_logger import HKLogger
import logging
from datetime import datetime
from functools import wraps
import os
import json

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Factory function để tạo Flask app"""
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config['SECRET_KEY'] = Config.SECRET_KEY
    
    # Khởi tạo data processor
    data_processor = DataProcessor(
        api_key=Config.API_KEY,
        spreadsheet_id=Config.SPREADSHEET_ID,
        range_name=Config.RANGE_NAME
    )
    
    # Khởi tạo HK logger
    hk_logger = HKLogger()
    
    # Lưu data processor và hk logger vào app context
    app.data_processor = data_processor
    app.hk_logger = hk_logger

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
            
            if department_code != '123':
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
            # Lấy dữ liệu phòng
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
            # Lấy dữ liệu báo cáo trong ngày (từ 8h15 đến hiện tại)
            now = datetime.now()
            start_time = now.replace(hour=8, minute=15, second=0, microsecond=0)
            # Nếu bây giờ là trước 8h15, thì lấy từ 8h15 ngày hôm trước
            if now < start_time:
                start_time = start_time - timedelta(days=1)
            
            # Lấy dữ liệu từ database
            report_data = HousekeepingReport.query.filter(
                HousekeepingReport.timestamp >= start_time
            ).order_by(HousekeepingReport.timestamp.desc()).all()

            # Chuyển đổi dữ liệu thành danh sách các dict
            report_list = []
            for report in report_data:
                report_list.append({
                    'timestamp': report.timestamp,
                    'user_name': report.user_name,
                    'room_no': report.room_no,
                    'action_type': report.action_type,
                    'new_status': report.new_status,
                    'action_detail': report.action_detail
                })

            # Tính toán thống kê
            statistics = calculate_hk_statistics(report_data)

            # Render template print_report.html và trả về
            return render_template('print_report.html', 
                                 report_data=report_list, 
                                 statistics=statistics,
                                 report_time=now)
        except Exception as e:
            logger.error(f"Lỗi xuất báo cáo HK: {e}")
            return "Lỗi khi tạo báo cáo", 500

    @app.route('/api/report/hk/clear', methods=['POST'])
    @login_required
    @fo_required
    def clear_hk_report():
        """API xóa toàn bộ lịch sử báo cáo HK (chỉ FO)"""
        try:
            # Implementation for clearing HK report logs
            log_file = os.path.join(Config.DATA_DIR, 'hk_activity_log.json')
            if os.path.exists(log_file):
                with open(log_file, 'w', encoding='utf-8') as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                
                logger.info("Đã xóa toàn bộ lịch sử báo cáo HK")
                return jsonify({
                    'success': True,
                    'message': 'Đã xóa toàn bộ lịch sử báo cáo HK',
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'File báo cáo không tồn tại'
                }), 404
                
        except Exception as e:
            logger.error(f"Lỗi xóa báo cáo HK: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    # ==================== API PHÂN QUYỀN ====================

    @app.route('/api/refresh', methods=['POST'])
    @login_required
    @fo_required
    def refresh_data():
        """API endpoint để refresh dữ liệu từ Google Sheets (chỉ FO)"""
        try:
            user_info = f"{session.get('user_info', {}).get('name', 'Unknown')} ({session.get('user_info', {}).get('department', 'Unknown')})"
            
            rooms = app.data_processor.update_from_google_sheets(user_info)
            
            logger.info(f"Data refreshed by {user_info}. Total rooms: {len(rooms)}")
            
            return jsonify({
                'success': True,
                'message': 'Dữ liệu đã được cập nhật thành công từ Google Sheets',
                'total_rooms': len(rooms),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error refreshing data: {e}")
            return jsonify({
                'success': False,
                'error': f'Lỗi cập nhật dữ liệu: {str(e)}'
            }), 500
    
    @app.route('/api/rooms/update', methods=['POST'])
    @login_required
    def update_room():
        """API endpoint để cập nhật thông tin một phòng"""
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
            app.data_processor.update_room_data(room_no, updated_data, user_info_str)
            
            # GHI LOG THAY ĐỔI TRẠNG THÁI PHÒNG
            if old_status and new_status and old_status != new_status:
                app.hk_logger.log_room_status_change(room_no, old_status, new_status, user_info.get('name', 'Unknown'))
            
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
        """API cho HK cập nhật nhanh trạng thái phòng"""
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
            app.data_processor.update_room_data(room_no, updated_data, user_info_str)
            
            # GHI LOG THAY ĐỔI TRẠNG THÁI PHÒNG
            app.hk_logger.log_room_status_change(room_no, old_status, new_status, user_info.get('name', 'Unknown'))
            
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
        return jsonify({
            'status': 'healthy',
            'service': 'Hotel Management Dashboard API',
            'timestamp': datetime.now().isoformat()
        })

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
        if not os.path.exists(Config.ROOMS_JSON):
            try:
                logger.info("Khởi tạo dữ liệu lần đầu từ Google Sheets...")
                app.data_processor.update_from_google_sheets('system_initialization')
                logger.info("Khởi tạo dữ liệu thành công")
            except Exception as e:
                logger.error(f"Lỗi khởi tạo dữ liệu: {e}")

    with app.app_context():
        initialize_data()

    return app

if __name__ == '__main__':
    app = create_app()
    
    print("🚀 Dashboard Quản Lý Khách Sạn ĐÃ ĐƯỢC NÂNG CẤP...")
    print("🔐 Đăng nhập: http://localhost:5000/login")
    print("🏨 Dashboard: http://localhost:5000/")
    print("📊 Dữ liệu được lưu tại: data/rooms.json")
    print("📈 Log HK được lưu tại: data/hk_activity_log.json")
    print("🎯 TÍNH NĂNG MỚI:")
    print("   • Hệ thống chuyển đổi trạng thái thông minh")
    print("   • ARR toggle: Bật/tắt thông tin khách sắp đến")
    print("   • Tự động xóa thông tin khách sắp đến khi tắt ARR")
    print("   • Phân quyền HK/FO chi tiết")
    print("   • Báo cáo hoạt động HK từ 8h15 đến hiện tại")
    print("   • Theo dõi lịch sử dọn phòng theo nhân viên")
    print("   • Tích hợp ghi log tự động cho tất cả thao tác HK")
    print("📄 In Tasksheet: http://localhost:5000/print-tasksheet (FO only)")
    print("🔗 API Health: http://localhost:5000/api/health")
    print("📋 API Báo cáo HK: http://localhost:5000/api/report/hk")
    print("🔄 API Refresh (FO only): POST http://localhost:5000/api/refresh")
    print("🗑️  API Clear Report (FO only): POST http://localhost:5000/api/report/hk/clear")
    
    app.run(
        host='0.0.0.0', 
        port=5000, 
        debug=app.config['DEBUG']
    )