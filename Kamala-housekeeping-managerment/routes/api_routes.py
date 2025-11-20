from flask import Blueprint, jsonify, request, session
from models.data_processor import DataProcessor
from models.daily_manager import DailyManager
import logging

logger = logging.getLogger(__name__)

api_bp = Blueprint('api', __name__)

# Khởi tạo managers
data_processor = DataProcessor(
    api_key='AIzaSyCY5tu6rUE7USAnr0ALlhBAKlx-wmLYv6A',
    spreadsheet_id='14-m1Wg2g2J75YYwZnqe_KV7nxLn1c_zVVT-uMxz-uJo',
    range_name='A2:J63'
)
daily_manager = DailyManager()

def check_permission(allowed_departments):
    """Kiểm tra quyền theo department"""
    user_info = session.get('user_info')
    if not user_info:
        return False
    return user_info.get('department') in allowed_departments

@api_bp.route('/system/status')
def system_status():
    """Kiểm tra trạng thái hệ thống"""
    system_ready = daily_manager.check_system_ready()
    system_info = daily_manager.get_system_info()
    
    return jsonify({
        'success': True,
        'system_ready': system_ready,
        'system_info': system_info
    })

@api_bp.route('/system/update-from-sheets', methods=['POST'])
def update_from_sheets():
    """Cập nhật dữ liệu từ Google Sheets - Chỉ FO được phép"""
    if not check_permission(['FO']):
        return jsonify({
            'success': False, 
            'error': 'Chỉ nhân viên FO được phép cập nhật từ Google Sheets'
        }), 403

    try:
        user_info = session.get('user_info')
        if not user_info:
            return jsonify({'success': False, 'error': 'Chưa đăng nhập'}), 401

        # Lấy dữ liệu mới từ Google Sheets
        rooms_data = data_processor.get_rooms_data(force_refresh=True)
        
        if not rooms_data:
            return jsonify({
                'success': False, 
                'error': 'Không thể lấy dữ liệu từ Google Sheets'
            }), 500

        # Cập nhật vào hệ thống
        success = daily_manager.update_from_sheets(
            rooms_data, 
            user_info['name']
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Cập nhật dữ liệu thành công',
                'rooms_updated': len(rooms_data)
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Lỗi khi lưu dữ liệu'
            }), 500

    except Exception as e:
        logger.error(f"Error in update-from-sheets: {e}")
        return jsonify({
            'success': False,
            'error': f'Lỗi hệ thống: {str(e)}'
        }), 500

@api_bp.route('/rooms')
def get_rooms():
    """Lấy danh sách tất cả phòng"""
    if not daily_manager.check_system_ready():
        return jsonify({
            'success': False,
            'error': 'Hệ thống chưa được cập nhật cho ngày hôm nay'
        }), 400

    rooms = daily_manager.get_rooms_data()
    return jsonify({
        'success': True,
        'data': rooms,
        'total': len(rooms)
    })

@api_bp.route('/rooms/<room_no>/status', methods=['POST'])
def update_room_status(room_no):
    """Cập nhật trạng thái phòng"""
    if not daily_manager.check_system_ready():
        return jsonify({
            'success': False,
            'error': 'Hệ thống chưa được cập nhật cho ngày hôm nay'
        }), 400

    user_info = session.get('user_info')
    if not user_info:
        return jsonify({'success': False, 'error': 'Chưa đăng nhập'}), 401

    data = request.get_json()
    if not data or 'new_status' not in data:
        return jsonify({'success': False, 'error': 'Thiếu trạng thái mới'}), 400

    new_status = data['new_status']
    department = user_info.get('department')
    
    # Kiểm tra quyền thay đổi trạng thái
    allowed_statuses = daily_manager.get_allowed_statuses(department)
    if new_status not in allowed_statuses:
        return jsonify({
            'success': False,
            'error': f'Bộ phận {department} không được phép đổi sang trạng thái {new_status}'
        }), 403

    # Thực hiện cập nhật
    success, message = daily_manager.update_room_status(
        room_no, 
        new_status, 
        user_info['name'], 
        department
    )
    
    if success:
        return jsonify({
            'success': True,
            'message': message
        })
    else:
        return jsonify({
            'success': False,
            'error': message
        }), 400

@api_bp.route('/rooms/<room_no>/history')
def get_room_history(room_no):
    """Lấy lịch sử thay đổi của phòng"""
    if not daily_manager.check_system_ready():
        return jsonify({
            'success': False,
            'error': 'Hệ thống chưa được cập nhật cho ngày hôm nay'
        }), 400

    history = daily_manager.get_room_history(room_no)
    return jsonify({
        'success': True,
        'data': history
    })

@api_bp.route('/daily-changes')
def get_daily_changes():
    """Lấy lịch sử thay đổi trong ngày"""
    if not daily_manager.check_system_ready():
        return jsonify({
            'success': False,
            'error': 'Hệ thống chưa được cập nhật cho ngày hôm nay'
        }), 400

    changes = daily_manager.get_daily_changes()
    return jsonify({
        'success': True,
        'data': changes
    })

@api_bp.route('/user/permissions')
def get_user_permissions():
    """Lấy thông tin quyền của user hiện tại"""
    user_info = session.get('user_info')
    if not user_info:
        return jsonify({'success': False, 'error': 'Chưa đăng nhập'}), 401

    department = user_info.get('department')
    allowed_statuses = daily_manager.get_allowed_statuses(department)
    
    return jsonify({
        'success': True,
        'data': {
            'user': user_info,
            'allowed_statuses': allowed_statuses,
            'can_update_sheets': department == 'FO'
        }
    })