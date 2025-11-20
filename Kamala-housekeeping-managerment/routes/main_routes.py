from flask import Blueprint, render_template, request, current_app

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def home():
    return render_template('dashboard.html')

@main_bp.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@main_bp.route('/room/<room_id>')
def room_detail(room_id):
    room_data = current_app.data_processor.get_room_by_id(room_id)
    if not room_data:
        return "Phòng không tồn tại", 404
    
    return render_template('room_detail.html', room=room_data)

@main_bp.route('/rooms')
def rooms_list():
    status_filter = request.args.get('status', 'all')
    page = int(request.args.get('page', 1))
    
    data_processor = current_app.data_processor
    
    if status_filter != 'all':
        rooms = data_processor.get_rooms_by_status(status_filter)
    else:
        rooms = data_processor.get_all_rooms()
    
    # Phân trang (giả sử 12 phòng mỗi trang)
    rooms_per_page = 12
    start_idx = (page - 1) * rooms_per_page
    end_idx = start_idx + rooms_per_page
    
    paginated_rooms = dict(list(rooms.items())[start_idx:end_idx])
    
    return render_template('dashboard.html', 
                         rooms=paginated_rooms,
                         status_filter=status_filter,
                         current_page=page,
                         total_pages=(len(rooms) + rooms_per_page - 1) // rooms_per_page)