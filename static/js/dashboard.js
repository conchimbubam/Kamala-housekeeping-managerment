// Dashboard JavaScript
let allRooms = [];
let currentFilter = 'all';

// DOM Content Loaded
document.addEventListener('DOMContentLoaded', function() {
    loadData();
    setupEventListeners();
});

// Event Listeners
function setupEventListeners() {
    // Filter buttons
    document.querySelectorAll('[data-filter]').forEach(button => {
        button.addEventListener('click', function() {
            // Set active button
            document.querySelectorAll('[data-filter]').forEach(btn => {
                btn.classList.remove('active');
            });
            this.classList.add('active');

            currentFilter = this.getAttribute('data-filter');
            filterRooms(currentFilter);
        });
    });

    // Enter key in password field
    document.getElementById('password')?.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            confirmUpdate();
        }
    });
}

// Load data from API
async function loadData() {
    showLoading();
    
    try {
        const [roomsResponse, fileInfoResponse] = await Promise.all([
            fetch('/api/rooms'),
            fetch('/api/file-info')
        ]);
        
        if (!roomsResponse.ok) {
            throw new Error('Failed to fetch data');
        }
        
        const roomsData = await roomsResponse.json();
        const fileInfoData = await fileInfoResponse.json();
        
        if (roomsData.success) {
            allRooms = roomsData.data;
            updateFileInfo(fileInfoData.data);
            displayRoomsByFloor(allRooms);
            updateFilterCounts(allRooms);
            showFloorsContainer();
            updateLastUpdated();
        } else {
            throw new Error('Invalid data format');
        }
    } catch (error) {
        console.error('Error loading data:', error);
        showError('Không thể tải dữ liệu: ' + error.message);
    }
}

// Update file info
function updateFileInfo(fileInfo) {
    const fileInfoElement = document.getElementById('file-info');
    
    if (fileInfo.error) {
        fileInfoElement.innerHTML = `<i class="fas fa-exclamation-triangle me-1"></i>${fileInfo.error}`;
        return;
    }
    
    const lastUpdated = fileInfo.last_updated ? new Date(fileInfo.last_updated).toLocaleString('vi-VN') : 'Chưa xác định';
    const source = fileInfo.source === 'google_sheets' ? 'Google Sheets' : 'Chỉnh sửa thủ công';
    
    fileInfoElement.innerHTML = `
        <i class="fas fa-database me-1"></i>
        Cập nhật: ${lastUpdated} | Nguồn: ${source} | Tổng phòng: ${fileInfo.total_rooms || 0}
    `;
}

// Calculate floor statistics
function calculateFloorStats(rooms) {
    const stats = {
        'vd': 0, 'od': 0, 'vc': 0, 'oc': 0, 
        'vd/arr': 0, 'vc/arr': 0, 'dnd': 0, 'nn': 0, 
        'lock': 0, 'ip': 0, 'do': 0, 'do/arr': 0
    };
    
    rooms.forEach(room => {
        const status = room.roomStatus;
        if (status in stats) {
            stats[status]++;
        }
    });
    
    return stats;
}

// Update filter counts với logic mới
function updateFilterCounts(rooms) {
    const counts = {
        'all': rooms.length,
        'occupied': 0,
        'arrival': 0,
        'cleaned': 0,
        'not-cleaned': 0,
        'checked': 0,
        'not-departed': 0
    };

    rooms.forEach(room => {
        const status = room.roomStatus;
        
        // Đang ở: OD, OC, DND, NN
        if (['od', 'oc', 'dnd', 'nn'].includes(status)) {
            counts.occupied++;
        }
        
        // Có khách đến: VD/ARR, VC/ARR, DO/ARR
        if (['vd/arr', 'vc/arr', 'do/arr'].includes(status)) {
            counts.arrival++;
        }
        
        // Phòng đã dọn: OC, VC, VC/ARR
        if (['oc', 'vc', 'vc/arr'].includes(status)) {
            counts.cleaned++;
        }
        
        // Phòng chưa dọn: VD, VD/ARR, OD, DND, NN, DO, DO/ARR
        if (['vd', 'vd/arr', 'od', 'dnd', 'nn', 'do', 'do/arr'].includes(status)) {
            counts['not-cleaned']++;
        }
        
        // Phòng đã kiểm: IP
        if (status === 'ip') {
            counts.checked++;
        }
        
        // Phòng chưa rời đi: DO, DO/ARR
        if (['do', 'do/arr'].includes(status)) {
            counts['not-departed']++;
        }
    });

    // Update DOM
    Object.keys(counts).forEach(filter => {
        const element = document.getElementById(`count-${filter}`);
        if (element) {
            element.textContent = counts[filter];
        }
    });
}

// Display rooms by floor với thống kê từng tầng
function displayRoomsByFloor(rooms) {
    const container = document.getElementById('floors-container');
    
    // Group by floor
    const floors = {};
    rooms.forEach(room => {
        const floor = room.roomNo.substring(0, room.roomNo.length - 2);
        if (!floors[floor]) {
            floors[floor] = [];
        }
        floors[floor].push(room);
    });
    
    // Sort floors
    const sortedFloors = Object.keys(floors).sort((a, b) => a - b);
    
    container.innerHTML = sortedFloors.map(floor => {
        const floorRooms = floors[floor];
        const floorStats = calculateFloorStats(floorRooms);
        const floorStatsHTML = createFloorStatsHTML(floorStats);

        return `
            <div class="floor-section">
                <div class="floor-header">
                    <h4 class="floor-title">
                        <i class="fas fa-layer-group me-2"></i>Tầng ${floor}
                        <span class="badge bg-primary ms-2">${floorRooms.length} phòng</span>
                    </h4>
                    <div class="floor-stats">
                        ${floorStatsHTML}
                    </div>
                </div>
                <div class="row">
                    ${floorRooms.map(room => createRoomCard(room)).join('')}
                </div>
            </div>
        `;
    }).join('');
}

// Create floor stats HTML - HIỂN THỊ ĐẦY ĐỦ CÁC TRẠNG THÁI
function createFloorStatsHTML(stats) {
    const statusConfig = [
        { key: 'vd', label: 'VD', color: 'bg-secondary' },
        { key: 'vc', label: 'VC', color: 'bg-light text-dark' },
        { key: 'vd/arr', label: 'VD/ARR', color: 'bg-secondary' },
        { key: 'vc/arr', label: 'VC/ARR', color: 'bg-light text-dark' },
        { key: 'do', label: 'DO', color: 'bg-warning text-dark' },
        { key: 'do/arr', label: 'DO/ARR', color: 'bg-warning text-dark' },
        { key: 'od', label: 'OD', color: 'bg-dark' },
        { key: 'oc', label: 'OC', color: 'bg-warning' },
        { key: 'dnd', label: 'DND', color: 'bg-warning' },
        { key: 'nn', label: 'NN', color: 'bg-warning' },
        { key: 'lock', label: 'Lock', color: 'bg-danger' },
        { key: 'ip', label: 'IP', color: 'bg-success' }
    ];

    return statusConfig.map(stat => {
        const count = stats[stat.key] || 0;
        // HIỂN THỊ TẤT CẢ CÁC TRẠNG THÁI, KỂ CẢ SỐ 0
        return `<span class="badge ${stat.color} me-1 mb-1 floor-stat-item">${stat.label}: ${count}</span>`;
    }).join('');
}

// Create room card HTML
function createRoomCard(room) {
    const status = room.roomStatus;
    const statusClass = `room-status-${status.replace('/', '-')}`;
    
    const hasCurrentGuest = room.currentGuest && room.currentGuest.name && room.currentGuest.name.trim() !== '';
    const hasNewGuest = room.newGuest && room.newGuest.name && room.newGuest.name.trim() !== '';
    
    let guestDisplay = '';
    
    if (hasCurrentGuest) {
        guestDisplay = `
            <div class="guest-name">${room.currentGuest.name}</div>
            <div class="guest-dates">
                ${formatDate(room.currentGuest.checkIn)} → ${formatDate(room.currentGuest.checkOut)}
            </div>
            <div class="guest-pax">${room.currentGuest.pax} khách</div>
        `;
    } else if (hasNewGuest) {
        guestDisplay = `
            <div class="guest-name">${room.newGuest.name}</div>
            <div class="guest-dates">
                ${formatDate(room.newGuest.checkIn)} → ${formatDate(room.newGuest.checkOut)}
            </div>
            <div class="guest-pax">${room.newGuest.pax} khách</div>
        `;
    } else {
        guestDisplay = '<div class="guest-name">Trống</div>';
    }
    
    return `
        <div class="col-6 col-sm-4 col-md-3 col-lg-2 col-xl-1">
            <div class="room-card ${statusClass}" onclick="showRoomEditModal('${room.roomNo}')">
                <div class="room-number">${room.roomNo}</div>
                <div class="guest-info">
                    ${guestDisplay}
                </div>
            </div>
        </div>
    `;
}

// Show update modal
function showUpdateModal() {
    // Reset form
    document.getElementById('password').value = '';
    document.getElementById('update-error').style.display = 'none';
    
    const updateModal = new bootstrap.Modal(document.getElementById('updateModal'));
    updateModal.show();
}

// Confirm update with password
async function confirmUpdate() {
    const password = document.getElementById('password').value;
    const updateButton = document.querySelector('#updateModal .btn-warning');
    const originalText = updateButton.innerHTML;
    
    // Validate password
    if (!password) {
        showUpdateError('Vui lòng nhập mật khẩu');
        return;
    }
    
    if (password !== '123456') {
        showUpdateError('Mật khẩu không chính xác');
        return;
    }
    
    // Show loading state
    updateButton.classList.add('btn-updating');
    updateButton.disabled = true;
    
    try {
        const response = await fetch('/api/refresh', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            // Close modal
            const updateModal = bootstrap.Modal.getInstance(document.getElementById('updateModal'));
            updateModal.hide();
            
            // Show success message and reload data
            showSuccessMessage('Dữ liệu đã được cập nhật thành công từ Google Sheets');
            loadData();
        } else {
            showUpdateError(data.error || 'Có lỗi xảy ra khi cập nhật dữ liệu');
        }
    } catch (error) {
        console.error('Update error:', error);
        showUpdateError('Lỗi kết nối khi cập nhật dữ liệu');
    } finally {
        // Restore button
        updateButton.classList.remove('btn-updating');
        updateButton.disabled = false;
        updateButton.innerHTML = originalText;
    }
}

// Show update error
function showUpdateError(message) {
    const errorElement = document.getElementById('update-error');
    const errorText = document.getElementById('update-error-text');
    
    errorText.textContent = message;
    errorElement.style.display = 'block';
}

// Filter rooms với LOGIC MỚI
function filterRooms(filter) {
    let filteredRooms = allRooms;

    switch (filter) {
        case 'occupied':
            // Đang ở: OD, OC, DND, NN
            filteredRooms = allRooms.filter(room => 
                ['od', 'oc', 'dnd', 'nn'].includes(room.roomStatus)
            );
            break;
        case 'arrival':
            // Có khách đến: VD/ARR, VC/ARR, DO/ARR
            filteredRooms = allRooms.filter(room => 
                ['vd/arr', 'vc/arr', 'do/arr'].includes(room.roomStatus)
            );
            break;
        case 'cleaned':
            // Phòng đã dọn: OC, VC, VC/ARR
            filteredRooms = allRooms.filter(room => 
                ['oc', 'vc', 'vc/arr'].includes(room.roomStatus)
            );
            break;
        case 'not-cleaned':
            // Phòng chưa dọn: VD, VD/ARR, OD, DND, NN, DO, DO/ARR
            filteredRooms = allRooms.filter(room => 
                ['vd', 'vd/arr', 'od', 'dnd', 'nn', 'do', 'do/arr'].includes(room.roomStatus)
            );
            break;
        case 'checked':
            // Phòng đã kiểm: IP
            filteredRooms = allRooms.filter(room => 
                room.roomStatus === 'ip'
            );
            break;
        case 'not-departed':
            // Phòng chưa rời đi: DO, DO/ARR
            filteredRooms = allRooms.filter(room => 
                ['do', 'do/arr'].includes(room.roomStatus)
            );
            break;
        default:
            filteredRooms = allRooms;
    }

    displayRoomsByFloor(filteredRooms);
    updateFilterCounts(filteredRooms);
}

// Helper functions
function formatDate(dateStr) {
    if (!dateStr || dateStr === '00-01-00') return '-';
    return dateStr;
}

// UI State Management
function showLoading() {
    document.getElementById('loading-spinner').style.display = 'block';
    document.getElementById('error-message').style.display = 'none';
    document.getElementById('floors-container').style.display = 'none';
}

function showFloorsContainer() {
    document.getElementById('loading-spinner').style.display = 'none';
    document.getElementById('error-message').style.display = 'none';
    document.getElementById('floors-container').style.display = 'block';
}

function showError(message) {
    document.getElementById('loading-spinner').style.display = 'none';
    document.getElementById('error-message').style.display = 'block';
    document.getElementById('floors-container').style.display = 'none';
    document.getElementById('error-text').textContent = message;
}

function updateLastUpdated() {
    const now = new Date();
    document.getElementById('last-updated').textContent = 
        `Cập nhật: ${now.toLocaleDateString('vi-VN')} ${now.toLocaleTimeString('vi-VN')}`;
}

// Show success message
function showSuccessMessage(message) {
    // Create toast element
    const toast = document.createElement('div');
    toast.className = 'toast align-items-center text-white bg-success border-0 position-fixed top-0 end-0 m-3';
    toast.style.zIndex = '1060';
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <i class="fas fa-check-circle me-2"></i>${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    
    document.body.appendChild(toast);
    
    // Show toast
    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
    
    // Remove toast after hide
    toast.addEventListener('hidden.bs.toast', () => {
        document.body.removeChild(toast);
    });
}