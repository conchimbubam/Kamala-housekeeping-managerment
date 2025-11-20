// Report JavaScript - Housekeeping Report Module

// Event Listeners for Reports
document.addEventListener('DOMContentLoaded', function() {
    setupReportEventListeners();
});

function setupReportEventListeners() {
    // HK Report button
    document.getElementById('btn-hk-report')?.addEventListener('click', showHKReportModal);
    
    // Export HK Report button
    document.getElementById('btn-export-hk-report')?.addEventListener('click', exportHKReport);
}

// Thêm các hàm mới cho báo cáo HK
function showHKReportModal() {
    fetch('/api/report/hk')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                populateHKReport(data.data, data.statistics);
                const hkReportModal = new bootstrap.Modal(document.getElementById('hkReportModal'));
                hkReportModal.show();
            } else {
                showReportAlert('error', 'Lỗi khi tải báo cáo: ' + data.error);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showReportAlert('error', 'Lỗi khi tải báo cáo HK');
        });
}

function populateHKReport(reportData, statistics) {
    // Điền dữ liệu vào bảng
    const tbody = document.getElementById('hk-report-body');
    tbody.innerHTML = '';
    
    if (reportData.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-4">
                    <i class="fas fa-inbox fa-2x mb-3"></i>
                    <br>
                    Không có dữ liệu báo cáo trong khoảng thời gian từ 8h15 đến hiện tại
                </td>
            </tr>
        `;
        return;
    }
    
    reportData.forEach((item, index) => {
        const row = document.createElement('tr');
        const timestamp = new Date(item.timestamp).toLocaleString('vi-VN');
        
        // Xác định badge màu cho loại thao tác
        let badgeClass = 'bg-secondary';
        if (item.action_type === 'dọn phòng trống') {
            badgeClass = 'bg-success';
        } else if (item.action_type === 'dọn phòng ở') {
            badgeClass = 'bg-warning text-dark';
        }
        
        row.innerHTML = `
            <td>${index + 1}</td>
            <td>
                <strong>${item.user_name}</strong>
                <br>
                <small class="text-muted">${getUserDepartment(item.user_name)}</small>
            </td>
            <td>
                <span class="badge bg-primary fs-6">${item.room_no}</span>
            </td>
            <td>
                <span class="badge ${badgeClass}">${item.action_type}</span>
            </td>
            <td>
                <div class="status-change">
                    <span class="new-status text-success">${item.new_status.toUpperCase()}</span>
                </div>
                <small class="text-muted d-block mt-1">${item.action_detail}</small>
            </td>
            <td>
                <small class="text-muted">${timestamp}</small>
            </td>
        `;
        tbody.appendChild(row);
    });
    
    // Điền thống kê
    populateHKStatistics(statistics);
}

function populateHKStatistics(statistics) {
    // Thống kê theo nhân viên
    const staffStats = document.getElementById('staff-stats');
    staffStats.innerHTML = '';
    
    if (Object.keys(statistics.staff_stats).length === 0) {
        staffStats.innerHTML = `
            <div class="text-center text-muted py-3">
                <i class="fas fa-users fa-lg mb-2"></i>
                <br>
                Chưa có thống kê
            </div>
        `;
        return;
    }
    
    Object.entries(statistics.staff_stats).forEach(([staff, stats]) => {
        const staffDiv = document.createElement('div');
        staffDiv.className = 'mb-3 p-3 border rounded bg-light';
        staffDiv.innerHTML = `
            <div class="d-flex justify-content-between align-items-start mb-2">
                <div class="fw-bold text-primary">${staff}</div>
                <span class="badge bg-primary fs-6">${stats.total}</span>
            </div>
            <div class="row small">
                <div class="col-6">
                    <i class="fas fa-bed text-success me-1"></i>
                    <span class="text-success">${stats['dọn phòng trống']}</span>
                </div>
                <div class="col-6">
                    <i class="fas fa-user-check text-warning me-1"></i>
                    <span class="text-warning">${stats['dọn phòng ở']}</span>
                </div>
            </div>
        `;
        staffStats.appendChild(staffDiv);
    });
    
    // Tổng quan
    const summaryStats = document.getElementById('summary-stats');
    summaryStats.innerHTML = `
        <div class="d-flex justify-content-between align-items-center mb-3 p-2 border-bottom">
            <span><i class="fas fa-list-check me-2"></i>Tổng thao tác</span>
            <strong class="text-primary">${statistics.total_actions}</strong>
        </div>
        <div class="d-flex justify-content-between align-items-center mb-3 p-2 border-bottom">
            <span><i class="fas fa-bed text-success me-2"></i>Dọn phòng trống</span>
            <strong class="text-success">${statistics.action_types['dọn phòng trống']}</strong>
        </div>
        <div class="d-flex justify-content-between align-items-center mb-3 p-2 border-bottom">
            <span><i class="fas fa-user-check text-warning me-2"></i>Dọn phòng ở</span>
            <strong class="text-warning">${statistics.action_types['dọn phòng ở']}</strong>
        </div>
        <div class="d-flex justify-content-between align-items-center mb-3 p-2">
            <span><i class="fas fa-users me-2"></i>Số nhân viên</span>
            <strong class="text-info">${Object.keys(statistics.staff_stats).length}</strong>
        </div>
    `;
}

function getUserDepartment(userName) {
    // Hàm giả định để lấy department từ tên user
    // Trong thực tế, bạn có thể lấy từ session hoặc API
    return 'HK'; // Mặc định là HK
}

function exportHKReport() {
    // Mở cửa sổ mới để in báo cáo
    window.open('/api/report/hk/export', '_blank');
}

// Hàm hiển thị thông báo cho report module
function showReportAlert(type, message) {
    const alertClass = type === 'success' ? 'alert-success' : 'alert-danger';
    const alertHtml = `
        <div class="alert ${alertClass} alert-dismissible fade show position-fixed top-0 end-0 m-3" style="z-index: 9999;">
            <strong>${type === 'success' ? 'Thành công!' : 'Lỗi!'}</strong> ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    document.body.insertAdjacentHTML('beforeend', alertHtml);
    
    // Tự động đóng thông báo sau 5 giây
    setTimeout(() => {
        const alerts = document.querySelectorAll('.alert');
        alerts.forEach(alert => {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        });
    }, 5000);
}