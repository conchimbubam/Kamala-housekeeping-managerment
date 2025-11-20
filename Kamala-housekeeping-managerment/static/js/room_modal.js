// Enhanced Room Modal with State Transition System and Activity Logging
let currentEditingRoom = null;
let currentRoomStatus = '';
let originalRoomStatus = ''; // Lưu trạng thái ban đầu để so sánh
let originalNote = ''; // Lưu ghi chú gốc để tracking

// State transition mapping based on requirements
const stateTransitions = {
    'do': ['vd'],
    'vd': ['vc', 'lock'],
    'lock': ['vd'],
    'vc': ['vd', 'ip'],
    'ip': ['vc'],
    'oc': ['od'],
    'od': ['oc', 'dnd', 'nn'],
    'dnd': ['nn', 'oc', 'od'],
    'nn': ['dnd', 'oc', 'od']
};

// Status descriptions
const statusDescriptions = {
    'vd': 'Vacant Dirty - Phòng trống chưa dọn',
    'vc': 'Vacant Clean - Phòng trống đã dọn',
    'do': 'Due Out - Sắp trả phòng',
    'od': 'Occupied Dirty - Đang ở, chưa dọn',
    'oc': 'Occupied Clean - Đang ở, đã dọn',
    'dnd': 'Do Not Disturb - Không làm phiền',
    'nn': 'No Service - Không cần dọn phòng',
    'lock': 'Lock - Khóa phòng',
    'ip': 'Inpected - Đã nhận phòng, đã kiểm tra'
};

// Check if status is vacant (empty room)
function isVacantStatus(status) {
    return ['vd', 'vc', 'vd/arr', 'vc/arr'].includes(status);
}

// Check if status requires clearing current guest info
function shouldClearCurrentGuest(fromStatus, toStatus) {
    const fromBase = fromStatus.replace('/arr', '');
    const toBase = toStatus.replace('/arr', '');
    
    // Clear current guest when transitioning to vacant status
    if (isVacantStatus(toStatus)) {
        return true;
    }
    
    // Clear current guest when transitioning from DO to VD
    if (fromBase === 'do' && toBase === 'vd') {
        return true;
    }
    
    return false;
}

// Check if should transfer new guest to current guest (ARR → IP)
function shouldTransferNewGuestToCurrent(fromStatus, toStatus) {
    // Chuyển từ trạng thái có ARR sang IP
    return fromStatus.endsWith('/arr') && toStatus === 'ip';
}

// Transfer new guest to current guest
function transferNewGuestToCurrentGuest() {
    const newName = document.getElementById('edit-new-name').value;
    const newCheckIn = document.getElementById('edit-new-checkin').value;
    const newCheckOut = document.getElementById('edit-new-checkout').value;
    const newPax = document.getElementById('edit-new-pax').value;
    
    // Chuyển thông tin từ newGuest sang currentGuest
    document.getElementById('edit-current-name').value = newName;
    document.getElementById('edit-current-checkin').value = newCheckIn;
    document.getElementById('edit-current-checkout').value = newCheckOut;
    document.getElementById('edit-current-pax').value = newPax;
    
    // Xóa thông tin newGuest
    document.getElementById('edit-new-name').value = '';
    document.getElementById('edit-new-checkin').value = '';
    document.getElementById('edit-new-checkout').value = '';
    document.getElementById('edit-new-pax').value = '';
    
    // Tắt ARR
    document.getElementById('arr-toggle').checked = false;
    
    showToast('Đã chuyển thông tin khách sắp đến thành khách hiện tại', 'success');
}

// Clear current guest information
function clearCurrentGuestInfo() {
    document.getElementById('edit-current-name').value = '';
    document.getElementById('edit-current-checkin').value = '';
    document.getElementById('edit-current-checkout').value = '';
    document.getElementById('edit-current-pax').value = 0;
}

// Show room edit modal - CẬP NHẬT ĐỂ LƯU TRẠNG THÁI GHI CHÚ CŨ
function showRoomEditModal(roomNo) {
    const room = allRooms.find(r => r.roomNo === roomNo);
    if (!room) {
        showToast('Không tìm thấy thông tin phòng', 'error');
        return;
    }
    
    currentEditingRoom = roomNo;
    currentRoomStatus = room.roomStatus || '';
    originalRoomStatus = room.roomStatus || '';
    originalNote = room.notes || ''; // Lưu ghi chú gốc để tracking
    
    // Parse current status for ARR
    const hasARR = currentRoomStatus.includes('/arr');
    const baseStatus = hasARR ? currentRoomStatus.replace('/arr', '') : currentRoomStatus;
    
    // Fill form data
    document.getElementById('modal-room-no').textContent = roomNo;
    
    // Current guest info - chỉ hiển thị, không chỉnh sửa
    // Nếu phòng trống, xóa thông tin khách hiện tại
    if (isVacantStatus(currentRoomStatus)) {
        clearCurrentGuestInfo();
    } else {
        document.getElementById('edit-current-name').value = room.currentGuest?.name || '';
        document.getElementById('edit-current-checkin').value = room.currentGuest?.checkIn || '';
        document.getElementById('edit-current-checkout').value = room.currentGuest?.checkOut || '';
        document.getElementById('edit-current-pax').value = room.currentGuest?.pax || 0;
    }
    
    // New guest info - chỉ điền nếu có ARR
    if (hasARR) {
        document.getElementById('edit-new-name').value = room.newGuest?.name || '';
        document.getElementById('edit-new-checkin').value = room.newGuest?.checkIn || '';
        document.getElementById('edit-new-checkout').value = room.newGuest?.checkOut || '';
        document.getElementById('edit-new-pax').value = room.newGuest?.pax || 0;
    } else {
        // Xóa thông tin khách sắp đến nếu không có ARR
        document.getElementById('edit-new-name').value = '';
        document.getElementById('edit-new-checkin').value = '';
        document.getElementById('edit-new-checkout').value = '';
        document.getElementById('edit-new-pax').value = 0;
    }
    
    // Room notes - KHÔNG reset khi mở modal
    document.getElementById('room-notes').value = room.notes || '';
    
    // Set ARR toggle
    const arrToggle = document.getElementById('arr-toggle');
    arrToggle.checked = hasARR;
    
    // Update UI
    updateCurrentStatusDisplay(baseStatus, hasARR);
    updateAvailableTransitions(baseStatus);
    updateARREligibility(baseStatus);
    toggleNewGuestSection(hasARR);
    
    // Update last updated time
    document.getElementById('last-update-time').textContent = new Date().toLocaleString('vi-VN');
    
    // Show modal
    const roomModal = new bootstrap.Modal(document.getElementById('roomModal'));
    roomModal.show();
}

// Update current status display
function updateCurrentStatusDisplay(status, hasARR = false) {
    const statusBadge = document.getElementById('current-status-badge');
    const arrBadge = document.getElementById('current-arr-badge');
    const statusDesc = document.getElementById('status-desc-text');
    const statusContainer = document.querySelector('.current-status-container');
    
    // Update badge
    statusBadge.textContent = status.toUpperCase();
    statusBadge.className = 'badge status-badge fs-5';
    statusBadge.classList.add(`status-${status}`);
    
    // Add animation
    statusBadge.classList.add('status-change-animation');
    setTimeout(() => {
        statusBadge.classList.remove('status-change-animation');
    }, 500);
    
    // Update ARR badge
    if (hasARR && canHaveARR(status)) {
        arrBadge.style.display = 'inline-block';
        statusContainer.classList.add('with-arr');
    } else {
        arrBadge.style.display = 'none';
        statusContainer.classList.remove('with-arr');
    }
    
    // Update description
    statusDesc.textContent = statusDescriptions[status] || 'Trạng thái không xác định';
}

// Update available transition buttons - Cập nhật cho layout dọc
function updateAvailableTransitions(currentStatus) {
    const transitionsContainer = document.getElementById('available-transitions');
    const availableTransitions = stateTransitions[currentStatus] || [];
    
    if (availableTransitions.length === 0) {
        transitionsContainer.innerHTML = `
            <div class="col-12 text-center text-muted">
                <i class="fas fa-info-circle me-1"></i>
                Không có chuyển đổi khả dụng
            </div>
        `;
        return;
    }
    
    // Sử dụng col-12 cho mobile, col-6 cho tablet trở lên
    transitionsContainer.innerHTML = availableTransitions.map(targetStatus => {
        const description = statusDescriptions[targetStatus] || '';
        return `
            <div class="col-12 col-sm-6 col-md-4 mb-2">
                <button type="button" class="btn transition-btn w-100 h-100 py-3" 
                        data-target="${targetStatus}"
                        onclick="changeRoomStatus('${targetStatus}')"
                        title="${description}">
                    <div class="fw-bold fs-6">${targetStatus.toUpperCase()}</div>
                    <small class="d-block mt-1">${description.split(' - ')[0]}</small>
                </button>
            </div>
        `;
    }).join('');
}

// Change room status
function changeRoomStatus(newStatus) {
    const currentBaseStatus = getCurrentBaseStatus();
    
    // Kiểm tra và chuyển thông tin khách sắp đến thành khách hiện tại nếu cần
    if (shouldTransferNewGuestToCurrent(currentRoomStatus, newStatus)) {
        transferNewGuestToCurrentGuest();
    }
    
    // Kiểm tra và xóa thông tin khách hiện tại nếu cần
    if (shouldClearCurrentGuest(currentRoomStatus, newStatus)) {
        clearCurrentGuestInfo();
        showToast('Đã xóa thông tin khách hiện tại', 'info');
    }
    
    // Update current status
    currentRoomStatus = newStatus;
    const arrToggle = document.getElementById('arr-toggle');
    
    // Nếu trạng thái mới không thể có ARR, tắt ARR và xóa thông tin
    if (cannotHaveARR(newStatus)) {
        arrToggle.checked = false;
        toggleNewGuestSection(false);
    }
    
    const hasARR = arrToggle.checked && canHaveARR(newStatus);
    
    updateCurrentStatusDisplay(newStatus, hasARR);
    updateAvailableTransitions(newStatus);
    updateARREligibility(newStatus);
    
    // Cập nhật: Chỉ hiển thị trạng thái mới trong thông báo
    showToast(`Đã chuyển trạng thái thành ${newStatus.toUpperCase()}`, 'success');
}

// Get current base status (without ARR)
function getCurrentBaseStatus() {
    return currentRoomStatus.replace('/arr', '');
}

// Check if status can have ARR
function canHaveARR(status) {
    return ['vd', 'vc', 'do'].includes(status);
}

// Check if status cannot have ARR
function cannotHaveARR(status) {
    return !canHaveARR(status);
}

// Update ARR eligibility
function updateARREligibility(currentStatus) {
    const arrToggle = document.getElementById('arr-toggle');
    const arrToggleSection = document.getElementById('arr-toggle-section');
    const arrStatusText = document.getElementById('arr-status-text');
    
    const canARR = canHaveARR(currentStatus);
    
    if (canARR) {
        arrToggle.disabled = false;
        arrToggleSection.classList.remove('disabled');
        arrStatusText.textContent = 'Đang bật - Áp dụng cho VD, VC, DO';
        
        // Giữ nguyên trạng thái toggle ARR hiện tại
        toggleNewGuestSection(arrToggle.checked);
    } else {
        arrToggle.checked = false;
        arrToggle.disabled = true;
        arrToggleSection.classList.add('disabled');
        arrStatusText.textContent = 'Tự động tắt - Không áp dụng cho trạng thái này';
        toggleNewGuestSection(false);
    }
    
    updateCurrentStatusDisplay(currentStatus, arrToggle.checked && canARR);
}

// Toggle new guest section - CHỈ HIỆN KHI CÓ ARR VÀ XÓA DỮ LIỆU KHI ẨN
function toggleNewGuestSection(show) {
    const newGuestSection = document.getElementById('new-guest-section');
    if (show) {
        newGuestSection.classList.remove('hidden');
    } else {
        newGuestSection.classList.add('hidden');
        // XÓA HOÀN TOÀN thông tin khách sắp đến khi ẩn section
        document.getElementById('edit-new-name').value = '';
        document.getElementById('edit-new-checkin').value = '';
        document.getElementById('edit-new-checkout').value = '';
        document.getElementById('edit-new-pax').value = 0;
    }
}

// Toggle ARR manually
function toggleARR() {
    const arrToggle = document.getElementById('arr-toggle');
    const baseStatus = getCurrentBaseStatus();
    
    if (canHaveARR(baseStatus)) {
        const newARRState = !arrToggle.checked;
        arrToggle.checked = newARRState;
        
        updateCurrentStatusDisplay(baseStatus, newARRState);
        toggleNewGuestSection(newARRState);
        
        // Thông báo có thông tin về việc xóa dữ liệu
        if (!newARRState) {
            showToast('Đã tắt ARR và xóa thông tin khách sắp đến', 'info');
        } else {
            showToast('Đã bật ARR - Có thể thêm thông tin khách sắp đến', 'success');
        }
    }
}

// Log activity for HK report - Ghi nhận thao tác cho báo cáo HK (CHỈ HIỂN THỊ TRẠNG THÁI MỚI)
function logHKActivity(oldStatus, newStatus, roomNo) {
    // Các thao tác quan trọng cần ghi log
    const importantTransitions = [
        { from: 'vd', to: 'vc', type: 'dọn phòng trống' },
        { from: 'vd/arr', to: 'vc/arr', type: 'dọn phòng trống' },
        { from: 'od', to: 'oc', type: 'dọn phòng ở' },
        { from: 'od', to: 'dnd', type: 'dọn phòng ở' },
        { from: 'od', to: 'nn', type: 'dọn phòng ở' }
    ];
    
    // Tìm kiếm thao tác quan trọng
    const transition = importantTransitions.find(t => 
        t.from === oldStatus && t.to === newStatus
    );
    
    if (transition) {
        // CẬP NHẬT: Chỉ hiển thị trạng thái mới trong log
        console.log(`[HK Activity Log] Phòng ${roomNo}: ${newStatus} (${transition.type})`);
        // Việc ghi log thực tế sẽ được xử lý ở server side trong data_processor.py
        // Hàm này chỉ để debug và tracking trên client
    }
}

// Save room changes - CẬP NHẬT ĐỂ GỬI GHI CHÚ
async function saveRoomChanges() {
    if (!currentEditingRoom) {
        showToast('Không có phòng đang được chỉnh sửa', 'error');
        return;
    }
    
    const saveButton = document.querySelector('#roomModal .btn-primary');
    const originalText = saveButton.innerHTML;
    
    try {
        // Show loading state
        saveButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Đang lưu...';
        saveButton.disabled = true;
        
        // Get current values
        const baseStatus = getCurrentBaseStatus();
        const hasARR = document.getElementById('arr-toggle').checked;
        const currentNote = document.getElementById('room-notes').value;
        
        // Determine final status
        let finalStatus = baseStatus;
        if (hasARR && canHaveARR(baseStatus)) {
            finalStatus = `${baseStatus}/arr`;
        }
        
        // GHI LOG HOẠT ĐỘNG NẾU CÓ THAY ĐỔI TRẠNG THÁI
        if (originalRoomStatus !== finalStatus) {
            logHKActivity(originalRoomStatus, finalStatus, currentEditingRoom);
        }
        
        // Collect form data - THÊM GHI CHÚ
        const formData = {
            roomNo: currentEditingRoom,
            updatedData: {
                roomStatus: finalStatus,
                notes: currentNote  // Gửi ghi chú hiện tại
            }
        };
        
        // Xử lý thông tin khách HIỆN TẠI - chỉ gửi nếu phòng không trống
        if (!isVacantStatus(finalStatus)) {
            formData.updatedData.currentGuest = {
                name: document.getElementById('edit-current-name').value,
                checkIn: document.getElementById('edit-current-checkin').value,
                checkOut: document.getElementById('edit-current-checkout').value,
                pax: parseInt(document.getElementById('edit-current-pax').value) || 0
            };
        } else {
            // Nếu phòng trống, XÓA HOÀN TOÀN thông tin khách hiện tại
            formData.updatedData.currentGuest = {
                name: '',
                checkIn: '',
                checkOut: '',
                pax: 0
            };
        }
        
        // Xử lý thông tin khách SẮP ĐẾN
        if (hasARR && canHaveARR(baseStatus)) {
            // Nếu ARR được bật, thêm thông tin khách sắp đến
            formData.updatedData.newGuest = {
                name: document.getElementById('edit-new-name').value,
                checkIn: document.getElementById('edit-new-checkin').value,
                checkOut: document.getElementById('edit-new-checkout').value,
                pax: parseInt(document.getElementById('edit-new-pax').value) || 0
            };
        } else {
            // Nếu ARR bị tắt, XÓA HOÀN TOÀN thông tin khách sắp đến
            formData.updatedData.newGuest = {
                name: '',
                checkIn: '',
                checkOut: '',
                pax: 0
            };
        }
        
        // Send update request
        const response = await fetch('/api/rooms/update', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(formData)
        });
        
        const data = await response.json();
        
        if (data.success) {
            // Close modal
            const roomModal = bootstrap.Modal.getInstance(document.getElementById('roomModal'));
            roomModal.hide();
            
            // Show success message - THÊM THÔNG BÁO VỀ GHI CHÚ
            let message = 'Thông tin phòng đã được cập nhật';
            if (hasARR) {
                message += ' (có thông tin khách sắp đến)';
            }
            if (isVacantStatus(finalStatus)) {
                message += ' (đã xóa thông tin khách hiện tại)';
            }
            if (originalNote !== currentNote) {
                message += ' - Đã cập nhật ghi chú';
            }
            
            showToast(message, 'success');
            
            // Reload data to update UI
            loadData();
        } else {
            showToast('Lỗi: ' + data.error, 'error');
        }
        
    } catch (error) {
        console.error('Error saving room changes:', error);
        showToast('Lỗi khi lưu thay đổi: ' + error.message, 'error');
    } finally {
        // Restore button
        saveButton.innerHTML = originalText;
        saveButton.disabled = false;
    }
}

// Initialize modal events
document.addEventListener('DOMContentLoaded', function() {
    // ARR toggle listener
    document.getElementById('arr-toggle')?.addEventListener('change', function() {
        const currentStatus = getCurrentBaseStatus();
        const hasARR = this.checked && canHaveARR(currentStatus);
        
        updateCurrentStatusDisplay(currentStatus, hasARR);
        toggleNewGuestSection(hasARR);
    });
    
    // Setup quick actions
    setupQuickActions();
});

// Quick action buttons
function setupQuickActions() {
    // This function can be used if you add quick action buttons later
}

// Toast function
function showToast(message, type = 'info') {
    const toastContainer = document.getElementById('toast-container') || createToastContainer();
    
    const toast = document.createElement('div');
    toast.className = `toast align-items-center text-white bg-${type === 'error' ? 'danger' : type} border-0`;
    toast.style.animation = 'slideInRight 0.3s ease';
    
    const icon = type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-triangle' : 'info-circle';
    
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <i class="fas fa-${icon} me-2"></i>${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    
    toastContainer.appendChild(toast);
    const bsToast = new bootstrap.Toast(toast, { delay: 3000 });
    bsToast.show();
    
    toast.addEventListener('hidden.bs.toast', () => {
        toast.remove();
    });
}

function createToastContainer() {
    const container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container position-fixed top-0 end-0 p-3';
    container.style.zIndex = '9999';
    document.body.appendChild(container);
    return container;
}