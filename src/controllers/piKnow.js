const path = require("path");
const ExcelReaderService = require("../models/excelSheed");
const { cpus } = require('os');
const fs = require('fs');
const { formatDuration, sleep } = require('../utils/helpers');
const ProxyManager = require('../services/ProxyManager');
const piknowService = require('../services/piknowService');

/**
 * Ghi log vào file
 * @param {string} message - Nội dung log
 */
function logToFile(message) {
  try {
    const logsDir = path.join(__dirname, '../logs');
    
    // Tạo thư mục logs nếu chưa tồn tại
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
    
    const date = new Date();
    const dateStr = date.toISOString().split('T')[0];
    const timestamp = `[${date.toLocaleTimeString()}]`;
    
    const logFilePath = path.join(logsDir, `pi-automate-piknow-logs-${dateStr}.txt`);
    
    // Thêm timestamp vào message
    const logMessage = `${timestamp} ${message}\n`;
    
    // Ghi log vào file
    fs.appendFileSync(logFilePath, logMessage, 'utf8');
  } catch (error) {
    console.error(`Lỗi khi ghi log: ${error.message}`);
  }
}

async function handlePiKnow(piknowCount) {
  // Khởi tạo các dịch vụ
  const proxyManager = new ProxyManager();
  const startTime = Date.now();
  
  logToFile(`======= BẮT ĐẦU TIẾN TRÌNH PIKNOW =======`);
  logToFile(`Phiên bản: v1.0 (Xử lý song song với đa luồng)`);
  
  try {
    // Khởi tạo ProxyManager
    logToFile(`Khởi tạo ProxyManager...`);
    await proxyManager.initialize();
    
    // Cấu hình cố định cho 2.000 tài khoản
    const FIXED_CONFIG = {
      userCount: 2000,           // Số lượng tài khoản xử lý
      useExcelUsers: true,
      excelPath: '',
      numCpus: 8,                // Số CPU sử dụng
      tasksPerCpu: 6,            // Tác vụ đồng thời mỗi CPU
      concurrencyLimit: 48,      // Tổng số luồng đồng thời
      debug: true
    };
    
    // Sử dụng cấu hình cố định
    let requestConfig = FIXED_CONFIG;
    
    // Nếu piknowCount là một số, nghĩa là được gọi từ main.js
    if (typeof piknowCount === 'number') {
      logToFile(`Được gọi từ giao diện với ${piknowCount} piknow cho mỗi user, áp dụng cấu hình cố định cho 2.000 tài khoản`);
    } 
    // Nếu piknowCount là object có chứa body (từ API)
    else if (piknowCount && piknowCount.body) {
      // Chỉ lấy đường dẫn file Excel nếu có
      if (piknowCount.body.excelPath) {
        requestConfig.excelPath = piknowCount.body.excelPath;
      }
      logToFile(`Được gọi từ API, áp dụng cấu hình cố định cho 2.000 tài khoản`);
    }
    
    console.log(`>> THÔNG SỐ CỐ ĐỊNH CHO 2.000 TÀI KHOẢN:`);
    console.log(`>> - Số CPU: ${requestConfig.numCpus}`);
    console.log(`>> - Tác vụ/CPU: ${requestConfig.tasksPerCpu}`);
    console.log(`>> - Tổng luồng đồng thời: ${requestConfig.concurrencyLimit}`);
    
    logToFile(`Thông số: CPUs=${requestConfig.numCpus}, Tasks/CPU=${requestConfig.tasksPerCpu}, Luồng=${requestConfig.concurrencyLimit}`);
    
    // Step 1: Đọc danh sách user từ Excel
    console.log(`\n>> STEP 1: Đọc danh sách user từ Excel...`);
    
    // Đường dẫn tương đối đến file Excel
    const dataDir = path.join(__dirname, '../data');
    // Sử dụng đường dẫn excelPath nếu có, ngược lại tìm file Excel trong thư mục data
    const excelFilePath = requestConfig.excelPath || path.join(dataDir, 'PI.xlsx');
    
    console.log(`>> Đọc danh sách user từ file: ${excelFilePath}`);
    
    // Tạo đối tượng ExcelReaderService với đường dẫn file
    const excelReader = new ExcelReaderService(excelFilePath);
    
    // Đọc tất cả dữ liệu từ file Excel
    const excelData = excelReader.readAllSheets();
    
    // Dựa vào cấu trúc cũ, lấy dữ liệu từ các cột cần thiết
    const uid = excelData["prxageng"]?.["uid"] || [];
    const piname = excelData["prxageng"]?.["piname"] || [];
    const ukey = excelData["prxageng"]?.["ukey"] || [];
    const proxy = excelData["prxageng"]?.["proxy"] || [];
    
    // Tạo danh sách user từ dữ liệu Excel 
    const users = [];
    for (let i = 0; i < uid.length; i++) {
      if (uid[i] && ukey[i] && piname[i]) {
        const proxyInfo = proxy[i] ? proxy[i].split(':') : null;
        
        users.push({
          uid: uid[i],
          piname: piname[i],
          ukey: ukey[i],
          active: true, // Giả định tất cả user đều active
          proxy: proxyInfo ? {
            host: proxyInfo[0],
            port: proxyInfo[1],
            name: proxyInfo[2],
            password: proxyInfo[3]
          } : null
        });
      }
    }
    
    logToFile(`STEP 1: Đọc được ${users.length} user từ Excel`);
    
    // Nếu không có user, kết thúc sớm
    if (users.length === 0) {
      logToFile(`Lỗi: Không tìm thấy dữ liệu user từ Excel`);
      return {
        success: false,
        message: "Không tìm thấy dữ liệu user từ file Excel",
      };
    }
    
    // Thêm proxy từ Excel vào pool nếu có
    const excelProxies = users
      .filter(user => user.proxy)
      .map(user => user.proxy);
    
    if (excelProxies.length > 0) {
      console.log(`>> Thêm ${excelProxies.length} proxy từ Excel vào pool`);
      proxyManager.addExcelProxies(excelProxies);
    }
    
    // Step 2: Lọc user theo các tiêu chí
    console.log(`\n>> STEP 2: Lọc danh sách user...`);
    
    // Lọc user có đủ thông tin để đăng nhập
    const validUsers = users.filter(user => user.uid && user.ukey && user.piname);
    
    // Lọc tiếp theo các tiêu chí khác nếu có (ví dụ: onlyActive)
    let filteredUsers = validUsers;
    if (requestConfig.onlyActive) {
      filteredUsers = validUsers.filter(user => user.active !== "0" && user.active !== false);
    }
    
    console.log(`>> Lọc được ${filteredUsers.length}/${users.length} user hợp lệ`);
    logToFile(`STEP 2: Lọc được ${filteredUsers.length}/${users.length} user hợp lệ`);
    
    // Step 3: Chọn số lượng user theo yêu cầu
    console.log(`\n>> STEP 3: Chọn ${requestConfig.userCount} user để thực hiện piknow...`);
    // Lấy ngẫu nhiên userCount user để thực hiện piknow
    const selectedUsers = filteredUsers.length > requestConfig.userCount ? 
      getRandomUsers(filteredUsers, requestConfig.userCount) : filteredUsers;
    
    console.log(`>> Đã chọn ${selectedUsers.length} user để thực hiện piknow`);
    logToFile(`STEP 3: Đã chọn ${selectedUsers.length} user để thực hiện piknow`);
    
    // STEP 3.5: Thiết lập proxy cho các user
    console.log(`\n>> STEP 3.5: Thiết lập proxy cho các user...`);
    logToFile(`STEP 3.5: Thiết lập proxy cho các user...`);
    
    // Gán proxy cho từng user để đảm bảo mỗi user đều có proxy
    const userObjects = selectedUsers.map(user => ({
      uid: user.uid,
      ukey: user.ukey,
      piname: user.piname,
      proxy: user.proxy || null
    }));
    
    const userAssignments = proxyManager.assignProxiesToUsers(userObjects);
    
    // Cập nhật thông tin proxy cho các user
    userAssignments.forEach(assignment => {
      const userIndex = userObjects.findIndex(u => u.uid === assignment.user.uid);
      if (userIndex !== -1) {
        userObjects[userIndex].proxy = assignment.proxy;
      }
    });
    
    console.log(`>> Đã gán proxy cho ${userAssignments.length}/${userObjects.length} user`);
    logToFile(`Đã gán proxy cho ${userAssignments.length}/${userObjects.length} user`);
    
    // Hiển thị thống kê proxy
    const proxyStats = proxyManager.getProxyStats();
    console.log(`>> Thống kê proxy: ${proxyStats.active}/${proxyStats.total} proxy hoạt động, ${proxyStats.fromExcel} từ Excel, ${proxyStats.fromRotating} từ rotating`);
    
    // Step 4: Thực hiện piknow
    console.log(`\n>> STEP 4: Bắt đầu thực hiện piknow...`);
    logToFile(`STEP 4: Bắt đầu thực hiện piknow với ${piknowCount} bài cho mỗi user`);
    
    // Cấu hình piknowService với cài đặt Cluster và Proxy
    piknowService.configureService({
      numWorkers: requestConfig.numCpus,
      concurrentTasksPerWorker: requestConfig.tasksPerCpu,
      proxyManager: proxyManager,
      users: userObjects
    });
    
    const result = await piknowService.startPiKnowProcess(piknowCount);
    
    const endTime = Date.now();
    const totalDuration = formatDuration(endTime - startTime);
    
    console.log(`\n>> Kết quả cuối cùng: ${result.success}/${result.total} piknow thành công`);
    console.log(`>> Thời gian chạy: ${totalDuration}`);
    
    logToFile(`====== KẾT QUẢ CUỐI CÙNG ======`);
    logToFile(`Thành công: ${result.success} piknow | Thất bại: ${result.failure} piknow`);
    logToFile(`Thời gian chạy: ${totalDuration}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH PIKNOW =======`);
    
    return { 
      success: result.success > 0,
      message: `Đã piknow ${result.success}/${result.total} lượt thành công!`,
      stats: {
        total: result.total,
        success: result.success,
        failure: result.failure,
        piknowedIds: result.piknowedIds,
        runtime: totalDuration
      }
    };
  } catch (error) {
    console.error(`❌ Lỗi không xử lý được: ${error.message}`);
    console.error(error.stack);
    
    logToFile(`====== LỖI NGHIÊM TRỌNG ======`);
    logToFile(`Lỗi: ${error.message}`);
    logToFile(`Stack: ${error.stack}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH PIKNOW (LỖI) =======`);
    
    return {
      success: false,
      message: `Đã xảy ra lỗi khi piknow: ${error.message}`,
      error: error.toString(),
      stack: error.stack
    };
  } finally {
    // Dọn dẹp tài nguyên
    proxyManager.stop();
    console.log('>> Đã dừng tất cả services');
    logToFile('Đã dừng tất cả services');
  }
}

function getRandomUsers(users, n) {
  const shuffled = [...users].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, n);
}

module.exports = handlePiKnow;