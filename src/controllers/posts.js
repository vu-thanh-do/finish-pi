const path = require('path');
const ExcelReaderService = require("../models/excelSheed");
const postService = require("../services/postService");
const ProxyManager = require('../services/ProxyManager');
const apiClient = require("../api/apiClient");
const { cpus } = require('os');
const fs = require('fs');
const { formatDuration, sleep } = require('../utils/helpers');

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
    
    const logFilePath = path.join(logsDir, `pi-automate-post-logs-${dateStr}.txt`);
    
    // Thêm timestamp vào message
    const logMessage = `${timestamp} ${message}\n`;
    
    // Ghi log vào file
    fs.appendFileSync(logFilePath, logMessage, 'utf8');
  } catch (error) {
    console.error(`Lỗi khi ghi log: ${error.message}`);
  }
}

function updateProgressStatus(total, completed, success, failure, running) {
  const percent = total > 0 ? Math.floor((completed / total) * 100) : 0;
  const bar = Array(20).fill('▒').map((char, i) => i < Math.floor(percent / 5) ? '█' : '▒').join('');
  
  console.log(`\n-------- TRẠNG THÁI TIẾN ĐỘ ĐĂNG BÀI --------`);
  console.log(`[${bar}] ${percent}% (${completed}/${total})`);
  console.log(`✅ Thành công: ${success} | ❌ Thất bại: ${failure} | ⏳ Đang xử lý: ${running}`);
  console.log(`-----------------------------------------------\n`);
}

async function handlePostArticles(req) {
  // Khởi tạo các dịch vụ
  const proxyManager = new ProxyManager();
  const startTime = Date.now();
  
  logToFile(`======= BẮT ĐẦU TIẾN TRÌNH ĐĂNG BÀI =======`);
  logToFile(`Phiên bản: v2.0 (Xử lý song song với đa luồng)`);
  
  try {
    // Khởi tạo ProxyManager và thiết lập cho apiClient
    logToFile(`Khởi tạo ProxyManager...`);
    await proxyManager.initialize();
    apiClient.setGlobalProxyManager(proxyManager);
    
    const postsPerUser = parseInt(req) || 2;
    console.log(`>> Yêu cầu thực hiện ${postsPerUser} bài viết mỗi user`);
    logToFile(`Yêu cầu thực hiện ${postsPerUser} bài viết mỗi user`);
    
    if (postsPerUser <= 0) {
      logToFile(`Số lượng bài/user <= 0, kết thúc`);
      return { success: true, message: "Không cần đăng bài" };
    }
    
    // Cấu hình cố định
    const totalCores = cpus().length;
    console.log(`>> Máy tính có ${totalCores} CPU cores`);
    
    const FIXED_CONFIG = {
      numCpus: Math.min(8, totalCores),  // Số CPU sử dụng, tối đa 8
      tasksPerCpu: 6,                    // Tác vụ đồng thời mỗi CPU
      concurrencyLimit: 48,              // Tổng số luồng đồng thời
      debug: true
    };
    
    console.log(`>> THÔNG SỐ CẤU HÌNH:`);
    console.log(`>> - Số CPU: ${FIXED_CONFIG.numCpus}`);
    console.log(`>> - Tác vụ/CPU: ${FIXED_CONFIG.tasksPerCpu}`);
    console.log(`>> - Tổng luồng đồng thời: ${FIXED_CONFIG.concurrencyLimit}`);
    
    logToFile(`Thông số: CPUs=${FIXED_CONFIG.numCpus}, Tasks/CPU=${FIXED_CONFIG.tasksPerCpu}, Luồng=${FIXED_CONFIG.concurrencyLimit}`);
    
    // Step 1: Cấu hình postService với cài đặt Cluster và Proxy
    console.log(`\n>> STEP 1: Cấu hình PostService...`);
    logToFile(`STEP 1: Cấu hình PostService...`);
    
    postService.configureService({
      numWorkers: FIXED_CONFIG.numCpus,
      concurrentTasksPerWorker: FIXED_CONFIG.tasksPerCpu,
      proxyManager: proxyManager
    });
    
    // Step 2: Bắt đầu tiến trình đăng bài
    console.log(`\n>> STEP 2: Bắt đầu tiến trình đăng bài...`);
    logToFile(`STEP 2: Bắt đầu tiến trình đăng bài với ${postsPerUser} bài viết mỗi user`);
    
    // Theo dõi tiến độ
    const progressInterval = setInterval(() => {
      const stats = postService.postClusterManager?.getStats() || { 
        totalTasks: 0, completedTasks: 0, successCount: 0, 
        failCount: 0, runningTasks: 0 
      };
      
      updateProgressStatus(
        stats.totalTasks,
        stats.completedTasks,
        stats.successCount,
        stats.failCount,
        stats.runningTasks
      );
      
      const memUsage = process.memoryUsage();
      console.log(`\n-------- MEMORY USAGE --------`);
      console.log(`Heap Used: ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`);
      console.log(`Heap Total: ${Math.round(memUsage.heapTotal / 1024 / 1024)}MB`);
      console.log(`RSS: ${Math.round(memUsage.rss / 1024 / 1024)}MB`);
      console.log(`-----------------------------\n`);
    }, 3000);
    
    // Thực hiện đăng bài
    const result = await postService.startPostProcess(postsPerUser);
    
    // Dừng theo dõi tiến độ
    clearInterval(progressInterval);
    
    // Hiển thị kết quả
    const endTime = Date.now();
    const totalDuration = formatDuration(endTime - startTime);
    
    console.log(`\n>> Kết quả cuối cùng: ${result.success}/${result.total} bài đăng thành công`);
    console.log(`>> Thời gian chạy: ${totalDuration}`);
    
    logToFile(`====== KẾT QUẢ CUỐI CÙNG ======`);
    logToFile(`Thành công: ${result.success} bài | Thất bại: ${result.failure} bài`);
    logToFile(`Thời gian chạy: ${totalDuration}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH ĐĂNG BÀI =======`);
    
    return { 
      success: result.success > 0,
      message: `Đã đăng ${result.success}/${result.total} bài viết thành công!`,
      stats: {
        total: result.total,
        success: result.success,
        failure: result.failure,
        runtime: totalDuration
      }
    };
  } catch (error) {
    console.error(`❌ Lỗi không xử lý được: ${error.message}`);
    console.error(error.stack);
    
    logToFile(`====== LỖI NGHIÊM TRỌNG ======`);
    logToFile(`Lỗi: ${error.message}`);
    logToFile(`Stack: ${error.stack}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH ĐĂNG BÀI (LỖI) =======`);
    
    return {
      success: false,
      message: `Đã xảy ra lỗi khi đăng bài: ${error.message}`,
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

module.exports = { handlePostArticles };
