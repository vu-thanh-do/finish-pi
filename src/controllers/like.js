const path = require('path');
const ExcelReaderService = require("../models/excelSheed");
const likeService = require("../services/likeService");
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
    
    const logFilePath = path.join(logsDir, `pi-automate-like-logs-${dateStr}.txt`);
    
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
  
  console.log(`\n-------- TRẠNG THÁI TIẾN ĐỘ LIKE --------`);
  console.log(`[${bar}] ${percent}% (${completed}/${total})`);
  console.log(`✅ Thành công: ${success} | ❌ Thất bại: ${failure} | ⏳ Đang xử lý: ${running}`);
  console.log(`----------------------------------------\n`);
}

async function handleLike(req) {
  // Khởi tạo các dịch vụ
  const proxyManager = new ProxyManager();
  const startTime = Date.now();
  
  logToFile(`======= BẮT ĐẦU TIẾN TRÌNH LIKE =======`);
  logToFile(`Phiên bản: v2.0 (Xử lý song song với đa luồng)`);
  
  try {
    // Khởi tạo ProxyManager và thiết lập cho apiClient
    logToFile(`Khởi tạo ProxyManager...`);
    await proxyManager.initialize();
    apiClient.setGlobalProxyManager(proxyManager);
    
    const likesPerUser = parseInt(req) || 3;
    console.log(`>> Yêu cầu thực hiện ${likesPerUser} lượt like mỗi user`);
    logToFile(`Yêu cầu thực hiện ${likesPerUser} lượt like mỗi user`);
    
    if (likesPerUser <= 0) {
      logToFile(`Số lượng like/user <= 0, kết thúc`);
      return { success: true, message: "Không cần like" };
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
    
    // Step 1: Cấu hình likeService với cài đặt Cluster và Proxy
    console.log(`\n>> STEP 1: Cấu hình LikeService...`);
    logToFile(`STEP 1: Cấu hình LikeService...`);
    
    likeService.configureService({
      numWorkers: FIXED_CONFIG.numCpus,
      concurrentTasksPerWorker: FIXED_CONFIG.tasksPerCpu,
      proxyManager: proxyManager
    });
    
    // Step 2: Bắt đầu tiến trình like
    console.log(`\n>> STEP 2: Bắt đầu tiến trình like...`);
    logToFile(`STEP 2: Bắt đầu tiến trình like với ${likesPerUser} lượt like mỗi user`);
    
    // Theo dõi tiến độ
    const progressInterval = setInterval(() => {
      const stats = likeService.likeClusterManager?.getStats() || { 
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
    
    // Thực hiện like
    const result = await likeService.startLikeProcess(likesPerUser);
    
    // Dừng theo dõi tiến độ
    clearInterval(progressInterval);
    
    // Hiển thị kết quả
    const endTime = Date.now();
    const totalDuration = formatDuration(endTime - startTime);
    
    console.log(`\n>> Kết quả cuối cùng: ${result.success}/${result.total} like thành công, trên ${result.uniqueArticles} bài viết độc nhất`);
    console.log(`>> Thời gian chạy: ${totalDuration}`);
    
    logToFile(`====== KẾT QUẢ CUỐI CÙNG ======`);
    logToFile(`Thành công: ${result.success} like | Thất bại: ${result.failure} like | Bài viết: ${result.uniqueArticles}`);
    logToFile(`Thời gian chạy: ${totalDuration}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH LIKE =======`);
    
    return { 
      success: result.success > 0,
      message: `Đã thực hiện ${result.success}/${result.total} like thành công!`,
      stats: {
        total: result.total,
        success: result.success,
        failure: result.failure,
        uniqueArticles: result.uniqueArticles,
        runtime: totalDuration
      }
    };
  } catch (error) {
    console.error(`❌ Lỗi không xử lý được: ${error.message}`);
    console.error(error.stack);
    
    logToFile(`====== LỖI NGHIÊM TRỌNG ======`);
    logToFile(`Lỗi: ${error.message}`);
    logToFile(`Stack: ${error.stack}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH LIKE (LỖI) =======`);
    
    return {
      success: false,
      message: `Đã xảy ra lỗi khi thực hiện like: ${error.message}`,
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

module.exports = { handleLike };
