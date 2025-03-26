const handleLikeEachOther = require('./src/controllers/likeEachOther');
const path = require('path');
const fs = require('fs');
const os = require('os');

// Thư mục logs
const logsDir = path.join(__dirname, 'src', 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

// Tạo file log
const logFile = path.join(logsDir, `test-like-${new Date().toISOString().slice(0, 10)}.log`);
const logger = fs.createWriteStream(logFile, { flags: 'a' });

// Function ghi log
const log = (message) => {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  logger.write(logMessage + '\n');
};

// Xác định đường dẫn tuyệt đối đến file Excel
const excelPath = path.join(__dirname, 'src', 'data', 'PI.xlsx');

log(`Bắt đầu chạy test like chéo...`);

// Kiểm tra xem file Excel có tồn tại không
if (!fs.existsSync(excelPath)) {
  log(`Lỗi: File Excel không tồn tại tại đường dẫn: ${excelPath}`);
  logger.end();
  process.exit(1);
}

log(`File Excel được tìm thấy tại: ${excelPath}`);

// Thông tin về số CPU có sẵn
const cpuCount = os.cpus().length;
log(`Máy tính có ${cpuCount} CPU có thể sử dụng.`);

// Hiển thị thông tin hệ thống để debug
log(`Hệ điều hành: ${os.platform()} ${os.release()}`);
log(`CPU: ${os.cpus()[0].model}`);
log(`RAM: ${Math.round(os.totalmem() / (1024 * 1024 * 1024))}GB`);
log(`Node.js: ${process.version}`);

const safeCpuCount = Math.max(1, Math.min(cpuCount - 1, 4));
const safeTasksPerCpu = 6; 

const safeUserCount = 60;

const mockRequest = {
  body: {
    userCount: safeUserCount,
    useExcelUsers: true,
    excelPath: excelPath,
    targetCount: safeUserCount,
    onlyActive: true,
    numCpus: safeCpuCount,
    tasksPerCpu: safeTasksPerCpu,
    concurrencyLimit: safeCpuCount * safeTasksPerCpu,
    debug: true
  }
};

log('Thông số cấu hình:');
log(`- Số lượng user: ${safeUserCount}`);
log(`- Số lượng CPUs: ${safeCpuCount}/${cpuCount}`);
log(`- Tasks/CPU: ${safeTasksPerCpu}`);
log(`- Total concurrency: ${safeCpuCount * safeTasksPerCpu}`);
log(`- File Excel: ${excelPath}`);

log('Thông số request:' + JSON.stringify(mockRequest.body, null, 2));

const startTime = Date.now();

process.on('uncaughtException', (error) => {
  log(`UNCAUGHT EXCEPTION: ${error.message}`);
  log(error.stack);
  logger.end();
  process.exit(1);
});

log('Bắt đầu chạy thử like chéo với ClusterManager...');

handleLikeEachOther(mockRequest)
  .then(result => {
    const duration = (Date.now() - startTime) / 1000;
    log(`Hoàn thành sau ${duration.toFixed(2)} giây.`);
    log(`Kết quả: ${JSON.stringify(result, null, 2)}`);
    logger.end();
  })
  .catch(error => {
    log(`Lỗi: ${error.message}`);
    log(error.stack);
    logger.end();
    process.exit(1);
  }); 