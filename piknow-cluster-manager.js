const cluster = require('cluster');
const os = require('os');
const { Worker } = require('worker_threads');
const path = require('path');
const fs = require('fs');

/**
 * Quản lý các worker thread cho PiKnow
 */
class PiKnowClusterManager {
  /**
   * Khởi tạo PiKnowClusterManager
   * @param {Object} options - Các tùy chọn
   */
  constructor(options = {}) {
    // Cấu hình worker
    this.numWorkers = options.numWorkers || os.cpus().length;
    this.concurrentTasksPerWorker = options.concurrentTasksPerWorker || 5;
    this.maxConcurrentTasks = this.numWorkers * this.concurrentTasksPerWorker;
    
    // Quản lý tác vụ
    this.taskQueue = [];
    this.runningTasks = 0;
    this.workers = [];
    this.results = [];
    this.failedTasks = []; // Lưu các task thất bại để xử lý lại sau
    this.taskRetryMap = new Map(); // Theo dõi số lần retry của mỗi task
    
    // Quản lý proxy
    this.proxyManager = options.proxyManager || null;
    
    // Theo dõi lỗi proxy
    this.proxyErrorCounts = new Map();
    this.proxyRateLimitCount = new Map(); // Theo dõi số lần bị rate limit của từng proxy
    this.maxProxyErrors = 5;
    this.proxyBlacklist = new Set(); // Lưu các proxy không hoạt động để tránh sử dụng lại
    
    // Theo dõi lỗi user
    this.userErrorCounts = new Map(); // Theo dõi số lần lỗi của từng user
    
    // Cấu hình
    this.workerTimeout = options.workerTimeout || 45000; // 45 giây
    this.retryDelay = options.retryDelay || 5000; // 5 giây
    this.proxyRotateInterval = options.proxyRotateInterval || 30; // Sau 30 tác vụ thì đổi proxy
    this.tasksProcessed = 0;
    this.useMissingTaskTracker = options.useMissingTaskTracker !== false; // Mặc định là true
    this.maxWorkerRetries = 3; // Số lần tối đa retry cho mỗi worker
    
    // Cấu hình theo dõi tiến độ
    this.lastProgressReport = 0;
    this.progressReportInterval = 5000; // 5 giây báo cáo tiến độ một lần
    
    // Theo dõi các task bị thiếu
    this.missingTasks = [];
    this.pendingTasksForUser = new Map(); // Lưu các task chưa hoàn thành cho mỗi user
    this.successTasksForUser = new Map(); // Lưu các task thành công cho mỗi user
    this.userTargetCounts = new Map(); // Lưu số lượng task mục tiêu cho mỗi user
    
    console.log(`>> [PiKnowCluster] Khởi tạo với ${this.numWorkers} CPUs, tối đa ${this.maxConcurrentTasks} tác vụ đồng thời`);
  }

  /**
   * Thực thi một danh sách các tác vụ piknow
   * @param {Array<Object>} tasks - Danh sách tác vụ piknow
   * @returns {Promise<Array>} - Kết quả của tất cả các tác vụ
   */
  async executeTasks(tasks) {
    if (!tasks || tasks.length === 0) {
      console.log(`>> [PiKnowCluster] Không có tác vụ nào để thực thi`);
      return [];
    }
    
    this.taskQueue = [...tasks];
    this.results = [];
    this.failedTasks = [];
    this.missingTasks = [];
    this.tasksProcessed = 0;
    
    // Khởi tạo theo dõi task trên mỗi user
    this.initializeUserTaskTracking(tasks);
    
    console.log(`>> [PiKnowCluster] Chuẩn bị thực thi ${tasks.length} tác vụ piknow song song với ${this.numWorkers} CPUs`);
    
    // Kiểm tra các user không có proxy trước khi bắt đầu
    const tasksWithoutProxy = tasks.filter(task => !task.piknowUser.proxy);
    if (tasksWithoutProxy.length > 0) {
      const uniqueUsersWithoutProxy = new Set(tasksWithoutProxy.map(task => task.piknowUser.piname));
      console.warn(`>> [PiKnowCluster] Cảnh báo: ${uniqueUsersWithoutProxy.size} user không có proxy`);
    }

    // Kiểm tra có proxyManager để phân bổ proxy không 
    if (this.proxyManager) {
      console.log(`>> [PiKnowCluster] Sử dụng ProxyManager để phân bổ proxy`);
      
      // Thống kê proxy
      const proxyStats = this.proxyManager.getProxyStats ? this.proxyManager.getProxyStats() : { total: 'không rõ' };
      console.log(`>> [PiKnowCluster] Số lượng proxy khả dụng: ${proxyStats.total}`);
    } else {
      console.log(`>> [PiKnowCluster] Không có ProxyManager, sẽ sử dụng proxy được gán trước`);
    }
    
    // Bắt đầu lên lịch tác vụ
    await this.scheduleNext();
    
    // Báo cáo tổng kết đầu tiên
    const successCount = this.results.filter(r => r.success).length;
    const failCount = this.results.length - successCount;
    console.log(`>> [PiKnowCluster] Kết quả thực thi: ${successCount}/${this.results.length} tác vụ thành công`);
    
    // Xử lý các tác vụ bị thiếu và thất bại nếu có
    if (this.failedTasks.length > 0 || this.getMissingTaskCount() > 0) {
      console.log(`>> [PiKnowCluster] Phát hiện ${this.failedTasks.length} tác vụ thất bại và ${this.getMissingTaskCount()} tác vụ còn thiếu`);
      
      // Nếu có tác vụ thất bại, thử thực hiện lại
      if (this.failedTasks.length > 0) {
        console.log(`>> [PiKnowCluster] Thử thực hiện lại ${this.failedTasks.length} tác vụ thất bại...`);
        await this.retryFailedTasks();
      }
      
      // Nếu có tác vụ còn thiếu, thử thực hiện thêm
      if (this.getMissingTaskCount() > 0) {
        await this.executeAdditionalTasks();
      }
    }
    
    // Tổng kết sau khi đã thực hiện các tác vụ bổ sung
    const finalSuccessCount = this.results.filter(r => r.success).length;
    const finalFailCount = this.results.length - finalSuccessCount;
    console.log(`>> [PiKnowCluster] Kết quả cuối cùng: ${finalSuccessCount}/${this.results.length} tác vụ thành công`);
    
    return this.results;
  }

  /**
   * Khởi tạo theo dõi task cho mỗi user
   */
  initializeUserTaskTracking(tasks) {
    this.pendingTasksForUser.clear();
    this.successTasksForUser.clear();
    this.userTargetCounts.clear();
    
    // Tính toán số lượng task cho mỗi user
    for (const task of tasks) {
      const userId = task.piknowUser.uid;
      
      if (!this.pendingTasksForUser.has(userId)) {
        this.pendingTasksForUser.set(userId, []);
        this.successTasksForUser.set(userId, 0);
      }
      
      this.pendingTasksForUser.get(userId).push({
        knowId: task.knowId,
        piknowText: task.piknowText
      });
      
      // Cập nhật số lượng mục tiêu
      this.userTargetCounts.set(userId, (this.userTargetCounts.get(userId) || 0) + 1);
    }
    
    console.log(`>> [PiKnowCluster] Theo dõi task cho ${this.pendingTasksForUser.size} users`);
  }

  /**
   * Lấy số lượng task còn thiếu
   */
  getMissingTaskCount() {
    let missingCount = 0;
    
    for (const [userId, targetCount] of this.userTargetCounts.entries()) {
      const successCount = this.successTasksForUser.get(userId) || 0;
      if (successCount < targetCount) {
        missingCount += (targetCount - successCount);
      }
    }
    
    return missingCount;
  }

  /**
   * Thực hiện thêm các task còn thiếu
   */
  async executeAdditionalTasks() {
    // Tạo danh sách các task bổ sung
    const additionalTasks = [];
    
    for (const [userId, targetCount] of this.userTargetCounts.entries()) {
      const successCount = this.successTasksForUser.get(userId) || 0;
      const pendingTasks = this.pendingTasksForUser.get(userId) || [];
      
      if (successCount < targetCount && pendingTasks.length > 0) {
        // Tìm user object
        const userTask = this.taskQueue.find(t => t.piknowUser.uid === userId);
        if (!userTask) continue;
        
        // Tạo task bổ sung cho user này
        const missingCount = targetCount - successCount;
        console.log(`>> [PiKnowCluster] User ${userTask.piknowUser.piname} còn thiếu ${missingCount} tasks`);
        
        // Tạo các task bổ sung
        for (let i = 0; i < Math.min(missingCount, pendingTasks.length); i++) {
          const pendingTask = pendingTasks[i];
          
          additionalTasks.push({
            piknowUser: userTask.piknowUser,
            knowId: pendingTask.knowId,
            piknowText: pendingTask.piknowText,
            isAdditional: true
          });
        }
      }
    }
    
    if (additionalTasks.length > 0) {
      console.log(`>> [PiKnowCluster] Thực hiện ${additionalTasks.length} tác vụ bổ sung`);
      
      // Reset task queue và thực hiện lại
      this.taskQueue = additionalTasks;
      this.failedTasks = [];
      
      // Đợi tất cả các task bổ sung hoàn thành
      await this.scheduleNext();
      
      // Cập nhật lại thống kê
      const additionalSuccess = this.results.filter(r => r.isAdditional && r.success).length;
      console.log(`>> [PiKnowCluster] Đã hoàn thành ${additionalSuccess}/${additionalTasks.length} tác vụ bổ sung`);
    } else {
      console.log(`>> [PiKnowCluster] Không có tác vụ bổ sung nào để thực hiện`);
    }
  }

  /**
   * Thực hiện lại các tác vụ thất bại
   */
  async retryFailedTasks() {
    if (this.failedTasks.length === 0) return;
    
    console.log(`>> [PiKnowCluster] Thực hiện lại ${this.failedTasks.length} tác vụ thất bại`);
    
    // Đảm bảo mỗi task thất bại đều có proxy mới
    for (const task of this.failedTasks) {
      // Thay đổi proxy cho task
      if (this.proxyManager) {
        try {
          const newProxy = this.proxyManager.getProxy();
          if (newProxy) {
            console.log(`>> [PiKnowCluster] Gán proxy mới ${newProxy.host}:${newProxy.port} cho tác vụ thất bại của user ${task.piknowUser.piname}`);
            task.piknowUser.proxy = newProxy;
          }
        } catch (error) {
          console.warn(`>> [PiKnowCluster] Không lấy được proxy mới: ${error.message}`);
        }
      }
    }
    
    // Reset task queue và thực hiện lại
    this.taskQueue = [...this.failedTasks];
    this.failedTasks = [];
    
    // Đợi tất cả các task thất bại hoàn thành
    await this.scheduleNext();
    
    // Cập nhật lại thống kê
    const retrySuccess = this.results.filter(r => r.isRetry && r.success).length;
    console.log(`>> [PiKnowCluster] Đã retry thành công ${retrySuccess}/${this.taskQueue.length} tác vụ thất bại`);
  }

  /**
   * Lên lịch và thực thi các tác vụ tiếp theo
   */
  async scheduleNext() {
    return new Promise((resolve) => {
      const checkCompletion = () => {
        // Kiểm tra đã hoàn thành chưa
        if (this.taskQueue.length === 0 && this.runningTasks === 0) {
          console.log(`>> [PiKnowCluster] Đã hoàn thành tất cả tác vụ`);
          resolve(this.results);
          return true;
        }
        return false;
      };
      
      // Nếu đã hoàn thành, không cần làm gì thêm
      if (checkCompletion()) return;
      
      // Lên lịch tác vụ mới
      const scheduleTask = async () => {
        // Lấy proxy mới nếu cần
        let rotateProxy = false;
        if (this.proxyManager && this.tasksProcessed > 0 && this.tasksProcessed % this.proxyRotateInterval === 0) {
          rotateProxy = true;
          console.log(`>> [PiKnowCluster] Đã xử lý ${this.tasksProcessed} tác vụ, đổi proxy cho các tác vụ tiếp theo`);
        }
        
        // Báo cáo tiến độ nếu cần
        this.reportProgress();
        
        // Thực thi nhiều tác vụ đồng thời
        while (this.taskQueue.length > 0 && this.runningTasks < this.maxConcurrentTasks) {
          const task = this.taskQueue.shift();
          
          // Đổi proxy nếu cần
          if (rotateProxy && this.proxyManager && typeof this.proxyManager.getProxy === 'function') {
            try {
              const newProxy = this.proxyManager.getProxy();
              if (newProxy) {
                task.piknowUser.proxy = newProxy;
              }
            } catch (error) {
              console.warn(`>> [PiKnowCluster] Lỗi khi lấy proxy mới: ${error.message}`);
            }
          }
          
          try {
            this.runningTasks++;
            this.executeTaskInWorker(task)
              .then(result => {
                this.results.push(result);
                this.runningTasks--;
                this.tasksProcessed++;
                
                // Cập nhật thống kê task cho user
                if (result.success) {
                  const userId = result.userId;
                  this.successTasksForUser.set(userId, (this.successTasksForUser.get(userId) || 0) + 1);
                } else {
                  // Nếu thất bại, lưu task để retry sau
                  // Kiểm tra xem task có đáng để retry không
                  if (this.shouldRetryTask(task, result.error)) {
                    task.isRetry = true;
                    task.retryCount = (task.retryCount || 0) + 1;
                    this.failedTasks.push(task);
                  }
                }
                
                // Báo cáo tiến độ
                this.reportProgress();
                
                // Lên lịch tác vụ tiếp theo
                if (!checkCompletion()) {
                  setImmediate(scheduleTask);
                }
              })
              .catch(error => {
                console.error(`>> [PiKnowCluster] Lỗi khi thực thi tác vụ:`, error);
                this.results.push({
                  success: false,
                  error: error.message || 'Lỗi không xác định',
                  knowId: task.knowId,
                  userId: task.piknowUser?.uid
                });
                this.runningTasks--;
                this.tasksProcessed++;
                
                // Lên lịch tác vụ tiếp theo
                if (!checkCompletion()) {
                  setImmediate(scheduleTask);
                }
              });
          } catch (error) {
            console.error(`>> [PiKnowCluster] Lỗi khi lên lịch tác vụ:`, error);
            this.results.push({
              success: false,
              error: error.message || 'Lỗi không xác định khi lên lịch',
              knowId: task.knowId,
              userId: task.piknowUser?.uid
            });
            this.runningTasks--;
            this.tasksProcessed++;
            
            // Lên lịch tác vụ tiếp theo
            if (!checkCompletion()) {
              setImmediate(scheduleTask);
            }
          }
        }
        
        // Nếu đã đạt đến số lượng tác vụ đồng thời tối đa, đợi thêm
        if (this.taskQueue.length > 0 && this.runningTasks >= this.maxConcurrentTasks) {
          setTimeout(() => {
            if (!checkCompletion()) {
              scheduleTask();
            }
          }, 100);
        }
      };
      
      // Bắt đầu lên lịch
      scheduleTask();
    });
  }

  /**
   * Báo cáo tiến độ chạy
   */
  reportProgress() {
    const now = Date.now();
    if (now - this.lastProgressReport < this.progressReportInterval) return;
    
    this.lastProgressReport = now;
    
    const successCount = this.results.filter(r => r.success).length;
    const failCount = this.results.length - successCount;
    const completion = ((this.results.length / (this.results.length + this.taskQueue.length + this.runningTasks)) * 100).toFixed(1);
    
    console.log(`>> [PiKnowCluster] Tiến độ: ${this.results.length}/${this.results.length + this.taskQueue.length + this.runningTasks} (${completion}%) - ${successCount} thành công, ${failCount} thất bại`);
  }

  /**
   * Kiểm tra xem có nên retry task không
   */
  shouldRetryTask(task, errorMessage) {
    // Nếu task đã retry nhiều lần, bỏ qua
    if (task.retryCount >= this.maxWorkerRetries) {
      return false;
    }
    
    // Nếu lỗi liên quan đến task không có nội dung, bỏ qua
    if (errorMessage && errorMessage.includes('không chứa piknowText')) {
      return false;
    }
    
    // Các lỗi tạm thời đáng retry
    const temporaryErrors = [
      'timeout',
      'socket hang up',
      'stream has been aborted',
      'network error',
      'ECONNRESET',
      'ECONNREFUSED',
      'ETIMEDOUT',
      '429',
      '500',
      '502',
      '503',
      '504'
    ];
    
    for (const err of temporaryErrors) {
      if (errorMessage && errorMessage.includes(err)) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * Thực thi một tác vụ trong worker thread
   * @param {Object} task - Tác vụ cần thực thi
   * @returns {Promise<Object>} - Kết quả của tác vụ
   */
  executeTaskInWorker(task) {
    return new Promise((resolve, reject) => {
      try {
        // Tìm đường dẫn worker
        const workerPath = this.findWorkerPath();
        
        // Kiểm tra và phân bổ proxy
        this.assignProxyToTask(task);
        
        // Kiểm tra tính hợp lệ của task
        try {
          this.validateTask(task);
        } catch (error) {
          // Nếu task không hợp lệ, trả về kết quả lỗi ngay lập tức
          console.error(`>> [PiKnowCluster] Task không hợp lệ: ${error.message}`);
          resolve({
            success: false,
            error: error.message,
            knowId: task.knowId,
            userId: task.piknowUser?.uid,
            isRetry: task.isRetry || false,
            isAdditional: task.isAdditional || false
          });
          return;
        }
        
        // Chuẩn bị dữ liệu cho worker
        const workerData = {
          piknowUser: task.piknowUser,
          knowId: task.knowId,
          proxy: task.piknowUser.proxy,
          piknowText: task.piknowText
        };
        
        // Tạo worker thread
        const worker = new Worker(workerPath, { workerData });
        
        // Thiết lập timeout để tránh worker bị treo
        const timeoutId = setTimeout(() => {
          console.error(`>> [PiKnowCluster] Worker xử lý quá ${this.workerTimeout/1000}s, buộc kết thúc`);
          this.terminateWorker(worker);
          resolve({
            success: false,
            error: `Tác vụ vượt quá thời gian (${this.workerTimeout/1000}s)`,
            knowId: task.knowId,
            userId: task.piknowUser?.uid,
            isRetry: task.isRetry || false,
            isAdditional: task.isAdditional || false
          });
        }, this.workerTimeout);
        
        // Xử lý các sự kiện từ worker
        worker.on('message', (message) => {
          // Xử lý các loại thông điệp khác nhau
          if (message.type === 'log') {
            const prefix = `[Worker-${task.knowId}] `;
            if (message.logType === 'error') {
              console.error(prefix + message.message);
            } else if (message.logType === 'debug') {
              // Bỏ qua debug log để giảm nhiễu
            } else {
              console.log(prefix + message.message);
            }
            return;
          }
          
          // Xử lý thông báo về rate limit
          if (message.type === 'rateLimit') {
            if (message.proxy) {
              const proxyKey = `${message.proxy.host}:${message.proxy.port}`;
              this.proxyRateLimitCount.set(proxyKey, (this.proxyRateLimitCount.get(proxyKey) || 0) + 1);
              console.warn(`>> [PiKnowCluster] Proxy ${proxyKey} bị rate limit (lần ${this.proxyRateLimitCount.get(proxyKey)})`);
              
              // Nếu proxy bị rate limit nhiều lần, đánh dấu không hoạt động
              if (this.proxyRateLimitCount.get(proxyKey) >= 3) {
                this.markProxyAsInactive(message.proxy);
              }
            }
            return;
          }
          
          // Xử lý thông báo về lỗi proxy
          if (message.type === 'proxyError') {
            if (message.proxy) {
              this.handleProxyError(message.proxy, message.statusCode || message.errorCode || 'unknown');
            }
            return;
          }
          
          clearTimeout(timeoutId);
          
          // Xử lý thông điệp kết quả
          this.terminateWorker(worker);
          
          // Thêm thông tin về retry và additional
          message.isRetry = task.isRetry || false;
          message.isAdditional = task.isAdditional || false;
          
          // Xử lý proxy lỗi
          if (!message.success && task.piknowUser.proxy) {
            this.handleProxyError(task.piknowUser.proxy, message.error);
          }
          
          resolve(message);
        });
        
        // Xử lý lỗi worker
        worker.on('error', (err) => {
          clearTimeout(timeoutId);
          console.error(`>> [PiKnowCluster] Lỗi worker:`, err);
          this.terminateWorker(worker);
          
          resolve({
            success: false,
            error: err.message || 'Lỗi không xác định trong worker',
            knowId: task.knowId,
            userId: task.piknowUser?.uid,
            isRetry: task.isRetry || false,
            isAdditional: task.isAdditional || false
          });
        });
        
        // Xử lý worker thoát
        worker.on('exit', (code) => {
          clearTimeout(timeoutId);
          
          if (code !== 0) {
            console.warn(`>> [PiKnowCluster] Worker thoát với mã ${code}`);
            resolve({
              success: false,
              error: `Worker thoát với mã ${code}`,
              knowId: task.knowId,
              userId: task.piknowUser?.uid,
              isRetry: task.isRetry || false,
              isAdditional: task.isAdditional || false
            });
          }
        });
        
        // Thêm worker vào danh sách
        this.workers.push(worker);
      } catch (error) {
        console.error(`>> [PiKnowCluster] Lỗi khi tạo worker:`, error);
        resolve({
          success: false,
          error: error.message || 'Lỗi không xác định khi khởi tạo worker',
          knowId: task.knowId,
          userId: task.piknowUser?.uid,
          isRetry: task.isRetry || false,
          isAdditional: task.isAdditional || false
        });
      }
    });
  }
  
  /**
   * Tìm đường dẫn file worker
   * @returns {string} - Đường dẫn worker
   */
  findWorkerPath() {
    // Thử các đường dẫn khác nhau
    const possiblePaths = [
      path.resolve(__dirname, 'piknow-worker.js'),
      path.join(__dirname, 'piknow-worker.js'),
      './piknow-worker.js',
      '../piknow-worker.js',
      path.resolve('./piknow-worker.js'),
      path.resolve('../piknow-worker.js')
    ];
    
    // Tìm đường dẫn đầu tiên tồn tại
    for (const testPath of possiblePaths) {
      if (fs.existsSync(testPath)) {
        console.log(`>> [PiKnowCluster] Tìm thấy worker tại: ${testPath}`);
        return testPath;
      }
    }
    
    // Nếu không tìm thấy, dùng đường dẫn mặc định
    console.warn(`>> [PiKnowCluster] Không tìm thấy worker ở các đường dẫn thông thường, dùng đường dẫn mặc định`);
    return path.resolve(__dirname, 'piknow-worker.js');
  }
  
  /**
   * Phân bổ proxy cho task
   * @param {Object} task - Task cần phân bổ proxy
   */
  assignProxyToTask(task) {
    // Nếu task đã có proxy và proxy đó hợp lệ và không trong blacklist, giữ nguyên
    if (task.piknowUser.proxy && 
        task.piknowUser.proxy.host && 
        task.piknowUser.proxy.port && 
        task.piknowUser.proxy.name && 
        task.piknowUser.proxy.password) {
      
      const proxyKey = `${task.piknowUser.proxy.host}:${task.piknowUser.proxy.port}`;
      if (!this.proxyBlacklist.has(proxyKey)) {
        return;
      } else {
        console.log(`>> [PiKnowCluster] Proxy ${proxyKey} trong blacklist, cần thay thế`);
      }
    }
    
    // Nếu có proxyManager, lấy proxy mới
    if (this.proxyManager) {
      try {
        if (typeof this.proxyManager.getProxy === 'function') {
          let retryCount = 0;
          let proxy = null;
          
          // Thử lấy proxy tới 3 lần
          while (!proxy && retryCount < 3) {
            proxy = this.proxyManager.getProxy();
            if (!proxy && retryCount < 2) {
              console.log(`>> [PiKnowCluster] Thử lấy proxy lần ${retryCount + 1}...`);
              // Đợi một chút trước khi thử lại
              require('timers').setTimeout(() => {}, 200);
            }
            retryCount++;
          }
          
          if (proxy) {
            const proxyKey = `${proxy.host}:${proxy.port}`;
            // Kiểm tra proxy không nằm trong blacklist
            if (!this.proxyBlacklist.has(proxyKey)) {
              task.piknowUser.proxy = proxy;
              console.log(`>> [PiKnowCluster] Gán proxy ${proxy.host}:${proxy.port} cho user ${task.piknowUser.piname}`);
            } else {
              console.warn(`>> [PiKnowCluster] Proxy ${proxyKey} trong blacklist, bỏ qua`);
              // Thử lấy proxy khác
              proxy = this.proxyManager.getProxy();
              if (proxy) {
                const newProxyKey = `${proxy.host}:${proxy.port}`;
                if (!this.proxyBlacklist.has(newProxyKey)) {
                  task.piknowUser.proxy = proxy;
                  console.log(`>> [PiKnowCluster] Gán proxy thay thế ${proxy.host}:${proxy.port} cho user ${task.piknowUser.piname}`);
                }
              }
            }
          } else {
            console.warn(`>> [PiKnowCluster] Không lấy được proxy cho user ${task.piknowUser.piname}`);
          }
        } else {
          console.warn(`>> [PiKnowCluster] ProxyManager không có phương thức getProxy()`);
        }
      } catch (error) {
        console.warn(`>> [PiKnowCluster] Lỗi khi lấy proxy: ${error.message}`);
      }
    }
  }
  
  /**
   * Kiểm tra tính hợp lệ của task
   * @param {Object} task - Task cần kiểm tra
   */
  validateTask(task) {
    if (!task.piknowUser) {
      throw new Error('Task không chứa thông tin user');
    }
    
    if (!task.knowId) {
      throw new Error('Task không chứa knowId');
    }
    
    // Nếu không có piknowText, worker sẽ sử dụng text mặc định
    if (!task.piknowText) {
      console.warn(`>> [PiKnowCluster] Task không chứa piknowText, worker sẽ sử dụng nội dung mặc định`);
    }
  }
  
  /**
   * Kết thúc worker an toàn
   * @param {Worker} worker - Worker cần kết thúc
   */
  terminateWorker(worker) {
    try {
      worker.terminate();
    } catch (error) {
      // Bỏ qua lỗi khi terminate
    }
    
    // Loại bỏ worker khỏi danh sách
    const index = this.workers.indexOf(worker);
    if (index !== -1) {
      this.workers.splice(index, 1);
    }
  }
  
  /**
   * Xử lý lỗi proxy
   * @param {Object} proxy - Proxy gặp lỗi
   * @param {string|number} errorInfo - Thông tin lỗi
   */
  handleProxyError(proxy, errorInfo) {
    if (!proxy || !proxy.host || !proxy.port) {
      return;
    }
    
    const proxyKey = `${proxy.host}:${proxy.port}`;
    console.warn(`>> [PiKnowCluster] Proxy ${proxyKey} gặp lỗi: ${errorInfo}`);
    
    // Kiểm tra và đếm lỗi proxy
    if (this.shouldMarkProxyAsInactive(proxy, errorInfo)) {
      // Đánh dấu proxy không hoạt động
      this.markProxyAsInactive(proxy);
      
      // Reset bộ đếm lỗi
      this.proxyErrorCounts.delete(proxyKey);
    } else {
      // Tăng bộ đếm lỗi
      const currentErrorCount = this.proxyErrorCounts.get(proxyKey) || 0;
      this.proxyErrorCounts.set(proxyKey, currentErrorCount + 1);
      
      // Nếu đạt ngưỡng lỗi tối đa, đánh dấu proxy không hoạt động
      if (currentErrorCount + 1 >= this.maxProxyErrors) {
        this.markProxyAsInactive(proxy);
        
        // Reset bộ đếm lỗi
        this.proxyErrorCounts.delete(proxyKey);
      }
    }
  }
  
  /**
   * Kiểm tra có nên đánh dấu proxy không hoạt động không
   * @param {Object} proxy - Proxy cần kiểm tra
   * @param {string|number} errorInfo - Thông tin lỗi
   * @returns {boolean} - Có nên đánh dấu không hoạt động không
   */
  shouldMarkProxyAsInactive(proxy, errorInfo) {
    if (!errorInfo) {
      return false;
    }
    
    // Chuyển errorInfo về string để dễ kiểm tra
    const errorStr = String(errorInfo);
    
    // Các lỗi proxy nghiêm trọng cần đánh dấu ngay
    const criticalErrors = [
      'ECONNREFUSED',
      'ETIMEDOUT',
      'socket hang up',
      'tunneling socket',
      'proxy authentication',
      '407',
      'timeout',
      'stream has been aborted'
    ];
    
    for (const criticalError of criticalErrors) {
      if (errorStr.includes(criticalError)) {
        return true;
      }
    }
    
    // Lỗi 429 (too many requests) nhiều lần cũng cần đánh dấu
    if (errorStr.includes('429')) {
      const proxyKey = `${proxy.host}:${proxy.port}`;
      const rateLimitCount = this.proxyRateLimitCount.get(proxyKey) || 0;
      return rateLimitCount >= 3;
    }
    
    return false;
  }
  
  /**
   * Đánh dấu proxy không hoạt động
   * @param {Object} proxy - Proxy cần đánh dấu
   */
  markProxyAsInactive(proxy) {
    if (!proxy || !proxy.host || !proxy.port) {
      return;
    }
    
    const proxyKey = `${proxy.host}:${proxy.port}`;
    console.log(`>> [PiKnowCluster] Đánh dấu proxy ${proxyKey} không hoạt động`);
    
    // Thêm vào blacklist
    this.proxyBlacklist.add(proxyKey);
    
    // Đánh dấu proxy không hoạt động nếu có ProxyManager
    if (this.proxyManager && typeof this.proxyManager.markProxyAsInactive === 'function') {
      this.proxyManager.markProxyAsInactive(proxy);
    }
  }

  /**
   * Lấy thống kê hiện tại của cluster
   */
  getStats() {
    const successCount = this.results.filter(r => r.success).length;
    
    return {
      totalTasks: this.results.length + this.taskQueue.length,
      completedTasks: this.results.length,
      successCount,
      failCount: this.results.length - successCount,
      pendingTasks: this.taskQueue.length,
      runningTasks: this.runningTasks,
      workers: this.workers.length,
      successRate: this.results.length > 0 ? 
        (successCount / this.results.length * 100).toFixed(2) + '%' : '0%'
    };
  }

  /**
   * Dọn dẹp tài nguyên
   */
  cleanup() {
    // Kết thúc tất cả worker
    for (const worker of this.workers) {
      this.terminateWorker(worker);
    }
    
    this.workers = [];
    this.taskQueue = [];
    this.runningTasks = 0;
    this.proxyErrorCounts.clear();
    this.proxyRateLimitCount.clear();
    this.proxyBlacklist.clear();
    
    console.log(`>> [PiKnowCluster] Đã dọn dẹp tài nguyên`);
  }
}

module.exports = PiKnowClusterManager; 