const cluster = require('cluster');
const os = require('os');
const { Worker } = require('worker_threads');
const path = require('path');

/**
 * Quản lý cluster và phân bổ tác vụ giữa các CPU
 */
class ClusterManager {
  constructor(options = {}) {
    // Số lượng CPU (workers) sẽ sử dụng, mặc định là tất cả CPU
    this.numWorkers = options.numWorkers || os.cpus().length;
    
    // Số lượng tác vụ xử lý đồng thời tối đa trên mỗi CPU/worker
    this.concurrentTasksPerWorker = options.concurrentTasksPerWorker || 10;
    
    // Proxy manager để lấy proxies
    this.proxyManager = options.proxyManager;
    
    // Số lượng tác vụ song song tối đa
    this.maxConcurrentTasks = this.numWorkers * this.concurrentTasksPerWorker;
    
    // Hàng đợi tác vụ
    this.taskQueue = [];
    
    // Số lượng tác vụ đang thực thi
    this.runningTasks = 0;
    
    // Danh sách các worker threads
    this.workers = [];
    
    // Kết quả tác vụ
    this.results = [];
    
    console.log(`>> Khởi tạo ClusterManager với ${this.numWorkers} CPUs và ${this.maxConcurrentTasks} tác vụ đồng thời tối đa`);
  }
  
  /**
   * Thực thi danh sách tác vụ like
   * @param {Array} tasks - Danh sách tác vụ cần thực hiện
   * @returns {Promise} - Promise sẽ resolve khi tất cả tác vụ hoàn thành
   */
  async executeTasks(tasks) {
    return new Promise(async (resolve) => {
      // Nếu không có tác vụ nào, trả về ngay
      if (!tasks || tasks.length === 0) {
        resolve([]);
        return;
      }
      
      console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ song song với ${this.numWorkers} CPUs`);
      
      // Thêm tất cả tác vụ vào hàng đợi
      this.taskQueue = [...tasks];
      
      // Số lượng tác vụ ban đầu
      const totalTasks = this.taskQueue.length;
      
      // Chức năng hoàn thành
      const checkCompletion = () => {
        if (this.results.length >= totalTasks) {
          console.log(`>> Đã hoàn thành ${this.results.length}/${totalTasks} tác vụ`);
          resolve(this.results);
        }
      };
      
      // Tạo promise để kiểm tra hoàn thành
      const executionPromise = new Promise(resolve => {
        // Chạy tác vụ mới khi có worker rảnh
        const scheduleTask = async () => {
          // Nếu còn tác vụ trong hàng đợi và chưa đạt giới hạn đồng thời
          while (this.taskQueue.length > 0 && this.runningTasks < this.maxConcurrentTasks) {
            const task = this.taskQueue.shift();
            this.runningTasks++;
            
            try {
              // Lấy proxy từ proxyManager nếu có
              let proxy = null;
              if (this.proxyManager && task.likeUser) {
                try {
                  proxy = await this.proxyManager.getRandomProxy(task.likeUser.uid);
                } catch (err) {
                  console.warn(`>> Không thể lấy proxy ngẫu nhiên cho user ${task.likeUser.uid}: ${err.message}`);
                  // Sử dụng proxy đã có trong task.likeUser nếu không lấy được từ ProxyManager
                  proxy = task.likeUser.proxy;
                }
              }
              
              // Thực thi tác vụ trong worker thread
              this.executeTaskInWorker(task, proxy)
                .then(result => {
                  this.results.push(result);
                  this.runningTasks--;
                  
                  // Kiểm tra nếu đã hoàn thành tất cả
                  if (this.results.length >= totalTasks) {
                    resolve();
                    return;
                  }
                  
                  // Chạy tác vụ tiếp theo
                  scheduleTask();
                })
                .catch(error => {
                  console.error(`>> Lỗi khi thực thi tác vụ: ${error.message}`);
                  this.results.push({
                    success: false,
                    error: error.message,
                    task
                  });
                  this.runningTasks--;
                  
                  // Kiểm tra nếu đã hoàn thành tất cả
                  if (this.results.length >= totalTasks) {
                    resolve();
                    return;
                  }
                  
                  // Chạy tác vụ tiếp theo
                  scheduleTask();
                });
            } catch (error) {
              console.error(`>> Lỗi khi khởi tạo tác vụ: ${error.message}`);
              this.results.push({
                success: false,
                error: error.message,
                task
              });
              this.runningTasks--;
            }
          }
        };
        
        // Khởi chạy tác vụ ban đầu
        scheduleTask();
      });
      
      // Đợi tất cả tác vụ hoàn thành
      await executionPromise;
      
      // Kiểm tra lại một lần nữa
      checkCompletion();
    });
  }
  
  /**
   * Thực thi một tác vụ trong worker thread
   * @param {Object} task - Tác vụ cần thực hiện
   * @param {Object} proxy - Proxy sẽ sử dụng
   * @returns {Promise} - Promise sẽ resolve khi tác vụ hoàn thành
   */
  executeTaskInWorker(task, proxy) {
    return new Promise((resolve, reject) => {
      try {
        // Đường dẫn đến file worker
        const workerPath = path.join(__dirname, 'worker.js');
        
        // Lấy proxy từ task hoặc từ ProxyManager nếu cần
        let proxyToUse = proxy;
        
        if (!proxyToUse && this.proxyManager && task.likeUser) {
          try {
            // Sử dụng getRandomProxy thay vì getProxyForUser
            proxyToUse = this.proxyManager.getRandomProxy(task.likeUser.uid);
          } catch (error) {
            console.warn(`>> Không thể lấy proxy cho user ${task.likeUser.uid}: ${error.message}`);
            // Sử dụng proxy đã có trong task.likeUser nếu không lấy được từ ProxyManager
            proxyToUse = task.likeUser.proxy;
          }
        }
        
        // Nếu vẫn không có proxy, trả về lỗi
        if (!proxyToUse) {
          console.error(`>> CẢNH BÁO: Không có proxy cho user ${task.likeUser.uid}!`);
          resolve({ 
            success: false, 
            userId: task.likeUser.uid,
            targetUserId: task.targetUserId,
            postId: task.postId,
            error: "Không có proxy khả dụng"
          });
          return;
        }
        
        // Tạo worker thread mới
        const worker = new Worker(workerPath, {
          workerData: {
            ...task,
            proxy: proxyToUse
          }
        });
        
        // Lắng nghe kết quả từ worker
        worker.on('message', (message) => {
          // Xử lý thông điệp log từ worker (tính năng mới)
          if (message.log) {
            const { type, message: logMessage } = message.log;
            if (type === 'error') {
              console.error(logMessage);
            } else {
              console.log(logMessage);
            }
            return;
          }
          
          if (message.error) {
            reject(new Error(message.error));
            return;
          }
          
          // Nếu worker báo cáo lỗi proxy, cập nhật ProxyManager
          if (message.result && !message.result.success && message.result.statusCode === 407) {
            try {
              if (this.proxyManager && message.result.proxyHost && message.result.proxyPort) {
                this.proxyManager.reportProxyError(
                  message.result.proxyHost,
                  message.result.proxyPort,
                  407
                );
              }
            } catch (proxyError) {
              console.warn(`>> Lỗi khi báo cáo proxy lỗi: ${proxyError.message}`);
            }
          }
          
          resolve(message.result);
        });
        
        // Lắng nghe lỗi từ worker
        worker.on('error', (error) => {
          reject(error);
        });
        
        // Lắng nghe sự kiện worker kết thúc
        worker.on('exit', (code) => {
          if (code !== 0) {
            reject(new Error(`Worker stopped with exit code ${code}`));
          }
        });
        
        // Thêm worker vào danh sách để theo dõi
        this.workers.push(worker);
      } catch (error) {
        reject(error);
      }
    });
  }
  
  /**
   * Lấy thống kê của cluster manager
   * @returns {Object} - Thống kê về tình trạng hiện tại
   */
  getStats() {
    return {
      numWorkers: this.numWorkers,
      maxConcurrentTasks: this.maxConcurrentTasks,
      runningTasks: this.runningTasks,
      queuedTasks: this.taskQueue.length,
      completedTasks: this.results.length,
      successTasks: this.results.filter(r => r.success).length,
      failedTasks: this.results.filter(r => !r.success).length
    };
  }
  
  /**
   * Dọn dẹp tài nguyên
   */
  cleanup() {
    // Kết thúc tất cả worker threads
    this.workers.forEach(worker => {
      worker.terminate();
    });
    
    this.workers = [];
    this.taskQueue = [];
    this.results = [];
    this.runningTasks = 0;
    
    console.log(`>> Đã dọn dẹp tài nguyên ClusterManager`);
  }
}

module.exports = ClusterManager; 