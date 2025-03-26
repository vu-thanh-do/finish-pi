const cluster = require('cluster');
const os = require('os');
const { Worker } = require('worker_threads');
const path = require('path');

/**
 * Quản lý cluster và phân bổ tác vụ giữa các CPU
 */
class ClusterManager {
  constructor(options = {}) {
    this.numWorkers = options.numWorkers || os.cpus().length;
    
    this.concurrentTasksPerWorker = options.concurrentTasksPerWorker || 10;
    
    this.proxyManager = options.proxyManager;
    
    this.maxConcurrentTasks = this.numWorkers * this.concurrentTasksPerWorker;
    
    this.taskQueue = [];
    
    this.runningTasks = 0;
    
    this.workers = [];
    
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
      if (!tasks || tasks.length === 0) {
        resolve([]);
        return;
      }
      
      console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ song song với ${this.numWorkers} CPUs`);
      
      this.taskQueue = [...tasks];
      
      const totalTasks = this.taskQueue.length;
      
      const checkCompletion = () => {
        if (this.results.length >= totalTasks) {
          console.log(`>> Đã hoàn thành ${this.results.length}/${totalTasks} tác vụ`);
          resolve(this.results);
        }
      };
      
      const executionPromise = new Promise(resolve => {
        const scheduleTask = async () => {
          while (this.taskQueue.length > 0 && this.runningTasks < this.maxConcurrentTasks) {
            const task = this.taskQueue.shift();
            this.runningTasks++;
            
            try {
              let proxy = null;
              if (this.proxyManager && task.likeUser) {
                try {
                  proxy = await this.proxyManager.getRandomProxy(task.likeUser.uid);
                } catch (err) {
                  console.warn(`>> Không thể lấy proxy ngẫu nhiên cho user ${task.likeUser.uid}: ${err.message}`);
                  proxy = task.likeUser.proxy;
                }
              }
              
              this.executeTaskInWorker(task, proxy)
                .then(result => {
                  this.results.push(result);
                  this.runningTasks--;
                  
                  if (this.results.length >= totalTasks) {
                    resolve();
                    return;
                  }
                  
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
                  
                  if (this.results.length >= totalTasks) {
                    resolve();
                    return;
                  }
                  
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
        
       
        scheduleTask();
      });
      
     
      await executionPromise;
      
      
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
        const workerPath = path.join(__dirname, 'worker.js');
        
        let proxyToUse = proxy;
        
        if (!proxyToUse && this.proxyManager && task.likeUser) {
          try {
            proxyToUse = this.proxyManager.getRandomProxy(task.likeUser.uid);
          } catch (error) {
            console.warn(`>> Không thể lấy proxy cho user ${task.likeUser.uid}: ${error.message}`);
            proxyToUse = task.likeUser.proxy;
          }
        }
        
       
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
        
       
        const worker = new Worker(workerPath, {
          workerData: {
            ...task,
            proxy: proxyToUse
          }
        });
        
       
        worker.on('message', (message) => {
         
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
        

        worker.on('error', (error) => {
          reject(error);
        });
        
      
        worker.on('exit', (code) => {
          if (code !== 0) {
            reject(new Error(`Worker stopped with exit code ${code}`));
          }
        });
        
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