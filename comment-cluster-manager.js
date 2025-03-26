const cluster = require('cluster');
const os = require('os');
const { Worker } = require('worker_threads');
const path = require('path');

class CommentClusterManager {
  constructor(options = {}) {
    this.numWorkers = options.numWorkers || os.cpus().length;
    this.concurrentTasksPerWorker = options.concurrentTasksPerWorker || 5;
    this.proxyManager = options.proxyManager || null;
    this.maxConcurrentTasks = this.numWorkers * this.concurrentTasksPerWorker;
    
    this.taskQueue = [];
    this.runningTasks = 0;
    this.workers = [];
    this.results = [];
    
    console.log(`>> [CommentCluster] Khởi tạo với ${this.numWorkers} CPUs, tối đa ${this.maxConcurrentTasks} tác vụ đồng thời`);
  }

  /**
   * Thực thi một danh sách các tác vụ comment
   * @param {Array<Object>} tasks - Danh sách tác vụ comment
   * @returns {Promise<Array>} - Kết quả của tất cả các tác vụ
   */
  async executeTasks(tasks) {
    if (!tasks || tasks.length === 0) {
      console.log(`>> [CommentCluster] Không có tác vụ nào để thực thi`);
      return [];
    }
    
    this.taskQueue = [...tasks];
    this.results = [];
    
    console.log(`>> [CommentCluster] Chuẩn bị thực thi ${tasks.length} tác vụ comment song song với ${this.numWorkers} CPUs`);

    // Kiểm tra proxy thiếu và cảnh báo
    const tasksWithoutProxy = tasks.filter(task => !task.commentUser.proxy);
    if (tasksWithoutProxy.length > 0) {
      const uniqueUsersWithoutProxy = [...new Set(tasksWithoutProxy.map(task => task.commentUser.uid))];
      uniqueUsersWithoutProxy.forEach(uid => {
        console.warn(`>> CẢNH BÁO: Không có proxy cho user ${uid}!`);
      });
    }
    
    // Bắt đầu lên lịch tác vụ
    await this.scheduleNext();
    
    return this.results;
  }

  /**
   * Lên lịch và thực thi các tác vụ tiếp theo
   */
  async scheduleNext() {
    return new Promise((resolve) => {
      // Nếu không còn tác vụ nào, kết thúc
      if (this.taskQueue.length === 0 && this.runningTasks === 0) {
        console.log(`>> [CommentCluster] Đã hoàn thành tất cả tác vụ`);
        resolve(this.results);
        return;
      }
      
      // Thực thi số lượng tác vụ đồng thời tối đa
      while (this.taskQueue.length > 0 && this.runningTasks < this.maxConcurrentTasks) {
        const task = this.taskQueue.shift();
        this.executeTaskInWorker(task)
          .then(result => {
            this.results.push(result);
            this.runningTasks--;
            
            // Báo cáo tiến độ sau mỗi 5 tác vụ hoàn thành
            if (this.results.length % 5 === 0 || this.results.length === 1 || this.taskQueue.length === 0) {
              const successCount = this.results.filter(r => r.success).length;
              const failCount = this.results.length - successCount;
              console.log(`>> [CommentCluster] Đã hoàn thành ${this.results.length}/${this.results.length + this.taskQueue.length} tác vụ (${successCount} thành công, ${failCount} thất bại)`);
            }
            
            // Lên lịch tác vụ tiếp theo
            if (this.taskQueue.length > 0 || this.runningTasks > 0) {
              setImmediate(() => this.scheduleNext());
            } else {
              console.log(`>> [CommentCluster] Đã hoàn thành ${this.results.length} tác vụ tổng cộng`);
              resolve(this.results);
            }
          })
          .catch(error => {
            console.error(`>> [CommentCluster] Lỗi khi thực thi tác vụ:`, error);
            this.results.push({
              success: false,
              error: error.message || 'Unknown error'
            });
            this.runningTasks--;
            
            // Lên lịch tác vụ tiếp theo
            if (this.taskQueue.length > 0 || this.runningTasks > 0) {
              setImmediate(() => this.scheduleNext());
            } else {
              resolve(this.results);
            }
          });
        
        this.runningTasks++;
      }
      
      // Nếu đã đạt đến số lượng tác vụ đồng thời tối đa, đợi tác vụ khác hoàn thành
      if (this.taskQueue.length > 0 && this.runningTasks >= this.maxConcurrentTasks) {
        setTimeout(() => this.scheduleNext().then(resolve), 100);
      } else if (this.runningTasks > 0) {
        // Đợi tất cả tác vụ đang chạy hoàn thành
        setTimeout(() => this.scheduleNext().then(resolve), 100);
      } else {
        resolve(this.results);
      }
    });
  }

  /**
   * Thực thi một tác vụ trong worker thread
   * @param {Object} task - Tác vụ cần thực thi
   * @returns {Promise<Object>} - Kết quả của tác vụ
   */
  executeTaskInWorker(task) {
    return new Promise((resolve, reject) => {
      try {
        // Xác định đường dẫn của worker
        const workerPath = path.resolve(__dirname, 'comment-worker.js');
        
        // Kiểm tra và cập nhật proxy nếu cần
        if (this.proxyManager && (!task.commentUser.proxy || !task.commentUser.proxy.host)) {
          const proxy = this.proxyManager.getProxy();
          if (proxy) {
            task.commentUser.proxy = proxy;
          }
        }
        
        // Tạo worker thread
        const worker = new Worker(workerPath, {
          workerData: {
            commentUser: task.commentUser,
            postId: task.postId,
            proxy: task.commentUser.proxy,
            commentText: task.commentText
          }
        });
        
        // Theo dõi các sự kiện của worker
        worker.on('message', (message) => {
          if (message.type === 'log' || message.type === 'error') {
            // Chỉ ghi log, không làm gì thêm
          } else {
            // Đánh dấu proxy bị lỗi nếu có vấn đề
            if (!message.success && this.proxyManager && task.commentUser.proxy) {
              if (message.error && (
                message.error.includes('ECONNREFUSED') ||
                message.error.includes('ETIMEDOUT') ||
                message.error.includes('socket hang up') ||
                message.error.includes('tunneling socket') ||
                message.error.includes('407')
              )) {
                this.proxyManager.markProxyAsInactive(task.commentUser.proxy);
              }
            }
            
            // Đánh dấu kết thúc tác vụ
            worker.terminate();
            resolve(message);
          }
        });
        
        worker.on('error', (err) => {
          console.error(`>> [CommentCluster] Lỗi worker:`, err);
          worker.terminate();
          reject(err);
        });
        
        worker.on('exit', (code) => {
          if (code !== 0) {
            console.error(`>> [CommentCluster] Worker exited with code ${code}`);
            reject(new Error(`Worker exited with code ${code}`));
          }
        });
        
        // Thêm worker vào danh sách để theo dõi
        this.workers.push(worker);
      } catch (error) {
        console.error(`>> [CommentCluster] Lỗi khi tạo worker:`, error);
        reject(error);
      }
    });
  }

  /**
   * Lấy thống kê hiện tại của cluster
   * @returns {Object} - Thống kê cluster
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
      workers: this.workers.length
    };
  }

  /**
   * Dọn dẹp tài nguyên
   */
  cleanup() {
    // Kết thúc tất cả worker đang chạy
    for (const worker of this.workers) {
      try {
        worker.terminate();
      } catch (error) {
        // Bỏ qua lỗi
      }
    }
    
    this.workers = [];
    this.taskQueue = [];
    this.runningTasks = 0;
    
    console.log(`>> [CommentCluster] Đã dọn dẹp tài nguyên`);
  }
}

module.exports = CommentClusterManager; 