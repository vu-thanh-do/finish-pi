const LikeClusterManager = require('../../like-cluster-manager');
const ExcelReaderService = require('../models/excelSheed');
const path = require('path');
const fs = require('fs');
const getArticleId = require('./getArticleId');

class LikeService {
  constructor() {
    this.likeClusterManager = null;
    this.proxyManager = null;
    this.numWorkers = 4;
    this.concurrentTasksPerWorker = 5;
    this.users = [];
    this.configured = false;
    this.articleIds = [];
  }

  configureService(config = {}) {
    this.numWorkers = config.numWorkers || this.numWorkers;
    this.concurrentTasksPerWorker = config.concurrentTasksPerWorker || this.concurrentTasksPerWorker;
    this.proxyManager = config.proxyManager || this.proxyManager;
    this.users = config.users || this.users;
    this.configured = true;

    console.log(`>> LikeService đã được cấu hình với ${this.numWorkers} workers và ${this.concurrentTasksPerWorker} tasks/worker`);
    if (this.proxyManager) {
      console.log(`>> Đã kết nối với ProxyManager (${this.proxyManager.getProxyStats().active} proxy hoạt động)`);
    }
    
    // Khởi tạo LikeClusterManager
    this.likeClusterManager = new LikeClusterManager({
      numWorkers: this.numWorkers,
      concurrentTasksPerWorker: this.concurrentTasksPerWorker,
      proxyManager: this.proxyManager
    });

    console.log(`>> LikeClusterManager đã được khởi tạo thành công`);
  }

  async getArticleIds(count = 10) {
    console.log(`>> Đang lấy ${count} article ID...`);
    try {
      // Lấy danh sách article IDs từ trang chủ
      const articleIds = await getArticleId(count);
      console.log(`>> Đã lấy được ${articleIds.length} article ID`);
      
      // Lưu cache để tái sử dụng
      this.articleIds = articleIds;
      
      return articleIds;
    } catch (error) {
      console.error(`>> Lỗi khi lấy article IDs: ${error.message}`);
      
      if (this.articleIds.length > 0) {
        console.log(`>> Sử dụng ${this.articleIds.length} article ID đã cache trước đó`);
        return this.articleIds;
      }
      
      // Trả về một ID mặc định nếu không có cache và gặp lỗi
      console.log(`>> Sử dụng article ID mặc định`);
      return ['58203589'];
    }
  }

  async generateLikeTasks(likesPerUser) {
    try {
      let userList = this.users;
      
      // Nếu không có users từ cấu hình, đọc từ Excel
      if (!userList || userList.length === 0) {
        // Đọc dữ liệu từ file Excel
        const excelPath = path.join(__dirname, '../data/PI.xlsx');
        console.log(`>> Đã tìm thấy file Excel tại: ${excelPath}`);
        const excelReader = new ExcelReaderService(excelPath);
        const excelData = excelReader.readAllSheets();
        
        // Lấy dữ liệu của các cột cần thiết
        const uid = excelData["prxageng"]?.["uid"] || [];
        const piname = excelData["prxageng"]?.["piname"] || [];
        const ukey = excelData["prxageng"]?.["ukey"] || [];
        const proxy = excelData["prxageng"]?.["proxy"] || [];
        const userAgent = excelData["prxageng"]?.["user_agent"] || [];
        
        // Tạo đối tượng user cho mỗi dòng
        userList = [];
        for (let i = 0; i < uid.length; i++) {
          if (uid[i] && piname[i] && ukey[i]) {
            const proxyInfo = proxy[i] ? proxy[i].split(':') : null;
            
            userList.push({
              uid: uid[i],
              piname: piname[i],
              ukey: ukey[i],
              userAgent: userAgent[i] || null,
              proxy: proxyInfo ? {
                host: proxyInfo[0],
                port: proxyInfo[1],
                name: proxyInfo[2],
                password: proxyInfo[3]
              } : null
            });
          }
        }
      }
      
      console.log(`>> Tìm thấy ${userList.length} users từ file Excel`);
      
      // Lấy các article IDs để like
      let articleIds = [];
      try {
        // Lấy số lượng article ID gấp đôi để đảm bảo đủ
        const targetArticleCount = Math.min(Math.max(likesPerUser * 2, 10), 50);
        articleIds = await this.getArticleIds(targetArticleCount);
        
        if (!articleIds || articleIds.length === 0) {
          throw new Error("Không thể lấy được article ID");
        }
      } catch (error) {
        console.error(`>> Lỗi khi lấy article IDs: ${error.message}`);
        // Sử dụng ID mặc định
        articleIds = ['58203589'];
        console.log(`>> Sử dụng article ID mặc định: ${articleIds[0]}`);
      }
      
      // Tạo các task like
      const tasks = [];
      
      // Với mỗi user, tạo số lượng like theo yêu cầu
      for (const user of userList) {
        // Kiểm tra user có proxy không, nếu không và có proxyManager thì gán proxy
        if (!user.proxy && this.proxyManager) {
          const proxy = this.proxyManager.getProxy();
          if (proxy) {
            user.proxy = proxy;
          } else {
            console.warn(`>> CẢNH BÁO: Không có proxy cho user ${user.uid}!`);
          }
        }
        
        // Tạo task cho mỗi lượt like của user
        for (let i = 0; i < likesPerUser; i++) {
          // Chọn ngẫu nhiên một articleId nếu có nhiều
          const articleId = articleIds.length > 1 ? 
            articleIds[Math.floor(Math.random() * articleIds.length)] : 
            articleIds[0];
          
          tasks.push({
            user: user,
            articleId: articleId
          });
        }
      }
      
      console.log(`>> Đã tạo ${tasks.length} tác vụ like cho ${userList.length} users (${likesPerUser} likes/user)`);
      return tasks;
    } catch (error) {
      console.error(`>> Lỗi khi tạo tác vụ like: ${error.message}`);
      throw error;
    }
  }

  async executeLikeTasks(tasks) {
    try {
      console.log(`>> Bắt đầu thực thi ${tasks.length} tác vụ like...`);
      
      // Nếu chưa cấu hình, khởi tạo LikeClusterManager với cấu hình mặc định
      if (!this.configured || !this.likeClusterManager) {
        console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ like song song với ${this.numWorkers} CPUs`);
        this.likeClusterManager = new LikeClusterManager({
          numWorkers: this.numWorkers,
          concurrentTasksPerWorker: this.concurrentTasksPerWorker,
          proxyManager: this.proxyManager
        });
      }
      
      // Thực thi các task và lấy kết quả
      const results = await this.likeClusterManager.executeTasks(tasks);
      
      // Tính toán số lượng thành công và thất bại
      const successCount = results.filter(result => result.success).length;
      const failCount = results.length - successCount;
      
      console.log(`>> Kết quả: ${successCount} like thành công, ${failCount} like thất bại`);
      
      // Dọn dẹp tài nguyên
      this.likeClusterManager.cleanup();
      console.log(`>> Đã dọn dẹp tài nguyên LikeClusterManager`);
      
      // Phân tích lỗi nếu có
      if (failCount > 0) {
        const failedResults = results.filter(result => !result.success);
        const errorCounts = {};
        
        failedResults.forEach(result => {
          const errorType = result.error || 'Unknown error';
          errorCounts[errorType] = (errorCounts[errorType] || 0) + 1;
        });
        
        console.log(`>> Lỗi trong quá trình like:`, Object.keys(errorCounts).length > 0 ? 
          Object.entries(errorCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([error, count]) => `${error}: ${count} lần`)
            .join(', ') 
          : 'Không có thông tin lỗi chi tiết'
        );
      }
      
      // Lưu trạng thái like
      this.saveLikeStatus(results);
      
      return {
        success: successCount,
        failure: failCount,
        total: tasks.length,
        uniqueArticles: [...new Set(results.map(r => r.articleId))].length
      };
    } catch (error) {
      console.error(`>> Lỗi khi thực thi tác vụ like: ${error.message}`);
      if (this.likeClusterManager) {
        this.likeClusterManager.cleanup();
      }
      throw error;
    }
  }

  async startLikeProcess(likesPerUser) {
    try {
      console.log(`>> Bắt đầu quá trình like với ${likesPerUser} lượt like mỗi user`);
      
      // Tạo các tác vụ like
      const tasks = await this.generateLikeTasks(likesPerUser);
      
      // Thực thi các tác vụ like
      const result = await this.executeLikeTasks(tasks);
      
      // Thử lại các tác vụ thất bại nếu có
      if (this.likeClusterManager && this.likeClusterManager.failedTasks.length > 0) {
        const failedTasks = [...this.likeClusterManager.failedTasks];
        console.log(`>> Thử lại ${failedTasks.length} tác vụ thất bại...`);
        
        const retriableFailures = failedTasks.filter(task => {
          if (!task.error) return false;
          return task.error.includes('ECONNREFUSED') || 
                task.error.includes('ETIMEDOUT') || 
                task.error.includes('socket hang up') || 
                task.error.includes('tunneling socket') ||
                task.error.includes('429') || 
                task.error.includes('500') || 
                task.error.includes('502') || 
                task.error.includes('503') || 
                task.error.includes('504');
        });
        
        if (retriableFailures.length > 0) {
          console.log(`>> Thử lại ${retriableFailures.length} tác vụ lỗi mạng/proxy...`);
          
          // Thực thi lại các tác vụ thất bại
          const retryResults = await this.executeLikeTasks(retriableFailures);
          
          // Cập nhật kết quả
          result.success += retryResults.success;
          result.failure = (result.failure - retriableFailures.length) + retryResults.failure;
          
          console.log(`>> Kết quả sau khi thử lại: ${result.success} thành công, ${result.failure} thất bại`);
        }
      }
      
      return {
        success: result.success,
        failure: result.failure,
        total: result.total,
        uniqueArticles: result.uniqueArticles
      };
    } catch (error) {
      console.error(`>> Lỗi trong quá trình like: ${error.message}`);
      return {
        success: 0,
        failure: 0,
        total: 0,
        uniqueArticles: 0,
        error: error.message
      };
    }
  }

  saveLikeStatus(results) {
    try {
      // Tạo thư mục logs nếu chưa tồn tại
      const logsDir = path.join(__dirname, '../logs');
      if (!fs.existsSync(logsDir)) {
        fs.mkdirSync(logsDir, { recursive: true });
      }
      
      // Tạo tên file log
      const date = new Date();
      const dateStr = date.toISOString().split('T')[0];
      const likeStatusPath = path.join(logsDir, `like-status-${dateStr}.json`);
      
      // Chuyển đổi kết quả thành định dạng dễ đọc
      const statusData = {
        timestamp: date.toISOString(),
        summary: {
          total: results.length,
          success: results.filter(r => r.success).length,
          failure: results.filter(r => !r.success).length,
          uniqueArticles: [...new Set(results.map(r => r.articleId))].length
        },
        likes: results.map(r => ({
          userId: r.userId,
          articleId: r.articleId,
          status: r.success ? 'success' : 'failed',
          message: r.message || null,
          error: r.error || null
        }))
      };
      
      // Ghi file
      fs.writeFileSync(likeStatusPath, JSON.stringify(statusData, null, 2), 'utf8');
      console.log(`>> Đã lưu trạng thái like vào file: ${likeStatusPath}`);
    } catch (error) {
      console.error(`>> Lỗi khi lưu trạng thái like: ${error.message}`);
    }
  }
}

// Export một instance của LikeService
module.exports = new LikeService(); 