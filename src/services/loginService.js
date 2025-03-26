const LoginClusterManager = require('../../login-cluster-manager');
const ExcelReaderService = require('../models/excelSheed');
const path = require('path');
const fs = require('fs');

class LoginService {
  constructor() {
    this.loginClusterManager = null;
    this.proxyManager = null;
    this.numWorkers = 4;
    this.concurrentTasksPerWorker = 5;
    this.users = [];
    this.configured = false;
  }

  configureService(config = {}) {
    this.numWorkers = config.numWorkers || this.numWorkers;
    this.concurrentTasksPerWorker = config.concurrentTasksPerWorker || this.concurrentTasksPerWorker;
    this.proxyManager = config.proxyManager || this.proxyManager;
    this.users = config.users || this.users;
    this.configured = true;

    console.log(`>> LoginService đã được cấu hình với ${this.numWorkers} workers và ${this.concurrentTasksPerWorker} tasks/worker`);
    if (this.proxyManager) {
      console.log(`>> Đã kết nối với ProxyManager (${this.proxyManager.getProxyStats().active} proxy hoạt động)`);
    }
    
    // Khởi tạo LoginClusterManager
    this.loginClusterManager = new LoginClusterManager({
      numWorkers: this.numWorkers,
      concurrentTasksPerWorker: this.concurrentTasksPerWorker,
      proxyManager: this.proxyManager
    });

    console.log(`>> LoginClusterManager đã được khởi tạo thành công`);
  }

  async generateLoginTasks(userCount) {
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
      
      // Giới hạn số lượng user theo userCount
      let selectedUsers = userList;
      if (userCount > 0 && userCount < userList.length) {
        // Lấy ngẫu nhiên userCount users
        selectedUsers = this.getRandomUsers(userList, userCount);
        console.log(`>> Đã chọn ngẫu nhiên ${selectedUsers.length} users từ tổng số ${userList.length} users`);
      }
      
      // Tạo các task đăng nhập
      const tasks = [];
      
      for (const user of selectedUsers) {
        // Kiểm tra user có proxy không, nếu không và có proxyManager thì gán proxy
        if (!user.proxy && this.proxyManager) {
          const proxy = this.proxyManager.getProxy();
          if (proxy) {
            user.proxy = proxy;
          } else {
            console.warn(`>> CẢNH BÁO: Không có proxy cho user ${user.uid}!`);
          }
        }
        
        tasks.push({
          user: user
        });
      }
      
      console.log(`>> Đã tạo ${tasks.length} tác vụ đăng nhập`);
      return tasks;
    } catch (error) {
      console.error(`>> Lỗi khi tạo tác vụ đăng nhập: ${error.message}`);
      throw error;
    }
  }

  async executeLoginTasks(tasks) {
    try {
      console.log(`>> Bắt đầu thực thi ${tasks.length} tác vụ đăng nhập...`);
      
      // Nếu chưa cấu hình, khởi tạo LoginClusterManager với cấu hình mặc định
      if (!this.configured || !this.loginClusterManager) {
        console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ đăng nhập song song với ${this.numWorkers} CPUs`);
        this.loginClusterManager = new LoginClusterManager({
          numWorkers: this.numWorkers,
          concurrentTasksPerWorker: this.concurrentTasksPerWorker,
          proxyManager: this.proxyManager
        });
      }
      
      // Thực thi các task và lấy kết quả
      const results = await this.loginClusterManager.executeTasks(tasks);
      
      // Tính toán số lượng thành công và thất bại
      const successCount = results.filter(result => result.success).length;
      const failCount = results.length - successCount;
      
      console.log(`>> Kết quả: ${successCount} đăng nhập thành công, ${failCount} đăng nhập thất bại`);
      
      // Dọn dẹp tài nguyên
      this.loginClusterManager.cleanup();
      console.log(`>> Đã dọn dẹp tài nguyên LoginClusterManager`);
      
      // Phân tích lỗi nếu có
      if (failCount > 0) {
        const failedResults = results.filter(result => !result.success);
        const errorCounts = {};
        
        failedResults.forEach(result => {
          const errorType = result.error || 'Unknown error';
          errorCounts[errorType] = (errorCounts[errorType] || 0) + 1;
        });
        
        console.log(`>> Lỗi trong quá trình đăng nhập:`, Object.keys(errorCounts).length > 0 ? 
          Object.entries(errorCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([error, count]) => `${error}: ${count} lần`)
            .join(', ') 
          : 'Không có thông tin lỗi chi tiết'
        );
      }
      
      // Lưu trạng thái đăng nhập
      this.saveLoginStatus(results);
      
      return {
        success: successCount,
        failure: failCount,
        total: tasks.length,
        loginResults: results.reduce((acc, curr) => {
          acc[curr.userId] = {
            success: curr.success,
            piname: curr.piname,
            error: curr.error
          };
          return acc;
        }, {})
      };
    } catch (error) {
      console.error(`>> Lỗi khi thực thi tác vụ đăng nhập: ${error.message}`);
      if (this.loginClusterManager) {
        this.loginClusterManager.cleanup();
      }
      throw error;
    }
  }

  async startLoginProcess(userCount) {
    try {
      console.log(`>> Bắt đầu quá trình đăng nhập cho ${userCount} tài khoản`);
      
      // Tạo các tác vụ đăng nhập
      const tasks = await this.generateLoginTasks(userCount);
      
      // Thực thi các tác vụ đăng nhập
      const result = await this.executeLoginTasks(tasks);
      
      return {
        success: result.success,
        failure: result.failure,
        total: result.total,
        loginResults: result.loginResults
      };
    } catch (error) {
      console.error(`>> Lỗi trong quá trình đăng nhập: ${error.message}`);
      return {
        success: 0,
        failure: 0,
        total: 0,
        error: error.message
      };
    }
  }

  getRandomUsers(users, n) {
    const shuffled = [...users].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, n);
  }

  saveLoginStatus(results) {
    try {
      // Tạo thư mục logs nếu chưa tồn tại
      const logsDir = path.join(__dirname, '../logs');
      if (!fs.existsSync(logsDir)) {
        fs.mkdirSync(logsDir, { recursive: true });
      }
      
      // Tạo tên file log
      const date = new Date();
      const dateStr = date.toISOString().split('T')[0];
      const loginStatusPath = path.join(logsDir, `login-status-${dateStr}.json`);
      
      // Chuyển đổi kết quả thành định dạng dễ đọc
      const statusData = {
        timestamp: date.toISOString(),
        summary: {
          total: results.length,
          success: results.filter(r => r.success).length,
          failure: results.filter(r => !r.success).length
        },
        users: results.map(r => ({
          userId: r.userId,
          piname: r.piname,
          status: r.success ? 'success' : 'failed',
          error: r.error || null
        }))
      };
      
      // Ghi file
      fs.writeFileSync(loginStatusPath, JSON.stringify(statusData, null, 2), 'utf8');
      console.log(`>> Đã lưu trạng thái đăng nhập vào file: ${loginStatusPath}`);
    } catch (error) {
      console.error(`>> Lỗi khi lưu trạng thái đăng nhập: ${error.message}`);
    }
  }
}

// Export một instance của LoginService
module.exports = new LoginService(); 