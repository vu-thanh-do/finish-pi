const CommentClusterManager = require('../../comment-cluster-manager');
const { getArticleId } = require('./getArticleId');
const ExcelReaderService = require('../models/excelSheed');
const path = require('path');

class CommentService {
  constructor() {
    this.commentClusterManager = null;
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

    console.log(`>> CommentService đã được cấu hình với ${this.numWorkers} workers và ${this.concurrentTasksPerWorker} tasks/worker`);
    if (this.proxyManager) {
      console.log(`>> Đã kết nối với ProxyManager (${this.proxyManager.getProxyStats().active} proxy hoạt động)`);
    }
    
    // Khởi tạo CommentClusterManager
    this.commentClusterManager = new CommentClusterManager({
      numWorkers: this.numWorkers,
      concurrentTasksPerWorker: this.concurrentTasksPerWorker,
      proxyManager: this.proxyManager
    });

    console.log(`>> CommentClusterManager đã được khởi tạo thành công`);
  }

  async getArticleIds(count) {
    console.log(`>> Đang lấy ${count} article ID...`);
    try {
      // Lấy danh sách article IDs từ trang chủ
      const articleIds = await getArticleId(count);
      console.log(`>> Đã lấy được ${articleIds.length} article ID`);
      return articleIds;
    } catch (error) {
      console.error(`>> Lỗi khi lấy article IDs: ${error.message}`);
      // Trả về một ID mặc định thay vì throw error
      return ['58203589'];
    }
  }

  async generateCommentTasks(commentCount) {
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
        
        // Tạo đối tượng user cho mỗi dòng
        userList = [];
        for (let i = 0; i < uid.length; i++) {
          if (uid[i] && piname[i] && ukey[i]) {
            const proxyInfo = proxy[i] ? proxy[i].split(':') : null;
            
            userList.push({
              uid: uid[i],
              piname: piname[i],
              ukey: ukey[i],
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
      
      // Lấy các article IDs để comment
      let articleIds = [];
      try {
        articleIds = await this.getArticleIds(commentCount);
        console.log(`>> Đã lấy được ${articleIds.length} article ID`);
      } catch (error) {
        console.error(`>> Lỗi khi lấy article IDs: ${error.message}`);
        // Sử dụng ID mặc định nếu không thể lấy được article IDs
        articleIds = ['58203589'];
        console.log(`>> Sử dụng article ID mặc định: ${articleIds[0]}`);
      }
      
      if (!articleIds || articleIds.length === 0) {
        articleIds = ['58203589'];
        console.log(`>> Không có article ID hợp lệ, sử dụng ID mặc định: ${articleIds[0]}`);
      }
      
      // Tạo một mảng các comment ngẫu nhiên
      const comments = [
        "Rất hay và bổ ích!",
        "Thông tin quá tuyệt vời!",
        "Cảm ơn vì bài viết!",
        "Tôi rất thích nội dung này",
        "Thật sự hữu ích!",
        "Tiếp tục cung cấp những nội dung như vậy!",
        "Rất thú vị để đọc!",
        "Bài viết hay quá!",
        "Tôi học được nhiều điều từ bài này",
        "Các điểm được trình bày rất rõ ràng",
        "Thông tin hữu ích!",
        "Tuyệt vời!"
      ];
      
      // Tạo các task comment
      const tasks = [];
      
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
        
        // Mỗi user sẽ comment trên tất cả các bài viết đã lấy
        for (const articleId of articleIds) {
          // Chọn ngẫu nhiên một comment
          const randomComment = comments[Math.floor(Math.random() * comments.length)];
          
          tasks.push({
            commentUser: user,
            postId: articleId,
            commentText: randomComment
          });
        }
      }
      
      console.log(`>> Đã tạo ${tasks.length} tác vụ comment (${userList.length} users x ${articleIds.length} bài viết)`);
      return tasks;
    } catch (error) {
      console.error(`>> Lỗi khi tạo tác vụ comment: ${error.message}`);
      throw error;
    }
  }

  async executeCommentTasks(tasks) {
    try {
      console.log(`>> Đã tạo ${tasks.length} tác vụ comment`);
      console.log(`>> Bắt đầu thực thi ${tasks.length} tác vụ comment...`);
      
      // Nếu chưa cấu hình, khởi tạo CommentClusterManager với cấu hình mặc định
      if (!this.configured || !this.commentClusterManager) {
        console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ comment song song với ${this.numWorkers} CPUs`);
        this.commentClusterManager = new CommentClusterManager({
          numWorkers: this.numWorkers,
          concurrentTasksPerWorker: this.concurrentTasksPerWorker,
          proxyManager: this.proxyManager
        });
      }
      
      // Thực thi các task và lấy kết quả
      const results = await this.commentClusterManager.executeTasks(tasks);
      
      // Tính toán số lượng thành công và thất bại
      const successCount = results.filter(result => result.success).length;
      const failCount = results.length - successCount;
      
      console.log(`>> Kết quả: ${successCount} comment thành công, ${failCount} comment thất bại`);
      
      // Dọn dẹp tài nguyên
      this.commentClusterManager.cleanup();
      console.log(`>> Đã dọn dẹp tài nguyên CommentClusterManager`);
      
      // Phân tích lỗi nếu có
      if (failCount > 0) {
        const failedResults = results.filter(result => !result.success);
        const errorCounts = {};
        
        failedResults.forEach(result => {
          const errorType = result.error || 'Unknown error';
          errorCounts[errorType] = (errorCounts[errorType] || 0) + 1;
        });
        
        console.log(`>> Lỗi trong quá trình comment:`, Object.keys(errorCounts).length > 0 ? 
          Object.entries(errorCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([error, count]) => `${error}: ${count} lần`)
            .join(', ') 
          : 'Không có thông tin lỗi chi tiết'
        );
      }
      
      return {
        success: successCount,
        failure: failCount,
        total: tasks.length
      };
    } catch (error) {
      console.error(`>> Lỗi khi thực thi tác vụ comment: ${error.message}`);
      if (this.commentClusterManager) {
        this.commentClusterManager.cleanup();
      }
      throw error;
    }
  }

  async startCommentProcess(commentCount) {
    try {
      console.log(`>> Bắt đầu quá trình comment với ${commentCount} comment cho mỗi user`);
      
      // Tạo các tác vụ comment
      const tasks = await this.generateCommentTasks(commentCount);
      
      // Thực thi các tác vụ comment
      const result = await this.executeCommentTasks(tasks);
      
      return {
        success: result.success,
        failure: result.failure,
        total: result.total
      };
    } catch (error) {
      console.error(`>> Lỗi trong quá trình comment: ${error.message}`);
      return {
        success: 0,
        failure: 0,
        total: 0,
        error: error.message
      };
    }
  }
}

// Export một instance của CommentService
module.exports = new CommentService(); 