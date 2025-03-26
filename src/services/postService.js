const PostClusterManager = require('../../post-cluster-manager');
const ExcelReaderService = require('../models/excelSheed');
const path = require('path');
const fs = require('fs');
const getImageUrl = require('./serviceGetImage');

// Thêm cache cho URL ảnh
const imageCache = {
  urls: [],
  maxSize: 10,
  lastIndex: 0,
  
  // Thêm URL ảnh vào cache
  addUrl(url) {
    if (!this.urls.includes(url)) {
      if (this.urls.length >= this.maxSize) {
        this.urls.shift(); // Xóa URL cũ nhất
      }
      this.urls.push(url);
    }
  },
  
  // Lấy URL ảnh từ cache
  getUrl() {
    if (this.urls.length === 0) {
      return null;
    }
    
    // Quay vòng qua danh sách ảnh
    this.lastIndex = (this.lastIndex + 1) % this.urls.length;
    return this.urls[this.lastIndex];
  },
  
  // Kiểm tra cache có ảnh không
  hasUrls() {
    return this.urls.length > 0;
  }
};

// Các hàm công cụ từ posts.js
function splitIntoWords(text) {
  return text.split(/\s+/).filter(word => word.length > 0);
}

function splitIntoPhrases(text) {
  return text.split(/[,.!?;]/)
    .map(chunk => chunk.trim())
    .filter(chunk => chunk.length > 0);
}

function getRandomElement(array) {
  return array[Math.floor(Math.random() * array.length)];
}

function generateMixedContent(sourceTexts, minParts = 2, maxParts = 4) {
  const wordPool = sourceTexts.reduce((acc, text) => {
    if (text) {
      acc.push(...splitIntoWords(text));
    }
    return acc;
  }, []);

  const phrasePool = sourceTexts.reduce((acc, text) => {
    if (text) {
      acc.push(...splitIntoPhrases(text));
    }
    return acc;
  }, []);

  const mixingStyle = Math.floor(Math.random() * 4);
  const parts = [];
  const numParts = Math.floor(Math.random() * (maxParts - minParts + 1)) + minParts;

  switch (mixingStyle) {
    case 0: 
      for (let i = 0; i < numParts; i++) {
        parts.push(getRandomElement(phrasePool));
      }
      return parts.join(', ');

    case 1: 
      for (let i = 0; i < numParts + 2; i++) {
        parts.push(getRandomElement(wordPool));
      }
      return parts.join(' ');

    case 2: 
      for (let i = 0; i < numParts; i++) {
        if (Math.random() > 0.5) {
          parts.push(getRandomElement(phrasePool));
        } else {
          const numWords = Math.floor(Math.random() * 3) + 1;
          const words = [];
          for (let j = 0; j < numWords; j++) {
            words.push(getRandomElement(wordPool));
          }
          parts.push(words.join(' '));
        }
      }
      return parts.join(', ');

    case 3: 
      const mainPhrase = getRandomElement(phrasePool);
      const words = [];
      for (let i = 0; i < 2; i++) {
        words.push(getRandomElement(wordPool));
      }
      return `${mainPhrase} ${words.join(' ')}`;
  }
}

function generateUniqueTitle(titles) {
  const title = generateMixedContent(titles, 2, 3);
  return `${title}`;
}

function generateUniqueContent(contents) {
  return generateMixedContent(contents, 3, 5);
}

class PostService {
  constructor() {
    this.postClusterManager = null;
    this.proxyManager = null;
    this.numWorkers = 4;
    this.concurrentTasksPerWorker = 5;
    this.users = [];
    this.configured = false;
    this.titles = [];
    this.contents = [];
    this.defaultImageUrl = 'https://asset.vcity.app/vfile/2024/11/25/01/1732528133865582447460541631585-thumb.jpg';
  }

  configureService(config = {}) {
    this.numWorkers = config.numWorkers || this.numWorkers;
    this.concurrentTasksPerWorker = config.concurrentTasksPerWorker || this.concurrentTasksPerWorker;
    this.proxyManager = config.proxyManager || this.proxyManager;
    this.users = config.users || this.users;
    this.titles = config.titles || this.titles;
    this.contents = config.contents || this.contents;
    this.configured = true;

    console.log(`>> PostService đã được cấu hình với ${this.numWorkers} workers và ${this.concurrentTasksPerWorker} tasks/worker`);
    if (this.proxyManager) {
      console.log(`>> Đã kết nối với ProxyManager (${this.proxyManager.getProxyStats().active} proxy hoạt động)`);
    }
    
    // Khởi tạo PostClusterManager
    this.postClusterManager = new PostClusterManager({
      numWorkers: this.numWorkers,
      concurrentTasksPerWorker: this.concurrentTasksPerWorker,
      proxyManager: this.proxyManager
    });

    console.log(`>> PostClusterManager đã được khởi tạo thành công`);
  }

  async generatePostTasks(postsPerUser) {
    try {
      let userList = this.users;
      let titleList = this.titles;
      let contentList = this.contents;
      
      // Nếu không có dữ liệu từ cấu hình, đọc từ Excel
      if (!userList || userList.length === 0 || !titleList || titleList.length === 0 || !contentList || contentList.length === 0) {
        // Đọc dữ liệu từ file Excel
        const excelPath = path.join(__dirname, '../data/PI.xlsx');
        console.log(`>> Đã tìm thấy file Excel tại: ${excelPath}`);
        const excelReader = new ExcelReaderService(excelPath);
        const excelData = excelReader.readAllSheets();
        
        // Đọc danh sách user
        if (!userList || userList.length === 0) {
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
          this.users = userList;
        }
        
        // Đọc tiêu đề và nội dung
        if (!titleList || titleList.length === 0) {
          titleList = excelData["title"]?.["titles"] || [];
          this.titles = titleList;
        }
        
        if (!contentList || contentList.length === 0) {
          contentList = excelData["title"]?.["contents"] || [];
          this.contents = contentList;
        }
      }
      
      console.log(`>> Tìm thấy ${userList.length} users, ${titleList.length} tiêu đề, ${contentList.length} nội dung`);
      
      // Tạo các task đăng bài
      const tasks = [];
      
      // Lấy ảnh trước để tránh lỗi 429
      console.log(`>> Lấy ảnh cho tất cả bài viết...`);
      
      // Kiểm tra cache ảnh
      if (!imageCache.hasUrls()) {
        // Lấy 3-5 ảnh để sử dụng luân phiên
        const maxImageToGet = 5;
        
        console.log(`>> Cache ảnh trống, lấy ${maxImageToGet} ảnh mới...`);
        let imageCount = 0;
        let failCount = 0;
        
        while (imageCount < maxImageToGet && failCount < 5) {
          try {
            // Chờ 1 giây giữa các lần lấy ảnh để tránh 429
            if (imageCount > 0) {
              await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            const imageUrl = await getImageUrl();
            imageCache.addUrl(imageUrl);
            console.log(`>> Đã lấy và cache ảnh thứ ${imageCount + 1}: ${imageUrl}`);
            imageCount++;
          } catch (error) {
            console.error(`❌ Lỗi khi lấy ảnh: ${error.message}`);
            failCount++;
            // Đợi lâu hơn nếu gặp lỗi 429
            if (error.message.includes('429')) {
              console.log(`>> Lỗi 429, đợi 5 giây trước khi thử lại...`);
              await new Promise(resolve => setTimeout(resolve, 5000));
            }
          }
        }
        
        // Nếu không lấy được ảnh nào, thêm ảnh mặc định
        if (!imageCache.hasUrls()) {
          imageCache.addUrl(this.defaultImageUrl);
          console.log(`>> Thêm ảnh mặc định vào cache: ${this.defaultImageUrl}`);
        }
      }
      
      console.log(`>> Hiện có ${imageCache.urls.length} ảnh trong cache: ${imageCache.urls.join(', ')}`);
      
      // Với mỗi user, tạo số lượng bài viết theo yêu cầu
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
        
        // Tạo task cho mỗi bài viết của user
        for (let i = 0; i < postsPerUser; i++) {
          // Tạo tiêu đề và nội dung ngẫu nhiên
          const uniqueTitle = generateUniqueTitle(titleList);
          const uniqueContent = generateUniqueContent(contentList);
          
          // Lấy URL ảnh từ cache thay vì gọi API mới
          const imageUrl = imageCache.getUrl();
          console.log(`>> Sử dụng ảnh từ cache cho user ${user.piname}, bài viết ${i+1}: ${imageUrl}`);
          
          tasks.push({
            user: user,
            title: uniqueTitle,
            content: uniqueContent,
            imageUrl: imageUrl
          });
        }
      }
      
      console.log(`>> Đã tạo ${tasks.length} tác vụ đăng bài cho ${userList.length} users (${postsPerUser} bài/user)`);
      return tasks;
    } catch (error) {
      console.error(`>> Lỗi khi tạo tác vụ đăng bài: ${error.message}`);
      throw error;
    }
  }

  async executePostTasks(tasks) {
    try {
      console.log(`>> Bắt đầu thực thi ${tasks.length} tác vụ đăng bài...`);
      
      // Nếu chưa cấu hình, khởi tạo PostClusterManager với cấu hình mặc định
      if (!this.configured || !this.postClusterManager) {
        console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ đăng bài song song với ${this.numWorkers} CPUs`);
        this.postClusterManager = new PostClusterManager({
          numWorkers: this.numWorkers,
          concurrentTasksPerWorker: this.concurrentTasksPerWorker,
          proxyManager: this.proxyManager
        });
      }
      
      // Thực thi các task và lấy kết quả
      const results = await this.postClusterManager.executeTasks(tasks);
      
      // Tính toán số lượng thành công và thất bại
      const successCount = results.filter(result => result.success).length;
      const failCount = results.length - successCount;
      
      console.log(`>> Kết quả: ${successCount} bài đăng thành công, ${failCount} bài đăng thất bại`);
      
      // Dọn dẹp tài nguyên
      this.postClusterManager.cleanup();
      console.log(`>> Đã dọn dẹp tài nguyên PostClusterManager`);
      
      // Phân tích lỗi nếu có
      if (failCount > 0) {
        const failedResults = results.filter(result => !result.success);
        const errorCounts = {};
        
        failedResults.forEach(result => {
          const errorType = result.error || 'Unknown error';
          errorCounts[errorType] = (errorCounts[errorType] || 0) + 1;
        });
        
        console.log(`>> Lỗi trong quá trình đăng bài:`, Object.keys(errorCounts).length > 0 ? 
          Object.entries(errorCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([error, count]) => `${error}: ${count} lần`)
            .join(', ') 
          : 'Không có thông tin lỗi chi tiết'
        );
      }
      
      // Lưu trạng thái đăng bài
      this.savePostStatus(results);
      
      return {
        success: successCount,
        failure: failCount,
        total: tasks.length
      };
    } catch (error) {
      console.error(`>> Lỗi khi thực thi tác vụ đăng bài: ${error.message}`);
      if (this.postClusterManager) {
        this.postClusterManager.cleanup();
      }
      throw error;
    }
  }

  async startPostProcess(postsPerUser) {
    try {
      console.log(`>> Bắt đầu quá trình đăng bài với ${postsPerUser} bài viết/user`);
      
      // Tạo các tác vụ đăng bài
      const tasks = await this.generatePostTasks(postsPerUser);
      
      // Thực thi các tác vụ đăng bài
      const result = await this.executePostTasks(tasks);
      
      // Thử lại các tác vụ thất bại nếu có
      if (this.postClusterManager && this.postClusterManager.failedTasks.length > 0) {
        const failedTasks = [...this.postClusterManager.failedTasks];
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
          const retryResults = await this.executePostTasks(retriableFailures);
          
          // Cập nhật kết quả
          result.success += retryResults.success;
          result.failure = (result.failure - retriableFailures.length) + retryResults.failure;
          
          console.log(`>> Kết quả sau khi thử lại: ${result.success} thành công, ${result.failure} thất bại`);
        }
      }
      
      return result;
    } catch (error) {
      console.error(`>> Lỗi trong quá trình đăng bài: ${error.message}`);
      return {
        success: 0,
        failure: 0,
        total: 0,
        error: error.message
      };
    }
  }

  savePostStatus(results) {
    try {
      // Tạo thư mục logs nếu chưa tồn tại
      const logsDir = path.join(__dirname, '../logs');
      if (!fs.existsSync(logsDir)) {
        fs.mkdirSync(logsDir, { recursive: true });
      }
      
      // Tạo tên file log
      const date = new Date();
      const dateStr = date.toISOString().split('T')[0];
      const postStatusPath = path.join(logsDir, `post-status-${dateStr}.json`);
      
      // Chuyển đổi kết quả thành định dạng dễ đọc
      const statusData = {
        timestamp: date.toISOString(),
        summary: {
          total: results.length,
          success: results.filter(r => r.success).length,
          failure: results.filter(r => !r.success).length
        },
        posts: results.map(r => ({
          userId: r.userId,
          title: r.title ? (r.title.length > 50 ? r.title.substring(0, 50) + '...' : r.title) : null,
          status: r.success ? 'success' : 'failed',
          message: r.message || null,
          error: r.error || null
        }))
      };
      
      // Ghi file
      fs.writeFileSync(postStatusPath, JSON.stringify(statusData, null, 2), 'utf8');
      console.log(`>> Đã lưu trạng thái đăng bài vào file: ${postStatusPath}`);
    } catch (error) {
      console.error(`>> Lỗi khi lưu trạng thái đăng bài: ${error.message}`);
    }
  }
}

// Export một instance của PostService
module.exports = new PostService(); 