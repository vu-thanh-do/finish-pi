// Tạo service mới BatchFetcher.js để lấy posts hiệu quả
class BatchFetcher {
  constructor(postsCache, proxyManager) {
    this.postsCache = postsCache;
    this.proxyManager = proxyManager;
    this.requestsPerSecond = 2; // Giảm xuống 2 request/s để tránh lỗi 429
    this.batchSize = 10; // Giảm kích thước batch để tránh quá tải
    this.batchDelay = 5000; // Tăng thời gian delay giữa các batch lên 5 giây
    this.maxTotalConcurrent = 5; // Giới hạn tổng số request đồng thời
    this.activeRequests = 0; // Đếm số request đang hoạt động
  }
  
  async fetchAllPostsInBatches(userList) {
    console.log(`>> Bắt đầu lấy posts cho ${userList.length} user theo batch...`);
    console.log(`>> Chiến lược: Giới hạn ${this.requestsPerSecond} request/giây, mỗi batch ${this.batchSize} user`);
    
    // Tạo các batch
    const batches = [];
    for (let i = 0; i < userList.length; i += this.batchSize) {
      batches.push(userList.slice(i, i + this.batchSize));
    }
    
    console.log(`>> Chia thành ${batches.length} batch, mỗi batch ${this.batchSize} user`);
    
    // Bắt đầu xử lý từng batch
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`>> Xử lý batch ${i+1}/${batches.length} (${batch.length} users)`);
      
      // Xử lý batch với số lượng giới hạn
      await this.processBatchWithRateLimit(batch);
      
      // Đợi giữa các batch để tránh quá tải
      if (i < batches.length - 1) {
        console.log(`>> Đợi ${this.batchDelay/1000}s trước khi xử lý batch tiếp theo...`);
        await new Promise(resolve => setTimeout(resolve, this.batchDelay));
      }
    }
    
    console.log(`>> Đã hoàn thành việc lấy posts cho ${userList.length} user`);
  }
  
  async processBatchWithRateLimit(batch) {
    // Khởi tạo mảng để lưu trữ promise khi hoàn thành
    const completedPromises = [];
    
    // Sử dụng hàng đợi thay vì Promise.all
    for (let i = 0; i < batch.length; i++) {
      const user = batch[i];
      
      // Đợi nếu đã đạt số lượng request đồng thời tối đa
      while (this.activeRequests >= this.maxTotalConcurrent) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      
      // Tính thời gian trễ để không vượt quá requestsPerSecond
      const delay = Math.floor(i / this.requestsPerSecond) * 1000;
      
      // Thực hiện request với rate limiting
      const promise = this.fetchWithDelay(user, delay);
      completedPromises.push(promise);
      
      // Đợi một chút sau mỗi request để tránh xử lý song song quá nhiều
      await new Promise(resolve => setTimeout(resolve, 300));
    }
    
    // Đợi tất cả các request trong batch hoàn thành
    await Promise.all(completedPromises);
  }
  
  async fetchWithDelay(user, delay) {
    try {
      this.activeRequests++;
      
      if (delay > 0) {
        await new Promise(resolve => setTimeout(resolve, delay));
      }

      // Thêm jitter ngẫu nhiên để làm cho request trông tự nhiên hơn
      const jitter = Math.floor(Math.random() * 500);
      await new Promise(resolve => setTimeout(resolve, jitter));
      
      // Sử dụng chính user để lấy bài viết của họ (mỗi người lấy bài viết của chính mình)
      return await this.postsCache.fetchUserPosts(user, 1);
    } catch (error) {
      console.error(`>> Lỗi khi lấy posts cho user ${user.uid}: ${error.message}`);
      return { userId: user.uid, postsCount: 0, posts: [] };
    } finally {
      this.activeRequests--;
    }
  }

  /**
   * Kiểm tra và thống kê các user không có bài viết
   * @param {Array} userIds - Danh sách ID user cần kiểm tra
   * @returns {Object} - Thống kê kết quả
   */
  checkPostsAvailability(userIds) {
    const result = {
      total: userIds.length,
      available: 0,
      missing: 0,
      usersWithPosts: [],
      usersWithoutPosts: []
    };

    for (const userId of userIds) {
      const hasPost = this.postsCache.hasPostsForUser(userId);
      if (hasPost) {
        result.available++;
        result.usersWithPosts.push(userId);
      } else {
        result.missing++;
        result.usersWithoutPosts.push(userId);
      }
    }

    console.log(`>> Kiểm tra bài viết: ${result.available}/${result.total} user có bài viết`);
    if (result.missing > 0) {
      console.log(`>> CẢNH BÁO: ${result.missing} user không có bài viết!`);
    }

    return result;
  }

  /**
   * Thử lấy lại bài viết cho các user chưa có
   * @param {Array} userIds - Danh sách ID user cần lấy lại
   * @param {Array} userObjects - Danh sách thông tin user
   */
  async retryMissingPosts(userIds, userObjects) {
    if (userIds.length === 0) {
      console.log('>> Không có user nào cần lấy lại bài viết');
      return;
    }

    console.log(`>> Thử lấy lại bài viết cho ${userIds.length} user...`);
    
    // Tìm thông tin đầy đủ của user từ userIds, ưu tiên thông tin từ userObjects
    const usersToRetry = [];
    for (const userId of userIds) {
      // Tìm user có thông tin đầy đủ trong userObjects
      const fullUser = userObjects.find(u => u.uid === userId);
      
      if (fullUser) {
        // Sử dụng thông tin đầy đủ nếu có (uid, ukey, piname, proxy)
        usersToRetry.push(fullUser);
      } else {
        // Nếu không có thông tin đầy đủ, chỉ dùng uid
        usersToRetry.push({ uid: userId });
      }
    }

    if (usersToRetry.length === 0) {
      console.log('>> Không tìm thấy thông tin user để lấy lại bài viết');
      return;
    }

    // Sử dụng batchSize nhỏ hơn cho việc retry để tăng khả năng thành công
    const smallBatchSize = 5; // Giảm kích thước batch khi retry
    const batches = [];
    for (let i = 0; i < usersToRetry.length; i += smallBatchSize) {
      batches.push(usersToRetry.slice(i, i + smallBatchSize));
    }

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`>> Lấy lại batch ${i+1}/${batches.length} (${batch.length} users)`);
      
      // Xử lý batch tuần tự và chậm để tránh bị chặn
      for (let j = 0; j < batch.length; j++) {
        const user = batch[j];
        await this.retryWithDelay(user, 1000); // Đợi 1 giây giữa các request
        
        // Đợi thêm giữa các user để tránh bị phát hiện là bot
        await new Promise(resolve => setTimeout(resolve, 1500));
      }
      
      // Đợi giữa các batch
      if (i < batches.length - 1) {
        console.log(`>> Đợi 8 giây trước batch tiếp theo...`);
        await new Promise(resolve => setTimeout(resolve, 8000));
      }
    }

    // Kiểm tra lại kết quả
    const retryResult = this.checkPostsAvailability(userIds);
    console.log(`>> Sau khi thử lại: ${retryResult.available}/${retryResult.total} user có bài viết`);
  }
  
  async retryWithDelay(user, delay) {
    if (delay > 0) {
      await new Promise(resolve => setTimeout(resolve, delay));
    }
    
    try {
      // Thử tối đa 3 lần
      for (let retry = 0; retry < 3; retry++) {
        console.log(`>> Đang thử lại lần ${retry+1}/3 cho user ${user.uid}...`);
        
        // Thêm jitter ngẫu nhiên để làm cho request trông tự nhiên hơn
        const jitter = Math.floor(Math.random() * 300);
        await new Promise(resolve => setTimeout(resolve, jitter));
        
        const result = await this.postsCache.fetchUserPosts(user, 1);
        if (result.posts && result.posts.length > 0) {
          console.log(`>> ✅ Đã lấy được bài viết cho user ${user.uid} (lần thử ${retry+1})`);
          return result;
        }
        
        // Nếu không thành công và còn lần thử, đợi một chút rồi thử lại
        if (retry < 2) {
          const waitTime = 2000 + (retry * 1000); // Tăng thời gian chờ sau mỗi lần thử
          console.log(`>> Đợi ${waitTime/1000}s trước khi thử lại...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }
      }
      
      console.log(`>> ❌ Không tìm thấy bài viết cho user ${user.uid} sau khi thử lại`);
      return { userId: user.uid, postsCount: 0, posts: [] };
    } catch (error) {
      console.error(`>> Lỗi khi lấy lại posts cho user ${user.uid}: ${error.message}`);
      return { userId: user.uid, postsCount: 0, posts: [] };
    }
  }
}

module.exports = BatchFetcher;
