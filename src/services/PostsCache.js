const { sleep } = require('../utils');
const getUserPosts = require('./getPostUser');

class PostsCache {
  constructor() {
    this.postsMap = new Map(); // userId -> posts array
    this.lastFetchTime = new Map(); // userId -> timestamp
    this.cacheDuration = 30 * 60 * 1000; // 30 phút
  }

  /**
   * Lấy bài viết mới nhất của tất cả user
   * @param {Array} users - Danh sách các user cần lấy bài viết
   * @param {number} postsPerUser - Số lượng bài viết cần lấy cho mỗi user
   */
  async fetchAllUsersPosts(users, postsPerUser = 1) {
    console.log(`>> Đang lấy ${postsPerUser} bài viết mới nhất cho ${users.length} users...`);
    
    // Thêm thời gian chờ ban đầu
    await sleep(5000); 
    
    // Chạy với batch nhỏ hơn để giảm tải
    const BATCH_SIZE = 5;
    for (let i = 0; i < users.length; i += BATCH_SIZE) {
      const batch = users.slice(i, i + BATCH_SIZE);
      console.log(`>> Xử lý batch ${i/BATCH_SIZE + 1}/${Math.ceil(users.length/BATCH_SIZE)}: ${batch.length} users`);
      
      // Xử lý song song trong batch
      await Promise.all(batch.map(user => this._fetchWithRetry(user, postsPerUser)));
      
      // Đợi giữa các batch
      if (i + BATCH_SIZE < users.length) {
        console.log(`>> Đợi 10 giây trước khi xử lý batch tiếp theo...`);
        await sleep(10000);
      }
    }
    
    // In thống kê cuối
    let usersWithPosts = 0;
    for (const [userId, posts] of this.postsMap.entries()) {
      if (posts.length > 0) usersWithPosts++;
    }
    console.log(`>> Hoàn tất: ${usersWithPosts}/${users.length} users có bài viết được cache`);
  }

  async _fetchWithRetry(user, count, maxRetries = 3) {
    for (let retry = 0; retry < maxRetries; retry++) {
      try {
        const result = await this.fetchUserPosts(user, count);
        if (result.posts.length > 0) {
          console.log(`>> [Retry ${retry}] Thành công: User ${user.uid} có ${result.posts.length} bài`);
          return result;
        } else {
          console.log(`>> [Retry ${retry}] User ${user.uid}: Không tìm thấy bài viết, thử lại...`);
          await sleep(3000 * (retry + 1));
        }
      } catch (error) {
        console.error(`>> [Retry ${retry}] Lỗi cho user ${user.uid}: ${error.message}`);
        await sleep(3000 * (retry + 1));
      }
    }
    console.log(`>> Đã thử ${maxRetries} lần nhưng không thể lấy bài viết cho user ${user.uid}`);
    return { userId: user.uid, postsCount: 0, posts: [] };
  }

  /**
   * Lấy bài viết cho một user cụ thể
   * @param {Object} user - Thông tin user
   * @param {number} count - Số lượng bài viết cần lấy
   */
  async fetchUserPosts(user, count = 1) {
    try {
      console.log(`>> Đang lấy ${count} bài viết mới nhất của user ${user.piname}...`);
      
      const posts = await getUserPosts(user);
      const postsToCache = posts.slice(0, count);
      
      // Lưu vào cache
      this.postsMap.set(user.uid, postsToCache);
      this.lastFetchTime.set(user.uid, Date.now());
      
      console.log(`>> Đã lấy ${postsToCache.length} bài viết cho user ${user.piname}`);
      return {
        userId: user.uid,
        postsCount: postsToCache.length,
        posts: postsToCache
      };
    } catch (error) {
      console.error(`>> Lỗi khi lấy bài viết cho user ${user.piname}:`, error.message);
      return {
        userId: user.uid,
        postsCount: 0,
        posts: []
      };
    }
  }

  /**
   * Lấy bài viết đã cache của một user
   * @param {string} userId - ID của user
   * @param {number} index - Vị trí bài viết (0 = mới nhất)
   */
  getPost(userId, index = 0) {
    const posts = this.postsMap.get(userId) || [];
    return posts[index] || null;
  }

  /**
   * Kiểm tra xem user có bài viết đã được cache hay không
   * @param {string} userId - ID của user
   */
  hasPostsForUser(userId) {
    const posts = this.postsMap.get(userId) || [];
    return posts.length > 0;
  }

  /**
   * Lấy thống kê về cache
   */
  getStats() {
    const userCount = this.postsMap.size;
    let postCount = 0;
    
    for (const posts of this.postsMap.values()) {
      postCount += posts.length;
    }
    
    return {
      userCount,
      postCount,
      averagePostsPerUser: userCount > 0 ? postCount / userCount : 0
    };
  }

  /**
   * Xóa cache
   */
  clearCache() {
    this.postsMap.clear();
    this.lastFetchTime.clear();
    console.log('>> Đã xóa cache bài viết');
  }

  // Thêm phương thức để debug
  debugPostsForUser(userId) {
    const posts = this.postsMap.get(userId) || [];
    console.log(`>> [DEBUG] User ${userId}: ${posts.length} bài viết cached`);
    if (posts.length > 0) {
      console.log(`>> [DEBUG] Bài đầu tiên: ${posts[0]}`);
    }
    return posts;
  }
}

module.exports = PostsCache; 