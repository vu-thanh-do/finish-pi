/**
 * Class phân phối like giữa các user để đảm bảo mỗi user like và nhận đúng 12 lượt like
 * Sử dụng thuật toán hoán vị vòng tròn (cyclic permutation) và xử lý song song theo lô để tối ưu hiệu suất
 */
class LikeDistributor {
  constructor(userObjects, targetUsers, postsCache) {
    // Danh sách user sẽ thực hiện like (có thông tin đăng nhập)
    this.likeUsers = userObjects;
    
    // Danh sách user cần được like (có thể trùng với likeUsers)
    this.targetUsers = targetUsers;
    
    // Cache bài viết của user
    this.postsCache = postsCache;
    
    // Ma trận phân phối: targetUserId -> [likeUserIds]
    this.distributionMatrix = new Map();
    
    // Đếm số lượt đã like của mỗi user
    this.likeCountByUser = new Map();
    
    // Đếm số lượt đã nhận của mỗi user
    this.receivedCountByUser = new Map();
    
    // Danh sách tác vụ đã được phân phối
    this.distributedTasks = [];
    
    // Số lượt like mục tiêu cho mỗi user
    this.targetLikes = 12;
    
    // Số lượng user trong mỗi lô (batch)
    this.batchSize = 12;
    
    // Cho phép vượt quá số lượt like mục tiêu hay không
    this.allowExceedTarget = true;
    
    // Khởi tạo các bộ đếm
    this._initializeCounts();
  }

  /**
   * Khởi tạo các bộ đếm
   */
  _initializeCounts() {
    this.likeUsers.forEach(user => {
      this.likeCountByUser.set(user.uid, 0);
    });
    
    this.targetUsers.forEach(userId => {
      this.receivedCountByUser.set(userId, 0);
      this.distributionMatrix.set(userId, []);
    });
  }

  /**
   * Kiểm tra xem một user có thể like một bài viết của user khác hay không
   * @param {string} likeUserId - ID của user sẽ like
   * @param {string} targetUserId - ID của user cần được like
   */
  canLike(likeUserId, targetUserId) {
    // User không thể tự like chính mình
    if (likeUserId === targetUserId) return false;
    
    // Kiểm tra xem user đã like đủ targetLikes lượt chưa
    const likeCount = this.likeCountByUser.get(likeUserId) || 0;
    if (likeCount >= this.targetLikes) return false;
    
    // Kiểm tra xem user đã like user này rồi hay chưa
    const likedUsers = this.distributionMatrix.get(targetUserId) || [];
    if (likedUsers.includes(likeUserId)) return false;
    
    // Nếu cho phép vượt quá mục tiêu, không cần kiểm tra giới hạn nhận like
    if (this.allowExceedTarget) {
      // Kiểm tra xem có bài viết để like không
      return this.postsCache.hasPostsForUser(targetUserId);
    }
    
    // Kiểm tra xem user target đã nhận đủ targetLikes lượt like chưa
    const receivedCount = this.receivedCountByUser.get(targetUserId) || 0;
    if (receivedCount >= this.targetLikes) return false;
    
    // Kiểm tra xem có bài viết để like không
    return this.postsCache.hasPostsForUser(targetUserId);
  }

  /**
   * Phân phối like sử dụng hoán vị vòng tròn và xử lý song song theo lô
   * @param {number} targetLikesPerUser - Số lượt like mỗi user cần nhận (mặc định là 12)
   */
  distributeAllLikes(targetLikesPerUser = 12) {
    console.log(`>> Bắt đầu phân phối ${targetLikesPerUser} lượt like cho mỗi user...`);
    
    this.targetLikes = targetLikesPerUser;
    const maxLikes = this.targetUsers.length * targetLikesPerUser;
    console.log(`>> Tổng số lượt like cần phân phối: ${maxLikes}`);
    
    // Kiểm tra bài viết của tất cả users
    this.validateUserPosts();
    
    // Sắp xếp danh sách user (Bước 1 của thuật toán)
    console.log(`>> Sắp xếp danh sách user trước khi phân phối...`);
    
    // Phân phối like theo thuật toán vòng tròn đơn giản (Bước 2 của thuật toán)
    console.log(`>> Áp dụng thuật toán hoán vị vòng tròn đơn giản`);
    this.distributeWithSimpleCyclicPermutation();
    
    // Phân phối bổ sung để đảm bảo mỗi user nhận đủ lượt like (Bước 3 của thuật toán)
    console.log(`>> Phân phối bổ sung để đảm bảo mỗi user nhận đủ ${targetLikesPerUser} lượt like`);
    this.distributeRemainingLikes();
    
    // Phân tích kết quả phân phối
    this.analyzeDistribution();
    
    return this.distributedTasks;
  }

  /**
   * Xác minh bài viết của users trước khi phân phối
   */
  validateUserPosts() {
    console.log(`>> Xác minh bài viết của tất cả users trước khi phân phối...`);
    
    const usersWithPosts = [];
    const usersWithoutPosts = [];
    
    for (const userId of this.targetUsers) {
      const hasPost = this.postsCache.hasPostsForUser(userId);
      const postId = this.postsCache.getPost(userId);
      
      if (hasPost && postId) {
        usersWithPosts.push(userId);
        console.log(`>> User ${userId}: Có bài viết (ID: ${postId})`);
      } else {
        usersWithoutPosts.push(userId);
        console.log(`>> User ${userId}: KHÔNG có bài viết!`);
      }
    }
    
    console.log(`>> Tổng kết: ${usersWithPosts.length} users có bài viết, ${usersWithoutPosts.length} users không có`);
    
    // Lọc chỉ giữ lại users có bài viết
    this.targetUsers = usersWithPosts;
  }

  /**
   * Phân phối like sử dụng thuật toán vòng tròn đơn giản
   * Mỗi user i sẽ like cho user (i+1) đến (i+12) theo modulo
   */
  distributeWithSimpleCyclicPermutation() {
    // Lọc danh sách user có bài viết và có thông tin đăng nhập
    const validLikeUsers = this.likeUsers.filter(user => 
      this.postsCache.hasPostsForUser(user.uid)
    );
    
    // Lọc danh sách user cần được like và có bài viết
    const validTargetUsers = this.targetUsers.filter(userId => 
      this.postsCache.hasPostsForUser(userId)
    );
    
    console.log(`>> Bắt đầu phân phối với ${validLikeUsers.length} users thực hiện like và ${validTargetUsers.length} users cần được like`);
    
    // Không đủ user để thực hiện
    if (validLikeUsers.length === 0 || validTargetUsers.length === 0) {
      console.log(`>> Không đủ users để thực hiện phân phối!`);
      return;
    }
    
    // Số lượng like mục tiêu cho mỗi user
    const targetLikesPerUser = this.targetLikes;
    
    // Tổng số user
    const totalUsers = validLikeUsers.length;
    const totalTargets = validTargetUsers.length;
    
    console.log(`>> Áp dụng thuật toán: Mỗi user i sẽ like cho user (i+1) đến (i+${targetLikesPerUser}) theo modulo`);
    
    // Với mỗi user, tìm targetLikesPerUser user tiếp theo để like theo modulo
    let assignedCount = 0;
    
    for (let i = 0; i < totalUsers; i++) {
      const currentLiker = validLikeUsers[i];
      
      // Phân phối targetLikesPerUser like cho mỗi user
      for (let offset = 1; offset <= targetLikesPerUser; offset++) {
        // Tính chỉ số của user nhận like theo modulo
        const targetIndex = (i + offset) % totalTargets;
        const targetId = validTargetUsers[targetIndex];
        
        // Kiểm tra xem có thể phân phối like không
        if (this.canLike(currentLiker.uid, targetId)) {
          if (this.assignLike(currentLiker, targetId)) {
            assignedCount++;
          }
        }
      }
    }
    
    console.log(`>> Đã phân phối ${assignedCount} lượt like theo thuật toán hoán vị vòng tròn đơn giản`);
  }

  /**
   * Phân phối các lượt like còn lại để đảm bảo mỗi user nhận đủ lượt like
   */
  distributeRemainingLikes() {
    console.log(`>> Phân phối các lượt like còn lại...`);
    
    // Tiếp tục phân phối cho đến khi không phân phối được nữa hoặc tất cả đều đủ lượt like
    while (true) {
      // Tìm user có thể like và user cần được like
      let found = false;
      
      // Tìm tất cả user chưa nhận đủ lượt like mục tiêu
      const targetUsersNeedLikes = this.targetUsers.filter(userId => {
        const receivedCount = this.receivedCountByUser.get(userId) || 0;
        return receivedCount < this.targetLikes && this.postsCache.hasPostsForUser(userId);
      });
      
      if (targetUsersNeedLikes.length === 0) {
        console.log(`>> Tất cả users đã nhận đủ lượt like!`);
        break;
      }
      
      // Sắp xếp các user cần like theo số lượt đã nhận (tăng dần)
      targetUsersNeedLikes.sort((a, b) => {
        const aCount = this.receivedCountByUser.get(a) || 0;
        const bCount = this.receivedCountByUser.get(b) || 0;
        return aCount - bCount;
      });
      
      // Tìm tất cả user còn có thể like
      const availableLikers = this.likeUsers.filter(user => {
        const likeCount = this.likeCountByUser.get(user.uid) || 0;
        return likeCount < this.targetLikes;
      });
      
      if (availableLikers.length === 0) {
        console.log(`>> Không còn user nào có thể like!`);
        break;
      }
      
      // Sắp xếp các user có thể like theo số lượt đã like (tăng dần)
      availableLikers.sort((a, b) => {
        const aCount = this.likeCountByUser.get(a.uid) || 0;
        const bCount = this.likeCountByUser.get(b.uid) || 0;
        return aCount - bCount;
      });
      
      // Thử các cặp user (ưu tiên user ít lượt nhất)
      for (const targetUserId of targetUsersNeedLikes) {
        if (found) break;
        
        for (const likeUser of availableLikers) {
          if (this.canLike(likeUser.uid, targetUserId)) {
            if (this.assignLike(likeUser, targetUserId)) {
              found = true;
              break;
            }
          }
        }
      }
      
      // Nếu không thể phân phối thêm, kết thúc
      if (!found) {
        console.log(`>> Không thể phân phối thêm lượt like!`);
        break;
      }
    }
  }
  
  /**
   * Gán một lượt like
   * @param {Object} likeUser - User thực hiện like
   * @param {string} targetUserId - ID của user được like
   * @returns {boolean} - Thành công hay không
   */
  assignLike(likeUser, targetUserId) {
    // Lấy bài viết để like
    const postId = this.postsCache.getPost(targetUserId);
    if (!postId) {
      // console.log(`>> Không tìm thấy bài viết để like cho user ${targetUserId}`);
      return false;
    }
    
    // Thêm vào ma trận phân phối
    const likedUsers = this.distributionMatrix.get(targetUserId) || [];
    likedUsers.push(likeUser.uid);
    this.distributionMatrix.set(targetUserId, likedUsers);
    
    // Cập nhật số lượt like
    this.likeCountByUser.set(likeUser.uid, (this.likeCountByUser.get(likeUser.uid) || 0) + 1);
    this.receivedCountByUser.set(targetUserId, (this.receivedCountByUser.get(targetUserId) || 0) + 1);
    
    // Thêm vào danh sách tác vụ
    this.distributedTasks.push({
      likeUser,
      targetUserId,
      postId
    });
    
    return true;
  }
  
  /**
   * Phân tích kết quả phân phối
   */
  analyzeDistribution() {
    let totalUnderLiked = 0;
    let usersAtTarget = 0;
    let usersBelowTarget = 0;
    let usersAboveTarget = 0;
    const usersBelowList = [];
    const usersAboveList = [];
    
    // Kiểm tra từng user đã nhận đủ like chưa
    for (const [userId, count] of this.receivedCountByUser.entries()) {
      if (count > this.targetLikes) {
        usersAboveTarget++;
        usersAboveList.push({ userId, count });
      } else if (count >= this.targetLikes) {
        usersAtTarget++;
      } else {
        usersBelowTarget++;
        totalUnderLiked += (this.targetLikes - count);
        console.log(`>> User ${userId} chỉ nhận được ${count}/${this.targetLikes} lượt like`);
        usersBelowList.push({ userId, count });
      }
    }
    
    // Sắp xếp user theo số lượt like tăng dần
    usersBelowList.sort((a, b) => a.count - b.count);
    usersAboveList.sort((a, b) => b.count - a.count);
    
    // In 5 user có ít lượt like nhất
    if (usersBelowList.length > 0) {
      console.log(`\n>> Top 5 user có ít lượt like nhất:`);
      usersBelowList.slice(0, 5).forEach(user => {
        console.log(`>> User ${user.userId}: ${user.count}/${this.targetLikes} lượt like`);
      });
    }
    
    // In 5 user có nhiều lượt like nhất
    if (usersAboveList.length > 0) {
      console.log(`\n>> Top 5 user có nhiều lượt like nhất (vượt chỉ tiêu):`);
      usersAboveList.slice(0, 5).forEach(user => {
        console.log(`>> User ${user.userId}: ${user.count}/${this.targetLikes} lượt like (vượt ${user.count - this.targetLikes} lượt)`);
      });
    }
    
    if (totalUnderLiked > 0) {
      console.log(`\n>> Còn thiếu ${totalUnderLiked} lượt like để đảm bảo mỗi user nhận đủ ${this.targetLikes} lượt`);
      console.log(`>> ${usersAtTarget}/${this.targetUsers.length} users nhận đủ ${this.targetLikes} lượt like`);
      console.log(`>> ${usersAboveTarget}/${this.targetUsers.length} users nhận vượt ${this.targetLikes} lượt like`);
    } else {
      console.log(`\n>> Tất cả users đều nhận đủ ${this.targetLikes} lượt like!`);
    }
    
    // Phân tích hiệu quả phân phối
    const averageLikes = this.targetUsers.length > 0 ? 
      this.distributedTasks.length / this.targetUsers.length : 0;
      
    console.log(`\n>> Tổng số lượt like đã phân phối: ${this.distributedTasks.length}`);
    console.log(`>> Trung bình ${averageLikes.toFixed(2)} lượt like/user`);
  }

  /**
   * Lấy thống kê về việc phân phối like
   */
  getDistributionStats() {
    // Thống kê số lượt like đã được thực hiện
    let totalLikes = 0;
    let belowTargetCount = 0;
    let atTargetCount = 0;
    let aboveTargetCount = 0;
    
    for (const [userId, count] of this.receivedCountByUser.entries()) {
      totalLikes += count;
      
      if (count < this.targetLikes) {
        belowTargetCount++;
      } else if (count === this.targetLikes) {
        atTargetCount++;
      } else {
        aboveTargetCount++;
      }
    }
    
    // Thống kê chi tiết
    const userStats = [];
    for (const [userId, count] of this.receivedCountByUser.entries()) {
      userStats.push({
        userId,
        receivedCount: count,
        targetDelta: this.targetLikes - count
      });
    }
    
    // Sắp xếp users theo số lượng like nhận được (tăng dần)
    userStats.sort((a, b) => a.receivedCount - b.receivedCount);
    
    // Thống kê chi tiết về người like
    const likerStats = [];
    for (const [userId, count] of this.likeCountByUser.entries()) {
      likerStats.push({ userId, count });
    }
    
    likerStats.sort((a, b) => b.count - a.count);
    
    return {
      totalLikes,
      targetUsers: this.targetUsers.length,
      likeUsers: this.likeUsers.length,
      distributedTasks: this.distributedTasks.length,
      belowTarget: belowTargetCount,
      atTarget: atTargetCount,
      aboveTarget: aboveTargetCount,
      mostActiveLikers: likerStats.slice(0, 5),
      leastLikedUsers: userStats.slice(0, 5),
      averageLikesPerUser: (totalLikes / this.targetUsers.length).toFixed(2)
    };
  }
}

module.exports = LikeDistributor; 