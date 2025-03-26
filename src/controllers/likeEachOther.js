const path = require('path');
const ExcelReaderService = require('../models/excelSheed');
const apiClient = require('../api/apiClient');
const qs = require("qs");
const getUserPosts = require("../services/getPostUser");
const { cpus } = require('os');
const os = require('os');
const PostsCache = require('../services/PostsCache');
const ProxyManager = require('../services/ProxyManager');
const LikeDistributor = require('../services/LikeDistributor');
const BatchFetcher = require('../services/BatchFetcher');
const fs = require('fs');
const { sleep, formatDuration } = require('../utils/helpers');
const ClusterManager = require('../../cluster-manager');

/**
 * Ghi log vào file
 * @param {string} message - Nội dung log
 */
function logToFile(message) {
  try {
    const logsDir = path.join(__dirname, '../logs');
    
    // Tạo thư mục logs nếu chưa tồn tại
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
    
    const date = new Date();
    const dateStr = date.toISOString().split('T')[0];
    const timestamp = `[${date.toLocaleTimeString()}]`;
    
    const logFilePath = path.join(logsDir, `pi-automate-logs-${dateStr}.txt`);
    
    // Thêm timestamp vào message
    const logMessage = `${timestamp} ${message}\n`;
    
    // Ghi log vào file
    fs.appendFileSync(logFilePath, logMessage, 'utf8');
  } catch (error) {
    console.error(`Lỗi khi ghi log: ${error.message}`);
  }
}

class TaskQueue {
  constructor(concurrencyLimit = 10000) {
    this.concurrencyLimit = concurrencyLimit;
    this.runningTasks = 0;
    this.queue = [];
    this.results = [];
    this.completedCount = 0;
    this.successCount = 0;
    this.failCount = 0;
    this.totalTasks = 0;
    this.userLastRequestTime = new Map();
    this.isProcessing = false;
    this.processInterval = null;
    this.gcInterval = null;
    this.likedPosts = new Map();
    this.userLikeCount = new Map();
    this.userReceivedCount = new Map();
    
    this.startProcessing();
    this.startGarbageCollection();
  }

  startProcessing() {
    if (this.processInterval) {
      clearInterval(this.processInterval);
    }
    this.processInterval = setInterval(() => {
      if (!this.isProcessing) {
        this.processQueue();
      }
    }, 100);
  }

  startGarbageCollection() {
    if (this.gcInterval) {
      clearInterval(this.gcInterval);
    }
    this.gcInterval = setInterval(() => {
      this.cleanupMemory();
    }, 300000);
  }

  cleanupMemory() {
    if (this.results.length > 1000) {
      this.results = this.results.slice(-1000);
    }
    
    const now = Date.now();
    for (const [userId, lastTime] of this.userLastRequestTime.entries()) {
      if (now - lastTime > 3600000) {
        this.userLastRequestTime.delete(userId);
      }
    }
  }

  async add(taskFn, userId, targetUserId) {
    return new Promise((resolve) => {
      this.queue.push({ taskFn, resolve, userId, targetUserId, addedTime: Date.now() });
      this.totalTasks++;
      
      if (!this.userLikeCount.has(userId)) {
        this.userLikeCount.set(userId, 0);
      }
      if (!this.userReceivedCount.has(targetUserId)) {
        this.userReceivedCount.set(targetUserId, 0);
      }
    });
  }

  async processQueue() {
    if (this.queue.length === 0 || this.runningTasks >= this.concurrencyLimit) {
      return;
    }

    this.isProcessing = true;
    
    try {
      const now = Date.now();
      
      const eligibleTasks = this.queue.filter(task => {
        const lastRequestTime = this.userLastRequestTime.get(task.userId) || 0;
        return (now - lastRequestTime) >= 2000;
      });

      if (eligibleTasks.length === 0) {
        return;
      }

      eligibleTasks.sort((a, b) => a.addedTime - b.addedTime);

      const taskIndex = this.queue.findIndex(t => t === eligibleTasks[0]);
      const { taskFn, resolve, userId, targetUserId } = this.queue.splice(taskIndex, 1)[0];

      this.runningTasks++;
      this.userLastRequestTime.set(userId, now);

      try {
        const result = await taskFn();
        this.completedCount++;
        if (result.success) {
          this.successCount++;
          
          this.userLikeCount.set(userId, (this.userLikeCount.get(userId) || 0) + 1);
          this.userReceivedCount.set(targetUserId, (this.userReceivedCount.get(targetUserId) || 0) + 1);
          
          if (result.targetUserId && result.postId) {
            if (!this.likedPosts.has(result.targetUserId)) {
              this.likedPosts.set(result.targetUserId, []);
            }
            this.likedPosts.get(result.targetUserId).push({
              postId: result.postId,
              likedBy: result.userId
            });
            
            logToFile(`✅ User ${userId} đã like thành công bài ${result.postId} của user ${targetUserId}`);
          }
        } else {
          this.failCount++;
          logToFile(`❌ User ${userId} like thất bại bài ${result.postId || 'N/A'} của user ${targetUserId} - Lỗi: ${result.error || 'Unknown error'}`);
        }
        this.results.push({ status: 'fulfilled', value: result });
        resolve(result);
      } catch (error) {
        this.completedCount++;
        this.failCount++;
        this.results.push({ status: 'rejected', reason: error.message });
        logToFile(`❌ Exception khi user ${userId} like bài của user ${targetUserId}: ${error.message}`);
        resolve({ success: false, error: error.message });
      }
    } finally {
      this.runningTasks--;
      this.isProcessing = false;
    }
  }

  get stats() {
    return {
      total: this.totalTasks,
      completed: this.completedCount,
      success: this.successCount,
      failure: this.failCount,
      pending: this.totalTasks - this.completedCount,
      running: this.runningTasks,
      queued: this.queue.length,
      likedPosts: Object.fromEntries(this.likedPosts),
      userLikeCounts: Object.fromEntries(this.userLikeCount),
      userReceivedCounts: Object.fromEntries(this.userReceivedCount),
      memoryUsage: process.memoryUsage()
    };
  }

  destroy() {
    if (this.processInterval) {
      clearInterval(this.processInterval);
    }
    if (this.gcInterval) {
      clearInterval(this.gcInterval);
    }
    this.queue = [];
    this.results = [];
    this.userLastRequestTime.clear();
    this.likedPosts.clear();
    this.userLikeCount.clear();
    this.userReceivedCount.clear();
  }
}

function updateProgressStatus(queue, proxyManager) {
  const { total, completed, success, failure, running } = queue.stats;
  const percent = total > 0 ? Math.floor((completed / total) * 100) : 0;
  const bar = Array(20).fill('▒').map((char, i) => i < Math.floor(percent / 5) ? '█' : '▒').join('');
  
  const statusText = `\n-------- TRẠNG THÁI TIẾN ĐỘ LIKE --------\n[${bar}] ${percent}% (${completed}/${total})\n✅ Thành công: ${success} | ❌ Thất bại: ${failure} | ⏳ Đang xử lý: ${running}\n🧵 Luồng đang chạy: ${running} | 🔄 Tối đa luồng: ${queue.concurrencyLimit}`;
  console.log(statusText);
  logToFile(statusText.replace(/\n/g, ' | '));
  
  if (proxyManager) {
    const proxyStats = proxyManager.getProxyStats();
    const proxyText = `\n-------- THÔNG TIN PROXY --------\n📊 Tổng số proxy: ${proxyStats.total} (Hoạt động: ${proxyStats.active}, Không hoạt động: ${proxyStats.inactive})\n🔄 Proxy từ Excel: ${proxyStats.fromExcel} | 🔄 Proxy xoay: ${proxyStats.fromRotating}`;
    console.log(proxyText);
    logToFile(proxyText.replace(/\n/g, ' | '));
  }
  
  console.log(`-----------------------------------------\n`);
}

function getRandomUsers(users, n) {
  const shuffled = [...users].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, n);
}

// Khởi tạo taskQueue ở phạm vi cấp cao để có thể truy cập từ khắp nơi
  let taskQueue = null;

// Thêm vào trước task queue
async function processLikesInParallel(userObjects, targetUserIds, postsCache, taskQueue, concurrencyLimit) {
  console.log(`\n>> Bắt đầu xử lý like song song với ${concurrencyLimit} luồng đồng thời`);
  logToFile(`Bắt đầu xử lý like song song với ${concurrencyLimit} luồng đồng thời`);
  
  // Tạo phép hoán vị với thuật toán vòng tròn đơn giản
  const likeAssignments = [];
  
  // Đảm bảo mỗi user thực hiện đúng 12 lượt like (hoặc ít hơn nếu không đủ target)
  const targetLikesPerUser = 12;
  const totalLikeUsers = userObjects.length;
  const totalTargetUsers = targetUserIds.length;
  
  console.log(`>> Tạo ma trận phân phối like cho ${totalLikeUsers} user, mỗi user like ${targetLikesPerUser} lượt`);
  
  // Tạo ma trận phân phối like: mỗi user like những user nào
  // Sử dụng hoán vị vòng tròn: user i sẽ like cho các user (i+1, i+2, ..., i+12) % totalTargetUsers
  for (let i = 0; i < totalLikeUsers; i++) {
    const likeUser = userObjects[i];
    const userAssignments = [];
    
    // Phân phối targetLikesPerUser lượt like cho mỗi user
    for (let offset = 1; offset <= targetLikesPerUser && offset <= totalTargetUsers; offset++) {
      // Tính chỉ số của user nhận like theo modulo
      const targetIndex = (i + offset) % totalTargetUsers;
      const targetId = targetUserIds[targetIndex];
      
      // Không like chính mình
      if (likeUser.uid !== targetId) {
        // Nếu user có bài viết để like
        if (postsCache.hasPostsForUser(targetId)) {
          const postId = postsCache.getPost(targetId);
          if (postId) {
            userAssignments.push({ targetUserId: targetId, postId });
          }
        }
      }
    }
    
    // Nếu user có lượt like đã được phân phối
    if (userAssignments.length > 0) {
      likeAssignments.push({
        likeUser,
        targets: userAssignments
      });
    }
  }
  
  console.log(`>> Đã phân phối tổng cộng ${likeAssignments.length} user thực hiện like`);
  
  // Đếm tổng số lượt like đã phân phối
  let totalLikeCount = 0;
  likeAssignments.forEach(assignment => {
    totalLikeCount += assignment.targets.length;
  });
  
  console.log(`>> Tổng số lượt like cần thực hiện: ${totalLikeCount}`);
  logToFile(`Đã phân phối ${totalLikeCount} lượt like cho ${likeAssignments.length} user`);
  
  // Thực thi song song các lượt like với Promise.all và cơ chế giới hạn concurrency
  const startTime = Date.now();
  const batchStartTimes = new Map();
  const batchResults = new Map();
  
  // Bước 1: Tạo các promise cho từng user
  const userPromises = likeAssignments.map((assignment, index) => {
    return () => new Promise(async (resolve) => {
      const { likeUser, targets } = assignment;
      const userStartTime = Date.now();
      batchStartTimes.set(likeUser.uid, userStartTime);
      
      console.log(`>> [${index + 1}/${likeAssignments.length}] User ${likeUser.piname || likeUser.uid} bắt đầu thực hiện ${targets.length} lượt like`);
      
      let successCount = 0;
      let failCount = 0;
      
      // Thực hiện tuần tự các lượt like cho mỗi user
      for (let i = 0; i < targets.length; i++) {
        const { targetUserId, postId } = targets[i];
        
        try {
          // Thêm tác vụ vào queue và đợi kết quả
          const result = await taskQueue.add(
          async () => {
              console.log(`>> User ${likeUser.piname} like bài ${postId} của user ${targetUserId} (${i+1}/${targets.length})`);
            
            const maxRetries = 3;
            let retryCount = 0;
            
            while (retryCount <= maxRetries) {
              try {
                if (retryCount > 0) {
                  console.log(`>> Thử lại lần ${retryCount}/${maxRetries} cho like bài ${postId}`);
                    await sleep(1000 * retryCount); // Giảm thời gian chờ xuống 1s
                }

                const api = apiClient(likeUser, { 
                  useProxyManager: true, 
                    timeout: 10000, // Giảm timeout xuống 10s
                    retries: 1
                });
                
                const payload = qs.stringify({
                  component: "article",
                  action: "like",
                  aid: postId,
                  user_name: likeUser.piname,
                  english_version: 0,
                  selected_country: 1,
                  selected_chain: 0,
                });

                const response = await api.post('/vapi', payload);
                
                if (response.data && response.data.time) {
                    console.log(`✅ User ${likeUser.piname} đã like thành công bài ${postId} (${i+1}/${targets.length})`);
                  return { success: true, postId, userId: likeUser.uid, targetUserId };
                } else {
                  console.log(`⚠️ Like bài ${postId} không thành công:`, response.data);
                  return { success: false, postId, userId: likeUser.uid, targetUserId, error: JSON.stringify(response.data) };
                }
              } catch (error) {
                  console.error(`❌ Lỗi khi like bài ${postId}:`, error.message);
                
                if (error.response) {
                  console.error(`Mã lỗi: ${error.response.status}`);
                  
                  if ([404, 429, 500, 502, 503, 504].includes(error.response.status)) {
                    retryCount++;
                    if (retryCount <= maxRetries) {
                        const delayTime = error.response.status === 429 ? 5000 : 2000 * retryCount;
                      console.log(`>> [Task] Sẽ thử lại sau ${delayTime/1000} giây...`);
                      await sleep(delayTime);
                      continue;
                    }
                  }
                }
                
                return { success: false, postId, userId: likeUser.uid, targetUserId, error: error.message };
              }
            }
            
            return { success: false, postId, userId: likeUser.uid, targetUserId, error: "Đã hết số lần thử lại" };
          },
          likeUser.uid,
          targetUserId
        );
          
          // Đếm số lượt thành công/thất bại
          if (result.success) {
            successCount++;
          } else {
            failCount++;
          }
          
        } catch (error) {
          console.error(`>> Lỗi khi thực hiện like:`, error);
          failCount++;
        }
      }
      
      const userFinishTime = Date.now();
      const userDuration = formatDuration(userFinishTime - userStartTime);
      
      console.log(`>> User ${likeUser.piname} đã hoàn thành: ${successCount} thành công, ${failCount} thất bại (${userDuration})`);
      logToFile(`User ${likeUser.piname} hoàn thành: ${successCount}/${targets.length} lượt like (${userDuration})`);
      
      batchResults.set(likeUser.uid, {
        successCount,
        failCount,
        totalAssigned: targets.length,
        duration: userDuration
      });
      
      resolve({
        user: likeUser.uid,
        success: successCount,
        fail: failCount,
        total: targets.length,
        duration: userDuration
      });
    });
  });
  
  // Bước 2: Thực thi song song với giới hạn concurrency
  const results = await runWithConcurrencyLimit(userPromises, concurrencyLimit);
  
  const endTime = Date.now();
  const totalDuration = formatDuration(endTime - startTime);
  
  // Phân tích kết quả
  let totalSuccess = 0;
  let totalFail = 0;
  
  results.forEach(result => {
    totalSuccess += result.success;
    totalFail += result.fail;
  });
  
  console.log(`\n>> Tổng kết xử lý song song:`);
  console.log(`>> - Tổng số lượt like thành công: ${totalSuccess}/${totalLikeCount}`);
  console.log(`>> - Tổng số lượt like thất bại: ${totalFail}/${totalLikeCount}`);
  console.log(`>> - Thời gian xử lý: ${totalDuration}`);
  logToFile(`Kết quả xử lý song song: ${totalSuccess} thành công, ${totalFail} thất bại, thời gian: ${totalDuration}`);
  
  // Tìm user nhanh nhất và chậm nhất
  let fastestUser = null;
  let fastestTime = Number.MAX_SAFE_INTEGER;
  let slowestUser = null;
  let slowestTime = 0;
  
  for (const [userId, result] of batchResults.entries()) {
    const timeInMs = (new Date(batchStartTimes.get(userId) + result.duration)) - batchStartTimes.get(userId);
    if (timeInMs < fastestTime) {
      fastestTime = timeInMs;
      fastestUser = userId;
    }
    if (timeInMs > slowestTime) {
      slowestTime = timeInMs;
      slowestUser = userId;
    }
  }
  
  if (fastestUser) {
    const fastestUserObj = userObjects.find(u => u.uid === fastestUser);
    console.log(`>> User nhanh nhất: ${fastestUserObj?.piname || fastestUser} (${formatDuration(fastestTime)})`);
  }
  
  if (slowestUser) {
    const slowestUserObj = userObjects.find(u => u.uid === slowestUser);
    console.log(`>> User chậm nhất: ${slowestUserObj?.piname || slowestUser} (${formatDuration(slowestTime)})`);
  }
  
  return {
    totalAssigned: totalLikeCount,
    successCount: totalSuccess,
    failCount: totalFail,
    duration: totalDuration
  };
}

/**
 * Hàm thực thi các promise với giới hạn concurrency
 * @param {Array<Function>} promiseFns - Mảng các hàm trả về promise
 * @param {number} concurrency - Số lượng promise thực thi đồng thời tối đa
 * @returns {Array} - Kết quả của tất cả các promise
 */
async function runWithConcurrencyLimit(promiseFns, concurrency) {
  const results = [];
  const executing = new Set();
  
  async function executePromise(promiseFn) {
    const p = promiseFn();
    executing.add(p);
    
    try {
      const res = await p;
      results.push(res);
    } catch (err) {
      results.push({ error: err });
    } finally {
      executing.delete(p);
    }
  }
  
  // Thực thi promise với số lượng giới hạn
  for (const promiseFn of promiseFns) {
    if (executing.size >= concurrency) {
      // Nếu đã đạt giới hạn, đợi một promise hoàn thành
      await Promise.race(executing);
    }
    
    // Thực thi promise tiếp theo
    executePromise(promiseFn);
  }
  
  // Đợi tất cả các promise đang thực thi hoàn thành
  await Promise.all(executing);
  
  return results;
}

async function processLikesWithCluster(userObjects, targetUserIds, postsCache, proxyManager, options = {}) {
  console.log(`\n>> Bắt đầu xử lý like với ClusterManager`);
  logToFile(`Bắt đầu xử lý like với ClusterManager`);
  
  const startTime = Date.now();
  const debug = !!options.debug;
  
  if (debug) {
    console.log(`>> [DEBUG] userObjects: ${userObjects.length}, targetUserIds: ${targetUserIds.length}`);
    console.log(`>> [DEBUG] ProxyManager stats:`, proxyManager.getProxyStats());
  }
  
  // Tạo phép hoán vị với thuật toán vòng tròn đơn giản
  const likeAssignments = [];
  
  // Đảm bảo mỗi user thực hiện đúng 12 lượt like (hoặc ít hơn nếu không đủ target)
  const targetLikesPerUser = 12;
  const totalLikeUsers = userObjects.length;
  const totalTargetUsers = targetUserIds.length;
  
  console.log(`>> Tạo ma trận phân phối like cho ${totalLikeUsers} user, mỗi user like ${targetLikesPerUser} lượt`);
  
  // Số lượng tác vụ đồng thời trên mỗi CPU/worker
  const concurrentTasksPerWorker = options.concurrentTasksPerWorker || 10;
  
  // Số lượng CPU sử dụng
  const numWorkers = options.numWorkers || os.cpus().length;
  
  if (debug) {
    console.log(`>> [DEBUG] Tham số xử lý:`);
    console.log(`>> [DEBUG] - CPUs: ${numWorkers}`);
    console.log(`>> [DEBUG] - Tasks per CPU: ${concurrentTasksPerWorker}`);
    console.log(`>> [DEBUG] - Total max concurrent tasks: ${numWorkers * concurrentTasksPerWorker}`);
  }
  
  // Tạo tasks để phân phối cho ClusterManager
  const tasks = [];
  const usersWithProxyCount = userObjects.filter(u => u.proxy).length;
  
  if (usersWithProxyCount < userObjects.length) {
    console.warn(`>> CẢNH BÁO: Chỉ có ${usersWithProxyCount}/${userObjects.length} user có proxy được gán!`);
    if (debug) {
      // Liệt kê 5 user đầu tiên không có proxy
      const usersWithoutProxy = userObjects.filter(u => !u.proxy).slice(0, 5);
      console.log(`>> [DEBUG] Users không có proxy (5 đầu tiên):`);
      usersWithoutProxy.forEach(u => console.log(`>> [DEBUG] - User ${u.uid}`));
    }
  }
  
  // Tạo ma trận phân phối like: mỗi user like những user nào
  // Sử dụng hoán vị vòng tròn: user i sẽ like cho các user (i+1, i+2, ..., i+12) % totalTargetUsers
  for (let i = 0; i < totalLikeUsers; i++) {
    const likeUser = userObjects[i];
    const userTasks = [];
    
    // Kiểm tra và ghi log nếu user không có proxy
    if (!likeUser.proxy && debug) {
      console.warn(`>> [DEBUG] User ${likeUser.uid} không có proxy!`);
    }
    
    // Phân phối targetLikesPerUser lượt like cho mỗi user
    for (let offset = 1; offset <= targetLikesPerUser && offset <= totalTargetUsers; offset++) {
      // Tính chỉ số của user nhận like theo modulo
      const targetIndex = (i + offset) % totalTargetUsers;
      const targetId = targetUserIds[targetIndex];
      
      // Không like chính mình
      if (likeUser.uid !== targetId) {
        // Nếu user có bài viết để like
        if (postsCache.hasPostsForUser(targetId)) {
          const postId = postsCache.getPost(targetId);
          if (postId) {
            userTasks.push({
              likeUser,
              targetUserId: targetId,
              postId
            });
          }
        }
      }
    }
    
    // Thêm tất cả tasks của user này vào danh sách tasks chung
    tasks.push(...userTasks);
  }
  
  console.log(`>> Đã tạo ${tasks.length} tác vụ like từ ${totalLikeUsers} user`);
  
  if (tasks.length === 0) {
    console.error(`>> Không có tác vụ like nào được tạo! Vui lòng kiểm tra dữ liệu đầu vào.`);
    return {
      totalAssigned: 0,
      successCount: 0,
      failCount: 0,
      duration: formatDuration(Date.now() - startTime),
      results: []
    };
  }
  
  // Khởi tạo ClusterManager 
  console.log(`>> Khởi tạo ClusterManager với ${numWorkers} workers và ${concurrentTasksPerWorker} tasks/worker...`);
  
  const clusterManager = new ClusterManager({
    numWorkers,
    concurrentTasksPerWorker,
    proxyManager // Đảm bảo truyền proxyManager vào đây
  });
  
  try {
    // Thực thi tất cả các tasks
    console.log(`>> Bắt đầu thực thi ${tasks.length} tác vụ like với ClusterManager...`);
    logToFile(`Bắt đầu thực thi ${tasks.length} tác vụ like với ClusterManager...`);
    
    // Thực thi tasks và lấy kết quả
    const results = await clusterManager.executeTasks(tasks);
    
    // Tính toán thống kê
    const endTime = Date.now();
    const totalDuration = formatDuration(endTime - startTime);
    
    // Số lượng thành công và thất bại
    const totalSuccess = results.filter(result => result.success).length;
    const totalFail = results.length - totalSuccess;
    
    console.log(`\n>> Tổng kết xử lý với ClusterManager:`);
    console.log(`>> - Tổng số lượt like thành công: ${totalSuccess}/${tasks.length}`);
    console.log(`>> - Tổng số lượt like thất bại: ${totalFail}/${tasks.length}`);
    console.log(`>> - Thời gian xử lý: ${totalDuration}`);
    logToFile(`Kết quả xử lý với ClusterManager: ${totalSuccess} thành công, ${totalFail} thất bại, thời gian: ${totalDuration}`);
    
    // Phân tích lỗi nếu có và trong chế độ debug
    if (totalFail > 0 && debug) {
      const failedResults = results.filter(result => !result.success);
      const errorCounts = {};
      
      failedResults.forEach(result => {
        const errorType = result.error || 'Unknown error';
        errorCounts[errorType] = (errorCounts[errorType] || 0) + 1;
      });
      
      console.log(`\n>> [DEBUG] Phân tích lỗi:`);
      Object.entries(errorCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .forEach(([error, count]) => {
          console.log(`>> [DEBUG] - ${error}: ${count} lần`);
        });
    }
    
    return {
      totalAssigned: tasks.length,
      successCount: totalSuccess,
      failCount: totalFail,
      duration: totalDuration,
      results
    };
    
  } finally {
    // Dọn dẹp tài nguyên
    clusterManager.cleanup();
  }
}

async function handleLikeEachOther(req) {
  // Khởi tạo các dịch vụ
  const proxyManager = new ProxyManager();
  const postsCache = new PostsCache();
  const startTime = Date.now();
  
  logToFile(`======= BẮT ĐẦU TIẾN TRÌNH LIKE CHÉO =======`);
  logToFile(`Phiên bản: v4.0 (Cải tiến tốc độ với thuật toán hoán vị vòng tròn và xử lý song song)`);
  
  try {
    // Khởi tạo ProxyManager và thiết lập cho apiClient
    logToFile(`Khởi tạo ProxyManager...`);
    await proxyManager.initialize();
    apiClient.setGlobalProxyManager(proxyManager);
    
    // Cấu hình cố định cho 2.000 tài khoản
    const FIXED_CONFIG = {
      userCount: 2000,           // Số lượng tài khoản xử lý
      useExcelUsers: true,
      excelPath: '',
      targetCount: 2000,
      onlyActive: true,
      numCpus: 8,                // Số CPU sử dụng
      tasksPerCpu: 6,            // Tác vụ đồng thời mỗi CPU
      concurrencyLimit: 48,      // Tổng số luồng đồng thời
      masterBatchSize: 200,      // Số tài khoản trong mỗi lô lớn
      subBatchSize: 12,          // Số lượt like mỗi tài khoản thực hiện
      parallelBatches: 10,       // Số lô nhỏ chạy song song
      debug: true
    };
    
    // Sử dụng cấu hình cố định, bỏ qua thông số đầu vào
    // Chỉ lấy excelPath từ tham số đầu vào nếu có
    let requestConfig = FIXED_CONFIG;
    
    // Nếu req là một số, nghĩa là được gọi từ main.js
    if (typeof req === 'number') {
      logToFile(`Được gọi từ giao diện với ${req} tài khoản, áp dụng cấu hình cố định cho 2.000 tài khoản`);
    } 
    // Nếu req là object có chứa body (từ API)
    else if (req && req.body) {
      // Chỉ lấy đường dẫn file Excel nếu có
      if (req.body.excelPath) {
        requestConfig.excelPath = req.body.excelPath;
      }
      logToFile(`Được gọi từ API, áp dụng cấu hình cố định cho 2.000 tài khoản`);
    }
    
    // Thông số batch từ cấu hình cố định
    const MASTER_BATCH_SIZE = requestConfig.masterBatchSize;
    const SUB_BATCH_SIZE = requestConfig.subBatchSize;
    const PARALLEL_BATCHES = requestConfig.parallelBatches;
    
    console.log(`>> THÔNG SỐ BATCH CỐ ĐỊNH CHO 2.000 TÀI KHOẢN:`);
    console.log(`>> - Master batch size: ${MASTER_BATCH_SIZE} users/batch`);
    console.log(`>> - Sub-batch size: ${SUB_BATCH_SIZE} users/sub-batch`);
    console.log(`>> - Số lượng batch song song: ${PARALLEL_BATCHES}`);
    console.log(`>> - Số luồng đồng thời tối đa: ${requestConfig.concurrencyLimit}`);
    
    logToFile(`Thông số batch: Master=${MASTER_BATCH_SIZE}, Sub=${SUB_BATCH_SIZE}, Song song=${PARALLEL_BATCHES}, Luồng=${requestConfig.concurrencyLimit}`);
    
    // Step 1: Đọc danh sách user từ Excel
    console.log(`\n>> STEP 1: Đọc danh sách user từ Excel...`);
    
    // Đường dẫn tương đối đến file Excel
    const dataDir = path.join(__dirname, '../data');
    // Sử dụng đường dẫn excelPath nếu có, ngược lại tìm file Excel trong thư mục data
    const excelFilePath = requestConfig.excelPath || path.join(dataDir, 'PI.xlsx');
    
    console.log(`>> Đọc danh sách user từ file: ${excelFilePath}`);
    
    // Tạo đối tượng ExcelReaderService với đường dẫn file
    const excelReader = new ExcelReaderService(excelFilePath);
    
    // Đọc tất cả dữ liệu từ file Excel
    const excelData = excelReader.readAllSheets();
    
    // Dựa vào cấu trúc cũ, lấy dữ liệu từ các cột cần thiết
    const uid = excelData["prxageng"]?.["uid"] || [];
    const piname = excelData["prxageng"]?.["piname"] || [];
    const ukey = excelData["prxageng"]?.["ukey"] || [];
    const proxy = excelData["prxageng"]?.["proxy"] || [];
    
    // Tạo danh sách user từ dữ liệu Excel 
    const users = [];
    for (let i = 0; i < uid.length; i++) {
      if (uid[i] && ukey[i] && piname[i]) {
        const proxyInfo = proxy[i] ? proxy[i].split(':') : null;
        
        users.push({
          uid: uid[i],
          ukey: ukey[i],
          piname: piname[i],
          active: true, // Giả định tất cả user đều active
          proxy: proxyInfo ? {
            host: proxyInfo[0],
            port: proxyInfo[1],
            name: proxyInfo[2],
            password: proxyInfo[3]
          } : null
        });
      }
    }
    
    logToFile(`STEP 1: Đọc được ${users.length} user từ Excel`);
    
    // Nếu không có user, kết thúc sớm
    if (users.length === 0) {
      logToFile(`Lỗi: Không tìm thấy dữ liệu user từ Excel`);
      return {
        success: false,
        message: "Không tìm thấy dữ liệu user từ file Excel",
      };
    }
    
    // Thêm proxy từ Excel vào pool nếu có
    const excelProxies = users
      .filter(user => user.proxy)
      .map(user => user.proxy);
    
    if (excelProxies.length > 0) {
      console.log(`>> Thêm ${excelProxies.length} proxy từ Excel vào pool`);
      proxyManager.addExcelProxies(excelProxies);
    }
    
    // Step 2: Lọc user theo các tiêu chí
    console.log(`\n>> STEP 2: Lọc danh sách user...`);
    
    // Lọc user có đủ thông tin để đăng nhập
    const validUsers = users.filter(user => user.uid && user.ukey && user.piname);
    
    // Lọc tiếp theo các tiêu chí khác nếu có (ví dụ: onlyActive)
    let filteredUsers = validUsers;
    if (requestConfig.onlyActive) {
      filteredUsers = validUsers.filter(user => user.active !== "0" && user.active !== false);
    }
    
    console.log(`>> Lọc được ${filteredUsers.length}/${users.length} user hợp lệ`);
    logToFile(`STEP 2: Lọc được ${filteredUsers.length}/${users.length} user hợp lệ`);
    
    // Step 3: Chọn số lượng user theo yêu cầu
    console.log(`\n>> STEP 3: Chọn ${requestConfig.userCount} user để thực hiện like...`);
    // Lấy ngẫu nhiên userCount user để thực hiện like
    const selectedUsers = filteredUsers.length > requestConfig.userCount ? 
      getRandomUsers(filteredUsers, requestConfig.userCount) : filteredUsers;
    
    console.log(`>> Đã chọn ${selectedUsers.length} user để thực hiện like`);
    logToFile(`STEP 3: Đã chọn ${selectedUsers.length} user để thực hiện like`);
    
    // Step 4: Chọn user để like (target)
    console.log(`\n>> STEP 4: Chọn ${requestConfig.targetCount} user để được like...`);
    
    // Lấy uid của tất cả user đã lọc để làm đối tượng được like
    const allUserIds = filteredUsers.map(user => user.uid);
    
    // Lấy ngẫu nhiên targetCount user để nhận like
    let targetUserIds = allUserIds.length > requestConfig.targetCount ? 
      getRandomUsers(allUserIds, requestConfig.targetCount) : allUserIds;
    
    console.log(`>> Đã chọn ${targetUserIds.length} user để được like`);
    logToFile(`STEP 4: Đã chọn ${targetUserIds.length} user để được like`);
    
    // Step 5: Tạo đối tượng chi tiết cho user
    console.log(`\n>> STEP 5: Chuẩn bị thông tin chi tiết của user...`);
    
    // Map user objects cho tất cả user (để sau này dễ tìm)
    const userMap = new Map();
    for (const user of users) {
      if (user.uid) {
        userMap.set(user.uid, user);
      }
    }
    
    // Lấy thông tin chi tiết cho các user được chọn để thực hiện like
    const userObjects = selectedUsers.map(user => ({
      uid: user.uid,
      ukey: user.ukey,
      piname: user.piname,
      proxy: user.proxy || null
    }));
    
    // Lấy thông tin chi tiết cho các user được chọn để nhận like
    const targetUserObjects = targetUserIds.map(uid => {
      const fullUser = userMap.get(uid);
      if (fullUser) {
        return {
          uid: fullUser.uid,
          ukey: fullUser.ukey,
          piname: fullUser.piname,
          proxy: fullUser.proxy || null
        };
      }
      return { uid };
    });
    
    console.log(`>> Đã chuẩn bị thông tin cho ${userObjects.length} user thực hiện like`);
    console.log(`>> Đã chuẩn bị thông tin cho ${targetUserObjects.length} user được like`);
    logToFile(`STEP 5: Chuẩn bị thông tin cho ${userObjects.length} user thực hiện like và ${targetUserObjects.length} user được like`);
    
    // Step 6: Lấy bài viết của tất cả user để làm dữ liệu like
    console.log(`\n>> STEP 6: Lấy bài viết của các user...`);
    logToFile(`STEP 6: Bắt đầu lấy bài viết của ${targetUserObjects.length} users`);
    
    // Sử dụng BatchFetcher để lấy bài viết hiệu quả
    const batchFetcher = new BatchFetcher(postsCache, proxyManager);
    
    // Lấy bài viết của tất cả target user
    await batchFetcher.fetchAllPostsInBatches(targetUserObjects);
    
    // Kiểm tra kết quả
    const postsStatus = batchFetcher.checkPostsAvailability(targetUserIds);
    console.log(`\n>> Kết quả lấy bài viết: ${postsStatus.available}/${postsStatus.total} user có bài viết`);
    logToFile(`Kết quả lấy bài viết: ${postsStatus.available}/${postsStatus.total} user có bài viết`);
    
    // Thử lấy lại bài viết cho các user chưa có
    if (postsStatus.missing > 0) {
      console.log(`\n>> Thử lấy lại bài viết cho ${postsStatus.missing} user...`);
      logToFile(`Thử lấy lại bài viết cho ${postsStatus.missing} user...`);
      
      await batchFetcher.retryMissingPosts(postsStatus.usersWithoutPosts, targetUserObjects);
      
      // Kiểm tra lại kết quả
      const finalPostsStatus = batchFetcher.checkPostsAvailability(targetUserIds);
      console.log(`\n>> Kết quả cuối cùng: ${finalPostsStatus.available}/${finalPostsStatus.total} user có bài viết`);
      logToFile(`Kết quả cuối cùng: ${finalPostsStatus.available}/${finalPostsStatus.total} user có bài viết`);
      
      // Cập nhật lại danh sách targetUserIds chỉ giữ các user có bài viết
      targetUserIds = finalPostsStatus.usersWithPosts;
    }
    
    // STEP 6.5: Thiết lập proxy cho các user
    console.log(`\n>> STEP 6.5: Thiết lập proxy cho các user...`);
    logToFile(`STEP 6.5: Thiết lập proxy cho các user...`);
    
    // Gán proxy cho từng user để đảm bảo mỗi user đều có proxy
    const userAssignments = proxyManager.assignProxiesToUsers(userObjects);
    
    // Cập nhật thông tin proxy cho các user
    userAssignments.forEach(assignment => {
      const userIndex = userObjects.findIndex(u => u.uid === assignment.user.uid);
      if (userIndex !== -1) {
        userObjects[userIndex].proxy = assignment.proxy;
      }
    });
    
    console.log(`>> Đã gán proxy cho ${userAssignments.length}/${userObjects.length} user`);
    logToFile(`Đã gán proxy cho ${userAssignments.length}/${userObjects.length} user`);
    
    // Hiển thị thống kê proxy
    const proxyStats = proxyManager.getProxyStats();
    console.log(`>> Thống kê proxy: ${proxyStats.active}/${proxyStats.total} proxy hoạt động, ${proxyStats.fromExcel} từ Excel, ${proxyStats.fromRotating} từ rotating`);
    
    // Thay thế Step 7 cũ với phần xử lý hiệu quả hơn sử dụng ClusterManager
    console.log(`\n>> STEP 7: Phân phối và thực hiện like song song với Cluster...`);
    logToFile(`STEP 7: Bắt đầu phân phối và thực hiện like song song với Cluster...`);
    
    // Xử lý like với ClusterManager
    const clusterResult = await processLikesWithCluster(
      userObjects, 
      targetUserIds, 
      postsCache, 
      proxyManager, 
      {
        numWorkers: requestConfig.numCpus,
        concurrentTasksPerWorker: requestConfig.tasksPerCpu,
        debug: requestConfig.debug
      }
    );
    
    console.log(`\n>> Kết quả cuối cùng: ${clusterResult.successCount} lượt like thành công, ${clusterResult.failCount} lượt thất bại`);
    console.log(`>> Thời gian chạy: ${clusterResult.duration}`);
    
    logToFile(`====== KẾT QUẢ CUỐI CÙNG ======`);
    logToFile(`Thành công: ${clusterResult.successCount} lượt like | Thất bại: ${clusterResult.failCount} lượt like`);
    logToFile(`Thời gian chạy: ${clusterResult.duration}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH LIKE CHÉO =======`);

    return { 
      success: clusterResult.successCount > 0,
      message: `Đã like ${clusterResult.successCount}/${clusterResult.totalAssigned} lượt thành công!`,
      stats: {
        total: clusterResult.totalAssigned,
        success: clusterResult.successCount,
        failure: clusterResult.failCount,
        runtime: clusterResult.duration
      }
    };
  } catch (error) {
    console.error(`❌ Lỗi không xử lý được: ${error.message}`);
    console.error(error.stack);
    
    logToFile(`====== LỖI NGHIÊM TRỌNG ======`);
    logToFile(`Lỗi: ${error.message}`);
    logToFile(`Stack: ${error.stack}`);
    logToFile(`======= KẾT THÚC TIẾN TRÌNH LIKE CHÉO (LỖI) =======`);
    
    return {
      success: false,
      message: `Đã xảy ra lỗi khi likeEachOther: ${error.message}`,
      error: error.toString(),
      stack: error.stack
    };
  } finally {
    // Dọn dẹp tài nguyên
    if (taskQueue) {
      taskQueue.destroy();
    }
    
    proxyManager.stop();
    console.log('>> Đã dừng tất cả services');
    logToFile('Đã dừng tất cả services');
  }
}

module.exports = handleLikeEachOther;
