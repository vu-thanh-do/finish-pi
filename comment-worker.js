const { parentPort, workerData } = require('worker_threads');
const axios = require('axios');
const qs = require("qs");

// Sử dụng try-catch để xử lý các cách import khác nhau
let HttpsProxyAgent;
try {
  // Cách 1: import cho phiên bản mới (>=5.0.0)
  const proxyAgentModule = require('https-proxy-agent');
  if (proxyAgentModule.HttpsProxyAgent) {
    HttpsProxyAgent = proxyAgentModule.HttpsProxyAgent;
  } else {
    HttpsProxyAgent = proxyAgentModule;
  }
} catch (error) {
  console.error('Không thể import HttpsProxyAgent:', error.message);
  
  if (parentPort) {
    parentPort.postMessage({
      success: false,
      error: `Lỗi import HttpsProxyAgent: ${error.message}`
    });
  }
  
  process.exit(1);
}

// Nhận dữ liệu từ luồng chính
const { commentUser, postId, proxy, commentText } = workerData;

// Function ghi log
function log(message, isError = false) {
  if (parentPort) {
    parentPort.postMessage({ type: isError ? 'error' : 'log', message });
  } else {
    isError ? console.error(message) : console.log(message);
  }
}

// Kiểm tra dữ liệu cần thiết
if (!commentUser || !postId || !commentText) {
  log(`Thiếu thông tin cần thiết cho comment (user: ${commentUser?.uid}, postId: ${postId})`, true);
  parentPort.postMessage({ 
    success: false, 
    error: "Thiếu thông tin cần thiết cho comment"
  });
} else {
  // Thực hiện comment
  performComment()
    .then(result => {
      parentPort.postMessage(result);
    })
    .catch(error => {
      log(`Lỗi khi comment (worker): ${error.message}`, true);
      parentPort.postMessage({ 
        success: false, 
        error: error.message,
        userId: commentUser.uid,
        postId
      });
    });
}

// Function để comment bài viết
async function performComment() {
  const maxRetries = 3;
  let retryCount = 0;
  
  while (retryCount <= maxRetries) {
    try {
      if (retryCount > 0) {
        log(`Thử lại lần ${retryCount}/${maxRetries} cho comment bài ${postId}`);
        // Tăng thời gian chờ mỗi lần retry
        await new Promise(resolve => setTimeout(resolve, 1000 * retryCount));
      }
      
      // Cấu hình axios
      const config = {
        timeout: 10000, // 10 giây
        headers: {
          'User-Agent': getRandomUserAgent(),
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Origin': 'https://pivoice.app',
          'Referer': 'https://pivoice.app/'
        }
      };
      
      // Thêm proxy nếu có
      if (proxy) {
        try {
          const proxyUrl = `http://${proxy.name}:${proxy.password}@${proxy.host}:${proxy.port}`;
          config.httpsAgent = new HttpsProxyAgent(proxyUrl);
          config.proxy = false; // Disable axios proxy để tránh xung đột
          log(`Sử dụng proxy: ${proxy.host}:${proxy.port} cho user ${commentUser.piname}`);
        } catch (proxyError) {
          log(`Lỗi khi cấu hình proxy: ${proxyError.message}`, true);
          // Tiếp tục mà không có proxy
          log(`Tiếp tục mà không có proxy cho user ${commentUser.piname}`);
        }
      } else {
        log(`Không có proxy cho user ${commentUser.piname}, tiếp tục không dùng proxy`);
      }
      
      // Tạo payload cho comment - API yêu cầu commentText, không phải content
      const payload = qs.stringify({
        component: "article",
        action: "comment",
        aid: postId,
        commentText: commentText, // Tham số đúng là commentText, không phải content
        user_name: commentUser.piname,
        uid: commentUser.uid,
        english_version: 0,
        selected_country: 1
      });
      
      // Log thông tin payload để kiểm tra
      log(`Payload: ${JSON.stringify(payload)}`);
      
      // Tạo client axios
      const client = axios.create(config);
      
      // Gửi comment
      log(`User ${commentUser.piname} đang comment bài ${postId}: "${commentText.substring(0, 20)}..."`);
      const response = await client.post('https://pivoice.app/vapi', payload);
      
      // Log response để debug
      log(`Response: ${JSON.stringify(response.data)}`);
      
      // Kiểm tra kết quả
      if (response.data && (response.data.status === 1 || response.data.time)) {
        log(`✅ User ${commentUser.piname} đã comment thành công bài ${postId}`);
        return { 
          success: true, 
          userId: commentUser.uid,
          postId,
          message: "Đã comment thành công"
        };
      } else {
        const errorMsg = response.data.message || JSON.stringify(response.data);
        log(`❌ Comment bài ${postId} không thành công: ${errorMsg}`, true);
        
        // Nếu lỗi liên quan đến hạn chế bình luận, tiếp tục retry
        if (response.data.status === 0 && response.data.message && 
            (response.data.message.includes("limit") || 
             response.data.message.includes("frequency") || 
             response.data.message.includes("too many"))) {
          retryCount++;
          continue;
        }
        
        return { 
          success: false, 
          userId: commentUser.uid,
          postId,
          error: errorMsg
        };
      }
    } catch (error) {
      log(`❌ Lỗi khi comment bài ${postId}: ${error.message}`, true);
      
      if (error.response) {
        const statusCode = error.response.status;
        log(`Mã lỗi: ${statusCode}`, true);
        
        // Retry với các lỗi mạng/server
        if ([404, 429, 500, 502, 503, 504].includes(statusCode)) {
          retryCount++;
          if (retryCount <= maxRetries) {
            const delayTime = statusCode === 429 ? 5000 : 2000 * retryCount;
            log(`Sẽ thử lại sau ${delayTime/1000} giây...`);
            continue;
          }
        }
      }
      
      // Nếu là lỗi timeout hoặc lỗi mạng khác, cũng retry
      if (error.code === 'ECONNABORTED' || error.message.includes('timeout') || 
          error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET') {
        retryCount++;
        if (retryCount <= maxRetries) {
          continue;
        }
      }
      
      return { 
        success: false, 
        userId: commentUser.uid,
        postId,
        error: error.message
      };
    }
  }
  
  return { 
    success: false, 
    userId: commentUser.uid,
    postId,
    error: "Đã hết số lần thử lại"
  };
}

// Hàm tạo User-Agent ngẫu nhiên
function getRandomUserAgent() {
  const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
  ];
  return userAgents[Math.floor(Math.random() * userAgents.length)];
} 