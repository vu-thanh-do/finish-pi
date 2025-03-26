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
const { user, articleId } = workerData;

// Function ghi log
function log(message, isError = false) {
  if (parentPort) {
    parentPort.postMessage({ type: isError ? 'error' : 'log', message });
  } else {
    isError ? console.error(message) : console.log(message);
  }
}

// Kiểm tra dữ liệu cần thiết
if (!user || !user.piname || !user.uid || !articleId) {
  log(`Thiếu thông tin cần thiết cho like (user: ${user?.uid}, article: ${articleId})`, true);
  parentPort.postMessage({ 
    success: false, 
    error: "Thiếu thông tin cần thiết cho like",
    userId: user?.uid || null,
    articleId: articleId || null
  });
} else {
  // Thực hiện like
  performLike()
    .then(result => {
      parentPort.postMessage(result);
    })
    .catch(error => {
      log(`Lỗi khi like (worker): ${error.message}`, true);
      parentPort.postMessage({ 
        success: false, 
        error: error.message,
        userId: user.uid,
        articleId
      });
    });
}

// Function để like bài viết
async function performLike() {
  const maxRetries = 3;
  let retryCount = 0;
  let currentUrlVariantIndex = 0;
  const urlVariants = ['/vapi', '/vapi/', 'vapi', '/api']; // Các biến thể URL để thử
  
  while (retryCount <= maxRetries) {
    try {
      if (retryCount > 0) {
        log(`Thử lại lần ${retryCount}/${maxRetries} cho like bài ${articleId}`);
        // Tăng thời gian chờ mỗi lần retry
        await new Promise(resolve => setTimeout(resolve, 1000 * retryCount));
        
        // Thử với các biến thể URL khác nhau nếu gặp lỗi 404
        currentUrlVariantIndex = (currentUrlVariantIndex + 1) % urlVariants.length;
        log(`Sử dụng URL: ${urlVariants[currentUrlVariantIndex]}`);
      }
      
      // Cấu hình axios
      const config = {
        baseURL: 'https://pivoice.app',
        timeout: 10000, // 10 giây
        headers: {
          'User-Agent': user.userAgent || getRandomUserAgent(),
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Origin': 'https://pivoice.app',
          'Referer': 'https://pivoice.app/'
        }
      };
      
      // Thêm proxy nếu có
      if (user.proxy) {
        try {
          const proxyUrl = `http://${user.proxy.name}:${user.proxy.password}@${user.proxy.host}:${user.proxy.port}`;
          config.httpsAgent = new HttpsProxyAgent(proxyUrl);
          config.proxy = false; // Disable axios proxy để tránh xung đột
          log(`Sử dụng proxy: ${user.proxy.host}:${user.proxy.port} (${user.piname})`);
        } catch (proxyError) {
          log(`Lỗi khi cấu hình proxy: ${proxyError.message}`, true);
          // Tiếp tục mà không có proxy
          log(`Tiếp tục mà không có proxy cho user ${user.piname}`);
        }
      } else {
        log(`Không có proxy cho user ${user.piname}, tiếp tục không dùng proxy`);
      }
      
      // Tạo payload cho like
      const payload = qs.stringify({
        component: "article",
        action: "like",
        aid: articleId,
        user_name: user.piname,
        uid: user.uid,
        ukey: user.ukey,
        english_version: 0,
        selected_country: 1,
        selected_chain: 0
      });
      
      // Tạo client axios
      const client = axios.create(config);
      
      // Gửi request like
      log(`User ${user.piname} đang like bài ${articleId}...`);
      const requestId = `req-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
      log(`>> [${requestId}] Gọi API: https://pivoice.app${urlVariants[currentUrlVariantIndex]}`);
      
      if (user.proxy) {
        log(`>> [${requestId}] Sử dụng proxy: ${user.proxy.host}:${user.proxy.port} (user)`);
      }
      
      const startTime = Date.now();
      // Sử dụng URL đã chọn
      const response = await client.post(urlVariants[currentUrlVariantIndex], payload);
      const requestTime = Date.now() - startTime;
      
      log(`>> [${requestId}] Nhận response ${response.status}, thời gian: ${requestTime}ms`);
      
      // Ghi log đầy đủ response data để debug
      try {
        log(`>> [${requestId}] Response data: ${JSON.stringify(response.data)}`);
      } catch (err) {
        log(`>> [${requestId}] Không thể ghi log response data: ${err.message}`);
      }
      
      // Kiểm tra kết quả - các phản hồi thành công có thể khác nhau
      if ((response.data && response.data.data) || // Mẫu phản hồi #1
          (response.data && response.data.time) ||  // Mẫu phản hồi #2
          (response.data && response.data.hasOwnProperty('data'))) { // Mẫu phản hồi #3
        
        log(`✅ User ${user.piname} đã like thành công bài ${articleId}`);
        return { 
          success: true, 
          userId: user.uid,
          articleId,
          message: "Đã like thành công"
        };
      } else {
        // Xử lý các trường hợp đã like trước đó
        if (response.data && response.data.message && (
            response.data.message.includes("đã like") || 
            response.data.message.includes("already") ||
            response.data.message.includes("Đã like")
        )) {
          log(`ℹ️ User ${user.piname} đã like bài ${articleId} trước đó`);
          return { 
            success: true, 
            userId: user.uid,
            articleId,
            message: "Đã like trước đó"
          };
        }
        
        const errorMsg = (response.data && response.data.message) ? response.data.message : "Like không thành công";
        log(`❌ Like bài ${articleId} không thành công: ${errorMsg}`, true);
        
        // Log thêm thông tin debug chi tiết
        if (response.data) {
          log(`Full response: ${JSON.stringify(response.data)}`, true);
        }
        
        // Tiếp tục retry nếu có lỗi nào đó
        retryCount++;
        continue;
      }
    } catch (error) {
      log(`❌ Lỗi khi like bài ${articleId}: ${error.message}`, true);
      
      if (error.response) {
        const statusCode = error.response.status;
        log(`Mã lỗi: ${statusCode}`, true);
        
        // Retry với các lỗi mạng/server
        if ([404, 429, 500, 502, 503, 504].includes(statusCode)) {
          retryCount++;
          if (retryCount <= maxRetries) {
            const delayTime = statusCode === 429 ? 10000 : 3000 * retryCount;
            log(`Sẽ thử lại sau ${delayTime/1000} giây...`);
            
            // Nếu lỗi 404, thử đổi endpoint URL
            if (statusCode === 404) {
              currentUrlVariantIndex = (currentUrlVariantIndex + 1) % urlVariants.length;
              log(`Sẽ thử với biến thể URL mới: ${urlVariants[currentUrlVariantIndex]}`);
            }
            
            await new Promise(resolve => setTimeout(resolve, delayTime));
            continue;
          }
        }
      }
      
      // Nếu là lỗi timeout hoặc lỗi mạng khác, cũng retry
      if (error.code === 'ECONNABORTED' || error.message.includes('timeout') || 
          error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET') {
        retryCount++;
        if (retryCount <= maxRetries) {
          await new Promise(resolve => setTimeout(resolve, 3000 * retryCount));
          continue;
        }
      }
      
      return { 
        success: false, 
        userId: user.uid,
        articleId,
        error: error.message
      };
    }
  }
  
  return { 
    success: false, 
    userId: user.uid,
    articleId,
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