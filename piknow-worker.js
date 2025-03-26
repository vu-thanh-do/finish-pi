const { parentPort, workerData } = require('worker_threads');
const axios = require('axios');
const qs = require("qs");
const { HttpsProxyAgent } = require('https-proxy-agent');

/**
 * Gửi log từ worker về thread chính
 * @param {string} message - Nội dung log
 * @param {string} type - Loại log (info, error, debug)
 */
function workerLog(message, type = 'info') {
  if (parentPort) {
    parentPort.postMessage({ type: 'log', logType: type, message });
  } else {
    console[type === 'error' ? 'error' : 'log'](message);
  }
}

/**
 * Gửi kết quả về thread chính
 * @param {Object} data - Dữ liệu kết quả
 */
function sendResult(data) {
  if (parentPort) {
    parentPort.postMessage({ ...data });
  } else {
    console.log('Kết quả:', data);
  }
}

/**
 * Gửi thông báo lỗi về thread chính
 * @param {Error|string} error - Lỗi
 * @param {Object} details - Chi tiết bổ sung
 */
function reportError(error, details = {}) {
  const errorMessage = typeof error === 'string' ? error : error.message || 'Lỗi không xác định';
  workerLog(`❌ Lỗi: ${errorMessage}`, 'error');
  
  sendResult({
    success: false,
    error: errorMessage,
    stack: error.stack,
    ...details
  });
}

// Khởi tạo dữ liệu từ thread chính
const { piknowUser, knowId, proxy, piknowText } = workerData || {};

// Danh sách tin nhắn mặc định nếu không có nội dung
const DEFAULT_MESSAGES = [
  "Rất hay và bổ ích!",
  "Thông tin quá tuyệt vời!",
  "Cảm ơn vì kiến thức này!",
  "Tôi rất thích nội dung của bạn",
  "Thật sự hữu ích!",
  "Tiếp tục cung cấp những kiến thức như vậy!",
  "Rất thú vị!",
  "Câu trả lời hay quá!",
  "Tôi học được nhiều điều từ bạn",
  "Kiến thức được trình bày rất rõ ràng",
  "Thông tin hữu ích!",
  "Tuyệt vời!"
];

// Kiểm tra dữ liệu đầu vào
if (!piknowUser || !knowId) {
  reportError('Thiếu thông tin cần thiết cho PiKnow', {
    userId: piknowUser?.uid,
    knowId
  });
} else {
  // Thực hiện piknow
  performPiKnow()
    .then(result => sendResult(result))
    .catch(error => reportError(error, {
      userId: piknowUser?.uid,
      knowId
    }));
}

/**
 * Thực hiện piknow bài viết
 */
async function performPiKnow() {
  const MAX_RETRIES = 3;
  let retryCount = 0;
  let lastError = null;
  
  // Kiểm tra và chuẩn bị nội dung PiKnow
  let finalPiknowText = piknowText;
  if (!piknowText || piknowText.includes('undefined') || piknowText === ', ...' || piknowText.trim() === '') {
    // Nếu không có nội dung hoặc nội dung không hợp lệ, chọn nội dung mặc định
    const randomIndex = Math.floor(Math.random() * DEFAULT_MESSAGES.length);
    finalPiknowText = DEFAULT_MESSAGES[randomIndex];
    workerLog(`⚠️ Phát hiện nội dung không hợp lệ "${piknowText}", đã chuyển sang nội dung mặc định`, 'debug');
  }
  
  workerLog(`🔍 Nội dung PiKnow cuối cùng: "${finalPiknowText.substring(0, 30)}${finalPiknowText.length > 30 ? '...' : ''}"`, 'debug');
  
  // Thử lại nhiều lần
  while (retryCount <= MAX_RETRIES) {
    try {
      if (retryCount > 0) {
        const delayTime = 2000 * Math.pow(1.5, retryCount);
        workerLog(`🔄 Thử lại lần ${retryCount}/${MAX_RETRIES} cho PiKnow bài ${knowId} sau ${delayTime/1000}s...`);
        await new Promise(resolve => setTimeout(resolve, delayTime));
      }
      
      // Cấu hình axios
      const config = {
        timeout: 20000, // Tăng thời gian chờ lên 20 giây
        headers: {
          'User-Agent': getRandomUserAgent(),
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
          'Origin': 'https://pivoice.app',
          'Referer': 'https://pivoice.app/'
        }
      };
      
      // Thêm proxy nếu có
      if (proxy) {
        if (!proxy.host || !proxy.port) {
          workerLog(`⚠️ Proxy không hợp lệ: thiếu host hoặc port`, 'error');
        } else {
          try {
            // Tạo URL proxy
            const proxyAuth = proxy.name && proxy.password 
              ? `${proxy.name}:${proxy.password}@` 
              : '';
            const proxyUrl = `http://${proxyAuth}${proxy.host}:${proxy.port}`;
            
            // Tạo agent
            config.httpsAgent = new HttpsProxyAgent(proxyUrl);
            config.proxy = false; // Tắt proxy mặc định của axios
            
            workerLog(`🔌 Sử dụng proxy: ${proxy.host}:${proxy.port} cho user ${piknowUser.piname}`);
          } catch (proxyError) {
            workerLog(`⚠️ Lỗi cấu hình proxy: ${proxyError.message}`, 'error');
            // Tiếp tục không có proxy nếu có lỗi
          }
        }
      } else {
        workerLog(`⚠️ Không có proxy cho PiKnow bài ${knowId}`, 'debug');
      }
      
      // Tạo payload
      const payload = qs.stringify({
        component: "know",
        action: "answer",
        message: finalPiknowText,
        user_name: piknowUser.piname,
        know_id: knowId,
        english_version: 0,
        selected_country: 1,
        selected_chain: 0
      });
      
      // Log thông tin request
      workerLog(`📤 User ${piknowUser.piname} PiKnow bài ${knowId}: "${finalPiknowText.substring(0, 20)}..."`);
      
      // Gửi request
      const response = await axios.post('https://pivoice.app/vapi', payload, config);
      
      // Kiểm tra response
      workerLog(`📥 Response status: ${response.status}`);
      
      // Kiểm tra kết quả thành công
      if (response.data && (response.data.status === 1 || response.data.time)) {
        workerLog(`✅ User ${piknowUser.piname} đã PiKnow thành công bài ${knowId}`);
        return {
          success: true,
          userId: piknowUser.uid,
          knowId,
          message: "PiKnow thành công"
        };
      } else {
        // Kiểm tra các loại lỗi từ API
        const errorMsg = response.data.message || JSON.stringify(response.data);
        workerLog(`❌ PiKnow bài ${knowId} không thành công: ${errorMsg}`, 'error');
        
        // Xử lý các lỗi cụ thể
        if (response.data.status === 0) {
          if (errorMsg.includes("limit") || 
              errorMsg.includes("frequency") || 
              errorMsg.includes("too many") ||
              errorMsg.includes("rate")) {
            
            // Lỗi liên quan rate limit
            workerLog(`⏱️ Gặp rate limit, đợi thêm thời gian...`);
            retryCount++;
            lastError = new Error(errorMsg);
            
            // Gửi thông báo về rate limit cho cluster manager
            if (parentPort) {
              parentPort.postMessage({ 
                type: 'rateLimit', 
                proxy: proxy ? { host: proxy.host, port: proxy.port } : null
              });
            }
            
            // Đợi lâu hơn với lỗi rate limit
            const backoffTime = Math.min(15000, 5000 * Math.pow(2, retryCount - 1));
            await new Promise(resolve => setTimeout(resolve, backoffTime));
            continue;
          }
        }
        
        return {
          success: false,
          userId: piknowUser.uid,
          knowId,
          error: errorMsg
        };
      }
    } catch (error) {
      lastError = error;
      workerLog(`❌ Lỗi khi PiKnow bài ${knowId}: ${error.message}`, 'error');
      
      // Xử lý lỗi HTTP
      if (error.response) {
        const statusCode = error.response.status;
        workerLog(`🔢 Mã lỗi HTTP: ${statusCode}`, 'error');
        
        if (error.response.data) {
          workerLog(`📄 Response data: ${JSON.stringify(error.response.data)}`, 'debug');
        }
        
        // Xử lý lỗi 429 (too many requests)
        if (statusCode === 429) {
          retryCount++;
          
          // Gửi thông báo về rate limit cho cluster manager
          if (parentPort) {
            parentPort.postMessage({ 
              type: 'rateLimit', 
              proxy: proxy ? { host: proxy.host, port: proxy.port } : null
            });
          }
          
          if (retryCount <= MAX_RETRIES) {
            // Đợi thời gian dài hơn đối với lỗi 429
            const backoffTime = 8000 * Math.pow(2, retryCount - 1);
            workerLog(`⏱️ Gặp lỗi 429, sẽ đợi ${backoffTime/1000}s trước khi thử lại...`);
            await new Promise(resolve => setTimeout(resolve, backoffTime));
            continue;
          }
        }
        // Các lỗi server khác
        else if ([500, 502, 503, 504].includes(statusCode)) {
          retryCount++;
          
          if (retryCount <= MAX_RETRIES) {
            const delayTime = 3000 * retryCount;
            workerLog(`🔄 Lỗi server ${statusCode}, thử lại sau ${delayTime/1000}s...`);
            await new Promise(resolve => setTimeout(resolve, delayTime));
            continue;
          }
        }
        // Lỗi không tìm thấy
        else if (statusCode === 404) {
          return {
            success: false,
            userId: piknowUser.uid,
            knowId,
            error: `Resource not found: ${error.message}`
          };
        }
        // Lỗi xác thực proxy
        else if (statusCode === 407) {
          // Thông báo về lỗi proxy cho cluster manager
          if (parentPort && proxy) {
            parentPort.postMessage({ 
              type: 'proxyError', 
              proxy: { host: proxy.host, port: proxy.port },
              statusCode: 407
            });
          }
          
          return {
            success: false,
            userId: piknowUser.uid,
            knowId,
            error: `Proxy authentication failed (407)`,
            proxyHost: proxy?.host,
            proxyPort: proxy?.port
          };
        }
        // Các lỗi khác
        else {
          retryCount++;
          
          if (retryCount <= MAX_RETRIES) {
            await new Promise(resolve => setTimeout(resolve, 2000 * retryCount));
            continue;
          }
        }
      } 
      // Lỗi không có response
      else if (error.request) {
        workerLog(`📶 Không nhận được phản hồi từ server`, 'error');
        retryCount++;
        
        if (retryCount <= MAX_RETRIES) {
          await new Promise(resolve => setTimeout(resolve, 2000 * retryCount));
          continue;
        }
      }
      // Lỗi kết nối
      else if (error.code === 'ECONNABORTED' || 
               error.code === 'ETIMEDOUT' || 
               error.code === 'ECONNREFUSED' || 
               error.code === 'ECONNRESET' ||
               error.message.includes('timeout') ||
               error.message.includes('abort')) {
        
        workerLog(`🔌 Lỗi kết nối: ${error.code || error.message}`, 'error');
        
        // Thông báo về lỗi proxy
        if (parentPort && proxy && (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT')) {
          parentPort.postMessage({ 
            type: 'proxyError', 
            proxy: { host: proxy.host, port: proxy.port },
            errorCode: error.code
          });
        }
        
        retryCount++;
        
        if (retryCount <= MAX_RETRIES) {
          const delayTime = 2000 * retryCount;
          await new Promise(resolve => setTimeout(resolve, delayTime));
          continue;
        }
      }
    }
  }
  
  // Nếu đã thử MAX_RETRIES lần mà vẫn thất bại
  return {
    success: false,
    userId: piknowUser.uid,
    knowId,
    error: lastError ? lastError.message : "Đã hết số lần thử lại"
  };
}

/**
 * Lấy User Agent ngẫu nhiên
 */
function getRandomUserAgent() {
  const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
  ];
  
  return userAgents[Math.floor(Math.random() * userAgents.length)];
} 