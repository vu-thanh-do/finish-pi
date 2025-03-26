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
const { user } = workerData;

// Function ghi log
function log(message, isError = false) {
  if (parentPort) {
    parentPort.postMessage({ type: isError ? 'error' : 'log', message });
  } else {
    isError ? console.error(message) : console.log(message);
  }
}

// Kiểm tra dữ liệu cần thiết
if (!user || !user.piname || !user.uid || !user.ukey) {
  log(`Thiếu thông tin cần thiết cho đăng nhập (user: ${user?.uid}, piname: ${user?.piname})`, true);
  parentPort.postMessage({ 
    success: false, 
    error: "Thiếu thông tin cần thiết cho đăng nhập",
    userId: user?.uid || null,
    piname: user?.piname || null
  });
} else {
  // Thực hiện đăng nhập
  performLogin()
    .then(result => {
      parentPort.postMessage(result);
    })
    .catch(error => {
      log(`Lỗi khi đăng nhập (worker): ${error.message}`, true);
      parentPort.postMessage({ 
        success: false, 
        error: error.message,
        userId: user.uid,
        piname: user.piname
      });
    });
}

// Function để đăng nhập
async function performLogin() {
  const maxRetries = 3;
  let retryCount = 0;
  
  while (retryCount <= maxRetries) {
    try {
      if (retryCount > 0) {
        log(`Thử lại lần ${retryCount}/${maxRetries} cho tài khoản ${user.piname}`);
        // Tăng thời gian chờ mỗi lần retry
        await new Promise(resolve => setTimeout(resolve, 2000 * retryCount));
      }
      
      // Cấu hình axios
      const config = {
        baseURL: 'https://pivoice.app',
        timeout: 15000, // 15 giây
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
      
      // Gửi request đăng nhập
      log(`User ${user.piname} đang đăng nhập...`);
      const requestId = `req-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
      log(`>> [${requestId}] Gọi API: https://pivoice.app/api`);
      
      // Thêm uid vào payload nếu có
      const payload = qs.stringify({
        component: "signin",
        action: "go",
        user_name: user.piname,
        ukey: user.ukey,
        uid: user.uid,  // Thêm uid vào payload
        english_version: 0,
        selected_country: 1,
        selected_chain: 0
      });
      
      log(`>> [${requestId}] Data: ${payload}`);
      if (user.proxy) {
        log(`>> [${requestId}] Sử dụng proxy: ${user.proxy.host}:${user.proxy.port} (user)`);
      }
      
      // Tạo client axios
      const client = axios.create(config);
      
      const startTime = Date.now();
      // Sử dụng path "/api" thay vì URL đầy đủ
      const response = await client.post('/api', payload);
      const requestTime = Date.now() - startTime;
      
      log(`>> [${requestId}] Nhận response ${response.status}, thời gian: ${requestTime}ms`);
      
      // Ghi log đầy đủ response data để debug
      try {
        log(`>> [${requestId}] Response data: ${JSON.stringify(response.data)}`);
      } catch (err) {
        log(`>> [${requestId}] Không thể ghi log response data: ${err.message}`);
      }
      
      // Kiểm tra kết quả - sửa đổi điều kiện kiểm tra
      if (response.data && response.data.status && response.data.task) {
        log(`✅ User ${user.piname} đã đăng nhập thành công`);
        return { 
          success: true, 
          userId: user.uid,
          piname: user.piname,
          message: "Đã đăng nhập thành công"
        };
      } else {
        const errorMsg = (response.data && response.data.message) ? response.data.message : "Đăng nhập không thành công";
        log(`❌ Đăng nhập tài khoản ${user.piname} không thành công: ${errorMsg}`, true);
        
        // Log thêm thông tin debug chi tiết
        if (response.data) {
          log(`Full response: ${JSON.stringify(response.data)}`, true);
        }
        
        // Tiếp tục retry nếu lỗi từ server
        if (response.data && response.data.status === 0) {
          retryCount++;
          continue;
        }
        
        return { 
          success: false, 
          userId: user.uid,
          piname: user.piname,
          error: errorMsg
        };
      }
    } catch (error) {
      log(`❌ Lỗi khi đăng nhập tài khoản ${user.piname}: ${error.message}`, true);
      
      if (error.response) {
        const statusCode = error.response.status;
        log(`Mã lỗi: ${statusCode}`, true);
        
        // Retry với các lỗi mạng/server
        if ([404, 429, 500, 502, 503, 504].includes(statusCode)) {
          retryCount++;
          if (retryCount <= maxRetries) {
            const delayTime = statusCode === 429 ? 10000 : 3000 * retryCount;
            log(`Sẽ thử lại sau ${delayTime/1000} giây...`);
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
        piname: user.piname,
        error: error.message
      };
    }
  }
  
  return { 
    success: false, 
    userId: user.uid,
    piname: user.piname,
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