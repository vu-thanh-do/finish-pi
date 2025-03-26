const axios = require("axios");
const { sleep } = require('../utils/helpers');
const fs = require('fs');
const path = require('path');
// Sử dụng thư viện user-agents để có User-Agent thực tế hơn
const UserAgent = require('user-agents');
const { HttpsProxyAgent } = require('https-proxy-agent');

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

/**
 * Tạo User-Agent ngẫu nhiên sử dụng thư viện user-agents
 * @returns {string} - User-Agent ngẫu nhiên từ thư viện
 */
function getRandomUserAgent() {
  try {
    // Tạo user-agent từ thư viện user-agents
    const userAgent = new UserAgent({ deviceCategory: 'desktop' });
    return userAgent.toString();
  } catch (error) {
    // Fallback nếu có lỗi với thư viện
    console.error(`Lỗi khi tạo user-agent: ${error.message}. Sử dụng user-agent mặc định.`);
    
    // Danh sách user-agent phổ biến dùng làm backup
    const userAgents = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    ];
    
    return userAgents[Math.floor(Math.random() * userAgents.length)];
  }
}

/**
 * Tạo thông tin trình duyệt giả
 * @returns {Object} - Các header giả cho trình duyệt
 */
function getFakeHeaders(user) {
  // Chọn User-Agent ngẫu nhiên
  const userAgent = user.userAgent || getRandomUserAgent();
  
  const languages = ['vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5', 'en-US,en;q=0.9', 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7', 'vi,en-US;q=0.9,en;q=0.8'];
  const language = languages[Math.floor(Math.random() * languages.length)];
  
  // Tạo Accept-Encoding ngẫu nhiên
  const encodings = ['gzip', 'deflate', 'br'];
  const acceptEncoding = encodings.sort(() => 0.5 - Math.random()).join(', ');
  
  // Tạo số ngẫu nhiên cho x-request-id
  const requestId = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
  
  return {
    'accept': '*/*',
    'accept-language': language,
    'accept-encoding': acceptEncoding,
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': `uid=${user.uid}; ukey=${user.ukey}; piname=${user.piname || user.uid}`,
    'origin': 'https://pivoice.app',
    'priority': 'u=1, i',
    'referer': 'https://pivoice.app/',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': userAgent,
    'x-requested-with': 'XMLHttpRequest',
    'x-request-id': requestId,
    'cache-control': 'no-cache',
    'pragma': 'no-cache'
  };
}

/**
 * Xử lý cấu hình proxy cho axios
 * @param {Object} proxy - Thông tin proxy
 * @returns {Object} - Cấu hình proxy cho axios
 */
function createProxyConfig(proxy) {
  if (!proxy) return undefined;
  
  try {
    // Cấu hình proxy cho axios
    const proxyAuth = proxy.name && proxy.password ? 
      `${encodeURIComponent(proxy.name)}:${encodeURIComponent(proxy.password)}` : '';
    
    const proxyStr = proxyAuth ? 
      `http://${proxyAuth}@${proxy.host}:${proxy.port}` : 
      `http://${proxy.host}:${proxy.port}`;
    
    // Tạo đối tượng HttpsProxyAgent
    const httpsAgent = new HttpsProxyAgent(proxyStr);
    
    return {
      proxy: false, // Vô hiệu hóa proxy tích hợp của axios
      httpsAgent: httpsAgent, // Sử dụng HttpsProxyAgent
      proxy_url: proxyStr // Lưu URL proxy để gỡ lỗi
    };
  } catch (error) {
    console.error(`Lỗi khi tạo cấu hình proxy: ${error.message}`);
    return undefined;
  }
}

/**
 * Lấy danh sách bài viết của user - Cải tiến: Mỗi user lấy bài viết của chính mình
 * @param {Object} user - Thông tin user cần lấy bài viết (cần đủ uid, ukey, piname)
 * @param {Object} options - Tùy chọn bổ sung (retries, timeout, useProxy)
 * @returns {Promise<Array>} - Danh sách ID bài viết
 */
async function getUserPosts(user, options = {}) {
  const maxRetries = options.retries || 3;
  const timeout = options.timeout || 15000;
  const useProxy = options.useProxy || false;
  let retries = 0;

  // Kiểm tra xem user có đủ thông tin không
  if (!user.uid) {
    console.error(`>> [Lỗi] User không có uid`);
    return [];
  }

  const logPrefix = `>> [USER_${user.uid}]`;
  console.log(`${logPrefix} Đang lấy bài viết của user ${user.uid}...`);
  logToFile(`Đang lấy bài viết của user ${user.uid}...`);
  
  // Chỉ lấy bài viết nếu user có ukey và piname
  const canUseOwnCredentials = user.ukey && user.piname;
  
  // Tạo user đại diện nếu không thể dùng thông tin xác thực của user
  const defaultAccount = {
    uid: '1792385',
    ukey: 'B3NRPPF387DXUXUECPYYBUY8XUYBXC',
    piname: 'hh56y96'
  };
  
  while (retries <= maxRetries) {
    try {
      if (retries > 0) {
        console.log(`${logPrefix} Thử lại lần ${retries}/${maxRetries} cho user ${user.uid}...`);
        logToFile(`Thử lại lần ${retries}/${maxRetries} cho user ${user.uid}...`);
        // Đợi lâu hơn sau mỗi lần thử lại
        await sleep(1000 + 500 * retries);
      }

      // Quyết định sử dụng thông tin xác thực của user hay của tài khoản mặc định
      const authUser = canUseOwnCredentials ? user : defaultAccount;
      
      // Tạo headers với các thông tin giả
      const headers = getFakeHeaders(authUser);

      // Payload chuẩn
      const payload = `action=SPEAKER-INFO&component=speaker&speaker_id=${user.uid}&vid=${user.uid}&english_version=0&selected_country=1&selected_chain=0`;

      // Cấu hình request
      const requestConfig = {
        url: 'https://pivoice.app/api',
        method: 'post',
        timeout,
        headers,
        data: payload,
        maxRedirects: 5,
        decompress: true
      };

      // Thêm proxy nếu được yêu cầu và có sẵn
      if (useProxy && user.proxy) {
        const proxyConfig = createProxyConfig(user.proxy);
        if (proxyConfig) {
          Object.assign(requestConfig, proxyConfig);
          logToFile(`Sử dụng proxy: ${user.proxy.host}:${user.proxy.port}`);
        }
      }

      // Thêm delay ngẫu nhiên trước khi gửi request
      const randomDelay = Math.floor(Math.random() * 300);
      await sleep(randomDelay);

      // Thực hiện request
      const startTime = Date.now();
      const response = await axios(requestConfig);
      const requestTime = Date.now() - startTime;
      
      logToFile(`Nhận response sau ${requestTime}ms, status: ${response.status}`);

      // Phân tích và trả về kết quả
      if (response.data && response.data.article) {
        const articles = response.data.article;
        
        if (Array.isArray(articles) && articles.length > 0) {
          console.log(`${logPrefix} Tìm thấy ${articles.length} bài viết của user ${user.uid}`);
          logToFile(`Tìm thấy ${articles.length} bài viết của user ${user.uid}`);
          
          // Log thông tin bài đầu tiên
          if (articles[0]) {
            console.log(`${logPrefix} Bài đầu tiên: ID=${articles[0].id}, Tiêu đề="${articles[0].title?.substring(0, 30)}..."`);
          }
          
          return articles.map(article => article.id);
        } else {
          console.log(`${logPrefix} User ${user.uid} không có bài viết nào`);
          return [];
        }
      } else {
        console.log(`${logPrefix} Phản hồi không có bài viết cho user ${user.uid}`);
        
        if (retries < maxRetries) {
          retries++;
          continue;
        }
        return [];
      }
    } catch (error) {
      console.error(`${logPrefix} Lỗi khi lấy bài viết: ${error.message}`);
      
      // Xử lý từng loại lỗi khác nhau
      if (error.response) {
        const statusCode = error.response.status;
        
        // Xử lý lỗi 407 Proxy Authentication Required
        if (statusCode === 407) {
          console.log(`${logPrefix} Lỗi 407: Xác thực proxy thất bại`);
          logToFile(`Lỗi 407: Xác thực proxy thất bại cho user ${user.uid}`);
          
          if (user.proxy) {
            // Log chi tiết về proxy
            logToFile(`Chi tiết proxy: ${user.proxy.host}:${user.proxy.port} (user:${user.proxy.name})`);
          }
          
          // Thử lại không dùng proxy nếu có thể
          if (canUseOwnCredentials && useProxy && retries < maxRetries) {
            console.log(`${logPrefix} Thử lại không dùng proxy...`);
            retries++;
            // Gọi lại nhưng không dùng proxy
            try {
              const result = await getUserPosts(user, { ...options, useProxy: false, retries: 1 });
              if (result.length > 0) return result;
            } catch (innerError) {
              console.error(`${logPrefix} Lỗi khi thử lại không dùng proxy: ${innerError.message}`);
            }
          }
        }
        
        // Lỗi 429 (Too Many Requests)
        else if (statusCode === 429) {
          console.log(`${logPrefix} Nhận lỗi 429 (Too Many Requests), đợi lâu hơn...`);
          if (retries < maxRetries) {
            retries++;
            // Đối với lỗi 429, tăng thời gian delay đáng kể
            await sleep(3000 + 2000 * retries);
            continue;
          }
        }
        
        // Các lỗi mạng khác
        else if ([500, 502, 503, 504].includes(statusCode)) {
          if (retries < maxRetries) {
            retries++;
            await sleep(2000 * retries);
            continue;
          }
        }
      } 
      // Lỗi kết nối hoặc timeout
      else if (!error.response || error.code === 'ECONNABORTED') {
        if (retries < maxRetries) {
          retries++;
          await sleep(1000 * retries);
          continue;
        }
      }
      
      // Nếu đã hết số lần thử lại hoặc không rơi vào các trường hợp trên
      if (retries >= maxRetries) {
        return [];
      }
    }
  }
  
  console.log(`${logPrefix} Đã hết số lần thử lại cho user ${user.uid}`);
  return [];
}

module.exports = getUserPosts;