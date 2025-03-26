const { parentPort, workerData } = require('worker_threads');
const axios = require('axios');
const qs = require('qs');
const { HttpsProxyAgent } = require('https-proxy-agent');


const { likeUser, targetUserId, postId, proxy } = workerData;


function workerLog(message, type = 'info') {

  if (type === 'error') {
    console.error(message);
  } else {
    console.log(message);
  }
  
 
  parentPort.postMessage({ 
    log: {
      type,
      message
    }
  });
}


if (!likeUser || !targetUserId || !postId) {
  const errorMsg = `[Worker] Thiếu thông tin cần thiết: ${!likeUser ? 'likeUser, ' : ''}${!targetUserId ? 'targetUserId, ' : ''}${!postId ? 'postId' : ''}`;
  workerLog(errorMsg, 'error');
  
  parentPort.postMessage({ 
    error: errorMsg
  });
  
  process.exit(1);
}

/**
 * Hàm thực hiện like bài viết
 * @returns {Promise} - Kết quả thực hiện
 */
async function performLike() {
  try {
   
    if (!proxy || !proxy.host || !proxy.port || !proxy.name || !proxy.password) {
      workerLog(`[Worker] ❌ Không có proxy hợp lệ để sử dụng cho user ${likeUser.uid}`, 'error');
      return {
        success: false,
        userId: likeUser.uid,
        targetUserId,
        postId,
        error: 'Không có proxy hợp lệ để sử dụng'
      };
    }
    
  
    const proxyUrl = `http://${proxy.name}:${proxy.password}@${proxy.host}:${proxy.port}`;
    
   
    let httpsAgent;
    try {
      httpsAgent = new HttpsProxyAgent(proxyUrl);
    } catch (error) {
      workerLog(`[Worker] ❌ Lỗi khởi tạo proxy agent: ${error.message}`, 'error');
      return {
        success: false,
        userId: likeUser.uid,
        targetUserId,
        postId,
        error: `Lỗi proxy agent: ${error.message}`
      };
    }

    
    const userAgent = getRandomUserAgent();

  
    const api = axios.create({
      baseURL: 'https://pivoice.app',
      timeout: 15000, // 15 seconds
      httpsAgent,
      headers: {
        'User-Agent': userAgent,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://pivoice.app',
        'Referer': 'https://pivoice.app/',
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    const payload = qs.stringify({
      component: "article",
      action: "like",
      aid: postId,
      user_name: likeUser.piname,
      english_version: 0,
      selected_country: 1,
      selected_chain: 0
    });

    workerLog(`[Worker] User ${likeUser.piname} đang like bài ${postId} của user ${targetUserId} (qua ${proxy.host})`);
    
  
    const jitter = Math.floor(Math.random() * 1000);
    await new Promise(resolve => setTimeout(resolve, jitter));
    
    const startTime = Date.now();
    const response = await api.post('/vapi', payload);
    const requestTime = Date.now() - startTime;
    
    if (response.data && response.data.time) {
      workerLog(`[Worker] ✅ User ${likeUser.piname} đã like thành công bài ${postId} (${requestTime}ms)`);
      
      return {
        success: true,
        userId: likeUser.uid,
        targetUserId,
        postId,
        time: response.data.time,
        responseTime: requestTime
      };
    } else {
      workerLog(`[Worker] ❌ User ${likeUser.piname} like thất bại:`, 'error');
      
      return {
        success: false,
        userId: likeUser.uid,
        targetUserId,
        postId,
        error: JSON.stringify(response.data),
        responseTime: requestTime
      };
    }
  } catch (error) {
    const errorMessage = error.message || 'Unknown error';
    let statusCode = error.response ? error.response.status : null;
    
    workerLog(`[Worker] ❌ Lỗi khi like bài ${postId}: ${errorMessage} (${statusCode || 'Không có mã lỗi'})`, 'error');
    
    
    if (
      errorMessage.includes('ECONNREFUSED') || 
      errorMessage.includes('ETIMEDOUT') || 
      errorMessage.includes('socket hang up') ||
      errorMessage.includes('tunneling socket') ||
      statusCode === 407
    ) {
      workerLog(`[Worker] ❌ Lỗi proxy: ${proxy.host}:${proxy.port}`, 'error');
    }
    
    return {
      success: false,
      userId: likeUser.uid,
      targetUserId,
      postId,
      error: errorMessage,
      statusCode,
      proxyHost: proxy.host,
      proxyPort: proxy.port
    };
  }
}

/**
 * Hàm sinh User-Agent ngẫu nhiên
 * @returns {string} User-Agent string
 */
function getRandomUserAgent() {
  const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1'
  ];
  
  return userAgents[Math.floor(Math.random() * userAgents.length)];
}

performLike()
  .then(result => {
    parentPort.postMessage({ result });
  })
  .catch(error => {
    workerLog(`[Worker] ❌ Lỗi không xử lý được: ${error.message}`, 'error');
    parentPort.postMessage({ 
      error: error.message,
      userId: likeUser.uid,
      targetUserId,
      postId
    });
  }); 