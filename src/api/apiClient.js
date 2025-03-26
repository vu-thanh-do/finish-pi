const axios = require('axios');
const { HttpsProxyAgent } = require('https-proxy-agent');
const fs = require('fs');
const path = require('path');
const configPath = path.join(__dirname, '../config.json');
const config = fs.existsSync(configPath) ? JSON.parse(fs.readFileSync(configPath, 'utf-8')) : {};
// Sử dụng thư viện user-agents để có User-Agent thực tế hơn
const UserAgent = require('user-agents');

// Proxy Manager toàn cục (sẽ được thiết lập từ bên ngoài)
let globalProxyManager = null;

/**
 * Thiết lập Proxy Manager toàn cục
 * @param {Object} proxyManager - Instance của ProxyManager
 */
const setGlobalProxyManager = (proxyManager) => {
    globalProxyManager = proxyManager;
    console.log('>> Đã thiết lập ProxyManager toàn cục cho apiClient');
};

/**
 * Tạo User-Agent ngẫu nhiên
 * @returns {string} - User-Agent ngẫu nhiên
 */
function getRandomUserAgent() {
    try {
        // Tạo user-agent từ thư viện user-agents, ưu tiên desktop
        const userAgent = new UserAgent({ deviceCategory: 'desktop' });
        return userAgent.toString();
    } catch (error) {
        console.error(`Lỗi khi tạo user-agent: ${error.message}. Sử dụng user-agent mặc định.`);
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36';
    }
}

/**
 * Tạo chuỗi proxy URL với cơ chế xử lý lỗi nâng cao
 * @param {Object} proxy - Thông tin proxy
 * @returns {string} - URL proxy
 */
function createProxyUrl(proxy) {
    if (!proxy) return null;
    
    try {
        // Đảm bảo mã hóa đúng thông tin đăng nhập
        const username = proxy.name ? encodeURIComponent(proxy.name) : '';
        const password = proxy.password ? encodeURIComponent(proxy.password) : '';
        
        // Tạo chuỗi auth nếu có thông tin đăng nhập
        const auth = username && password ? `${username}:${password}@` : '';
        
        // Tạo chuỗi proxy URL
        return `http://${auth}${proxy.host}:${proxy.port}`;
    } catch (error) {
        console.error(`Lỗi khi tạo proxy URL: ${error.message}`);
        return null;
    }
}

const apiClient = (user, options = {}) => {
    console.log(`>> Tạo API client cho user: ${user.piname}`);
    
    // Sử dụng proxy từ user hoặc lấy ngẫu nhiên từ proxy manager
    let proxyConfig = user.proxy;
    let proxySource = 'user';
    
    if (options.useProxyManager && globalProxyManager) {
        try {
            proxyConfig = globalProxyManager.getRandomProxy(user.uid);
            proxySource = `pool (${proxyConfig.source})`;
            console.log(`>> Sử dụng proxy từ pool cho user ${user.piname}: ${proxyConfig.host}:${proxyConfig.port}`);
        } catch (error) {
            console.warn(`>> Không thể lấy proxy từ pool: ${error.message}. Sử dụng proxy từ user.`);
        }
    }
    
    // Tạo proxy URL và proxy agent
    let httpsAgent = undefined;
    let proxyUrl = null;
    
    if (proxyConfig) {
        proxyUrl = createProxyUrl(proxyConfig);
        if (proxyUrl) {
            try {
                httpsAgent = new HttpsProxyAgent(proxyUrl);
                console.log(`>> Đã tạo proxy agent với URL: ${proxyUrl.replace(/:[^:@]*@/, ':****@')}`);
            } catch (error) {
                console.error(`>> Lỗi khi tạo proxy agent: ${error.message}`);
                httpsAgent = undefined;
            }
        }
    }
    
    // Tạo instance axios với timeout đã cấu hình
    const timeout = options.timeout || 20000;
    const retries = options.retries || 2;
    let currentRetry = 0;
    
    // Sử dụng User-Agent từ user hoặc tạo ngẫu nhiên
    const userAgent = user.userAgent || getRandomUserAgent();
    
    const axiosInstance = axios.create({
        baseURL: 'https://pivoice.app',
        httpsAgent,
        timeout,
        maxContentLength: 5 * 1024 * 1024,
        maxBodyLength: 5 * 1024 * 1024,
        maxRedirects: 5,
        decompress: true,
        headers: {
            'Accept': '*/*',
            'Accept-Language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': `uid=${user.uid}; ukey=${user.ukey}; piname=${user.piname}`,
            'User-Agent': userAgent,
            'Origin': 'https://pivoice.app',
            'Referer': 'https://pivoice.app/',
            'X-Requested-With': 'XMLHttpRequest',
            'Priority': 'u=1, i',
            'Sec-Ch-Ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
    });
    
    // Thêm thông tin proxy vào instance để sử dụng trong interceptor
    axiosInstance.proxyInfo = {
        host: proxyConfig?.host,
        port: proxyConfig?.port,
        source: proxySource,
        url: proxyUrl ? proxyUrl.replace(/:[^:@]*@/, ':****@') : null
    };
    
    // Request interceptor
    axiosInstance.interceptors.request.use(config => {
        config.requestId = `req-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        console.log(`>> [${config.requestId}] Gọi API: ${config.baseURL}${config.url || ''}`);
        console.log(`>> [${config.requestId}] Data: ${config.data || 'Không có'}`);
        console.log(`>> [${config.requestId}] Sử dụng proxy: ${axiosInstance.proxyInfo.host}:${axiosInstance.proxyInfo.port} (${axiosInstance.proxyInfo.source})`);
        
        // Thêm thông tin request để debug
        config._startTime = Date.now();
        
        return config;
    }, error => {
        console.error(`❌ Lỗi khi gửi request: ${error.message}`);
        return Promise.reject(error);
    });

    // Response interceptor với xử lý retry và thay đổi proxy
    axiosInstance.interceptors.response.use(response => {
        if (response.config && response.config.requestId) {
            const requestTime = Date.now() - (response.config._startTime || Date.now());
            console.log(`>> [${response.config.requestId}] Nhận response thành công, status: ${response.status}, thời gian: ${requestTime}ms`);
        }
        // Reset lại số lần retry khi thành công
        currentRetry = 0;
        return response;
    }, async error => {
        // Xử lý lỗi và retry nếu cần
        if (error.config && error.config.requestId) {
            const requestId = error.config.requestId;
            
            // Tính thời gian request
            const requestTime = Date.now() - (error.config._startTime || Date.now());
            
            // Xử lý các loại lỗi khác nhau
            if (error.code === 'ECONNABORTED') {
                console.error(`❌ [${requestId}] Request timeout sau ${error.config.timeout}ms: ${error.message}`);
            } else if (error.response) {
                const statusCode = error.response.status;
                console.error(`❌ [${requestId}] Lỗi HTTP ${statusCode}: ${error.message} (thời gian: ${requestTime}ms)`);
                
                // Báo cáo lỗi proxy cho ProxyManager nếu có
                if (globalProxyManager && [429, 403, 407, 502, 503, 504].includes(statusCode)) {
                    globalProxyManager.reportProxyError(
                        axiosInstance.proxyInfo.host, 
                        axiosInstance.proxyInfo.port, 
                        statusCode
                    );
                    
                    // Xử lý đặc biệt cho lỗi 407 (Proxy Authentication Required)
                    if (statusCode === 407) {
                        console.error(`❌ [${requestId}] Lỗi xác thực proxy 407: ${axiosInstance.proxyInfo.url || 'N/A'}`);
                        
                        // Thử lại không dùng proxy nếu có sự cho phép và có proxyManager
                        const shouldTryWithoutProxy = options.fallbackToNoProxy !== false;
                        if (shouldTryWithoutProxy && globalProxyManager && currentRetry < retries) {
                            console.log(`>> [${requestId}] Thử lại không sử dụng proxy...`);
                            
                            // Tạo lại config không dùng proxy
                            error.config.httpsAgent = undefined;
                            error.config.proxyAuth = undefined;
                            
                            try {
                                return await axios(error.config);
                            } catch (noProxyError) {
                                console.error(`❌ [${requestId}] Thử lại không proxy cũng thất bại: ${noProxyError.message}`);
                            }
                        }
                    }
                }
            } else {
                console.error(`❌ [${requestId}] Lỗi mạng: ${error.message}`);
            }
            
            // Xử lý retry với proxy mới nếu:
            // 1. Chưa vượt quá số lần retry tối đa
            // 2. Lỗi là 429, 407 hoặc lỗi mạng/timeout 
            // 3. Có ProxyManager toàn cục
            const shouldRetry = currentRetry < retries && 
                ((error.response && [429, 407].includes(error.response.status)) || 
                error.code === 'ECONNABORTED' || 
                !error.response);
                
            if (shouldRetry && globalProxyManager && options.useProxyManager) {
                currentRetry++;
                console.log(`>> [${requestId}] Thử lại lần ${currentRetry}/${retries} với proxy mới...`);
                
                // Tạo lại client với proxy mới
                try {
                    // Đợi một chút trước khi retry
                    const delayTime = error.response?.status === 429 ? 3000 : 1000;
                    await new Promise(resolve => setTimeout(resolve, delayTime));
                    
                    // Lấy proxy mới
                    const newProxy = globalProxyManager.getRandomProxy(user.uid);
                    console.log(`>> [${requestId}] Thay đổi proxy: ${newProxy.host}:${newProxy.port}`);
                    
                    // Tạo proxy agent mới
                    const newProxyUrl = createProxyUrl(newProxy);
                    if (newProxyUrl) {
                        error.config.httpsAgent = new HttpsProxyAgent(newProxyUrl);
                        
                        // Cập nhật thông tin proxy trong instance
                        axiosInstance.proxyInfo = {
                            host: newProxy.host,
                            port: newProxy.port,
                            source: `pool (${newProxy.source})`,
                            url: newProxyUrl.replace(/:[^:@]*@/, ':****@')
                        };
                        
                        // Thử lại request
                        error.config._startTime = Date.now(); // Reset thời gian bắt đầu
                        return axios(error.config);
                    }
                } catch (retryError) {
                    console.error(`❌ [${requestId}] Không thể retry: ${retryError.message}`);
                }
            }
        }
        
        return Promise.reject(error);
    });
    
    return axiosInstance;
};

module.exports = apiClient;
module.exports.setGlobalProxyManager = setGlobalProxyManager;
