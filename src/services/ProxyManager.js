const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { HttpsProxyAgent } = require('https-proxy-agent');

class ProxyManager {
  constructor() {
    this.proxyPool = [];
    this.rotatingKeys = [];
    this.lastRotateTime = new Map();
    this.rotatingInterval = null;
    this.cleanupInterval = null;
    this.apiRotateDelay = 60000; // 60 giây mỗi key
    this.proxyLifetime = 13 * 60 * 1000; // 13 phút
    this.lastProxyResponse = null;
    this.rotationActive = false;
    this.proxyErrorCounts = new Map(); // Đếm lỗi cho mỗi proxy
    this.proxyAuthFailures = new Set(); // Lưu các proxy bị lỗi xác thực
    this.proxyBlacklist = new Set(); // Danh sách đen proxy
  }

  /**
   * Khởi tạo proxy manager và bắt đầu quay vòng
   */
  async initialize() {
    console.log('>> Khởi tạo Proxy Manager...');
    // Đọc các key cho proxy xoay từ file
    await this.loadRotatingKeys();
    
    // Bắt đầu quay vòng proxy
    await this.startProxyRotation();
    
    // Bắt đầu dọn dẹp proxy
    this.startCleanup();
    
    console.log(`>> Đã tải ${this.rotatingKeys.length} key cho proxy xoay`);
    console.log('>> ProxyManager đã sẵn sàng');
  }

  /**
   * Đọc key từ file keyxoay.txt
   */
  async loadRotatingKeys() {
    try {
      const keyFilePath = path.join(__dirname, '../data/keyxoay.txt');
      if (!fs.existsSync(keyFilePath)) {
        console.error('Không tìm thấy file keyxoay.txt');
        return;
      }
      
      // Đọc nội dung file
      const keyData = fs.readFileSync(keyFilePath, 'utf-8');
      
      // Xử lý tách key theo nhiều loại ký tự xuống dòng có thể có
      const keys = keyData
        .replace(/\r\n/g, '\n')  // Chuẩn hóa xuống dòng Windows
        .replace(/\r/g, '\n')    // Chuẩn hóa xuống dòng Mac cũ
        .split('\n')
        .map(key => key.trim())
        .filter(key => key.length > 0);
      
      // Kiểm tra kết quả
      if (keys.length === 0) {
        console.error('Không tìm thấy key nào trong file');
        
        // Hiển thị nội dung file để debug
        console.log('Nội dung file keyxoay.txt:');
        console.log(keyData);
        
        // Thử tách theo khoảng trắng để xem có phải vấn đề về định dạng không
        const alternativeKeys = keyData.split(/\s+/).filter(k => k.length > 0);
        if (alternativeKeys.length > 0) {
          console.log(`Tìm thấy ${alternativeKeys.length} key sau khi tách theo khoảng trắng`);
          this.rotatingKeys = alternativeKeys;
        }
      } else {
        this.rotatingKeys = keys;
      }
      
      console.log(`>> Đã tải ${this.rotatingKeys.length} key cho proxy xoay`);
      
      // In ra 5 key đầu tiên để kiểm tra
      this.rotatingKeys.slice(0, 5).forEach((key, index) => {
        console.log(`>> Key ${index+1}: ${key}`);
      });
      
      if (this.rotatingKeys.length !== 20) {
        console.warn(`>> Cảnh báo: Số lượng key (${this.rotatingKeys.length}) không khớp với dự kiến (20 key)`);
      }
    } catch (error) {
      console.error('Lỗi khi đọc file keyxoay.txt:', error.message);
    }
  }

  /**
   * Thêm proxy từ Excel vào pool
   * @param {Array} excelProxies - Danh sách proxy từ Excel
   */
  addExcelProxies(excelProxies) {
    if (!excelProxies || excelProxies.length === 0) return;
    
    console.log(`>> Thêm ${excelProxies.length} proxy từ Excel vào pool`);
    
    excelProxies.forEach(proxy => {
      // Xác minh định dạng proxy trước khi thêm vào pool
      if (this.isValidProxy(proxy)) {
        this.proxyPool.push({
          ...proxy,
          source: 'excel',
          addedTime: Date.now(),
          lastUseTime: null,
          useCount: 0,
          errorCount: 0,
          status: 'active'
        });
      } else {
        console.warn(`>> Bỏ qua proxy không hợp lệ: ${proxy.host}:${proxy.port}`);
      }
    });
    
    console.log(`>> Tổng số proxy trong pool: ${this.proxyPool.length}`);
  }

  /**
   * Kiểm tra proxy có hợp lệ không (định dạng chuẩn và có tất cả thông tin cần thiết)
   * @param {Object} proxy - Thông tin proxy
   * @returns {boolean} - Proxy có hợp lệ không
   */
  isValidProxy(proxy) {
    // Kiểm tra các trường bắt buộc
    if (!proxy) return false;
    if (!proxy.host || !proxy.port) return false;
    if (!proxy.name || !proxy.password) return false;
    
    // Kiểm tra định dạng port
    if (!/^\d+$/.test(proxy.port)) return false;
    if (parseInt(proxy.port) <= 0 || parseInt(proxy.port) > 65535) return false;
    
    // Kiểm tra xem proxy đã có trong danh sách đen chưa
    const proxyKey = `${proxy.host}:${proxy.port}`;
    if (this.proxyBlacklist.has(proxyKey)) return false;
    
    return true;
  }

  /**
   * Phương thức mới: Phân bổ proxy tối ưu cho danh sách user
   * @param {Array} userList - Danh sách user cần phân bổ proxy
   * @returns {Array} - Mảng các cặp user-proxy đã được phân bổ
   */
  assignProxiesToUsers(userList) {
    console.log(`>> Đang phân bổ proxy tối ưu cho ${userList.length} user...`);
    
    // Lấy tất cả proxy đang hoạt động, ưu tiên Excel và loại bỏ các proxy bị lỗi xác thực
    const excelProxies = this.proxyPool.filter(p => 
      p.status === 'active' && p.source === 'excel' && !this.proxyAuthFailures.has(`${p.host}:${p.port}`));
      
    const rotatingProxies = this.proxyPool.filter(p => 
      p.status === 'active' && p.source === 'rotating' && !this.proxyAuthFailures.has(`${p.host}:${p.port}`));
      
    console.log(`>> Có ${excelProxies.length} proxy Excel và ${rotatingProxies.length} proxy xoay khả dụng (sau khi lọc lỗi xác thực)`);
    
    // Kết quả phân bổ
    const result = [];
    
    // Phân bổ theo mô hình xoay vòng
    // Cố gắng sử dụng proxy Excel tối đa trước
    const maxExcelUsers = Math.min(userList.length, excelProxies.length);
    
    console.log(`>> Phân bổ ${maxExcelUsers} user đầu tiên với proxy Excel`);
    for (let i = 0; i < maxExcelUsers; i++) {
      const proxyIndex = i % excelProxies.length;
      const proxy = excelProxies[proxyIndex];
      
      result.push({
        user: userList[i],
        proxy: {
          host: proxy.host,
          port: proxy.port,
          name: proxy.name,
          password: proxy.password,
          source: 'excel',
          index: proxyIndex
        }
      });
      
      // Cập nhật số lần sử dụng
      proxy.useCount = (proxy.useCount || 0) + 1;
    }
    
    // Phân bổ còn lại với proxy xoay nếu có
    if (maxExcelUsers < userList.length) {
      const remainingUsers = userList.slice(maxExcelUsers);
      
      if (rotatingProxies.length > 0) {
        console.log(`>> Phân bổ ${remainingUsers.length} user còn lại với proxy xoay`);
        
        for (let i = 0; i < remainingUsers.length; i++) {
          const proxyIndex = i % rotatingProxies.length;
          const proxy = rotatingProxies[proxyIndex];
          
          result.push({
            user: remainingUsers[i],
            proxy: {
              host: proxy.host,
              port: proxy.port,
              name: proxy.name,
              password: proxy.password,
              source: 'rotating',
              index: proxyIndex
            }
          });
          
          // Cập nhật số lần sử dụng
          proxy.useCount = (proxy.useCount || 0) + 1;
        }
      } else {
        // Không đủ proxy xoay, quay lại dùng proxy Excel
        console.log(`>> Không có proxy xoay, dùng lại proxy Excel cho ${remainingUsers.length} user còn lại`);
        
        for (let i = 0; i < remainingUsers.length; i++) {
          const proxyIndex = i % excelProxies.length;
          const proxy = excelProxies[proxyIndex];
          
          result.push({
            user: remainingUsers[i],
            proxy: {
              host: proxy.host,
              port: proxy.port,
              name: proxy.name,
              password: proxy.password,
              source: 'excel',
              index: proxyIndex
            }
          });
          
          // Cập nhật số lần sử dụng
          proxy.useCount = (proxy.useCount || 0) + 1;
        }
      }
    }
    
    // Hiển thị tổng kết phân bổ
    const excelUsed = result.filter(r => r.proxy.source === 'excel').length;
    const rotatingUsed = result.filter(r => r.proxy.source === 'rotating').length;
    
    console.log(`>> Đã phân bổ ${result.length} user với proxy:`);
    console.log(`   - ${excelUsed} user dùng proxy Excel`);
    console.log(`   - ${rotatingUsed} user dùng proxy xoay`);
    
    return result;
  }

  /**
   * Bắt đầu định kỳ làm sạch proxy hết hạn
   */
  startCleanup() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    
    this.cleanupInterval = setInterval(() => {
      this.cleanupExpiredProxies();
    }, 30000); // Kiểm tra mỗi 30 giây
  }

  /**
   * Làm sạch các proxy đã hết hạn
   */
  cleanupExpiredProxies() {
    const now = Date.now();
    const initialCount = this.proxyPool.length;
    
    this.proxyPool = this.proxyPool.filter(proxy => {
      // Giữ lại proxy từ Excel vô thời hạn
      if (proxy.source === 'excel') return true;
      
      // Kiểm tra proxy xoay đã quá thời gian sống
      const age = now - proxy.addedTime;
      return age < this.proxyLifetime;
    });
    
    const removedCount = initialCount - this.proxyPool.length;
    if (removedCount > 0) {
      console.log(`>> Đã xóa ${removedCount} proxy hết hạn, còn lại ${this.proxyPool.length}`);
    }
  }

  /**
   * Bắt đầu quá trình xoay proxy với các key
   */
  async startProxyRotation() {
    if (this.rotationActive) return;
    
    this.rotationActive = true;
    console.log('>> Bắt đầu quá trình xoay proxy...');
    
    // Gọi lần đầu cho tất cả key
    const rotatePromises = this.rotatingKeys.map(key => this.rotateProxyForKey(key));
    await Promise.allSettled(rotatePromises);
    
    // Tiếp tục quay vòng mỗi giây
    if (this.rotatingInterval) {
      clearInterval(this.rotatingInterval);
    }
    
    this.rotatingInterval = setInterval(() => {
      // Lấy thời gian hiện tại
      const now = Date.now();
      
      // Với mỗi key, kiểm tra xem có thể xoay không
      this.rotatingKeys.forEach(key => {
        const lastRotate = this.lastRotateTime.get(key) || 0;
        // Xoay nếu đã qua delay
        if (now - lastRotate >= this.apiRotateDelay) {
          this.rotateProxyForKey(key).catch(err => {
            console.error(`>> Lỗi khi xoay proxy cho key ${key}:`, err.message);
          });
        }
      });
    }, 60000); // Kiểm tra mỗi phút một lần
  }

  /**
   * Lấy proxy mới từ API cho key cụ thể
   * @param {string} key - Key để lấy proxy xoay
   */
  async rotateProxyForKey(key) {
    try {
      console.log(`>> Đang xoay proxy cho key: ${key}`);
      
      const url = `https://proxyxoay.shop/api/get.php?key=${key}&nhamang=random&tinhthanh=0`;
      
      // Thay đổi cấu hình axios để xử lý cả HTML và JSON
      const response = await axios.get(url, {
        headers: {
          'Cookie': 'PHPSESSID=jhdhtfhuksu4oefui10k529p72',
          'Accept': '*/*',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        },
        timeout: 10000,
        responseType: 'text'  // Lấy phản hồi dưới dạng text thay vì json
      });
      
      // Xử lý phản hồi dưới dạng text
      let responseData;
      try {
        // Thử parse như JSON
        responseData = JSON.parse(response.data);
        console.log(`>> Phản hồi JSON: ${JSON.stringify(responseData)}`);
      } catch (e) {
        // Nếu không phải JSON, xử lý như HTML
        console.log(`>> Phản hồi không phải JSON, xử lý như text/html`);
        
        // Tìm thông tin proxy trong text
        const html = response.data;
        
        if (html.includes('proxy nay se die sau')) {
          // Trích xuất thông tin proxy từ HTML
          const proxyHttpMatch = html.match(/proxyhttp">(.*?)<\/td>/);
          const proxyHttp = proxyHttpMatch ? proxyHttpMatch[1] : null;
          
          const timeMatch = html.match(/proxy nay se die sau (\d+)s/);
          const time = timeMatch ? timeMatch[1] : '780';
          
          const networkMatch = html.match(/Nha Mang">(.*?)<\/td>/);
          const network = networkMatch ? networkMatch[1] : 'unknown';
          
          const locationMatch = html.match(/Vi Tri">(.*?)<\/td>/);
          const location = locationMatch ? locationMatch[1] : 'unknown';
          
          responseData = {
            status: 100,
            message: `proxy nay se die sau ${time}s`,
            proxyhttp: proxyHttp,
            "Nha Mang": network,
            "Vi Tri": location
          };
          
          console.log(`>> Đã trích xuất thông tin proxy từ HTML: ${JSON.stringify(responseData)}`);
        } else if (html.includes('Con') && html.includes('moi co the doi proxy')) {
          // Phản hồi chờ thêm thời gian
          const waitTimeMatch = html.match(/Con (\d+)s moi co the doi proxy/);
          const waitTime = waitTimeMatch ? waitTimeMatch[1] : '60';
          
          responseData = {
            status: 101,
            message: `Con ${waitTime}s moi co the doi proxy`
          };
          
          console.log(`>> Cần đợi thêm: ${JSON.stringify(responseData)}`);
        } else {
          // Trường hợp lỗi khác
          responseData = {
            status: 102,
            message: "key khong ton tai hoac het han",
            rawResponse: html.substring(0, 200) // Lưu một phần của HTML để debug
          };
          
          console.log(`>> Không xác định được phản hồi HTML, xem như lỗi key`);
        }
      }
      
      // Tiếp tục xử lý như trước, nhưng với responseData đã được xử lý đúng
      this.lastRotateTime.set(key, Date.now());
      this.lastProxyResponse = responseData;
      
      if (responseData.status === 100 && responseData.proxyhttp) {
        const proxyInfo = responseData.proxyhttp.split(':');
        
        if (proxyInfo.length === 4) {
          const newProxy = {
            host: proxyInfo[0],
            port: proxyInfo[1],
            name: proxyInfo[2],
            password: proxyInfo[3],
            source: 'rotating',
            key: key,
            addedTime: Date.now(),
            expireTime: Date.now() + (responseData.message.match(/(\d+)s/) ? parseInt(RegExp.$1) * 1000 : this.proxyLifetime),
            lastUseTime: null,
            useCount: 0,
            errorCount: 0,
            status: 'active',
            nhaMang: responseData['Nha Mang'] || 'unknown',
            viTri: responseData['Vi Tri'] || 'unknown'
          };
          
          // Kiểm tra proxy mới trước khi thêm vào pool
          if (this.isValidProxy(newProxy) && !this.proxyAuthFailures.has(`${newProxy.host}:${newProxy.port}`)) {
            this.proxyPool.push(newProxy);
            console.log(`>> Đã thêm proxy mới: ${proxyInfo[0]}:${proxyInfo[1]}`);
          } else {
            console.warn(`>> Không thêm proxy không hợp lệ hoặc đã biết là lỗi: ${proxyInfo[0]}:${proxyInfo[1]}`);
          }
        } else {
          console.error(`>> Định dạng proxy không hợp lệ: ${responseData.proxyhttp}`);
        }
      } else if (responseData.status === 101) {
        const waitTime = responseData.message.match(/Con (\d+)s/) ? parseInt(RegExp.$1) : 60;
        console.log(`>> Key ${key} cần đợi thêm ${waitTime}s trước khi xoay tiếp`);
        
        this.lastRotateTime.set(key, Date.now() - this.apiRotateDelay + (waitTime * 1000));
      } else {
        console.error(`>> Lỗi khi lấy proxy: ${JSON.stringify(responseData)}`);
      }
    } catch (error) {
      console.error(`>> Lỗi khi xoay proxy cho key ${key}:`, error.message);
    }
  }

  /**
   * Lấy proxy ngẫu nhiên từ pool
   * @param {string} userId - ID của user sẽ sử dụng proxy
   * @returns {Object} Thông tin proxy
   */
  getRandomProxy(userId) {
    if (this.proxyPool.length === 0) {
      throw new Error('Không có proxy khả dụng trong pool');
    }
    
    // Lọc các proxy đang hoạt động và không bị lỗi xác thực
    const activeProxies = this.proxyPool.filter(p => {
      const proxyKey = `${p.host}:${p.port}`;
      return p.status === 'active' && !this.proxyAuthFailures.has(proxyKey);
    });
    
    if (activeProxies.length === 0) {
      throw new Error('Không có proxy hoạt động trong pool sau khi lọc lỗi 407');
    }
    
    // Sắp xếp proxy theo số lượt sử dụng, ưu tiên proxy có ít lỗi và ít sử dụng
    activeProxies.sort((a, b) => {
      // Ưu tiên proxy có ít lỗi hơn
      const aErrors = this.proxyErrorCounts.get(`${a.host}:${a.port}`) || 0;
      const bErrors = this.proxyErrorCounts.get(`${b.host}:${b.port}`) || 0;
      
      if (aErrors !== bErrors) {
        return aErrors - bErrors;
      }
      
      // Nếu số lỗi bằng nhau, ưu tiên proxy ít sử dụng hơn
      return (a.useCount || 0) - (b.useCount || 0);
    });
    
    // Lấy proxy tốt nhất từ 30% đầu danh sách
    const topProxiesCount = Math.max(1, Math.floor(activeProxies.length * 0.3));
    const topProxies = activeProxies.slice(0, topProxiesCount);
    
    // Chọn ngẫu nhiên từ danh sách đã lọc
    const randomIndex = Math.floor(Math.random() * topProxies.length);
    const selectedProxy = topProxies[randomIndex];
    
    // Cập nhật thông tin sử dụng
    selectedProxy.lastUseTime = Date.now();
    selectedProxy.useCount = (selectedProxy.useCount || 0) + 1;
    
    return {
      host: selectedProxy.host,
      port: selectedProxy.port,
      name: selectedProxy.name,
      password: selectedProxy.password,
      source: selectedProxy.source,
      index: randomIndex,
      useCount: selectedProxy.useCount
    };
  }

  /**
   * Báo cáo proxy lỗi
   * @param {string} host - Host của proxy
   * @param {string} port - Port của proxy
   * @param {number} errorCode - Mã lỗi HTTP
   */
  reportProxyError(host, port, errorCode) {
    if (!host || !port) return;
    
    const proxyKey = `${host}:${port}`;
    const currentErrorCount = this.proxyErrorCounts.get(proxyKey) || 0;
    this.proxyErrorCounts.set(proxyKey, currentErrorCount + 1);
    
    const proxyIndex = this.proxyPool.findIndex(p => 
      p.host === host && p.port === port
    );
    
    if (proxyIndex === -1) return;
    
    const proxy = this.proxyPool[proxyIndex];
    proxy.errorCount = (proxy.errorCount || 0) + 1;
    
    // Xử lý lỗi 407 (Proxy Authentication Required)
    if (errorCode === 407) {
      console.warn(`>> Lỗi xác thực proxy 407 cho ${host}:${port}, thêm vào danh sách đen`);
      this.proxyAuthFailures.add(proxyKey);
      
      // Với proxy xoay, loại bỏ luôn khỏi pool
      if (proxy.source === 'rotating') {
        console.log(`>> Loại bỏ proxy xoay ${host}:${port} do lỗi xác thực 407`);
        this.proxyPool.splice(proxyIndex, 1);
      } else {
        // Với proxy Excel, giữ lại nhưng đánh dấu inactive
        proxy.status = 'inactive';
        console.log(`>> Đánh dấu proxy Excel ${host}:${port} không hoạt động do lỗi xác thực 407`);
      }
      
      // Thêm vào blacklist
      this.proxyBlacklist.add(proxyKey);
      return;
    }
    
    // Đánh dấu không dùng nếu lỗi 429 hoặc nhiều lỗi liên tiếp
    if (errorCode === 429 || proxy.errorCount >= 5) {
      if (proxy.source === 'excel') {
        // Đánh dấu proxy Excel là không hoạt động nếu lỗi 429
        proxy.status = 'inactive';
        console.log(`>> Đánh dấu proxy ${host}:${port} không hoạt động tạm thời do lỗi ${errorCode}`);
        
        // Đặt lịch kích hoạt lại sau 5 phút
        setTimeout(() => {
          if (this.proxyPool[proxyIndex]) {
            this.proxyPool[proxyIndex].status = 'active';
            this.proxyPool[proxyIndex].errorCount = 0;
            console.log(`>> Kích hoạt lại proxy ${host}:${port}`);
          }
        }, 5 * 60 * 1000);
      } else {
        // Xóa proxy xoay khỏi pool nếu lỗi 429
        this.proxyPool.splice(proxyIndex, 1);
        console.log(`>> Xóa proxy xoay ${host}:${port} khỏi pool do lỗi ${errorCode}`);
      }
    }
  }

  /**
   * Lấy số lượng proxy hiện có
   */
  getProxyStats() {
    const total = this.proxyPool.length;
    const active = this.proxyPool.filter(p => p.status === 'active').length;
    const fromExcel = this.proxyPool.filter(p => p.source === 'excel').length;
    const fromRotating = this.proxyPool.filter(p => p.source === 'rotating').length;
    const authFailed = this.proxyAuthFailures.size;
    
    return {
      total,
      active,
      inactive: total - active,
      fromExcel,
      fromRotating,
      authFailures: authFailed,
      keys: this.rotatingKeys.length
    };
  }

  /**
   * Dừng proxy manager
   */
  stop() {
    if (this.rotatingInterval) {
      clearInterval(this.rotatingInterval);
      this.rotatingInterval = null;
    }
    
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    
    this.rotationActive = false;
    console.log('>> Dừng ProxyManager');
  }
}

module.exports = ProxyManager;