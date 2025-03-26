const apiClient = require("../api/apiClient");
const qs = require("qs");

/**
 * Lấy danh sách bài PiKnow của một user
 * @param {Object} user - Thông tin người dùng
 * @param {Object} options - Các tùy chọn bổ sung
 * @returns {Promise<Array<string>>} - Danh sách ID bài PiKnow
 */
async function getAllPostPiKnow(user, options = {}) {
  const retry = options.retry || 0;
  const maxRetries = options.maxRetries || 3;
  const proxyManager = options.proxyManager || null;
  const delayBetweenRetries = options.delayBetweenRetries || 3000;

  try {
    console.log(`>> Đang lấy article ID từ trang chủ PiKnow cho user ${user.piname}`);

    // Log thông tin user để debug
    console.log(`>> Thông tin user: ${user.piname}, UID: ${user.uid}`);
    
    // Tạo client API với thông tin người dùng
    const apiOptions = {
      useProxyManager: true,
      timeout: 10000,
      retries: 1
    };
    
    const api = apiClient(user, apiOptions);

    // Tạo payload cho request lấy bài PiKnow
    const payload = qs.stringify({
      component: "know",
      action: "get-list",
      search: "",
      user_name: user.piname,
      english_version: 0,
      selected_country: 1,
      selected_chain: 0,
    });

    // Gửi request lấy danh sách bài PiKnow
    const response = await api.post("/vapi", payload);

    // Log response status
    console.log(`>> Response status cho user ${user.piname}: ${response.status}`);
    
    if (response.data && response.data.data && response.data.data.status === 1 && Array.isArray(response.data.data.data)) {
      const knows = response.data.data.data;
      const knowIds = knows.map(item => item.id);
      console.log(`✅ Đã lấy được ${knowIds.length} bài PiKnow cho user ${user.piname}`);
      return knowIds;
    } else {
      console.error(`❌ Dữ liệu PiKnow không đúng định dạng cho user ${user.piname}:`, JSON.stringify(response.data));
      
      // Thử phương thức fallback với action=index nếu action=get-list không thành công
      if (!options.usedFallback) {
        console.log(`>> 🔄 Thử phương thức thay thế với action=index cho user ${user.piname}`);
        
        // Tạo payload fallback
        const fallbackPayload = qs.stringify({
          component: "know",
          action: "index",
          user_name: user.piname,
          english_version: 0,
          selected_country: 1,
          selected_chain: 0,
        });
        
        try {
          const fallbackResponse = await api.post("/vapi", fallbackPayload);
          console.log(`>> Response status (fallback) cho user ${user.piname}: ${fallbackResponse.status}`);
          
          if (fallbackResponse.data && fallbackResponse.data.data && Array.isArray(fallbackResponse.data.data.knows)) {
            const fallbackKnows = fallbackResponse.data.data.knows;
            const fallbackKnowIds = fallbackKnows.map(item => item.id);
            console.log(`✅ Đã lấy được ${fallbackKnowIds.length} bài PiKnow (phương thức thay thế) cho user ${user.piname}`);
            return fallbackKnowIds;
          } else {
            console.error(`❌ Dữ liệu PiKnow không đúng định dạng (phương thức thay thế) cho user ${user.piname}:`, JSON.stringify(fallbackResponse.data));
          }
        } catch (fallbackError) {
          console.error(`❌ Lỗi khi thử phương thức thay thế cho user ${user.piname}: ${fallbackError.message}`);
        }
      }
      
      return [];
    }
  } catch (error) {
    console.error(`❌ Lỗi khi lấy post PiKnow cho user ${user.piname}: ${error.message}`);
    
    if (error.response) {
      console.error(`Mã lỗi: ${error.response.status}`);
      console.error(`URL gọi: ${error.config?.url}`);
      console.error(`URL đầy đủ: ${error.config?.baseURL}${error.config?.url}`);
      console.error(`Phương thức: ${error.config?.method.toUpperCase()}`);
      
      // Nếu gặp lỗi 429 và còn cơ hội retry
      if (error.response.status === 429 && retry < maxRetries) {
        console.log(`>> 🔄 Gặp lỗi 429 khi lấy bài PiKnow cho user ${user.piname}, sẽ thử lại sau ${delayBetweenRetries/1000}s (lần ${retry + 1}/${maxRetries})`);
        
        // Đổi proxy nếu có ProxyManager
        if (proxyManager) {
          // Đánh dấu proxy hiện tại là không hoạt động
          if (user.proxy) {
            proxyManager.markProxyAsInactive(user.proxy);
          }
          
          // Lấy proxy mới
          const newProxy = proxyManager.getProxy();
          if (newProxy) {
            console.log(`>> 🔄 Đổi proxy cho user ${user.piname}: ${newProxy.host}:${newProxy.port}`);
            user.proxy = newProxy;
          }
        }
        
        // Chờ một khoảng thời gian trước khi thử lại
        await new Promise(resolve => setTimeout(resolve, delayBetweenRetries));
        
        // Thử lại với proxy mới
        return getAllPostPiKnow(user, { 
          ...options, 
          retry: retry + 1,
          delayBetweenRetries: delayBetweenRetries * 1.5, // Tăng thời gian chờ giữa các lần retry
          usedFallback: options.usedFallback // Giữ trạng thái đã thử fallback
        });
      }
    }
    
    return [];
  }
}

module.exports = getAllPostPiKnow;
