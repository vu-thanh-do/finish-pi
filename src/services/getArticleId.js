const axios = require("axios");

/**
 * Lấy danh sách article ID từ trang chủ Pivoice
 * @param {number} count - Số lượng article cần lấy
 * @returns {Promise<string[]>} - Mảng các article ID
 */
async function getArticleId(count = 10) {
  try {
    // URL API để lấy danh sách bài viết
    const url = 'https://pivoice.app/vapi';
    
    // Dữ liệu request
    const data = {
      action: 'list',
      component: 'article',
      english_version: '0', 
      selected_country: '1',
      selected_chain: '0'
    };
    
    // Header request
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
      'Content-Type': 'application/x-www-form-urlencoded'
    };
    
    // Gửi request để lấy danh sách bài viết
    const response = await axios.post(url, new URLSearchParams(data).toString(), { headers });
    
    if (response.status === 200 && response.data) {
      // Xử lý response để lấy danh sách article ID
      let articleIds = [];
      
      if (response.data.items && Array.isArray(response.data.items)) {
        // Lọc các ID bài viết
        articleIds = response.data.items
          .filter(item => item.id)
          .map(item => item.id)
          .slice(0, count); // Giới hạn số lượng theo count
      }
      
      if (articleIds.length === 0) {
        console.log('>> Không tìm thấy ID bài viết từ API, sử dụng ID mặc định');
        return ['58203589']; // Trả về ID mặc định nếu không tìm thấy
      }
      
      return articleIds;
    } else {
      console.error(`>> Lỗi khi lấy danh sách bài viết: HTTP ${response.status}`);
      return ['58203589']; // Trả về ID mặc định nếu có lỗi
    }
  } catch (error) {
    console.error(`>> Lỗi khi lấy danh sách bài viết: ${error.message}`);
    return ['58203589']; // Trả về ID mặc định nếu có lỗi
  }
}

module.exports = {
  getArticleId
};
