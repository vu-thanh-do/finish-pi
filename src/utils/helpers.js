/**
 * Các hàm tiện ích chung cho toàn bộ ứng dụng
 */

/**
 * Tạm dừng thực thi trong khoảng thời gian xác định
 * @param {number} ms - Thời gian tạm dừng tính bằng mili giây
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Tạo ID ngẫu nhiên
 * @param {number} length - Độ dài của ID
 * @returns {string} ID ngẫu nhiên
 */
function generateRandomId(length = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

/**
 * Gom nhóm mảng thành các batch nhỏ hơn
 * @param {Array} array - Mảng cần chia
 * @param {number} batchSize - Kích thước mỗi batch
 * @returns {Array<Array>} Mảng các batch
 */
function batchArray(array, batchSize) {
  const batches = [];
  for (let i = 0; i < array.length; i += batchSize) {
    batches.push(array.slice(i, i + batchSize));
  }
  return batches;
}

/**
 * Lấy ngẫu nhiên n phần tử từ mảng
 * @param {Array} array - Mảng ban đầu
 * @param {number} n - Số phần tử cần lấy
 * @returns {Array} Mảng n phần tử ngẫu nhiên
 */
function getRandomElements(array, n) {
  const shuffled = [...array].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, n);
}

/**
 * Format thời gian (ms) thành chuỗi dễ đọc
 * @param {number} ms - Thời gian tính bằng mili giây
 * @returns {string} Chuỗi thời gian dễ đọc
 */
function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${Math.floor(ms / 1000)}s`;
  if (ms < 3600000) return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
  return `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
}

/**
 * In thanh tiến độ vào console
 * @param {number} current - Giá trị hiện tại
 * @param {number} total - Tổng giá trị
 * @param {number} barLength - Độ dài thanh tiến độ (mặc định: 20)
 * @returns {string} Chuỗi biểu diễn thanh tiến độ
 */
function progressBar(current, total, barLength = 20) {
  const percent = total > 0 ? Math.min(Math.floor((current / total) * 100), 100) : 0;
  const filledLength = Math.floor((percent / 100) * barLength);
  const bar = Array(barLength).fill('▒').map((char, i) => i < filledLength ? '█' : '▒').join('');
  return `[${bar}] ${percent}% (${current}/${total})`;
}

/**
 * Trì hoãn thực thi hàm
 * @param {Function} fn - Hàm cần thực thi
 * @param {number} wait - Thời gian trì hoãn (ms)
 * @returns {Function} Hàm đã được trì hoãn
 */
function debounce(fn, wait) {
  let timeout;
  return function(...args) {
    const context = this;
    clearTimeout(timeout);
    timeout = setTimeout(() => fn.apply(context, args), wait);
  };
}

/**
 * Giới hạn tần suất gọi hàm
 * @param {Function} fn - Hàm cần giới hạn
 * @param {number} limit - Số lần gọi tối đa trong khoảng thời gian
 * @param {number} interval - Khoảng thời gian (ms)
 * @returns {Function} Hàm đã được giới hạn tần suất
 */
function rateLimit(fn, limit, interval) {
  const calls = [];
  return async function(...args) {
    const now = Date.now();
    calls.push(now);
    
    // Loại bỏ các lần gọi cũ hơn khoảng thời gian
    while (calls.length > 0 && calls[0] < now - interval) {
      calls.shift();
    }
    
    // Nếu số lần gọi vượt quá giới hạn, đợi
    if (calls.length > limit) {
      const delay = interval - (now - calls[0]);
      await sleep(delay);
    }
    
    return fn.apply(this, args);
  };
}

module.exports = {
  sleep,
  generateRandomId,
  batchArray,
  getRandomElements,
  formatDuration,
  progressBar,
  debounce,
  rateLimit
}; 