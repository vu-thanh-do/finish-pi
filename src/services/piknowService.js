const PiKnowClusterManager = require('../../piknow-cluster-manager');
const getAllPostPiKnow = require('./getAllPostPiKnow');
const ExcelReaderService = require('../models/excelSheed');
const path = require('path');

function splitIntoWords(text) {
  return text.split(/\s+/).filter(word => word.length > 0);
}

function splitIntoPhrases(text) {
  return text.split(/[,.!?;]/)
    .map(chunk => chunk.trim())
    .filter(chunk => chunk.length > 0);
}

function getRandomElement(array) {
  return array[Math.floor(Math.random() * array.length)];
}

function generateMixedPiKnowMessage(piknowMessages) {
  const wordPool = piknowMessages.reduce((acc, text) => {
    if (text) {
      acc.push(...splitIntoWords(text));
    }
    return acc;
  }, []);

  const phrasePool = piknowMessages.reduce((acc, text) => {
    if (text) {
      acc.push(...splitIntoPhrases(text));
    }
    return acc;
  }, []);

  const mixingStyle = Math.floor(Math.random() * 5);

  switch (mixingStyle) {
    case 0:
      return getRandomElement(piknowMessages);

    case 1:
      const numWords = Math.floor(Math.random() * 2) + 2;
      const words = [];
      for (let i = 0; i < numWords; i++) {
        words.push(getRandomElement(wordPool));
      }
      return words.join(' ');

    case 2:
      const phrase = getRandomElement(phrasePool);
      const word = getRandomElement(wordPool);
      return `${phrase} ${word}`;

    case 3:
      const phrases = [
        getRandomElement(phrasePool),
        getRandomElement(phrasePool)
      ];
      return phrases.join(', ');

    case 4:
      const firstWord = getRandomElement(wordPool);
      const middlePhrase = getRandomElement(phrasePool);
      const lastWord = getRandomElement(wordPool);
      return `${firstWord} ${middlePhrase} ${lastWord}`;
  }
}

class PiKnowService {
  constructor() {
    this.piknowClusterManager = null;
    this.proxyManager = null;
    this.numWorkers = 4;
    this.concurrentTasksPerWorker = 5;
    this.users = [];
    this.configured = false;
    this.userPostsMap = new Map(); // Lưu trữ danh sách bài PiKnow của từng user
    this.piknowMessages = [];
  }

  configureService(config = {}) {
    this.numWorkers = config.numWorkers || this.numWorkers;
    this.concurrentTasksPerWorker = config.concurrentTasksPerWorker || this.concurrentTasksPerWorker;
    this.proxyManager = config.proxyManager || this.proxyManager;
    this.users = config.users || this.users;
    this.configured = true;

    console.log(`>> PiKnowService đã được cấu hình với ${this.numWorkers} workers và ${this.concurrentTasksPerWorker} tasks/worker`);
    if (this.proxyManager) {
      if (typeof this.proxyManager.getProxyStats === 'function') {
        console.log(`>> Đã kết nối với ProxyManager (${this.proxyManager.getProxyStats().active} proxy hoạt động)`);
      } else {
        console.log(`>> Đã kết nối với ProxyManager (không thể lấy số lượng proxy hoạt động)`);
      }
    }
    
    // Khởi tạo PiKnowClusterManager
    const clusterConfig = {
      numWorkers: this.numWorkers,
      concurrentTasksPerWorker: this.concurrentTasksPerWorker
    };
    
    // Chỉ thêm proxyManager vào cấu hình nếu nó tồn tại và có phương thức getProxy
    if (this.proxyManager && typeof this.proxyManager.getProxy === 'function') {
      clusterConfig.proxyManager = this.proxyManager;
    }
    
    this.piknowClusterManager = new PiKnowClusterManager(clusterConfig);

    console.log(`>> PiKnowClusterManager đã được khởi tạo thành công`);
  }

  async getKnowIds(user) {
    console.log(`>> Đang lấy bài PiKnow cho user ${user.piname}...`);
    try {
      // Lấy danh sách bài PiKnow của user
      const knowIds = await getAllPostPiKnow(user);
      console.log(`>> Đã lấy được ${knowIds.length} bài PiKnow cho user ${user.piname}`);
      return knowIds;
    } catch (error) {
      console.error(`>> Lỗi khi lấy danh sách bài PiKnow cho user ${user.piname}: ${error.message}`);
      throw error;
    }
  }

  async generatePiKnowTasks(piknowCount) {
    try {
      let userList = this.users;
      
      // Nếu không có users từ cấu hình, đọc từ Excel
      if (!userList || userList.length === 0) {
        // Đọc dữ liệu từ file Excel
        const excelPath = path.join(__dirname, '../data/PI.xlsx');
        console.log(`>> Đã tìm thấy file Excel tại: ${excelPath}`);
        const excelReader = new ExcelReaderService(excelPath);
        const excelData = excelReader.readAllSheets();
        
        // Lấy dữ liệu của các cột cần thiết
        const uid = excelData["prxageng"]?.["uid"] || [];
        const piname = excelData["prxageng"]?.["piname"] || [];
        const ukey = excelData["prxageng"]?.["ukey"] || [];
        const proxy = excelData["prxageng"]?.["proxy"] || [];
        const piknow = excelData["piknow"]?.["piknow"] || [];
        
        // Lưu các tin nhắn PiKnow để tạo nội dung
        this.piknowMessages = piknow.filter(msg => msg && msg.trim() !== '');
        
        if (this.piknowMessages.length === 0) {
          this.piknowMessages = [
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
        }
        
        console.log(`>> Đã tải ${this.piknowMessages.length} mẫu nội dung PiKnow từ Excel`);
        
        // Tạo đối tượng user cho mỗi dòng
        userList = [];
        for (let i = 0; i < uid.length; i++) {
          if (uid[i] && piname[i] && ukey[i]) {
            const proxyInfo = proxy[i] ? proxy[i].split(':') : null;
            
            userList.push({
              uid: uid[i],
              piname: piname[i],
              ukey: ukey[i],
              proxy: proxyInfo ? {
                host: proxyInfo[0],
                port: proxyInfo[1],
                name: proxyInfo[2],
                password: proxyInfo[3]
              } : null
            });
          }
        }
      }
      
      console.log(`>> Tìm thấy ${userList.length} users từ file Excel`);
      
      // Lấy danh sách bài PiKnow cho từng user
      console.log(`>> Đang lấy danh sách bài PiKnow cho ${userList.length} users...`);
      
      // Danh sách user đã thử lấy PiKnow nhưng thất bại do lỗi 429
      const usersWithoutPiKnow = [];
      // Số lần retry tối đa cho mỗi user
      const maxUserRetries = 3;
      
      // Lấy bài PiKnow cho từng user với xử lý batch để tránh quá tải
      const BATCH_SIZE = 20; // Số lượng user xử lý cùng lúc
      const userBatches = [];
      
      // Chia userList thành các batch nhỏ
      for (let i = 0; i < userList.length; i += BATCH_SIZE) {
        userBatches.push(userList.slice(i, i + BATCH_SIZE));
      }
      
      console.log(`>> Chia thành ${userBatches.length} batch, mỗi batch ${BATCH_SIZE} users`);
      
      for (let batchIndex = 0; batchIndex < userBatches.length; batchIndex++) {
        const userBatch = userBatches[batchIndex];
        console.log(`>> Đang xử lý batch ${batchIndex + 1}/${userBatches.length}...`);
        
        // Xử lý từng batch với Promise.all
        await Promise.all(userBatch.map(async (user) => {
          let retryCount = 0;
          let success = false;
          
          while (retryCount < maxUserRetries && !success) {
            try {
              // Nếu user chưa có trong userPostsMap hoặc đã thử nhưng thất bại, thì lấy lại
              if (!this.userPostsMap.has(user.uid) || this.userPostsMap.get(user.uid).length === 0) {
                // Thay đổi proxy nếu đây là lần retry và có ProxyManager
                if (retryCount > 0 && this.proxyManager) {
                  try {
                    if (typeof this.proxyManager.getProxy === 'function') {
                      const newProxy = this.proxyManager.getProxy();
                      if (newProxy) {
                        console.log(`>> 🔄 Thay đổi proxy cho user ${user.piname} do lỗi trước đó: ${newProxy.host}:${newProxy.port}`);
                        user.proxy = newProxy;
                      }
                    } else {
                      console.warn(`>> CẢNH BÁO: proxyManager không có phương thức getProxy()`);
                    }
                  } catch (error) {
                    console.warn(`>> CẢNH BÁO: Lỗi khi lấy proxy mới: ${error.message}`);
                  }
                }
                
                // Thêm delay giữa các lần gọi API để tránh lỗi 429
                if (retryCount > 0) {
                  const delay = 3000 * (retryCount + 1);
                  console.log(`>> Chờ ${delay/1000}s trước khi thử lại cho user ${user.piname}`);
                  await new Promise(resolve => setTimeout(resolve, delay));
                }
                
                const options = {
                  maxRetries: 2,
                  delayBetweenRetries: 3000
                };
                
                // Chỉ thêm proxyManager vào options nếu nó tồn tại và có phương thức getProxy
                if (this.proxyManager && typeof this.proxyManager.getProxy === 'function') {
                  options.proxyManager = this.proxyManager;
                }
                
                try {
                  const knowIds = await getAllPostPiKnow(user, options);
                  
                  if (knowIds && knowIds.length > 0) {
                    this.userPostsMap.set(user.uid, knowIds);
                    success = true;
                    console.log(`>> Đã lấy được ${knowIds.length} bài PiKnow cho user ${user.piname}`);
                  } else {
                    retryCount++;
                    console.log(`>> Không có bài PiKnow hoặc lỗi. Thử lại lần ${retryCount}/${maxUserRetries} cho user ${user.piname}`);
                  }
                } catch (error) {
                  retryCount++;
                  console.error(`>> Lỗi khi lấy bài PiKnow cho user ${user.piname}: ${error.message}`);
                  // Thêm random delay để tránh lỗi 429
                  const randomDelay = 1000 + Math.floor(Math.random() * 2000);
                  await new Promise(resolve => setTimeout(resolve, randomDelay));
                }
              } else {
                // User đã có bài PiKnow trong cache
                success = true;
              }
            } catch (error) {
              retryCount++;
              console.error(`>> Lỗi khi lấy bài PiKnow cho user ${user.piname}: ${error.message}`);
            }
          }
          
          // Nếu sau tất cả các lần thử vẫn không thành công, thêm vào danh sách user không có PiKnow
          if (!success) {
            usersWithoutPiKnow.push(user);
          }
        }));
        
        // Thêm delay giữa các batch
        if (batchIndex < userBatches.length - 1) {
          console.log(`>> Đợi 5 giây trước khi xử lý batch tiếp theo...`);
          await new Promise(resolve => setTimeout(resolve, 5000));
        }
      }
      
      // Báo cáo số lượng user không có PiKnow
      if (usersWithoutPiKnow.length > 0) {
        console.log(`>> Có ${usersWithoutPiKnow.length}/${userList.length} user không lấy được bài PiKnow sau nhiều lần thử`);
      }
      
      // Tạo các task PiKnow
      const tasks = [];
      const usedIds = new Map(); // Để theo dõi ID đã sử dụng cho mỗi user
      let totalRequiredTasks = 0; // Tổng số tác vụ cần thực hiện
      
      // Đếm số lượng user có bài PiKnow
      const usersWithPiKnow = userList.filter(user => {
        const userKnowIds = this.userPostsMap.get(user.uid) || [];
        return userKnowIds.length > 0;
      });
      
      console.log(`>> Có ${usersWithPiKnow.length}/${userList.length} users có bài PiKnow khả dụng`);
      
      // Ưu tiên xử lý proxy trước
      if (this.proxyManager) {
        const userAssignments = [];
        
        try {
          // Nếu proxyManager có phương thức assignProxiesToUsers, sử dụng nó
          if (typeof this.proxyManager.assignProxiesToUsers === 'function') {
            const proxyAssignments = this.proxyManager.assignProxiesToUsers(usersWithPiKnow);
            if (proxyAssignments && proxyAssignments.length > 0) {
              console.log(`>> Đã phân bổ proxy cho ${proxyAssignments.length}/${usersWithPiKnow.length} users`);
              
              // Cập nhật proxy từ phân bổ
              proxyAssignments.forEach(assignment => {
                const userIndex = usersWithPiKnow.findIndex(u => u.uid === assignment.user.uid);
                if (userIndex !== -1) {
                  usersWithPiKnow[userIndex].proxy = assignment.proxy;
                }
              });
            }
          } else {
            // Phân bổ proxy thủ công nếu không có assignProxiesToUsers
            console.log(`>> Phân bổ proxy thủ công cho ${usersWithPiKnow.length} users`);
            for (const user of usersWithPiKnow) {
              if (!user.proxy && this.proxyManager) {
                try {
                  if (typeof this.proxyManager.getProxy === 'function') {
                    const proxy = this.proxyManager.getProxy();
                    if (proxy) {
                      user.proxy = proxy;
                      console.log(`>> Đã gán proxy ${proxy.host}:${proxy.port} cho user ${user.piname}`);
                    }
                  }
                } catch (error) {
                  console.warn(`>> CẢNH BÁO: Lỗi khi lấy proxy cho user ${user.piname}: ${error.message}`);
                }
              }
            }
          }
        } catch (error) {
          console.warn(`>> CẢNH BÁO: Lỗi khi phân bổ proxy cho users: ${error.message}`);
        }
      }
      
      // Tiếp tục với các user có bài PiKnow
      for (const user of usersWithPiKnow) {
        // Kiểm tra user có proxy không, nếu không thì bỏ qua hoặc lấy lại
        if (!user.proxy && this.proxyManager) {
          try {
            if (typeof this.proxyManager.getProxy === 'function') {
              const proxy = this.proxyManager.getProxy();
              if (proxy) {
                user.proxy = proxy;
                console.log(`>> Đã gán proxy cho user ${user.piname}: ${proxy.host}:${proxy.port}`);
              } else {
                console.warn(`>> CẢNH BÁO: Không lấy được proxy từ proxyManager cho user ${user.piname}`);
                // Vẫn tiếp tục với user không có proxy, worker sẽ xử lý
              }
            } else {
              console.warn(`>> CẢNH BÁO: proxyManager không có phương thức getProxy() cho user ${user.uid}`);
            }
          } catch (error) {
            console.warn(`>> CẢNH BÁO: Lỗi khi lấy proxy cho user ${user.piname}: ${error.message}`);
          }
        }
        
        // Kiểm tra user có bài PiKnow không
        const userKnowIds = this.userPostsMap.get(user.uid) || [];
        if (userKnowIds.length === 0) {
          console.log(`>> Bỏ qua user ${user.piname} vì không có bài PiKnow`);
          continue;
        }
        
        // Tính tổng số tác vụ cần thực hiện
        totalRequiredTasks += piknowCount;
        
        if (!usedIds.has(user.uid)) {
          usedIds.set(user.uid, new Set());
        }
        const userUsedIds = usedIds.get(user.uid);
        
        // Tạo piknowCount tasks cho mỗi user
        for (let i = 0; i < piknowCount; i++) {
          // Lấy danh sách ID chưa sử dụng
          let availableIds = userKnowIds.filter(id => !userUsedIds.has(id));
          // Nếu đã dùng hết, reset danh sách
          if (availableIds.length === 0) {
            userUsedIds.clear();
            availableIds = userKnowIds;
          }
          
          // Chọn ngẫu nhiên một ID
          const randomIndex = Math.floor(Math.random() * availableIds.length);
          const selectedId = availableIds[randomIndex];
          userUsedIds.add(selectedId);
          
          // Tạo nội dung PiKnow ngẫu nhiên bảo đảm hợp lệ
          let piknowText = "";
          try {
            piknowText = generateMixedPiKnowMessage(this.piknowMessages);
            // Kiểm tra nội dung không có undefined và không rỗng
            if (!piknowText || piknowText.includes('undefined') || piknowText.trim() === '') {
              const defaultMessages = [
                "Rất hay và bổ ích!",
                "Thông tin quá tuyệt vời!",
                "Cảm ơn vì kiến thức này!",
                "Tôi rất thích nội dung của bạn",
                "Thật sự hữu ích!"
              ];
              const randomIndex = Math.floor(Math.random() * defaultMessages.length);
              piknowText = defaultMessages[randomIndex];
              console.log(`>> Đã tạo nội dung mặc định cho task (${i+1}/${piknowCount}) của user ${user.piname}`);
            }
          } catch (error) {
            // Nếu có lỗi khi tạo nội dung, sử dụng nội dung mặc định
            piknowText = "Rất hay và bổ ích!";
            console.warn(`>> CẢNH BÁO: Lỗi khi tạo nội dung PiKnow: ${error.message}`);
          }
          
          tasks.push({
            piknowUser: user,
            knowId: selectedId,
            piknowText
          });
        }
      }
      
      // Kiểm tra xem đã đủ số lượng tác vụ chưa
      if (tasks.length < totalRequiredTasks) {
        console.log(`>> ⚠️ CHÚ Ý: Chỉ tạo được ${tasks.length}/${totalRequiredTasks} tác vụ cần thiết (${totalRequiredTasks - tasks.length} thiếu)`);
      } else {
        console.log(`>> ✅ Đã tạo đủ ${tasks.length} tác vụ PiKnow theo yêu cầu`);
      }
      
      console.log(`>> Đã tạo ${tasks.length} tác vụ PiKnow`);
      return {
        tasks,
        totalRequired: totalRequiredTasks, 
        missingCount: totalRequiredTasks - tasks.length
      };
    } catch (error) {
      console.error(`>> Lỗi khi tạo tác vụ PiKnow: ${error.message}`);
      throw error;
    }
  }

  async executePiKnowTasks(tasks) {
    try {
      console.log(`>> Đã tạo ${tasks.length} tác vụ PiKnow`);
      console.log(`>> Bắt đầu thực thi ${tasks.length} tác vụ PiKnow...`);
      
      // Nếu chưa cấu hình, khởi tạo PiKnowClusterManager với cấu hình mặc định
      if (!this.configured || !this.piknowClusterManager) {
        console.log(`>> Chuẩn bị thực thi ${tasks.length} tác vụ PiKnow song song với ${this.numWorkers} CPUs`);
        
        const clusterConfig = {
          numWorkers: this.numWorkers,
          concurrentTasksPerWorker: this.concurrentTasksPerWorker,
          workerTimeout: 45000, // 45 giây
          proxyRotateInterval: 30, // Đổi proxy sau 30 tác vụ
          useMissingTaskTracker: true // Kích hoạt theo dõi task thiếu
        };
        
        // Chỉ thêm proxyManager vào cấu hình nếu nó tồn tại và có phương thức getProxy
        if (this.proxyManager && typeof this.proxyManager.getProxy === 'function') {
          clusterConfig.proxyManager = this.proxyManager;
        }
        
        this.piknowClusterManager = new PiKnowClusterManager(clusterConfig);
      }
      
      // Thực thi các task và lấy kết quả
      const results = await this.piknowClusterManager.executeTasks(tasks);
      
      // Tính toán số lượng thành công và thất bại
      const successCount = results.filter(result => result.success).length;
      const failCount = results.length - successCount;
      
      console.log(`>> Kết quả: ${successCount} PiKnow thành công, ${failCount} PiKnow thất bại`);
      
      // Dọn dẹp tài nguyên
      this.piknowClusterManager.cleanup();
      console.log(`>> Đã dọn dẹp tài nguyên PiKnowClusterManager`);
      
      // Phân tích lỗi nếu có
      if (failCount > 0) {
        const failedResults = results.filter(result => !result.success);
        const errorCounts = {};
        
        failedResults.forEach(result => {
          const errorType = result.error || 'Unknown error';
          errorCounts[errorType] = (errorCounts[errorType] || 0) + 1;
        });
        
        console.log(`>> Lỗi trong quá trình PiKnow:`, Object.keys(errorCounts).length > 0 ? 
          Object.entries(errorCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([error, count]) => `${error}: ${count} lần`)
            .join(', ') 
          : 'Không có thông tin lỗi chi tiết'
        );
      }
      
      return {
        success: successCount,
        failure: failCount,
        total: tasks.length,
        piknowedIds: results.filter(r => r.success).map(r => r.knowId),
        missedCount: tasks.length - successCount - failCount
      };
    } catch (error) {
      console.error(`>> Lỗi khi thực thi tác vụ PiKnow: ${error.message}`);
      if (this.piknowClusterManager) {
        this.piknowClusterManager.cleanup();
      }
      throw error;
    }
  }

  async startPiKnowProcess(piknowCount) {
    try {
      console.log(`>> Bắt đầu quá trình PiKnow với ${piknowCount} bài cho mỗi user`);
      
      // Tạo các tác vụ PiKnow
      const { tasks, totalRequired, missingCount } = await this.generatePiKnowTasks(piknowCount);
      
      if (tasks.length === 0) {
        console.error(">> Không có tác vụ PiKnow nào được tạo!");
        return {
          success: 0,
          failure: 0,
          total: 0,
          piknowedIds: [],
          error: "Không có tác vụ PiKnow nào được tạo"
        };
      }
      
      // Thực thi các tác vụ PiKnow
      const result = await this.executePiKnowTasks(tasks);
      
      // Kiểm tra nếu số lượng PiKnow thành công chưa đủ so với yêu cầu
      const targetSuccess = totalRequired;
      const currentSuccess = result.success;
      
      if (currentSuccess < targetSuccess) {
        console.log(`\n>> ⚠️ CHÚ Ý: Chỉ hoàn thành ${currentSuccess}/${targetSuccess} PiKnow thành công theo yêu cầu`);
        console.log(`>> Còn thiếu ${targetSuccess - currentSuccess} PiKnow thành công`);
        
        // Tính số lượng tác vụ còn thiếu cần thực hiện bổ sung
        const additionalTasksCount = targetSuccess - currentSuccess;
        if (additionalTasksCount > 0 && missingCount === 0) {
          console.log(`>> 🔄 Thực hiện bổ sung ${additionalTasksCount} tác vụ PiKnow để đạt mục tiêu`);
          
          // Tạo thêm tác vụ PiKnow để đạt đủ số lượng
          const { tasks: additionalTasks } = await this.generatePiKnowTasks(additionalTasksCount);
          
          if (additionalTasks.length > 0) {
            console.log(`>> 🔄 Đã tạo ${additionalTasks.length} tác vụ PiKnow bổ sung`);
            
            // Thực hiện các tác vụ bổ sung
            const additionalResult = await this.executePiKnowTasks(additionalTasks);
            
            // Tính toán kết quả tổng hợp
            const finalResult = {
              success: result.success + additionalResult.success,
              failure: result.failure + additionalResult.failure,
              total: result.total + additionalResult.total,
              piknowedIds: [...result.piknowedIds, ...additionalResult.piknowedIds],
              isRetried: true
            };
            
            console.log(`\n>> Kết quả sau khi thực hiện bổ sung: ${finalResult.success}/${finalResult.total} PiKnow thành công`);
            return finalResult;
          } else {
            console.log(`>> ❌ Không thể tạo thêm tác vụ PiKnow bổ sung`);
          }
        }
      }
      
      return {
        success: result.success,
        failure: result.failure,
        total: result.total,
        piknowedIds: result.piknowedIds
      };
    } catch (error) {
      console.error(`>> Lỗi trong quá trình PiKnow: ${error.message}`);
      return {
        success: 0,
        failure: 0,
        total: 0,
        piknowedIds: [],
        error: error.message
      };
    }
  }
}

// Export một instance của PiKnowService
module.exports = new PiKnowService(); 