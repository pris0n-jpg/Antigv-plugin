import config from '../config/config.js';
import logger from '../utils/logger.js';
import accountService from '../services/account.service.js';
import quotaService from '../services/quota.service.js';
import oauthService from '../services/oauth.service.js';

/**
 * 自定义API错误类，包含HTTP状态码
 */
class ApiError extends Error {
  constructor(message, statusCode, responseText) {
    super(message);
    this.name = 'ApiError';
    this.statusCode = statusCode;
    this.responseText = responseText;
  }
}

/**
 * 多账号API客户端
 * 支持从数据库获取账号并进行轮询
 */
class MultiAccountClient {
  constructor() {
  }

  /**
   * 获取可用的账号token（带配额检查）
   * @param {string} user_id - 用户ID
   * @param {string} model_name - 模型名称
   * @param {Object} user - 用户对象（包含prefer_shared）
   * @returns {Promise<Object>} 账号对象
   */
  async getAvailableAccount(user_id, model_name, user) {
    // 确保 prefer_shared 有明确的值（默认为0 - 专属优先）
    const preferShared = user?.prefer_shared ?? 0;
    let accounts = [];
    
    logger.info(`========== 开始获取可用账号 ==========`);
    logger.info(`用户信息 - user_id=${user_id}, prefer_shared=${preferShared} (原始值: ${user?.prefer_shared}), model=${model_name}`);
    logger.info(`用户对象完整信息: ${JSON.stringify(user)}`);
    
    // 根据用户优先级选择cookie
    if (preferShared === 1) {
      // 共享优先：先尝试共享cookie，再尝试专属cookie
      logger.info(`执行共享优先策略...`);
      const sharedAccounts = await accountService.getAvailableAccounts(null, 1);
      const dedicatedAccounts = await accountService.getAvailableAccounts(user_id, 0);
      accounts = sharedAccounts.concat(dedicatedAccounts);
      logger.info(`共享优先模式 - 共享账号=${sharedAccounts.length}个, 专属账号=${dedicatedAccounts.length}个, 总计=${accounts.length}个`);
      logger.info(`共享账号列表: ${JSON.stringify(sharedAccounts.map(a => ({ cookie_id: a.cookie_id, is_shared: a.is_shared, user_id: a.user_id })))}`);
      logger.info(`专属账号列表: ${JSON.stringify(dedicatedAccounts.map(a => ({ cookie_id: a.cookie_id, is_shared: a.is_shared, user_id: a.user_id })))}`);
    } else {
      // 专属优先：先尝试专属cookie，再尝试共享cookie
      logger.info(`执行专属优先策略...`);
      const dedicatedAccounts = await accountService.getAvailableAccounts(user_id, 0);
      const sharedAccounts = await accountService.getAvailableAccounts(null, 1);
      accounts = dedicatedAccounts.concat(sharedAccounts);
      logger.info(`专属优先模式 - 专属账号=${dedicatedAccounts.length}个, 共享账号=${sharedAccounts.length}个, 总计=${accounts.length}个`);
      logger.info(`专属账号列表: ${JSON.stringify(dedicatedAccounts.map(a => ({ cookie_id: a.cookie_id, is_shared: a.is_shared, user_id: a.user_id })))}`);
      logger.info(`共享账号列表: ${JSON.stringify(sharedAccounts.map(a => ({ cookie_id: a.cookie_id, is_shared: a.is_shared, user_id: a.user_id })))}`);
    }

    if (accounts.length === 0) {
      throw new Error('没有可用的账号，请添加账号');
    }

    // 过滤出对该模型可用的账号
    const availableAccounts = [];
    for (const account of accounts) {
      const isAvailable = await quotaService.isModelAvailable(account.cookie_id, model_name);
      if (isAvailable) {
        // 如果是共享cookie，检查用户共享配额池
        if (account.is_shared === 1) {
          // 获取该模型所属的配额共享组
          const sharedModels = quotaService.getQuotaSharedModels(model_name);
          
          // 检查用户是否有该共享组中任意模型的配额
          let hasQuota = false;
          for (const sharedModel of sharedModels) {
            const userQuota = await quotaService.getUserModelSharedQuotaPool(user_id, sharedModel);
            if (userQuota && userQuota.quota > 0) {
              hasQuota = true;
              logger.info(`用户共享配额可用: user_id=${user_id}, model=${model_name}, shared_model=${sharedModel}, quota=${userQuota.quota}`);
              break;
            }
          }
          
          if (!hasQuota) {
            logger.warn(`用户共享配额不足: user_id=${user_id}, model=${model_name}, checked_models=${sharedModels.join(', ')}`);
            continue; // 跳过此共享cookie
          }
        }
        availableAccounts.push(account);
      }
    }

    if (availableAccounts.length === 0) {
      throw new Error(`所有账号对模型 ${model_name} 的配额已耗尽或用户共享配额不足`);
    }

    // 根据优先级选择账号：优先从第一优先级的账号池中随机选择
    let selectedPool = [];
    let poolType = '';
    
    if (preferShared === 1) {
      // 共享优先：先尝试从共享账号中选择
      const sharedAvailable = availableAccounts.filter(acc => acc.is_shared === 1);
      if (sharedAvailable.length > 0) {
        selectedPool = sharedAvailable;
        poolType = '共享账号池';
      } else {
        selectedPool = availableAccounts.filter(acc => acc.is_shared === 0);
        poolType = '专属账号池（共享池无可用账号）';
      }
    } else {
      // 专属优先：先尝试从专属账号中选择
      const dedicatedAvailable = availableAccounts.filter(acc => acc.is_shared === 0);
      if (dedicatedAvailable.length > 0) {
        selectedPool = dedicatedAvailable;
        poolType = '专属账号池';
      } else {
        selectedPool = availableAccounts.filter(acc => acc.is_shared === 1);
        poolType = '共享账号池（专属池无可用账号）';
      }
    }

    // 从选定的池中随机选择
    const randomIndex = Math.floor(Math.random() * selectedPool.length);
    const account = selectedPool[randomIndex];
    
    logger.info(`========== 最终选择账号 ==========`);
    logger.info(`从${poolType}的${selectedPool.length}个账号中随机选择第${randomIndex}个`);
    logger.info(`选中账号: cookie_id=${account.cookie_id}, is_shared=${account.is_shared}, user_id=${account.user_id}`);
    logger.info(`所有配额可用账号: ${JSON.stringify(availableAccounts.map(a => ({ cookie_id: a.cookie_id, is_shared: a.is_shared, user_id: a.user_id })))}`);

    // 检查token是否过期，如果过期则刷新
    if (accountService.isTokenExpired(account)) {
      logger.info(`账号token已过期，正在刷新: cookie_id=${account.cookie_id}`);
      const tokenData = await oauthService.refreshAccessToken(account.refresh_token);
      const expires_at = Date.now() + (tokenData.expires_in * 1000);
      await accountService.updateAccountToken(account.cookie_id, tokenData.access_token, expires_at);
      account.access_token = tokenData.access_token;
      account.expires_at = expires_at;
    }

    return account;
  }

  /**
   * 生成助手响应（使用多账号）
   * @param {Object} requestBody - 请求体
   * @param {Function} callback - 回调函数
   * @param {string} user_id - 用户ID
   * @param {string} model_name - 模型名称
   */
  async generateResponse(requestBody, callback, user_id, model_name, user) {
    const account = await this.getAvailableAccount(user_id, model_name, user);
    
    // 判断是否为 Gemini 模型（不输出 <think> 标记）
    const isGeminiModel = model_name.startsWith('gemini-');
    
    // 对话开始前实时获取quota，如果为0则重新选择cookie
    let quotaBefore = null;
    let retryCount = 0;
    const maxRetries = 5;
    
    while (retryCount < maxRetries) {
      try {
        // 先更新该cookie的最新quota
        await this.refreshCookieQuota(account.cookie_id, account.access_token);
        
        const quotaInfo = await quotaService.getQuota(account.cookie_id, model_name);
        quotaBefore = quotaInfo ? parseFloat(quotaInfo.quota) : null;
        
        // 如果quota为0，轮换cookie
        if (quotaBefore !== null && quotaBefore <= 0) {
          logger.warn(`Cookie配额已耗尽，轮换cookie: cookie_id=${account.cookie_id}, quota=${quotaBefore}`);
          retryCount++;
          if (retryCount < maxRetries) {
            // 重新获取可用账号（随机选择会选到不同的cookie）
            const newAccount = await this.getAvailableAccount(user_id, model_name, user);
            Object.assign(account, newAccount);
            continue;
          } else {
            throw new Error(`已尝试${maxRetries}次，所有cookie的配额都已耗尽`);
          }
        }
        
        logger.info(`对话开始 - cookie_id=${account.cookie_id}, model=${model_name}, quota_before=${quotaBefore}, 尝试次数=${retryCount + 1}`);
        break;
      } catch (error) {
        if (retryCount >= maxRetries - 1) {
          throw error;
        }
        logger.warn('获取对话前quota失败，重试:', error.message);
        retryCount++;
      }
    }
    
    const url = config.api.url;
    
    const requestHeaders = {
      'Host': config.api.host,
      'User-Agent': config.api.userAgent,
      'Authorization': `Bearer ${account.access_token}`,
      'Content-Type': 'application/json',
      'Accept-Encoding': 'gzip'
    };
    
    let response;
    
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: requestHeaders,
        body: JSON.stringify(requestBody)
      });
      
      if (!response.ok) {
        const responseText = await response.text();
        
        if (response.status === 403) {
          logger.warn(`账号没有使用权限，已禁用: cookie_id=${account.cookie_id}`);
          await accountService.updateAccountStatus(account.cookie_id, 0);
        }
        throw new ApiError(responseText, response.status, responseText);
      }
      
    } catch (error) {
      throw error;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let thinkingStarted = false;
    let toolCalls = [];
    let generatedImages = [];
    let buffer = ''; // 用于处理跨chunk的JSON

    let chunkCount = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      
      const chunk = decoder.decode(value, { stream: true });
      chunkCount++;
      
      buffer += chunk;
      
      const lines = buffer.split('\n');
      // 保留最后一行(可能不完整)
      buffer = lines.pop() || '';
      
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        
        const jsonStr = line.slice(6).trim();
        if (!jsonStr) continue;
        
        try {
          const data = JSON.parse(jsonStr);
          
          const parts = data.response?.candidates?.[0]?.content?.parts;
          
          if (parts) {
            for (const part of parts) {
              if (part.thought === true) {
                if (isGeminiModel) {
                  // Gemini 模型：将 thought 内容当作普通文本返回
                  callback({ type: 'text', content: part.text || '' });
                } else {
                  // 其他模型：使用 <think> 标记包裹
                  if (!thinkingStarted) {
                    callback({ type: 'thinking', content: '<think>\n' });
                    thinkingStarted = true;
                  }
                  callback({ type: 'thinking', content: part.text || '' });
                }
              } else if (part.text !== undefined) {
                // 过滤掉空的非thought文本
                if (part.text.trim() === '') {
                  continue;
                }
                if (thinkingStarted && !isGeminiModel) {
                  callback({ type: 'thinking', content: '\n</think>\n' });
                  thinkingStarted = false;
                }
                callback({ type: 'text', content: part.text });
              } else if (part.inlineData) {
                // 处理生成的图像
                generatedImages.push({
                  mimeType: part.inlineData.mimeType,
                  data: part.inlineData.data
                });
                callback({
                  type: 'image',
                  image: {
                    mimeType: part.inlineData.mimeType,
                    data: part.inlineData.data
                  }
                });
              } else if (part.functionCall) {
                toolCalls.push({
                  id: part.functionCall.id,
                  type: 'function',
                  function: {
                    name: part.functionCall.name,
                    arguments: JSON.stringify(part.functionCall.args)
                  }
                });
              }
            }
          }
          
          if (data.response?.candidates?.[0]?.finishReason) {
            if (thinkingStarted && !isGeminiModel) {
              callback({ type: 'thinking', content: '\n</think>\n' });
              thinkingStarted = false;
            }
            if (toolCalls.length > 0) {
              callback({ type: 'tool_calls', tool_calls: toolCalls });
              toolCalls = [];
            }
          }
        } catch (e) {
          logger.warn(`JSON解析失败: ${e.message}`);
        }
      }
    }

    // 对话完成后，更新配额信息并记录消耗
    try {
      const quotaAfter = await this.updateQuotaAfterCompletion(account.cookie_id, model_name);
      
      // 记录配额消耗（所有cookie都记录）
      if (quotaBefore !== null && quotaAfter !== null) {
        await quotaService.recordQuotaConsumption(
          user_id,
          account.cookie_id,
          model_name,
          quotaBefore,
          quotaAfter,
          account.is_shared
        );
        const consumed = parseFloat(quotaBefore) - parseFloat(quotaAfter);
        logger.info(`配额消耗已记录 - user_id=${user_id}, is_shared=${account.is_shared}, consumed=${consumed.toFixed(4)}`);
      } else {
        logger.warn(`无法记录配额消耗 - quotaBefore=${quotaBefore}, quotaAfter=${quotaAfter}`);
      }
    } catch (error) {
      logger.error('更新配额或记录消耗失败:', error.message, error.stack);
      // 不影响主流程，只记录错误
    }
  }

  /**
   * 获取可用模型列表
   * @param {string} user_id - 用户ID
   * @returns {Promise<Object>} 模型列表
   */
  async getAvailableModels(user_id) {
    // 获取任意一个可用账号
    const accounts = await accountService.getAvailableAccounts(user_id);
    
    if (accounts.length === 0) {
      throw new Error('没有可用的账号');
    }

    const account = accounts[0];

    // 检查token是否过期
    if (accountService.isTokenExpired(account)) {
      const tokenData = await oauthService.refreshAccessToken(account.refresh_token);
      const expires_at = Date.now() + (tokenData.expires_in * 1000);
      await accountService.updateAccountToken(account.cookie_id, tokenData.access_token, expires_at);
      account.access_token = tokenData.access_token;
    }

    const modelsUrl = config.api.modelsUrl;
    
    const requestHeaders = {
      'Host': config.api.host,
      'User-Agent': config.api.userAgent,
      'Authorization': `Bearer ${account.access_token}`,
      'Content-Type': 'application/json',
      'Accept-Encoding': 'gzip'
    };
    const requestBody = {};
    
    let response;
    let data;
    
    try {
      response = await fetch(modelsUrl, {
        method: 'POST',
        headers: requestHeaders,
        body: JSON.stringify(requestBody)
      });
      
      data = await response.json();
      
      if (!response.ok) {
        throw new ApiError(JSON.stringify(data), response.status, JSON.stringify(data));
      }
      
    } catch (error) {
      throw error;
    }
    
    // 更新配额信息
    if (data.models) {
      await quotaService.updateQuotasFromModels(account.cookie_id, data.models);
    }

    const models = data?.models || {};
    return {
      object: 'list',
      data: Object.keys(models).map(id => ({
        id,
        object: 'model',
        created: Math.floor(Date.now() / 1000),
        owned_by: 'google'
      }))
    };
  }

  /**
   * 刷新cookie的quota（实时获取）
   * @param {string} cookie_id - Cookie ID
   * @param {string} access_token - Access Token
   * @returns {Promise<void>}
   */
  async refreshCookieQuota(cookie_id, access_token) {
    const modelsUrl = config.api.modelsUrl;
    
    try {
      const requestHeaders = {
        'Host': config.api.host,
        'User-Agent': config.api.userAgent,
        'Authorization': `Bearer ${access_token}`,
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip'
      };
      const requestBody = {};
      
      const response = await fetch(modelsUrl, {
        method: 'POST',
        headers: requestHeaders,
        body: JSON.stringify(requestBody)
      });
      
      if (response.ok) {
        const data = await response.json();
        
        if (data.models) {
          await quotaService.updateQuotasFromModels(cookie_id, data.models);
        }
      }
    } catch (error) {
      logger.warn(`刷新quota失败: cookie_id=${cookie_id}`, error.message);
    }
  }

  /**
   * 对话完成后更新配额
   * @param {string} cookie_id - Cookie ID
   * @param {string} model_name - 模型名称
   * @returns {Promise<number|null>} 更新后的quota值
   */
  async updateQuotaAfterCompletion(cookie_id, model_name) {
    const account = await accountService.getAccountByCookieId(cookie_id);
    if (!account) {
      logger.warn(`账号不存在: cookie_id=${cookie_id}`);
      return null;
    }

    await this.refreshCookieQuota(cookie_id, account.access_token);
    
    // 返回更新后的quota值
    const quotaInfo = await quotaService.getQuota(cookie_id, model_name);
    return quotaInfo ? quotaInfo.quota : null;
  }
}

const multiAccountClient = new MultiAccountClient();
export default multiAccountClient;