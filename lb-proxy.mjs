#!/usr/bin/env node
// lb-proxy.mjs — 统一前端代理 (单一数据源: 聚合接口)
// 启动时从聚合接口拉取全部模型, 按「家族」自动分成若干组(deepseek-flash / deepseek-pro / glm / kimi /
// qwen / minimax / gpt / gemini / grok / step / hy ...)。每个组内多渠道 round-robin(雨露均沾) +
// 故障转移 + 每渠道 token 预算轮换 + 渠道失败冷却。对客户端隐藏一切渠道错误, 少重试, 尽量流畅。
// 运行时: 单文件, 只用 node 内置模块, 零依赖, 轻量。
//
// 用法: node lb-proxy.mjs [--port 8399]
// 客户端: baseURL=http://127.0.0.1:8399/v1   model=<组名, 如 deepseek-flash>   (key 任意)
//        也可用聚合接口的精确渠道 id(如 buddy/deepseek-v4.1-flash)直连单条渠道。
//
// 环境变量:
//   LB_PORT / LB_HOST            本代理监听
//   LB_AGG_BASE / LB_AGG_KEY     上游聚合接口(唯一数据源)
//   LB_CHANNEL_BUDGET            每渠道 token 预算(默认 1_000_000), 达到后轮换(雨露均沾)
//   LB_COOLDOWN_MS               指数退避起点(默认 3000=3s), 连续失败逐次翻倍至 LB_COOLDOWN_MS_MAX
//   LB_ROUTE_TIMEOUT_MS          单渠道响应/首包超时(默认 30000), 超时即切换
//   LB_LOW_PREFIX                要降到最低优先级的提供商前缀(逗号分隔, 如 qodercn) —— EAC 用
//   LB_REFRESH_MS                重新拉取模型列表的周期(默认 600000)
//   LB_SYSTEM                    首条非 system 时补的中性 system(部分渠道强制要求首条为 system)
//   LB_FALLBACK                  跨模型免费兜底: 原组全不可用时改用其它免费模型顶上(默认 1 开; 0 关)
//   LB_FALLBACK_MAX              每次最多尝试多少条兜底渠道(默认 8)
//   LB_RETRY                     连接类失败当场廉价重试次数(默认 1)
//   LB_DEADLINE_MS               单请求软时限(默认 50000)
//   LB_PAID_RESORT               付费底牌渠道(逗号分隔, 默认 workbuddy/deepseek-v4.1-flash-sg); 全免费顶不上才用; 空=关
//   LB_PAID_BEFORE_FALLBACK      设 1 则付费底牌先于跨模型兜底(默认否: 先用免费其它模型)
//   说明: deepseek-flash 与 deepseek-pro 为两个独立集合, 互不归纳; flash 家族免费渠道(含 vision-exp)全部并入 deepseek-flash

import http from 'node:http';
import https from 'node:https';
import stream from 'node:stream';
import { URL } from 'node:url';

const ENDPOINT = process.env.LB_AGG_BASE || 'http://127.0.0.1:18900/v1';
const AGG_KEY = process.env.LB_AGG_KEY || 'ofm-n2KSsfvJsc34lpINnEOV5reZsX9JQ3cw';
const PORT = Number(process.env.LB_PORT) || 8399;
const HOST = process.env.LB_HOST || '127.0.0.1';
const CHANNEL_BUDGET = Number(process.env.LB_CHANNEL_BUDGET) || 1_000_000;
const COOLDOWN_MS = Number(process.env.LB_COOLDOWN_MS) || 3_000;          // 指数退避起点: 3s(瞬态故障几秒即回场)
// 上游整体连接失败(ECONNREFUSED/ENOTFOUND 等)用短冷却, 避免聚合接口瞬态抖动就把整池冷却 5 分钟
const CONNECT_COOLDOWN_MS = Number(process.env.LB_CONNECT_COOLDOWN_MS) || 3_000; // 连接类失败也从3s起退避
const COOLDOWN_MAX_MS = Number(process.env.LB_COOLDOWN_MS_MAX) || 120_000; // 冷却封顶: 瞬态故障别一锁十几分钟(默认2分钟, 到点即重新参与)
const ROUTE_TIMEOUT_MS = Number(process.env.LB_ROUTE_TIMEOUT_MS) || 25_000;
const STREAM_IDLE_MS = Number(process.env.LB_STREAM_IDLE_MS) || 180_000; // 已提交的流, 超过该时长仍无新字节视为卡死→销毁并冷却该渠道
const PRESTREAM_IDLE_MS = Number(process.env.LB_PRESTREAM_IDLE_MS) || 60_000; // 尚未见到真实内容时的"墙钟"上限(从开流起算, 无视 ping 保活), 超时即换渠道(防上游 300s idle 拖死)
const POSTSTREAM_IDLE_MS = Number(process.env.LB_POSTSTREAM_IDLE_MS) || 60_000; // 已提交后"无新内容"的墙钟上限(ping 不续期): 上游 300s idle 前先截断, 客户端拿到干净 [DONE]
const LOW_PREFIXES = (process.env.LB_LOW_PREFIX || '').split(',').map(s => s.trim()).filter(Boolean);
// 渠道可靠度分层(越小越优先; 仅影响冷启动优先命中/轮询起点。实测 buddy/opencode/workbuddy/lobsterai 稳, deepseek-ai/qoder 常超时)
const PROVIDER_TIER = { buddy: 0, opencode: 1, workbuddy: 2, lobsterai: 3, 'z-ai': 4, moonshotai: 4, 'deepseek-ai': 5, codearts: 6, qoder: 7, qodercn: 7 };
const providerRank = p => (LOW_PREFIXES.includes(p) ? 999 : (PROVIDER_TIER[p] ?? 50));
const REFRESH_MS = Number(process.env.LB_REFRESH_MS) || 600_000;
const SYSTEM_FALLBACK = process.env.LB_SYSTEM || 'You are a helpful assistant.';
// ---------- 兜底 / 熔断(夜间自动化不挂) ----------
// 原组全不可用 → 阶段二改用「其它免费模型」顶上 → 阶段三上「付费底牌」(同家族, 如 deepseek-v4.1-flash-sg)
const FALLBACK = (process.env.LB_FALLBACK || '1') !== '0';        // 跨模型免费兜底(默认开; =0 关)
const FALLBACK_MAX = Number(process.env.LB_FALLBACK_MAX) || 8;    // 每次最多试多少条兜底渠道
const RETRY = Number(process.env.LB_RETRY) || 1;                  // 连接类失败当场廉价重试次数
const DEADLINE_MS = Number(process.env.LB_DEADLINE_MS) || 50_000;
const AFFINITY = (process.env.LB_AFFINITY || '1') !== '0';   // 会话亲和: 同一对话稳定命中同一渠道, 保 KV/前缀缓存、减少切换(=0 关, 退回纯轮询) // 单请求软时限, 防止一次串太多超时
const PAID_BEFORE_FALLBACK = process.env.LB_PAID_BEFORE_FALLBACK === '1'; // 付费底牌是否先于跨模型兜底(默认否: 先用免费其它模型)
const PAID_RESORT = (process.env.LB_PAID_RESORT || 'workbuddy/deepseek-v4.1-flash-sg').split(',').map(s => s.trim()).filter(Boolean); // 付费底牌渠道(全免费顶不上才用; 空=关)
// 兜底顺序(便宜/通用优先); 未列出的组排最后。注意: 不含 deepseek-flash/deepseek-pro(两个 ds 集合严格分开, 互不归纳)
const FALLBACK_ORDER = ['glm-flash', 'kimi', 'qwen', 'glm', 'minimax', 'doubao', 'gpt', 'gemini', 'hy', 'openpangu', 'grok', 'step', 'nemotron', 'multimodal'];

// ---------------- 模型分类 ----------------
// 已知的「暗号名 → 标准组」(来自用户首份清单: qoder/dfmodel=DeepSeek-Flash, dmodel=DeepSeek-V4-Pro ...)
const ALIAS = { dfmodel: 'deepseek-flash', dmodel: 'deepseek-pro' };
// workbuddy 的通用档位别名(非具体模型), 不纳入路由
const EXCLUDE_BASES = new Set(['default-model', 'fast-model', 'balanced-model', 'primary-model', 'deep-model']);
// 高配/高价版本直接忽略: Max / high-speed / ultra 等(用户要求规避)
// 高配/高价版本直接忽略: Max / high-speed / ultra 等(用户要求规避); 带边界判定, 避免误杀 minimax 这类含 "max" 子串的正常模型
const PREMIUM = /(?:^|[-_/:.\s])(?:max|highspeed|high-speed|ultra|ultimate|super|flagship)(?:$|[-_/:.\s])/i;

const baseOf = id => id.includes('/') ? id.split('/')[1] : id;
const providerOf = id => id.includes('/') ? id.split('/')[0] : '';

function classify(id) {
  const b = baseOf(id).toLowerCase().replace(/:free$/, '');
  if (EXCLUDE_BASES.has(b) || /-sg$/.test(b)) return null;   // 付费 SG + 通用档位
  if (PREMIUM.test(b)) return null;                          // 忽略 Max / high-speed / ultra 等高配版本
  if (ALIAS[b]) return ALIAS[b];
  const isFlash = b.includes('flash');
  if (b.includes('deepseek')) return isFlash ? 'deepseek-flash' : (b.includes('pro') ? 'deepseek-pro' : 'deepseek'); // deepseek 全系列(含 v4-flash/v4.1-flash/vision-exp)归 flash/pro, 两个集合严格分开
  if (b.includes('vision') || /(?:^|[-_])omni/.test(b) || /-\d+v(?:[-_.]|$)/.test(b)) return 'multimodal'; // 视觉/omni/glm-5v 类单独低优先级成组
  if (b.includes('gemini')) return 'gemini';
  if (b.includes('grok')) return 'grok';
  if (b.includes('glm')) return isFlash ? 'glm-flash' : 'glm';
  if (b.includes('kimi')) return 'kimi';
  if (b.includes('qwen')) return 'qwen';
  if (b.includes('minimax')) return 'minimax';
  if (b.includes('doubao')) return 'doubao';
  if (b.includes('gpt')) return 'gpt';
  if (b.startsWith('step')) return 'step';
  if (b.startsWith('hy')) return 'hy';
  if (b.includes('nemotron')) return 'nemotron';
  if (b.includes('openpangu')) return 'openpangu';
  if (b.includes('mimo')) return 'mimo';                                   // 小米 MiMo
  if (b.includes('longcat')) return 'longcat';
  if (b.includes('space-bunny') || b.includes('spacebunny')) return 'space-bunny';
  if (b.includes('north-mini')) return 'cohere';                           // cohere north-mini-code
  if (b.includes('lfm')) return 'lfm';                                     // liquid lfm
  if (b.includes('dots')) return 'dots';                                   // dots-studio dots-3
  if (b.includes('inkling')) return 'inkling';                             // thinkingmachines
  if (b.includes('laguna')) return 'laguna';                               // poolside
  if (b.includes('xing')) return 'xingchen';                               // xingchen-agi xing4.0
  return null; // 未知/暗号渠道不强行归类(保质量), 仍可用精确 id 直连
}

// 从 base 里取主版本号(取第一处数字, 如 glm-5.3→5.3, kimi-k2.6→2.6, deepseek-v4.1-flash→4.1)
function versionNum(base) { const m = String(base).match(/(\d+(?:\.\d+)?)/); return m ? parseFloat(m[1]) : -1; }
// 同一提供商同组只取一个: 优先稳定版(去掉 preview/experimental, 除非全是) → 避开安全过滤类 → 取「中等档次」(≥2个版本取次高, 规避最新最高) → 同版本多变体时优先 flash/lite/air/mini 轻量档(便宜)
function pickBestForProvider(items) {
  let pool = items.filter(x => !/preview|experimental/.test(x.base));
  if (!pool.length) pool = items;
  const useful = pool.filter(x => !/content-safety|guardrail/.test(x.base)); // 安全过滤类模型不适合对话
  if (useful.length) pool = useful;
  const vers = [...new Set(pool.map(x => x.version))].sort((a, b) => b - a);
  const target = vers[Math.floor(vers.length / 2)];                        // 取版本中位数=中等档次(避开最新最高, 也不选最老)
  let cand = pool.filter(x => x.version === target);
  if (cand.length > 1) {
    const cheap = cand.filter(x => /flash|lite|air|mini/.test(x.base));       // 同档多变体: 优先轻量/低价变体
    if (cheap.length) cand = cheap;
  }
  return cand[0];
}

// 硬排除(免费组不路由): 付费 sg / 高配(Max·高速·ultra·super) / workbuddy 通用档位
function isHardExcluded(id) {
  const b = baseOf(id).toLowerCase().replace(/:free$/, '');
  return EXCLUDE_BASES.has(b) || /-sg$/.test(b) || PREMIUM.test(b);
}
// 付费底牌渠道归属哪个家族组(忽略 sg 排除; 仅 deepseek 家族目前有付费底牌)
function paidGroupOf(id) {
  const b = baseOf(id).toLowerCase().replace(/:free$/, '');
  if (b.includes('deepseek')) return b.includes('flash') ? 'deepseek-flash' : (b.includes('pro') ? 'deepseek-pro' : 'deepseek');
  return null;
}

// ---------------- 运行期状态 ----------------
let groups = {};            // 组名 -> [{id, provider}] (已排序)
let paidGroups = {};        // 付费底牌: 组名 -> [{id, provider}] (全免费顶不上才用)
const ids = new Set();      // 全部可路由的精确渠道 id
const runtime = new Map();  // id -> {tokensUsed, requests, failures, cooldownUntil, lastError}
let rr = 0;                 // round-robin 游标
const sticky = new Map();  // 会话亲和: 对话键 → 锁定的 home 渠道 id(持久化, 只在 home 自身失败/冷却时才重选)

const st = id => runtime.get(id) || (runtime.set(id, { tokensUsed: 0, requests: 0, failures: 0, cooldownUntil: 0, lastError: '' }), runtime.get(id));
function isHealthy(id) { const s = runtime.get(id); return !s || (s.cooldownUntil <= Date.now() && s.tokensUsed < CHANNEL_BUDGET); }
const notCooling = id => (runtime.get(id)?.cooldownUntil || 0) <= Date.now();
function markFail(id, msg) {
  const s = st(id); s.failures++; s.lastError = msg;
  const connErr = /ECONNREFUSED|ENOTFOUND|EAI_AGAIN|EHOSTUNREACH|ENETUNREACH|ECONNRESET|EPIPE/.test(msg); // 上游整体不可达
  // 指数退避: 3s 起步, 连续失败逐次翻倍, 很快回场不锁死; 连接类封顶更低(60s)
  const cap = connErr ? Math.min(COOLDOWN_MAX_MS, 60_000) : COOLDOWN_MAX_MS;
  const backoff = Math.round(Math.min(cap, COOLDOWN_MS * Math.pow(2, Math.min(s.failures - 1, 8))) * (0.8 + Math.random() * 0.4)); // ±20% 抖动: 避免整组同步恢复造成"全不可用"
  s.cooldownUntil = Date.now() + backoff;
  console.log(`[lb] 渠道 ${id} 失败(${msg}), 冷却 ${(backoff / 1000).toFixed(1)}s (第${s.failures}次连败)`);
}
function markSuccess(id, tokens) { const s = st(id); s.requests++; s.tokensUsed += tokens; if (s.failures > 0) { s.failures = 0; s.cooldownUntil = 0; } }
function pickRoute(list) {
  const notCool = list.filter(r => notCooling(r.id));
  const inBudget = notCool.filter(r => (runtime.get(r.id)?.tokensUsed || 0) < CHANNEL_BUDGET);
  if (inBudget.length) return inBudget[rr++ % inBudget.length];               // 常规: 未到预算 round-robin(雨露均沾)
  if (notCool.length) { notCool.sort((a, b) => (runtime.get(a.id).tokensUsed - runtime.get(b.id).tokensUsed)); return notCool[0]; } // 全到软预算: 用烧得最少的, 尽量不停
  return null;                                                                // 都在真正的冷却中
}
// 会话亲和(粘性): 同一对话(同一 system+首个 user 内容)稳定落在同一渠道, 保 KV 缓存; 不同对话天然分散→仍雨露均沾
function hashStr(s) { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h >>> 0; }
function affinityKey(inBody, messages) {
  try {
    const sys = (messages[0] && messages[0].role === 'system') ? String(messages[0].content || '') : '';
    const fu = (messages.find(m => m.role === 'user') || {}).content;
    const fuS = typeof fu === 'string' ? fu : (fu ? JSON.stringify(fu).slice(0, 300) : '');
    return (inBody.user ? String(inBody.user) : '') + '|' + sys.slice(0, 300) + '|' + fuS.slice(0, 300); // 只用恒定前缀(不含整段历史), 保证同任务每轮同键
  } catch { return null; }
}
function pickSticky(list, key) {
  const n = list.length; if (!n) return null;
  const start = (AFFINITY && key) ? (hashStr(key) % n) : (rr++ % n);
  const notCool = r => (runtime.get(r.id)?.cooldownUntil || 0) <= Date.now();
  const inBudget = r => (runtime.get(r.id)?.tokensUsed || 0) < CHANNEL_BUDGET;
  for (const ok of [r => notCool(r) && inBudget(r), r => notCool(r)]) for (let i = 0; i < n; i++) { const r = list[(start + i) % n]; if (ok(r)) return r; }
  return null;
}

function resolveGroupName(model) {
  if (groups[model]) return model;
  const c = classify(model);               // 把裸模型名(如 deepseek-v4.1-flash)也归到组
  if (c && groups[c]) return c;
  return null;
}
// 精确渠道 id → 原样单条直连; 否则当家族组/裸名
function resolveList(model) {
  if (!model) return null;
  if (ids.has(model)) return [{ id: model, provider: providerOf(model) }];
  const gn = resolveGroupName(model);
  return gn ? groups[gn] : null;
}

// ---------------- 网络 ----------------
async function fetchWithTimeout(base, pathAndQuery, { method = 'GET', headers = {}, body = null, timeoutMs = 20000 }) {
  const u = new URL(base + pathAndQuery);
  const mod = u.protocol === 'https:' ? https : http;
  const opts = { method, hostname: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80), path: u.pathname + u.search, headers };
  return new Promise((resolve, reject) => {
    const req = mod.request(opts, res => { try { res.socket?.setTimeout(0); } catch {} resolve(res); }); // 透传长流时不切
    req.setTimeout(timeoutMs, () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}
async function readAll(res) { const chunks = []; for await (const c of res) chunks.push(Buffer.from(c)); return Buffer.concat(chunks); }
const errText = buf => (buf.toString('utf8') || '').slice(0, 200).replace(/\s+/g, ' ');
function sendJson(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(obj)); }
function ensureSystemFirst(messages) {
  const msgs = Array.isArray(messages) ? messages.map(m => m) : [];
  if (!msgs.length || msgs[0]?.role !== 'system') msgs.unshift({ role: 'system', content: SYSTEM_FALLBACK });
  return msgs;
}
const tokensOf = usage => usage ? (usage.total_tokens ?? (usage.prompt_tokens || 0) + (usage.completion_tokens || 0)) : 0;
// 识别「渠道错误」: 聚合接口常把上游错误以 `<厂商>: 描述` 前缀塞进看似正常的响应(甚至 HTTP 200 / 流式 content 里)
const ERROR_PROVIDER = /^\s*(codearts|workbuddy|buddy|qoder|qodercn|lobsterai|z-ai|moonshotai|kimi|deepseek-ai|opencode|nvidia|openai|pi-ai|stealth|reflection|poolside|cohere|liquid|dots-studio|xingchen-agi|thinkingmachines)\s*[:：]/i;
const ERROR_PHRASE = /request param is invalid|please check it|请求被拦截|请重新发送|model_error|is not system prompt|参数(错误|非法|无效)|invalid\s+(request\s+)?param|invalid_request_error|余额(不足|已用完|不足以)|(额度|配额)(不足|已用尽|耗尽|超额)|quota (exceeded|insufficient|exhausted|not enough)|rate limit(ed)? (exceeded)?|too many requests|temporarily unavailable|upstream (error|unavailable|overloaded|busy)|overloaded|context length (exceeded|too long)|maximum context length|service (is )?(currently )?unavailable|不(支持|接受|支援)图片|不(支持|接受)图像|does not support (any )?image|image input.{0,20}(not|unsupport)|stream ended before a completion|turn may be incomplete|incomplete turn|idle timeout|stream idle timeout|timed out after|connection (reset|closed)|premature close|no choices returned|closed the stream|before (its|the) finish token|without answering|finish token|所有账号均不可用|均不可用/i;
function isErrorText(t) {
  if (typeof t !== 'string') return false;
  if (ERROR_PROVIDER.test(t) || ERROR_PHRASE.test(t)) return true;
  const n = t.replace(/\s+/g, ' ').trim(); // 上游可能把错误文本拆行/多空格("pi-ai stream\nidle timeout"), 归一化后再判
  return ERROR_PROVIDER.test(n) || ERROR_PHRASE.test(n);
}
// 判断一个 SSE chunk 是否含「错误」或「真实内容」(role/空 content/ping 不算内容)
function classifyChunk(s) {
  let err = false, hasContent = false;
  if (isErrorText(s)) err = true;
  for (const line of s.split('\n')) {
    if (!line.startsWith('data:')) continue;
    const p = line.slice(5).trim(); if (!p || p === '[DONE]') continue;
    let ev; try { ev = JSON.parse(p); } catch { continue; }
    if (ev.error) err = true;
    const d = ev.choices && ev.choices[0] && ev.choices[0].delta;
    if (d) {
      if (typeof d.content === 'string' && d.content.trim()) { hasContent = true; if (isErrorText(d.content)) err = true; }
      if (typeof d.reasoning_content === 'string' && d.reasoning_content.trim()) hasContent = true;
    }
  }
  return { err, hasContent };
}
// 已知纯文本渠道(用户清单标注), 图像请求时排到最后; 其余未知的靠"不支持图片"错误兜底换渠道
const TEXT_ONLY = new Set(['lobsterai/deepseek-v4-flash']);
function hasImageInput(messages) { for (const m of (messages || [])) { const c = m && m.content; if (Array.isArray(c)) for (const p of c) if (p && (p.type === 'image_url' || p.type === 'input_image' || p.image_url)) return true; } return false; }

// ---------------- 拉取并分类模型 ----------------
async function refreshModels() {
  try {
    const res = await fetchWithTimeout(ENDPOINT, '/models', { headers: { 'Authorization': `Bearer ${AGG_KEY}` }, timeoutMs: 8000 });
    if (res.statusCode !== 200) { res.resume(); return false; }
    const j = JSON.parse((await readAll(res)).toString('utf8'));
    const list = (j.data || []).map(m => m && m.id).filter(Boolean);
    const liveIds = new Set(list);
    const g = {}; ids.clear();
    let i = 0;
    for (const id of list) {
      if (isHardExcluded(id)) continue;             // 付费(sg)/高配/通用档位 不进免费组
      ids.add(id);                                  // 其余(含暗号 qoder/gmodel 等)均可「原样直连」转发
      const grp = classify(id); if (!grp) continue; // 已知家族才建组; 暗号仅精确 id 直连
      const base = baseOf(id).toLowerCase().replace(/:free$/, '');
      (g[grp] ||= []).push({ id, provider: providerOf(id), base, version: versionNum(base), _i: i++ });
    }
    for (const k of Object.keys(g)) {
      const byProv = {};
      for (const r of g[k]) (byProv[r.provider] ||= []).push(r);
      let routes;
      if (k === 'deepseek-flash' || k === 'deepseek-pro') {
        // DeepSeek 两个集合: 免费渠道全部绑进组(可用性优先); 由每渠道预算+round-robin 摊额度; flash 与 pro 严格分开
        routes = Object.values(byProv).flat();
      } else {
        // 其它家族: 同一 provider 只留一个(中档), 省每日额度
        routes = Object.values(byProv).map(pickBestForProvider);
      }
      routes.sort((a, b) => providerRank(a.provider) - providerRank(b.provider) || a._i - b._i);
      g[k] = routes;
    }
    groups = g;
    // 付费底牌: 全免费顶不上时才用, 归入对应家族组, 最低优先级
    const pg = {};
    for (const id of PAID_RESORT) {
      if (!liveIds.has(id)) continue;
      const grp = paidGroupOf(id); if (!grp) continue;
      ids.add(id);
      (pg[grp] ||= []).push({ id, provider: providerOf(id) });
    }
    paidGroups = pg;
    console.log(`[lb] 模型已刷新: ${Object.keys(groups).length} 组 / ${ids.size} 渠道; 付费底牌: ${Object.keys(pg).length ? Object.entries(pg).map(([k, v]) => k + '×' + v.length).join(',') : '无'}`);
    return true;
  } catch (e) { console.log('[lb] 模型刷新失败:', e.message); return false; }
}

// ---------------- 请求处理 ----------------
// 兜底候选: 其它免费模型(不含被排除的组; ds 两集合不互相归纳)
function fallbackCandidates(excludeSet) {
  const out = [], seen = new Set();
  const push = g => { if (excludeSet.has(g) || !groups[g]) return; for (const r of groups[g]) if (!seen.has(r.id)) { seen.add(r.id); out.push(r); } };
  FALLBACK_ORDER.forEach(push);
  for (const g of Object.keys(groups)) push(g);
  return out;
}
// 依次尝试一批渠道, 第一个成功即 true; 至少给一次机会, 之后受软时限约束
async function tryList(res, routes, wantStream, messages, rest, deadline) {
  for (let i = 0; i < routes.length; i++) {
    if (i > 0 && Date.now() >= deadline) break;
    if ((await attempt(res, routes[i], wantStream, messages, rest)).ok) return true;
  }
  return false;
}
// 尝试单条渠道: 成功→写好响应并 {ok:true}; 失败→{ok:false, transient}。transient=连接类(可当场廉价重试)
// 把上游 SSE 泵给客户端(首选层/兜底层共用)。默认"整段缓存·完整才提交"模式:
//  - 渠道错误/只发ping不开工/中途卡死/无完成事件 → 一个字节都不发给客户端, 由上层换渠道重试
//  - 只有完整成功才一次性提交全部 SSE → 客户端永远拿到完整回答, 永远看不到失败原因
//  - 错误文本常被拆成多个 delta("pi-ai stream " + "idle timeout...")逐块判会漏: 用"滚动尾巴"整体判定
// LB_STREAM_LIVE=1 → 退回"见首字即提交"实时模式(首字快, 但提交后出错无法重试, 只能吞掉错误干净收尾)
const STREAM_LIVE = process.env.LB_STREAM_LIVE === '1';
// 客户端未指定 max_tokens/max_completion_tokens 时注入的默认输出上限(0=不注入):
// 不少渠道自带默认值偏小(4k左右), 长回答会被 finish_reason=length 截断; 仅在客户端没显式给值时兜底
const DEFAULT_MAX_TOKENS = Number(process.env.LB_DEFAULT_MAX_TOKENS ?? 8192);
function withDefaultMaxTokens(b) {
  if (DEFAULT_MAX_TOKENS > 0 && !b.max_tokens && !b.max_completion_tokens && !b.max_output_tokens) b.max_tokens = DEFAULT_MAX_TOKENS;
  return b;
}
function pumpStream(up, res, r) {
  return new Promise(resolve => {
    let sawError = false, sawFinish = false, sawDone = false, truncated = false, outBytes = 0, usage = null, committed = false, contentLen = 0;
    const accepted = [];      // 缓存模式: 通过校验的事件块, 完整成功后才提交
    const buffer = [];        // 实时模式: 提交前的原始块
    let carry = '', recent = ''; // SSE 事件可能跨 TCP 包被截断 → 按 "\n\n" 事件边界累积解析; recent: 近期正文尾巴(拆片错误文本整体判)
    let idleTimer = null, preTimer = null, postTimer = null;
    const clearTimers = () => { for (const t of [idleTimer, preTimer, postTimer]) if (t) clearTimeout(t); };
    const armIdle = () => { if (idleTimer) clearTimeout(idleTimer); idleTimer = setTimeout(() => { truncated = true; try { up.destroy(); } catch {} }, STREAM_IDLE_MS); };
    const armPost = () => { if (postTimer) clearTimeout(postTimer); postTimer = setTimeout(() => { truncated = true; try { up.destroy(); } catch {} }, POSTSTREAM_IDLE_MS); };
    preTimer = setTimeout(() => { try { up.destroy(); } catch {} }, PRESTREAM_IDLE_MS); // 提交前墙钟上限: 只见ping无内容也会到点强制切
    const handleEvent = (block) => {
      let hasReal = false, evErr = false;
      const isData = /^[ \t]*data:/m.test(block);
      for (const line of block.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const p = line.slice(5).trim();
        if (p === '[DONE]') { sawFinish = true; sawDone = true; continue; }
        if (!p) continue;
        let ev = null; try { ev = JSON.parse(p); } catch {}
        if (!ev) continue;
        if (ev.error) { evErr = true; sawError = true; continue; }
        if (ev.usage) usage = ev.usage;
        const c = ev.choices && ev.choices[0];
        if (!c) continue;
        if (c.finish_reason) sawFinish = true;
        const d = c.delta || {};
        const t = typeof d.content === 'string' ? d.content : (typeof d.reasoning_content === 'string' ? d.reasoning_content : '');
        if (t.length) {
          hasReal = true; contentLen += t.length;
          recent = (recent + t).slice(-160);               // 只留最近 160 字, 正常正文不会命中错误短语
          if (isErrorText(t) || isErrorText(recent)) { evErr = true; sawError = true; } // 拆片错误文本也能抓住
        }
      }
      return { hasReal, evErr, keep: isData && !evErr };
    };
    const finish = () => {
      clearTimers();
      if (carry.trim()) { const ev = handleEvent(carry); if (ev.keep) accepted.push(carry.replace(/\n+$/, '')); carry = ''; }
      const ok = !sawError && !truncated && sawFinish && contentLen > 0; // 必须有真实内容才算成功(空回答对客户端没用, 换渠道)
      if (STREAM_LIVE && committed) {
        // 实时模式已提交过内容, 无法重试: 出错只能吞掉并补 [DONE] 干净收尾(客户端拿到部分回答而非错误文本)
        if (!sawDone) { try { res.write('data: [DONE]\n\n'); } catch {} }
        try { res.end(); } catch {}
        if (ok) markSuccess(r.id, tokensOf(usage) || Math.ceil(outBytes / 4));
        else markFail(r.id, sawError ? '流式中途渠道错误(已吞掉·干净收尾)' : (truncated ? '流式卡死截断(已干净收尾)' : '流式未正常结束(已干净收尾)'));
        return resolve({ ok: true }); // 对客户端算成功(已提交, 拿到的是部分回答)
      }
      if (ok) { // 缓存模式完整成功: 此刻才提交, 客户端一次性拿到完整 SSE
        try {
          res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-LB-Channel': r.id });
          if (accepted.length) { try { res.write(accepted.join('\n\n') + '\n\n'); } catch {} }
          if (!sawDone) { try { res.write('data: [DONE]\n\n'); } catch {} } // 上游没给 [DONE] 也补一个, 客户端解析更稳
        } catch {}
        try { res.end(); } catch {}
        markSuccess(r.id, tokensOf(usage) || Math.ceil(outBytes / 4));
        return resolve({ ok: true });
      }
      // 失败: 未向客户端发任何字节 → 上层换渠道重试(绝不把失败转嫁客户端)
      resolve({ ok: false, reason: sawError ? '上游返回渠道错误' : (truncated ? '流式卡死无新内容' : (contentLen > 0 ? '流式未正常结束' : '流式无有效内容')) });
    };
    const tap = new stream.Transform({
      transform(chunk, _enc, cb) {
        outBytes += chunk.length;
        carry = (carry + chunk.toString('utf8')).replace(/\r\n/g, '\n');
        let hasReal = false, evErr = false, idx;
        while ((idx = carry.indexOf('\n\n')) >= 0) {
          const block = carry.slice(0, idx); carry = carry.slice(idx + 2);
          const ev = handleEvent(block);
          if (ev.hasReal) hasReal = true;
          if (ev.evErr) evErr = true;
          else if (!STREAM_LIVE && ev.keep) accepted.push(block); // 缓存模式: 只收合格事件
        }
        if (STREAM_LIVE) {
          if (evErr) return cb(); // 实时模式: 错误块一律不转发(提交前不提交, 提交后吞掉)
          if (!committed) {
            buffer.push(chunk);
            if (hasReal) {
              committed = true; armIdle(); armPost(); if (preTimer) clearTimeout(preTimer);
              try { res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-LB-Channel': r.id }); } catch {}
              for (const b of buffer) { try { res.write(b); } catch {} } buffer.length = 0;
            }
            return cb();
          }
          if (hasReal) armIdle(); // 有新内容才续字节空闲; ping 不续
          armPost();              // "无新内容"墙钟: ping 也不能续(防上游 300s idle)
          try { res.write(chunk); } catch {}
          return cb();
        }
        // 缓存模式: 已收进 accepted, 这里只武装看护计时
        if (hasReal) { armIdle(); armPost(); if (preTimer) clearTimeout(preTimer); }
        cb();
      },
    });
    up.on('end', finish); up.on('error', finish); up.on('close', finish); tap.on('error', finish);
    up.pipe(tap);
  });
}
async function attempt(res, r, wantStream, messages, rest) {
  const upBody = { ...rest, model: r.id, messages, stream: wantStream };
  if (wantStream) upBody.stream_options = { include_usage: true };
  withDefaultMaxTokens(upBody);
  const bodyStr = JSON.stringify(upBody);
  const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${AGG_KEY}`, Accept: wantStream ? 'text/event-stream' : 'application/json' };
  let up;
  try {
    up = await fetchWithTimeout(ENDPOINT, '/chat/completions', { method: 'POST', headers, body: bodyStr, timeoutMs: ROUTE_TIMEOUT_MS });
  } catch (e) {
    markFail(r.id, e.message);
    const conn = /ECONNREFUSED|ENOTFOUND|EAI_AGAIN|EHOSTUNREACH|ENETUNREACH|ECONNRESET|EPIPE/.test(e.message);
    return { ok: false, transient: conn };
  }
  if (up.statusCode !== 200) {
    const buf = await readAll(up).catch(() => Buffer.from(''));
    markFail(r.id, `HTTP ${up.statusCode} ${errText(buf)}`);
    return { ok: false, transient: false };
  }
  if (wantStream) {
    const pr = await pumpStream(up, res, r);
    if (!pr.ok) { try { up.destroy(); } catch {}; return { ok: false, transient: false }; } // 失败未发任何字节 → 上层换渠道重试
    return { ok: true };
  } else {
    const buf = await readAll(up).catch(() => Buffer.from(''));
    if (!buf.length) { markFail(r.id, '空响应'); return { ok: false, transient: false }; }
    let j = null; try { j = JSON.parse(buf.toString('utf8')); } catch {}
    const msg = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
    if (!j || j.error || !Array.isArray(j.choices) || !j.choices.length) { markFail(r.id, `无效响应 ${errText(buf)}`); return { ok: false, transient: false }; }
    if (typeof msg === 'string' && isErrorText(msg)) { markFail(r.id, `渠道错误: ${msg.slice(0, 60)}`); return { ok: false, transient: false }; } // 200 里塞了渠道错误 → 换渠道
    if (typeof msg !== 'string' || !msg.trim()) { markFail(r.id, '空内容'); return { ok: false, transient: false }; } // 200 但正文为空 → 对客户端没用, 换渠道
    markSuccess(r.id, tokensOf(j.usage) || Math.ceil(buf.length / 4));
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'X-LB-Channel': r.id });
    return res.end(buf), { ok: true };
  }
}

async function handleChat(req, res, rawBody) {
  let inBody; try { inBody = JSON.parse(rawBody); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
  let list = resolveList(inBody.model);       // 精确渠道id → 原样直连; 否则当家族组/裸名
  if (!list && Object.keys(groups).length === 0) {            // 可能聚合接口启动慢, 现拉一次
    if (await refreshModels()) list = resolveList(inBody.model);
  }
  if (!list) return sendJson(res, 404, { error: { message: `未知模型组 ${inBody.model}。可用: ${Object.keys(groups).join(', ')}` } });

  const wantStream = inBody.stream === true;
  const messages = ensureSystemFirst(inBody.messages);
  const rest = { ...inBody }; delete rest.model; delete rest.stream; delete rest.stream_options;

  let lastErr = '';
  const routeList = hasImageInput(messages) ? list.slice().sort((a, b) => (TEXT_ONLY.has(a.id) ? 1 : 0) - (TEXT_ONLY.has(b.id) ? 1 : 0)) : list;
  const affKey = affinityKey(inBody, messages);
  let pool = routeList;
  if (AFFINITY && affKey) {
    let homeId = sticky.get(affKey);
    const homeOk = id => { const x = routeList.find(z => z.id === id); return !!x && notCooling(id) && ((runtime.get(id)?.tokensUsed || 0) < CHANNEL_BUDGET); };
    if (!homeId || !homeOk(homeId)) { const p = pickRoute(routeList); if (p) { homeId = p.id; if (sticky.size > 5000) sticky.delete(sticky.keys().next().value); sticky.set(affKey, homeId); } }
    if (homeId) { const h = routeList.find(z => z.id === homeId); if (h) pool = [h, ...routeList.filter(z => z.id !== homeId)]; }
  } else { const s = rr++ % routeList.length; pool = routeList.slice(s).concat(routeList.slice(0, s)); }
  for (let i = 0; i < pool.length; i++) {
    const r = pool[i];
    if ((runtime.get(r.id)?.cooldownUntil || 0) > Date.now()) continue; // 冷却中 → 试下一条
    const upBody = { ...rest, model: r.id, messages, stream: wantStream };
    if (wantStream) upBody.stream_options = { include_usage: true };
    withDefaultMaxTokens(upBody);

    let up;
    try {
      up = await fetchWithTimeout(ENDPOINT, '/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AGG_KEY}`, 'Accept': wantStream ? 'text/event-stream' : 'application/json' },
        body: JSON.stringify(upBody), timeoutMs: ROUTE_TIMEOUT_MS,
      });
    } catch (e) { markFail(r.id, e.message); lastErr = e.message; continue; }

    if (up.statusCode !== 200) {
      const buf = await readAll(up).catch(() => Buffer.from(''));
      markFail(r.id, `HTTP ${up.statusCode} ${errText(buf)}`); lastErr = `HTTP ${up.statusCode}`; continue;
    }

    if (wantStream) {
      const pr = await pumpStream(up, res, r);
      if (!pr.ok) { try { up.destroy(); } catch {}; lastErr = pr.reason || 'stream failed'; continue; } // 失败未发任何字节 → 试下一条
      return;
    } else {
      const buf = await readAll(up).catch(() => Buffer.from(''));
      if (!buf.length) { markFail(r.id, '空响应'); lastErr = 'empty body'; continue; }          // 空体: 换渠道
      let j = null; try { j = JSON.parse(buf.toString('utf8')); } catch {}
      const _msg = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
      if (!j || j.error || !Array.isArray(j.choices) || !j.choices.length) { markFail(r.id, `无效响应 ${errText(buf)}`); lastErr = 'bad body'; continue; }
      if (typeof _msg === 'string' && isErrorText(_msg)) { markFail(r.id, '渠道错误: ' + _msg.slice(0, 50)); lastErr = 'channel error'; continue; }
      if (typeof _msg !== 'string' || !_msg.trim()) { markFail(r.id, '空内容'); lastErr = 'empty content'; continue; } // 200 但正文为空 → 换渠道
      markSuccess(r.id, tokensOf(j.usage) || Math.ceil(buf.length / 4));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'X-LB-Channel': r.id });
      return res.end(buf);
    }
  }
  // 阶段二: 跨模型免费兜底(其它免费模型顶上; 精确 id 直连/关兜底时跳过; ds 两集合不互相归纳)
  // 阶段三: 付费底牌(同家族, 如 deepseek-v4.1-flash-sg), 全免费都顶不上时才用
  const groupName = resolveGroupName(inBody.model);
  const exactMode = ids.has(inBody.model);
  const deadline = Date.now() + DEADLINE_MS;
  // (routeList 已在函数上方声明, 避免重复声明)
  const excludeSet = new Set();
  if (groupName) { excludeSet.add(groupName); if (groupName.startsWith('deepseek')) { excludeSet.add('deepseek-flash'); excludeSet.add('deepseek-pro'); } }
  const cross = (!exactMode && FALLBACK) ? fallbackCandidates(excludeSet).filter(x => notCooling(x.id)).slice(0, FALLBACK_MAX) : [];
  const paid = (paidGroups[groupName] || []).filter(x => notCooling(x.id));
  const post = PAID_BEFORE_FALLBACK ? [...paid, ...cross] : [...cross, ...paid];
  if (await tryList(res, post, wantStream, messages, rest, deadline)) return;
  console.log('[lb] 503 ' + (inBody.model || '') + ' | 尝试渠道/原因: ' + [...list, ...post].map(x => x.id + ':' + (runtime.get(x.id)?.lastError || '冷却中/未试').slice(0, 40)).join('  |  '));
  // 全在冷却时的兜底探测: 瞬态全挂后常已恢复却仍在冷却; 无视冷却试一条"最快恢复"的渠道, 成就不 503
  const anyAll = [...list, ...post];
  if (anyAll.length) {
    const probe = anyAll.reduce((a, b) => ((runtime.get(b.id)?.cooldownUntil || 0) < (runtime.get(a.id)?.cooldownUntil || 0) ? b : a));
    if (await attempt(res, probe, wantStream, messages, rest)) return;
  }
  return sendJson(res, 503, { error: { message: '所有渠道(免费/其它模型/付费底牌)暂不可用, 请稍候重试' } });
}

// ---------------- HTTP 服务 ----------------
function serve() {
  http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    if (req.method === 'GET' && (url.pathname === '/v1/models' || url.pathname === '/models'))
      return sendJson(res, 200, { object: 'list', data: Object.keys(groups).sort().map(n => ({ id: n, object: 'model', owned_by: 'lb-proxy', channels: groups[n].length, models: groups[n].map(r => r.id), paid: (paidGroups[n] || []).map(r => r.id) })) });
    if (req.method === 'GET' && url.pathname === '/health') {
      const routes = {}; for (const [id, s] of runtime) routes[id] = s;
      return sendJson(res, 200, { ok: true, groups: Object.keys(groups).length, channels: ids.size, routes });
    }
    if (req.method === 'GET' && url.pathname === '/v1/refresh') { await refreshModels(); return sendJson(res, 200, { ok: true, groups: Object.keys(groups) }); }
    if (req.method === 'POST' && (url.pathname === '/v1/chat/completions' || url.pathname === '/chat/completions'))
      return handleChat(req, res, await readAll(req)).catch(e => { console.error('[lb] handleChat error(本次请求失败, 进程继续):', (e && e.message) || e); try { sendJson(res, 503, { error: { message: 'proxy internal error, 请重试' } }); } catch {} });
    sendJson(res, 404, { error: { message: 'not found' } });
  }).listen(PORT, HOST, () => {
    console.log(`lb-proxy 已启动: http://${HOST}:${PORT}/v1`);
    console.log(`数据源: ${ENDPOINT} | 每渠道预算 ${CHANNEL_BUDGET} tokens | 切换超时 ${ROUTE_TIMEOUT_MS}ms | 冷却 ${COOLDOWN_MS / 1000}s起·指数退避(封顶${COOLDOWN_MAX_MS / 1000}s) | 跨模型兜底 ${FALLBACK ? '开' : '关'} | 付费底牌 ${PAID_RESORT.length ? PAID_RESORT.join(',') : '关'}`);
  });
}

// ---------------- 启动 ----------------
// 常驻关键: 任何单请求异常/未处理的 Promise 拒绝都不允许让进程退出(否则夜里一崩全停)
process.on('uncaughtException', e => console.error('[lb] uncaughtException(已吞掉, 进程继续):', (e && e.message) || e));
process.on('unhandledRejection', e => console.error('[lb] unhandledRejection(已吞掉, 进程继续):', (e && (e.message || e))));
(async () => {
  let ok = await refreshModels(), tries = 0;
  while (!ok && tries++ < 3) { await new Promise(r => setTimeout(r, 2000)); ok = await refreshModels(); }
  if (!ok) console.log('[lb] 启动暂未拉到模型, 后台将继续重试; 首个请求也会触发拉取');
  setInterval(refreshModels, REFRESH_MS);
  serve();
})();
