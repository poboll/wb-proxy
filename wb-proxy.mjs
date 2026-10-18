#!/usr/bin/env node
// wb-proxy.mjs — WorkBuddy 免费 deepseek-v4.1-flash 的 OpenAI 兼容反代
//
// 用法:
//   node wb-proxy.mjs login                  # 浏览器扫码/登录一次, 保存 token 到 ~/.wb-proxy/auth.json
//   node wb-proxy.mjs serve [--port 8377] [--key <local-api-key>]
//
// 客户端接入: baseURL=http://127.0.0.1:8377/v1 , apiKey=<local-api-key 或任意值>, model=deepseek-v4.1-flash
// 同时支持 Chat Completions (/v1/chat/completions) 和 Responses (/v1/responses, Codex 用)

import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { exec } from 'node:child_process';

const ENDPOINT = process.env.WB_ENDPOINT || 'https://www.workbuddy.ai';
const PREFIX = '/v2/plugin';
const CHAT_URL = `${ENDPOINT}/v2/chat/completions`;
const DEFAULT_MODEL = 'deepseek-v4.1-flash';
// 当前 WorkBuddy 限时免费(credit=0)的模型; step-5-preview 促销 2026-10-26 截止, 其余到 2026-11-01
const FREE_MODELS = ['deepseek-v4.1-flash', 'hy3', 'step-5-preview'];
const PAID_MODELS = ['deepseek-v4.1-flash-sg'];
// 付费模型开关: 默认禁用(拦截), 避免意外扣费; 设 WB_ALLOW_PAID=1 并重启可重新开启
const ALLOW_PAID = process.env.WB_ALLOW_PAID === '1';
const AUTH_DIR = path.join(os.homedir(), '.wb-proxy');
// 默认鉴权文件; 设 WB_AUTH_FILE 可让 login 把 token 存到指定路径(用于构建账号池, 不覆盖当前在用文件)
const AUTH_FILE = process.env.WB_AUTH_FILE || path.join(AUTH_DIR, 'auth.json');

// ---------------- 弹性网络层 ----------------
// 本机若有 fake-ip 模式 VPN(Clash/Surge 等), 系统 DNS 会返回 198.18.x.x 假地址,
// 依赖 VPN 隧道转发; 隧道故障时自动改用公共 DoH 解析真实 IP 直连。

const dohCache = new Map();

async function dohResolve(host) {
  if (dohCache.has(host)) return dohCache.get(host);
  const res = await fetch(`https://223.5.5.5/resolve?name=${host}&type=A`, { signal: AbortSignal.timeout(5000) });
  const j = await res.json();
  const ip = j?.Answer?.find(a => a.type === 1)?.data;
  if (!ip) throw new Error('DoH 解析失败: ' + host);
  dohCache.set(host, ip);
  return ip;
}

function httpsRequest(urlStr, { method = 'GET', headers = {}, body = null } = {}, forcedIp = null) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const opts = { method, headers };
    if (forcedIp) {
      opts.servername = u.hostname;
      opts.lookup = (_h, _o, cb) => cb(null, forcedIp, 4);
    }
    const req = https.request(urlStr, opts, resolve);
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function resilientRequest(urlStr, opts = {}) {
  try {
    return await httpsRequest(urlStr, opts);
  } catch (e) {
    const host = new URL(urlStr).hostname;
    const ip = await dohResolve(host).catch(() => null);
    if (!ip) throw e;
    return httpsRequest(urlStr, opts, ip);
  }
}

async function readAll(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks).toString('utf8');
}

// 支持自定义端点(账号池里不同账号可能归属不同 region: 国际版 workbuddy.ai / 国内版 workbuddy.cn)
async function apiJsonAt(base, method, p, { body, headers = {} } = {}) {
  const res = await resilientRequest(`${base}${p}`, {
    method,
    headers: { 'Content-Type': 'application/json', ...headers },
    body: body ? JSON.stringify(body) : null,
  });
  const text = await readAll(res);
  let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
  return { status: res.statusCode, json };
}
// 默认走全局 ENDPOINT(国际版); 国内版账号通过各自 domain 走 apiJsonAt
async function apiJson(method, p, opts) { return apiJsonAt(ENDPOINT, method, p, opts); }

// 逐条解析 SSE data: 事件
async function* sseEvents(stream) {
  const dec = new TextDecoder();
  let buf = '';
  for await (const chunk of stream) {
    buf += dec.decode(chunk, { stream: true });
    let idx;
    while ((idx = buf.indexOf('\n\n')) >= 0) {
      const raw = buf.slice(0, idx); buf = buf.slice(idx + 2);
      for (const line of raw.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const payload = line.slice(5).trim();
        if (!payload || payload === '[DONE]') continue;
        let ev; try { ev = JSON.parse(payload); } catch { continue; }
        yield ev;
      }
    }
  }
}

// ---------------- auth ----------------

function loadAuth() {
  try { return JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8')); } catch { return null; }
}

function saveAuth(a) {
  fs.mkdirSync(AUTH_DIR, { recursive: true });
  fs.writeFileSync(AUTH_FILE, JSON.stringify(a, null, 2), { mode: 0o600 });
}

async function login() {
  const { json } = await apiJson('POST', `${PREFIX}/auth/state?platform=CLI`, { body: {} });
  const d = json?.data;
  if (!d?.authUrl || !d?.state) { console.error('获取 authUrl 失败:', JSON.stringify(json)); process.exit(1); }
  console.log('\n请复制下面链接, 在已登录目标账号的浏览器 / 无痕窗口中手动打开并完成授权');
  console.log('(默认不再自动打开浏览器, 避免用错账号; 如需自动打开请设 WB_OPEN=1)\n');
  console.log('  ' + d.authUrl + '\n');
  if (process.env.WB_OPEN === '1') exec(`open ${JSON.stringify(d.authUrl)}`);
  console.log('等待登录完成(最长 10 分钟)...');
  const deadline = Date.now() + 600_000;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 3000));
    const { json: t } = await apiJson('GET', `${PREFIX}/auth/token?state=${d.state}`);
    const data = t?.data?.data ?? t?.data;
    if (data?.accessToken) {
      const auth = { ...data, savedAt: Date.now() };
      if (!auth.expiresAt && auth.expiresIn) auth.expiresAt = Date.now() + auth.expiresIn * 1000;
      saveAuth(auth);
      console.log(`\n登录成功! token 已保存到 ${AUTH_FILE}`);
      return;
    }
    if (t?.code && t.code !== 11217) { console.error('登录失败:', JSON.stringify(t)); process.exit(1); }
    process.stdout.write('.');
  }
  console.error('\n登录超时'); process.exit(1);
}

// 把 token 持久化到指定文件(账号池里的每个账号各存各的)
function saveAuthToFile(a, file) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(a, null, 2), { mode: 0o600 });
}

// ---------------- 账号池 / 负载均衡 ----------------
// 账号池目录 ~/.wb-proxy/pool/*.json, 每个文件一个账号(来自 WB_AUTH_FILE=... login);
// 不存在池时回退到单一 auth.json(向后兼容)。运行时按各自 domain 路由(国际版/国内版通吃)。
const POOL_DIR = path.join(AUTH_DIR, 'pool');
const ACTIVE_FILE = path.join(AUTH_DIR, 'active'); // 钉选账号名; 内容写 auto 表示不钉选
const LB_STRATEGY = process.env.WB_LB || 'roundrobin'; // roundrobin | random
const DEFAULT_COOLDOWN_MS = Number(process.env.WB_COOLDOWN_MS) || 60_000; // 限流类冷却时长
let pool = [];
let rrCursor = 0;

// 重新扫描磁盘, 合并新增账号, 同时保留内存中的冷却/计时状态
function syncPool() {
  const found = new Map();
  if (fs.existsSync(POOL_DIR)) {
    for (const f of fs.readdirSync(POOL_DIR).sort()) {
      if (!f.endsWith('.json')) continue;
      const file = path.join(POOL_DIR, f);
      let auth; try { auth = JSON.parse(fs.readFileSync(file, 'utf8')); } catch { continue; }
      if (!auth?.accessToken) continue;
      found.set(f.slice(0, -5), { name: f.slice(0, -5), file, auth });
    }
  }
  if (!found.size && fs.existsSync(AUTH_FILE)) {
    let auth; try { auth = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8')); } catch { auth = null; }
    if (auth?.accessToken) found.set('default', { name: 'default', file: AUTH_FILE, auth });
  }
  const next = [];
  for (const [name, info] of found) {
    const prev = pool.find(e => e.name === name);
    next.push({ ...info, cooldownUntil: prev?.cooldownUntil || 0, lastUsed: prev?.lastUsed || 0 });
  }
  pool = next;
}

function readPinned() {
  try { const s = fs.readFileSync(ACTIVE_FILE, 'utf8').trim(); return s && s !== 'auto' ? s : null; } catch { return null; }
}

// 本轮可参与的账号顺序: 跳过冷却中的; 钉选则只保留钉选账号
function orderedHealthyEntries() {
  const pinned = process.env.WB_ACCOUNT || readPinned();
  const now = Date.now();
  const healthy = pool.filter(e => e.cooldownUntil <= now);
  if (!healthy.length) return [];
  if (pinned) {
    const e = healthy.find(e => e.name === pinned);
    return e ? [e] : [];
  }
  const n = pool.length;
  const start = rrCursor % n;
  const order = [];
  for (let i = 0; i < n; i++) {
    const e = pool[(start + i) % n];
    if (e.cooldownUntil <= now) order.push(e);
  }
  rrCursor = (start + 1) % n;
  return order;
}

// 解析上游限流返回里的重置时间(如 code 6004 的 "reset at 2026-10-17 13:00:31 UTC+8")
function parseResetTime(text) {
  try {
    const m = String(text).match(/reset at\s*([\d\-]+\s*[\d:]+)(?:\s*(UTC\+8|GMT\+8))?/i);
    if (m) {
      const ds = m[1].replace(/\//g, '-');
      const t = Date.parse(ds + (m[2] ? ' GMT+8' : ''));
      if (!Number.isNaN(t) && t > Date.now()) return t;
    }
  } catch { /* ignore */ }
  return null;
}

// 按账号各自的 domain 刷新 token(写回该账号自己的文件)
async function refreshEntry(entry) {
  if (!entry?.auth?.refreshToken) return entry.auth;
  const expiresAt = entry.auth.expiresAt ?? 0;
  if (Date.now() < expiresAt - 120_000) return entry.auth; // 还有 2 分钟以上
  const { status, json } = await apiJsonAt(entry.auth.domain ? 'https://' + entry.auth.domain : ENDPOINT, 'POST', `${PREFIX}/auth/token/refresh`, {
    body: {},
    headers: { 'X-Refresh-Token': entry.auth.refreshToken, Authorization: `Bearer ${entry.auth.accessToken}` },
  });
  const data = json?.data?.data ?? json?.data;
  if (status === 200 && data?.accessToken) {
    const next = { ...entry.auth, ...data, savedAt: Date.now() };
    if (next.expiresIn) next.expiresAt = Date.now() + next.expiresIn * 1000;
    entry.auth = next;
    saveAuthToFile(next, entry.file);
  }
  return entry.auth;
}

// 账号池负载均衡 + 故障转移:
//  - 轮询选择健康账号
//  - 401: 刷新该账号 token 后重试一次
//  - 429/配额耗尽: 把该账号冷却, 自动切下一个账号(直到池里账号用尽)
async function withPoolRetry(upstreamBody) {
  syncPool();
  if (!pool.length) return { error: [401, { error: { message: '账号池为空, 先运行: WB_AUTH_FILE=~/.wb-proxy/pool/<账号>.json node wb-proxy.mjs login' } }] };
  const order = orderedHealthyEntries();
  if (!order.length) {
    const soonest = Math.min(...pool.map(e => e.cooldownUntil));
    return { error: [429, { error: { message: `账号池所有账号均处于冷却(最早 ${new Date(soonest).toLocaleString()}), 请稍后重试` } }] };
  }
  const tried401 = new Set();
  for (const entry of order) {
    let auth = await refreshEntry(entry);
    let up;
    try { up = await callUpstream(auth, upstreamBody); }
    catch (e) { return { error: [502, { error: { message: 'upstream connect failed: ' + e.message } }] }; }
    // 401: 刷新后重试同账号一次
    if (up.status === 401 && !tried401.has(entry.name)) {
      up.stream.destroy();
      entry.auth.expiresAt = 0; saveAuthToFile(entry.auth, entry.file);
      tried401.add(entry.name);
      try { up = await callUpstream(await refreshEntry(entry), upstreamBody); }
      catch (e) { return { error: [502, { error: { message: 'upstream connect failed: ' + e.message } }] }; }
    }
    if (up.ok) { entry.lastUsed = Date.now(); return { up, entry }; }
    const text = await up.text().catch(() => '');
    const isRate = up.status === 429 || /usage exceeds frequency limit|quota|额度|频率|frequency limit/i.test(text);
    if (isRate) {
      const reset = parseResetTime(text);
      entry.cooldownUntil = reset || Date.now() + DEFAULT_COOLDOWN_MS;
      console.log(`[lb] 账号 ${entry.name} 限流, 冷却至 ${new Date(entry.cooldownUntil).toLocaleTimeString()}${reset ? ' (按上游重置时间)' : ''}, 切换到下一账号`);
      continue; // 试下一个账号
    }
    // 其它错误(模型不存在/参数错误等)直接返回, 不重试
    return { up, entry: null, detail: text };
  }
  return { error: [429, { error: { message: '账号池所有账号当前均限流/配额耗尽, 已自动冷却, 请稍后重试' } }] };
}

// ---------------- 公共 ----------------

const DEFAULT_SYSTEM_PROMPT = process.env.WB_SYSTEM_PROMPT || 'You are a helpful assistant.';

// 从 JWT 里解出用户 id (sub), 网关头要求 X-User-Id
function jwtSub(token) {
  try {
    const p = token.split('.')[1];
    return JSON.parse(Buffer.from(p, 'base64url').toString('utf8')).sub || null;
  } catch { return null; }
}

function sendJson(res, code, obj) {
  const s = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(s);
}

// 模拟官方 WorkBuddy CLI 客户端的完整请求头(从 CODEBUDDY_BASE_URL 抓包获得),
// 网关按渠道分配限流桶, 裸请求会被当成外部调用更容易 429
const CLIENT_VERSION = '2.156.0';
const CONVERSATION_ID = crypto.randomUUID(); // 每个代理进程一个会话 id
function officialClientHeaders() {
  const msgId = crypto.randomUUID().replace(/-/g, '');
  const convReqId = crypto.randomUUID().replace(/-/g, '');
  return {
    'User-Agent': `CLI/${CLIENT_VERSION} WorkBuddy AI/${CLIENT_VERSION}`,
    'X-Requested-With': 'XMLHttpRequest',
    'X-IDE-Type': 'CLI',
    'X-IDE-Name': 'CLI',
    'X-IDE-Version': CLIENT_VERSION,
    'X-Agent-Intent': 'craft',
    'X-Agent-Purpose': 'conversation',
    'X-Agent-Type': 'main',
    'X-CodeBuddy-Request': '1',
    'X-Private-Data': 'true',
    'X-Product': 'SaaS',
    'X-Domain': new URL(ENDPOINT).hostname,
    'X-Conversation-ID': CONVERSATION_ID,
    'X-Request-ID': msgId,
    'X-Conversation-Message-ID': msgId,
    'X-Conversation-Request-ID': convReqId,
    'X-Root-Request-ID': convReqId,
  };
}

async function callUpstream(auth, upstreamBody) {
  // 按账号各自的 domain 路由: 国际版 www.workbuddy.ai / 国内版 www.workbuddy.cn
  const ep = auth?.domain ? 'https://' + auth.domain : ENDPOINT;
  const chatUrl = `${ep}/v2/chat/completions`;
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${auth.accessToken}`,
    Accept: 'text/event-stream',
    ...officialClientHeaders(),
  };
  headers['X-Domain'] = new URL(ep).hostname; // 覆盖 officialClientHeaders 里写死的全局域名
  const uid = auth?.account?.uid ?? auth?.uid ?? jwtSub(auth.accessToken);
  if (uid) headers['X-User-Id'] = encodeURIComponent(String(uid));
  if (process.env.WB_DEBUG) {
    fs.writeFileSync(path.join(AUTH_DIR, 'last-request.json'), JSON.stringify(upstreamBody, null, 2));
  }
  const res = await resilientRequest(chatUrl, { method: 'POST', headers, body: JSON.stringify(upstreamBody) });
  return {
    ok: res.statusCode >= 200 && res.statusCode < 300,
    status: res.statusCode,
    stream: res,
    text: () => readAll(res),
  };
}

// 上游只支持流式; 聚合 SSE 成单个 chat.completion 对象
async function collectChatCompletion(stream) {
  let content = '', reasoning = '', id = '', model = DEFAULT_MODEL, created = Math.floor(Date.now() / 1000), usage = null, finish = 'stop';
  const toolCalls = {};
  for await (const ev of sseEvents(stream)) {
    id = ev.id || id; model = ev.model || model; created = ev.created || created;
    if (ev.usage) usage = ev.usage;
    const ch = ev.choices?.[0];
    if (!ch) continue;
    if (ch.finish_reason) finish = ch.finish_reason;
    const d = ch.delta || {};
    if (d.content) content += d.content;
    if (d.reasoning_content) reasoning += d.reasoning_content;
    for (const tc of d.tool_calls || []) {
      const t = toolCalls[tc.index ?? 0] ??= { id: '', type: 'function', function: { name: '', arguments: '' } };
      if (tc.id) t.id += tc.id;
      if (tc.function?.name) t.function.name += tc.function.name;
      if (tc.function?.arguments) t.function.arguments += tc.function.arguments;
    }
  }
  const msg = { role: 'assistant', content };
  if (reasoning) msg.reasoning_content = reasoning;
  const tcs = Object.values(toolCalls).filter(t => t.function.name || t.function.arguments);
  if (tcs.length) msg.tool_calls = tcs;
  return { id, object: 'chat.completion', created, model, choices: [{ index: 0, message: msg, finish_reason: finish }], usage };
}

// (已废弃) 单账号重试逻辑已被 withPoolRetry(账号池负载均衡 + 故障转移) 取代

// ---------------- Chat Completions ----------------

async function handleChat(req, res, body) {
  let inBody;
  try { inBody = JSON.parse(body); } catch { return sendJson(res, 400, { error: { message: 'invalid json body' } }); }

  const wantStream = inBody.stream === true;
  // 网关安全策略: 首条消息必须是 system prompt; 不认 developer 角色, 归一成 system
  const messages = (Array.isArray(inBody.messages) ? inBody.messages : [])
    .map(m => m?.role === 'developer' ? { ...m, role: 'system' } : m);
  if (!messages.length || messages[0]?.role !== 'system') {
    messages.unshift({ role: 'system', content: DEFAULT_SYSTEM_PROMPT });
  }
  const upstreamBody = {
    ...inBody,
    messages,
    model: inBody.model && inBody.model !== 'default' ? inBody.model : DEFAULT_MODEL,
    stream: true, // 上游只支持流式
  };
  // 付费模型禁用: 默认拦截, 避免意外扣费(设 WB_ALLOW_PAID=1 可放行)
  if (!ALLOW_PAID && PAID_MODELS.includes(upstreamBody.model)) {
    return sendJson(res, 403, { error: { message: `付费模型 ${upstreamBody.model} 已被本代理禁用, 以避免扣费。如确需使用请设置 WB_ALLOW_PAID=1 后重启代理。` } });
  }

  const r = await withPoolRetry(upstreamBody);
  if (r.error) return sendJson(res, r.error[0], r.error[1]);
  const { up } = r;
  if (!up.ok) {
    const text = r.detail || await up.text().catch(() => '');
    return sendJson(res, up.status, { error: { message: `upstream ${up.status}`, detail: text.slice(0, 2000) } });
  }

  if (wantStream) {
    res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    up.stream.pipe(res);
    up.stream.on('error', () => res.destroy());
    req.on('close', () => up.stream.destroy());
  } else {
    sendJson(res, 200, await collectChatCompletion(up.stream));
  }
}

// ---------------- Responses API (Codex 等新版客户端用) ----------------

// Responses 请求 → Chat Completions 请求 (只透传上游认识的字段)
function responsesToChat(inBody) {
  const messages = [];
  if (inBody.instructions) messages.push({ role: 'system', content: String(inBody.instructions) });
  const input = inBody.input;
  if (typeof input === 'string') {
    messages.push({ role: 'user', content: input });
  } else if (Array.isArray(input)) {
    let pendingCalls = null; // 连续的 function_call 合并成一条 assistant 消息 (OpenAI 并行工具调用格式)
    const flushCalls = () => {
      if (pendingCalls?.length) messages.push({ role: 'assistant', content: '', tool_calls: pendingCalls });
      pendingCalls = null;
    };
    for (const item of input) {
      if (!item) continue;
      const t = item.type;
      if (t === 'message' || (!t && item.role)) {
        flushCalls();
        // WorkBuddy 网关不认 developer 角色, 归一成 system
        const role = (item.role === 'developer') ? 'system' : (item.role || 'user');
        let parts = item.content;
        if (typeof parts === 'string') { messages.push({ role, content: parts }); continue; }
        if (!Array.isArray(parts)) continue;
        const mapped = [];
        for (const p of parts) {
          if (!p) continue;
          if (p.type === 'input_text' || p.type === 'output_text' || p.type === 'text') mapped.push({ type: 'text', text: p.text ?? '' });
          else if (p.type === 'input_image' && p.image_url) mapped.push({ type: 'image_url', image_url: { url: p.image_url } });
          else if (p.type === 'image_url') mapped.push(p);
        }
        if (!mapped.length) continue;
        messages.push({ role, content: mapped.length === 1 && mapped[0].type === 'text' ? mapped[0].text : mapped });
      } else if (t === 'function_call') {
        (pendingCalls ??= []).push({ id: item.call_id || item.id, type: 'function', function: { name: item.name, arguments: item.arguments ?? '' } });
        continue;
      } else if (t === 'function_call_output') {
        flushCalls();
        messages.push({ role: 'tool', tool_call_id: item.call_id, content: typeof item.output === 'string' ? item.output : JSON.stringify(item.output) });
      } else {
        flushCalls();
      }
      // reasoning / item_reference 等直接忽略
    }
    flushCalls();
  }
  const chat = { model: inBody.model || DEFAULT_MODEL, messages, stream: true };
  if (typeof inBody.temperature === 'number') chat.temperature = inBody.temperature;
  if (typeof inBody.top_p === 'number') chat.top_p = inBody.top_p;
  if (typeof inBody.max_output_tokens === 'number') chat.max_tokens = inBody.max_output_tokens;
  if (typeof inBody.parallel_tool_calls === 'boolean') chat.parallel_tool_calls = inBody.parallel_tool_calls;
  // Responses API 用 reasoning:{effort} 表达推理强度, 上游只认 reasoning_effort, 需要翻译
  // (原先漏了这一步, 导致客户端设的推理强度被静默丢弃)
  const reasoningEffort = inBody.reasoning?.effort ?? inBody.reasoning_effort;
  if (reasoningEffort) chat.reasoning_effort = reasoningEffort;
  if (inBody.stop) chat.stop = inBody.stop;
  if (Array.isArray(inBody.tools)) {
    const tools = inBody.tools.filter(t => t?.type === 'function')
      .map(t => ({ type: 'function', function: { name: t.name, description: t.description, parameters: t.parameters } }));
    if (tools.length) chat.tools = tools;
  }
  if (typeof inBody.tool_choice === 'string') chat.tool_choice = inBody.tool_choice;
  else if (inBody.tool_choice?.type === 'function' && inBody.tool_choice.name) chat.tool_choice = { type: 'function', function: { name: inBody.tool_choice.name } };
  const fmt = inBody.text?.format;
  if (fmt?.type === 'json_object') chat.response_format = { type: 'json_object' };
  else if (fmt?.type === 'json_schema') chat.response_format = { type: 'json_schema', json_schema: { name: fmt.name, schema: fmt.schema, strict: fmt.strict } };
  return chat;
}

function mapUsage(u) {
  if (!u) return null;
  return {
    input_tokens: u.prompt_tokens ?? 0,
    input_tokens_details: { cached_tokens: u.prompt_tokens_details?.cached_tokens ?? 0 },
    output_tokens: u.completion_tokens ?? 0,
    output_tokens_details: { reasoning_tokens: u.completion_tokens_details?.reasoning_tokens ?? 0 },
    total_tokens: u.total_tokens ?? 0,
  };
}

// chat.completion 对象 → Responses 响应对象
function chatToResponseObject(cc, status = 'completed') {
  const output = [];
  const msg = cc.choices?.[0]?.message || {};
  if (msg.reasoning_content) {
    output.push({ id: 'rs_' + cc.id, type: 'reasoning', summary: [{ type: 'summary_text', text: msg.reasoning_content }] });
  }
  if (msg.content) {
    output.push({ id: 'msg_' + cc.id, type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: msg.content, annotations: [] }] });
  }
  for (const tc of msg.tool_calls || []) {
    output.push({ id: 'fc_' + (tc.id || cc.id), type: 'function_call', status: 'completed', call_id: tc.id, name: tc.function?.name || '', arguments: tc.function?.arguments || '' });
  }
  return {
    id: 'resp_' + cc.id, object: 'response', created_at: cc.created, status,
    model: cc.model, output, parallel_tool_calls: true,
    usage: mapUsage(cc.usage),
  };
}

// 把上游 chat SSE 实时翻译成 Responses SSE 事件流
async function streamChatAsResponses(upStream, res) {
  res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  let seq = 0;
  const respId = 'resp_' + Math.random().toString(36).slice(2);
  const created = Math.floor(Date.now() / 1000);
  let model = DEFAULT_MODEL, usage = null;
  const items = [];           // 已完成的 output items
  let cur = null;             // 当前打开的 item: {kind:'reasoning'|'message'|'function_call', ...}
  const send = (type, data) => res.write(`event: ${type}\ndata: ${JSON.stringify({ type, sequence_number: seq++, ...data })}\n\n`);
  const respObj = (status) => ({ id: respId, object: 'response', created_at: created, status, model, output: items, parallel_tool_calls: true, usage: mapUsage(usage) });
  const closeCur = () => {
    if (!cur) return;
    if (cur.kind === 'reasoning') {
      send('response.reasoning_summary_text.done', { item_id: cur.id, output_index: cur.index, summary_index: 0, text: cur.text });
      send('response.reasoning_summary_part.done', { item_id: cur.id, output_index: cur.index, summary_index: 0, part: { type: 'summary_text', text: cur.text } });
      send('response.output_item.done', { output_index: cur.index, item: { id: cur.id, type: 'reasoning', summary: [{ type: 'summary_text', text: cur.text }] } });
      items.push({ id: cur.id, type: 'reasoning', summary: [{ type: 'summary_text', text: cur.text }] });
    } else if (cur.kind === 'message') {
      send('response.output_text.done', { item_id: cur.id, output_index: cur.index, content_index: 0, text: cur.text });
      send('response.content_part.done', { item_id: cur.id, output_index: cur.index, content_index: 0, part: { type: 'output_text', text: cur.text, annotations: [] } });
      const item = { id: cur.id, type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: cur.text, annotations: [] }] };
      send('response.output_item.done', { output_index: cur.index, item });
      items.push(item);
    } else if (cur.kind === 'function_call') {
      send('response.function_call_arguments.done', { item_id: cur.id, output_index: cur.index, arguments: cur.args });
      const item = { id: cur.id, type: 'function_call', status: 'completed', call_id: cur.callId, name: cur.name, arguments: cur.args };
      send('response.output_item.done', { output_index: cur.index, item });
      items.push(item);
    }
    cur = null;
  };
  const openReasoning = () => {
    closeCur();
    cur = { kind: 'reasoning', id: 'rs_' + respId + '_' + items.length, index: items.length, text: '' };
    send('response.output_item.added', { output_index: cur.index, item: { id: cur.id, type: 'reasoning', summary: [] } });
    send('response.reasoning_summary_part.added', { item_id: cur.id, output_index: cur.index, summary_index: 0, part: { type: 'summary_text', text: '' } });
  };
  const openMessage = () => {
    closeCur();
    cur = { kind: 'message', id: 'msg_' + respId + '_' + items.length, index: items.length, text: '' };
    send('response.output_item.added', { output_index: cur.index, item: { id: cur.id, type: 'message', status: 'in_progress', role: 'assistant', content: [] } });
    send('response.content_part.added', { item_id: cur.id, output_index: cur.index, content_index: 0, part: { type: 'output_text', text: '', annotations: [] } });
  };
  const openFunctionCall = (tc) => {
    closeCur();
    cur = { kind: 'function_call', id: 'fc_' + (tc.id || respId + '_' + items.length), index: items.length, callId: tc.id || '', name: '', args: '' };
    send('response.output_item.added', { output_index: cur.index, item: { id: cur.id, type: 'function_call', status: 'in_progress', call_id: cur.callId, name: '', arguments: '' } });
  };

  send('response.created', { response: respObj('in_progress') });
  send('response.in_progress', { response: respObj('in_progress') });

  for await (const ev of sseEvents(upStream)) {
    model = ev.model || model;
    if (ev.usage) usage = ev.usage;
    const d = ev.choices?.[0]?.delta || {};
    if (d.reasoning_content) {
      if (cur?.kind !== 'reasoning') openReasoning();
      cur.text += d.reasoning_content;
      send('response.reasoning_summary_text.delta', { item_id: cur.id, output_index: cur.index, summary_index: 0, delta: d.reasoning_content });
    }
    if (d.content) {
      if (cur?.kind !== 'message') openMessage();
      cur.text += d.content;
      send('response.output_text.delta', { item_id: cur.id, output_index: cur.index, content_index: 0, delta: d.content });
    }
    for (const tc of d.tool_calls || []) {
      const isNew = cur?.kind !== 'function_call' || (tc.id && cur.callId && tc.id !== cur.callId);
      if (isNew) openFunctionCall(tc);
      if (tc.id) cur.callId = tc.id;
      if (tc.function?.name) cur.name += tc.function.name;
      const a = tc.function?.arguments || '';
      if (a) {
        cur.args += a;
        send('response.function_call_arguments.delta', { item_id: cur.id, output_index: cur.index, delta: a });
      }
    }
  }
  closeCur();
  send('response.completed', { response: respObj('completed') });
  res.end();
}

async function handleResponses(req, res, body) {
  let inBody;
  try { inBody = JSON.parse(body); } catch { return sendJson(res, 400, { error: { message: 'invalid json body' } }); }
  const wantStream = inBody.stream === true;
  const chatBody = responsesToChat(inBody);
  // 付费模型禁用: 默认拦截, 避免意外扣费(设 WB_ALLOW_PAID=1 可放行)
  if (!ALLOW_PAID && PAID_MODELS.includes(chatBody.model)) {
    return sendJson(res, 403, { error: { message: `付费模型 ${chatBody.model} 已被本代理禁用, 以避免扣费。如确需使用请设置 WB_ALLOW_PAID=1 后重启代理。` } });
  }
  if (!chatBody.messages.length || chatBody.messages[0]?.role !== 'system') {
    chatBody.messages.unshift({ role: 'system', content: DEFAULT_SYSTEM_PROMPT });
  }

  const r = await withPoolRetry(chatBody);
  if (r.error) return sendJson(res, r.error[0], r.error[1]);
  const { up } = r;
  if (!up.ok) {
    const text = r.detail || await up.text().catch(() => '');
    return sendJson(res, up.status, { error: { message: `upstream ${up.status}`, detail: text.slice(0, 2000) } });
  }

  if (wantStream) {
    req.on('close', () => up.stream.destroy());
    await streamChatAsResponses(up.stream, res);
  } else {
    sendJson(res, 200, chatToResponseObject(await collectChatCompletion(up.stream)));
  }
}

// ---------------- serve ----------------

function serve(port, localKey, host = '127.0.0.1') {
  syncPool();
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    if (localKey) {
      const k = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
      if (k !== localKey) return sendJson(res, 401, { error: { message: 'invalid local api key' } });
    }
    if (req.method === 'GET' && (url.pathname === '/v1/models' || url.pathname === '/models')) {
      return sendJson(res, 200, { object: 'list', data: [
        ...FREE_MODELS.map(id => ({ id, object: 'model', owned_by: 'workbuddy', free: true })),
        ...(ALLOW_PAID ? PAID_MODELS : []).map(id => ({ id, object: 'model', owned_by: 'workbuddy', free: false })),
      ] });
    }
    if (req.method === 'POST' && (url.pathname === '/v1/chat/completions' || url.pathname === '/chat/completions')) {
      return handleChat(req, res, await readAll(req));
    }
    if (req.method === 'POST' && (url.pathname === '/v1/responses' || url.pathname === '/responses')) {
      return handleResponses(req, res, await readAll(req));
    }
    if (req.method === 'GET' && url.pathname === '/health') {
      syncPool();
      const now = Date.now();
      return sendJson(res, 200, {
        ok: true,
        poolSize: pool.length,
        healthy: pool.filter(e => e.cooldownUntil <= now).length,
        accounts: pool.map(e => ({ name: e.name, domain: e.auth.domain || 'www.workbuddy.ai', healthy: e.cooldownUntil <= now, cooldownUntil: e.cooldownUntil })),
      });
    }
    if (req.method === 'GET' && (url.pathname === '/v1/accounts' || url.pathname === '/accounts')) {
      syncPool();
      const now = Date.now();
      return sendJson(res, 200, {
        object: 'account_pool',
        strategy: (process.env.WB_ACCOUNT || readPinned()) ? 'pinned' : LB_STRATEGY,
        pinned: process.env.WB_ACCOUNT || readPinned() || null,
        data: pool.map(e => ({
          name: e.name,
          domain: e.auth.domain || 'www.workbuddy.ai',
          healthy: e.cooldownUntil <= now,
          cooldownUntil: e.cooldownUntil,
          expiresAt: e.auth.expiresAt || null,
          sub: (() => { try { return JSON.parse(Buffer.from((e.auth.accessToken || '').split('.')[1] || '', 'base64url').toString('utf8')).sub; } catch { return null; } })(),
        })),
      });
    }
    sendJson(res, 404, { error: { message: 'not found' } });
  });
  server.listen(port, host, () => {
    console.log(`wb-proxy 已启动: http://${host}:${port}/v1  (model: ${DEFAULT_MODEL})`);
    console.log('端点: /v1/chat/completions (OpenAI)  +  /v1/responses (Codex)');
  });
}

// ---------------- main ----------------

const [, , cmd, ...rest] = process.argv;
if (cmd === 'login') {
  login();
} else if (cmd === 'serve') {
  const port = Number(rest[rest.indexOf('--port') + 1]) || 8377;
  const ki = rest.indexOf('--key');
  const hi = rest.indexOf('--host');
  const host = hi >= 0 ? rest[hi + 1] : '127.0.0.1';
  if (host !== '127.0.0.1' && host !== '::1' && ki < 0) {
    console.error('监听非本机地址时必须加 --key 保护接口, 否则任何人都能用你的额度');
    process.exit(1);
  }
  serve(port, ki >= 0 ? rest[ki + 1] : null, host);
} else {
  console.log('用法: node wb-proxy.mjs login | serve [--port 8377] [--host 0.0.0.0] [--key xxx]');
}
