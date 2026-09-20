const CONFIG = {
  PROJECT_NAME: 'stockai-2api',
  PROJECT_VERSION: '2.0.0',
  API_MASTER_KEY: '1',
  UPSTREAM_ORIGIN: 'https://free.stockai.trade',
  UPSTREAM_API_URL: 'https://free.stockai.trade/api/chat',
  MODELS: [
    'openrouter/free',
    'inception/mercury-2.5',
    'dots3-note-prev',
    'Qwen3.8-27B',
    'google/translategemma',
    'openrouter/free-search',
    'zmimo-v2.5-tts',
  ],
  DEFAULT_MODEL: 'openrouter/free',
  UPSTREAM_HEADERS: {
    authority: 'free.stockai.trade',
    accept: '*/*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'content-type': 'application/json',
    origin: 'https://free.stockai.trade',
    referer: 'https://free.stockai.trade/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    priority: 'u=1, i',
  },
};

export default {
  async fetch(request, env) {
    const apiKey = env.API_MASTER_KEY || CONFIG.API_MASTER_KEY;
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders() });
    if (url.pathname === '/') return handleUI(request, apiKey);
    if (url.pathname === '/v1/models') return handleModels();
    if (url.pathname === '/v1/chat/completions') return handleChatCompletions(request, apiKey);

    return createErrorResponse(`路径未找到: ${url.pathname}`, 404, 'not_found');
  },
};

async function handleChatCompletions(request, apiKey) {
  if (!verifyAuth(request, apiKey)) {
    return createErrorResponse('Unauthorized', 401, 'auth_error');
  }

  const requestId = `chatcmpl-${crypto.randomUUID()}`;

  try {
    const body = await request.json();
    const model = body.model || CONFIG.DEFAULT_MODEL;
    const stream = shouldStreamResponse(body, request);
    const upstreamResponse = await fetchUpstreamChat(body, model);

    if (!upstreamResponse.ok) {
      const errorText = await upstreamResponse.text();
      return createErrorResponse(`上游服务错误 (${upstreamResponse.status}): ${errorText}`, upstreamResponse.status, 'upstream_error');
    }

    if (!upstreamResponse.body) {
      return createErrorResponse('上游返回为空', 502, 'empty_upstream_body');
    }

    if (stream) {
      return streamAsOpenAI(upstreamResponse.body, { model, requestId });
    }

    return readAsOpenAIJson(upstreamResponse.body, { model, requestId });
  } catch (error) {
    return createErrorResponse(error.message || 'Unknown error', error.status || 500, error.code || 'internal_error');
  }
}

function handleModels() {
  return jsonResponse({
    object: 'list',
    data: CONFIG.MODELS.map((id) => ({
      id,
      object: 'model',
      created: Math.floor(Date.now() / 1000),
      owned_by: 'stockai-2api',
    })),
  });
}

async function fetchUpstreamChat(body, model) {
  const upstreamPayload = {
    model,
    webSearch: false,
    id: body.id || generateRandomId(16),
    messages: normalizeMessages(body.messages),
    trigger: 'submit-message',
  };

  return fetch(CONFIG.UPSTREAM_API_URL, {
    method: 'POST',
    headers: CONFIG.UPSTREAM_HEADERS,
    body: JSON.stringify(upstreamPayload),
  });
}

function normalizeMessages(messages) {
  if (!Array.isArray(messages)) return [];

  return messages.map((message) => ({
    id: generateRandomId(16),
    role: message?.role || 'user',
    parts: [{ type: 'text', text: normalizeMessageContent(message?.content) }],
  }));
}

function normalizeMessageContent(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';

  return content
    .map((part) => {
      if (typeof part === 'string') return part;
      if (!part || typeof part !== 'object') return '';
      if (part.type === 'text' && typeof part.text === 'string') return part.text;
      if (typeof part.content === 'string') return part.content;
      return '';
    })
    .filter(Boolean)
    .join('\n');
}

function shouldStreamResponse(body, request) {
  if (typeof body?.stream === 'boolean') return body.stream;
  const accept = (request.headers.get('accept') || '').toLowerCase();
  return accept.includes('text/event-stream');
}

function verifyAuth(request, apiKey) {
  if (apiKey === '1') return true;
  return request.headers.get('Authorization') === ('Bearer ' + apiKey);
}

function corsHeaders(headers = {}) {
  return {
    ...headers,
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  };
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: corsHeaders({ 'Content-Type': 'application/json; charset=utf-8' }),
  });
}

function createErrorResponse(message, status = 500, code = 'api_error') {
  return jsonResponse({ error: { message, type: 'api_error', code } }, status);
}

function generateRandomId(length) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i += 1) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function streamAsOpenAI(upstreamBody, { model, requestId }) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  (async () => {
    try {
      await writeSSE(writer, encoder, createChunk(requestId, model, { role: 'assistant' }));

      for await (const event of iterateUpstreamEvents(upstreamBody)) {
        if (event.done) break;
        const delta = extractEventDelta(event.data);
        if (delta.reasoning) {
          await writeSSE(writer, encoder, createChunk(requestId, model, { reasoning_content: delta.reasoning }));
        }
        if (delta.content) {
          await writeSSE(writer, encoder, createChunk(requestId, model, { content: delta.content }));
        }
      }

      await writeSSE(writer, encoder, {
        id: requestId,
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model,
        choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
      });
      await writer.write(encoder.encode('data: [DONE]\n\n'));
    } catch (error) {
      await writeSSE(writer, encoder, createChunk(requestId, model, { content: `\n\n[Error: ${error.message}]` }, 'error'));
    } finally {
      await writer.close();
    }
  })();

  return new Response(readable, {
    headers: corsHeaders({
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
    }),
  });
}

async function readAsOpenAIJson(upstreamBody, { model, requestId }) {
  let content = '';
  let reasoning = '';

  for await (const event of iterateUpstreamEvents(upstreamBody)) {
    if (event.done) break;
    const delta = extractEventDelta(event.data);
    if (delta.reasoning) reasoning += delta.reasoning;
    if (delta.content) content += delta.content;
  }

  return jsonResponse({
    id: requestId,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      message: {
        role: 'assistant',
        content,
        ...(reasoning ? { reasoning_content: reasoning } : {}),
      },
      finish_reason: 'stop',
    }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  });
}

function createChunk(requestId, model, delta, finishReason = null) {
  return {
    id: requestId,
    object: 'chat.completion.chunk',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  };
}

async function writeSSE(writer, encoder, payload) {
  await writer.write(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`));
}

async function* iterateUpstreamEvents(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const parsed = parseSSEBuffer(buffer);
    buffer = parsed.remainingBuffer;

    for (const payload of parsed.payloads) {
      if (payload === '[DONE]') {
        yield { done: true, data: null };
        return;
      }

      const data = tryParseJSON(payload);
      if (data) yield { done: false, data };
    }
  }

  buffer += decoder.decode();
  const flushed = parseSSEBuffer(buffer, true);
  for (const payload of flushed.payloads) {
    if (payload === '[DONE]') {
      yield { done: true, data: null };
      return;
    }

    const data = tryParseJSON(payload);
    if (data) yield { done: false, data };
  }
}

function parseSSEBuffer(buffer, flush = false) {
  let normalized = buffer.replace(/\r/g, '');
  const payloads = [];

  while (true) {
    const boundary = normalized.indexOf('\n\n');
    if (boundary === -1) break;

    const eventBlock = normalized.slice(0, boundary);
    normalized = normalized.slice(boundary + 2);
    const payload = extractSSEPayload(eventBlock);
    if (payload) payloads.push(payload);
  }

  while (true) {
    const newlineIndex = normalized.indexOf('\n');
    if (newlineIndex === -1) break;

    const line = normalized.slice(0, newlineIndex);
    if (!line.startsWith('data:')) break;

    const payload = line.slice(5).trimStart().trimEnd();
    if (!payload) {
      normalized = normalized.slice(newlineIndex + 1);
      continue;
    }

    if (payload !== '[DONE]' && !isCompleteJSON(payload)) break;
    payloads.push(payload);
    normalized = normalized.slice(newlineIndex + 1);
    if (normalized.startsWith('\n')) normalized = normalized.slice(1);
  }

  if (flush && normalized.trim()) {
    const payload = extractSSEPayload(normalized) || normalized.trim().replace(/^data:\s*/, '');
    if (payload && (payload === '[DONE]' || isCompleteJSON(payload))) {
      payloads.push(payload);
      normalized = '';
    }
  }

  return { payloads, remainingBuffer: normalized };
}

function extractSSEPayload(eventBlock) {
  if (!eventBlock) return '';
  const lines = eventBlock.split('\n');
  const dataLines = [];

  for (const line of lines) {
    if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
  }

  if (!dataLines.length) return '';
  return dataLines.join('\n').trim();
}

function isCompleteJSON(payload) {
  try {
    JSON.parse(payload);
    return true;
  } catch {
    return false;
  }
}

function tryParseJSON(payload) {
  try {
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

function extractEventDelta(data) {
  if (!data || typeof data !== 'object') return { content: '', reasoning: '' };

  if (data.type === 'reasoning-delta') {
    return { content: '', reasoning: readTextValue(data.delta) };
  }

  if (data.type === 'text-delta') {
    return { content: readTextValue(data.delta), reasoning: '' };
  }

  if (typeof data.type === 'string' && (
    data.type === 'start' ||
    data.type === 'start-step' ||
    data.type === 'reasoning-start' ||
    data.type === 'reasoning-end' ||
    data.type === 'text-start' ||
    data.type === 'text-end' ||
    data.type === 'finish-step' ||
    data.type === 'finish'
  )) {
    return { content: '', reasoning: '' };
  }

  const direct = readTextValue(data.delta)
    || readTextValue(data.text)
    || readTextValue(data.content)
    || readTextValue(data.message?.content)
    || readTextList(data.delta)
    || readTextList(data.content)
    || readTextList(data.message?.content);

  if (direct) return { content: direct, reasoning: '' };

  if (Array.isArray(data.choices)) {
    for (const choice of data.choices) {
      const choiceText = readTextValue(choice?.delta?.content)
        || readTextList(choice?.delta?.content)
        || readTextValue(choice?.message?.content)
        || readTextList(choice?.message?.content);
      if (choiceText) return { content: choiceText, reasoning: '' };
    }
  }

  return { content: '', reasoning: '' };
}

function readTextValue(value) {
  if (typeof value === 'string') return value;
  if (!value || typeof value !== 'object') return '';
  return typeof value.text === 'string' ? value.text : typeof value.content === 'string' ? value.content : '';
}

function readTextList(value) {
  if (!Array.isArray(value)) return '';
  return value.map(readTextValue).filter(Boolean).join('');
}

function handleUI(request, apiKey) {
  const origin = new URL(request.url).origin;
  const html = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${CONFIG.PROJECT_NAME}</title>
  <style>
    body{margin:0;background:#0b1020;color:#e5e7eb;font:14px/1.5 system-ui,sans-serif;display:grid;grid-template-columns:360px 1fr;height:100vh}
    aside{padding:20px;border-right:1px solid #1f2937;background:#111827;overflow:auto}
    main{display:flex;flex-direction:column;padding:20px;gap:12px}
    .card{background:#111827;border:1px solid #1f2937;border-radius:12px;padding:14px;margin-bottom:12px}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-all;color:#93c5fd}
    textarea,select{width:100%;box-sizing:border-box;background:#0f172a;color:#e5e7eb;border:1px solid #334155;border-radius:10px;padding:10px}
    button{width:100%;padding:10px;border:0;border-radius:10px;background:#2563eb;color:#fff;font-weight:600;cursor:pointer}
    button:disabled{opacity:.6;cursor:not-allowed}
    #chat{flex:1;overflow:auto;border:1px solid #1f2937;border-radius:12px;padding:16px;background:#020617}
    .msg{padding:12px 14px;border-radius:12px;margin-bottom:12px;white-space:pre-wrap}
    .user{background:#1d4ed8}
    .assistant{background:#111827;border:1px solid #1f2937}
    #logs{height:140px;overflow:auto;border:1px solid #1f2937;border-radius:12px;padding:12px;background:#020617;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
  </style>
</head>
<body>
  <aside>
    <div class="card">
      <div>API Key</div>
      <div class="mono">${apiKey}</div>
    </div>
    <div class="card">
      <div>Endpoint</div>
      <div class="mono">${origin}/v1/chat/completions</div>
    </div>
    <div class="card">
      <div style="margin-bottom:8px">模型</div>
      <select id="model">${CONFIG.MODELS.map((model) => `<option value="${model}">${model}</option>`).join('')}</select>
      <div style="margin:12px 0 8px">问题</div>
      <textarea id="prompt" rows="6">你好</textarea>
      <label style="display:flex;gap:8px;align-items:center;margin:12px 0">
        <input id="stream" type="checkbox" checked>
        <span>流式输出</span>
      </label>
      <button id="send">发送</button>
    </div>
  </aside>
  <main>
    <div id="chat"></div>
    <div id="logs"></div>
  </main>
  <script>
    const API_KEY = ${JSON.stringify(apiKey)};
    const ENDPOINT = ${JSON.stringify(`${origin}/v1/chat/completions`)};

    const chatEl = document.getElementById('chat');
    const logsEl = document.getElementById('logs');
    const sendBtn = document.getElementById('send');

    function log(message) {
      const line = document.createElement('div');
      line.textContent = '[' + new Date().toLocaleTimeString() + '] ' + message;
      logsEl.appendChild(line);
      logsEl.scrollTop = logsEl.scrollHeight;
    }

    function addMessage(role, text) {
      const div = document.createElement('div');
      div.className = 'msg ' + role;
      div.textContent = text;
      chatEl.appendChild(div);
      chatEl.scrollTop = chatEl.scrollHeight;
      return div;
    }

    function parseSSE(buffer, flush = false) {
      let normalized = buffer.replace(/\r/g, '');
      const payloads = [];

      while (true) {
        const boundary = normalized.indexOf('\n\n');
        if (boundary === -1) break;
        const block = normalized.slice(0, boundary);
        normalized = normalized.slice(boundary + 2);
        const payload = extractPayload(block);
        if (payload) payloads.push(payload);
      }

      while (true) {
        const newline = normalized.indexOf('\n');
        if (newline === -1) break;
        const line = normalized.slice(0, newline);
        if (!line.startsWith('data:')) break;
        const payload = line.slice(5).trim();
        normalized = normalized.slice(newline + 1);
        if (normalized.startsWith('\n')) normalized = normalized.slice(1);
        if (payload) payloads.push(payload);
      }

      if (flush && normalized.trim()) {
        const payload = extractPayload(normalized) || normalized.trim().replace(/^data:\s*/, '');
        if (payload) {
          payloads.push(payload);
          normalized = '';
        }
      }

      return { payloads, remainingBuffer: normalized };
    }

    function extractPayload(block) {
      const dataLines = [];
      for (const line of block.split('\n')) {
        if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
      }
      return dataLines.length ? dataLines.join('\n').trim() : '';
    }

    sendBtn.addEventListener('click', async () => {
      const prompt = document.getElementById('prompt').value.trim();
      const model = document.getElementById('model').value;
      const stream = document.getElementById('stream').checked;
      if (!prompt) return;

      sendBtn.disabled = true;
      chatEl.innerHTML = '';
      addMessage('user', prompt);
      const assistant = addMessage('assistant', stream ? '...' : '请求中...');
      log('开始请求');

      try {
        const response = await fetch(ENDPOINT, {
          method: 'POST',
          headers: {
            Authorization: 'Bearer ' + API_KEY,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model,
            stream,
            messages: [{ role: 'user', content: prompt }],
          }),
        });

        if (!response.ok) {
          const error = await response.json();
          throw new Error(error.error?.message || '请求失败');
        }

        if (!stream) {
          const data = await response.json();
          assistant.textContent = data.choices?.[0]?.message?.content || '';
          log('非流式完成');
          return;
        }

        assistant.textContent = '';
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let content = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const parsed = parseSSE(buffer);
          buffer = parsed.remainingBuffer;

          for (const payload of parsed.payloads) {
            if (payload === '[DONE]') continue;
            const json = JSON.parse(payload);
            const delta = json.choices?.[0]?.delta || {};
            if (typeof delta.content === 'string' && delta.content) {
              content += delta.content;
              assistant.textContent = content;
            }
          }
        }

        buffer += decoder.decode();
        const flushed = parseSSE(buffer, true);
        for (const payload of flushed.payloads) {
          if (payload === '[DONE]') continue;
          const json = JSON.parse(payload);
          const delta = json.choices?.[0]?.delta || {};
          if (typeof delta.content === 'string' && delta.content) {
            content += delta.content;
            assistant.textContent = content;
          }
        }

        log('流式完成');
      } catch (error) {
        assistant.textContent = 'Error: ' + error.message;
        log('错误: ' + error.message);
      } finally {
        sendBtn.disabled = false;
      }
    });
  </script>
</body>
</html>`;

  return new Response(html, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  });
}
