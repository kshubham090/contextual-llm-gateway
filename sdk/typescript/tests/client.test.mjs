import assert from "node:assert/strict";
import test from "node:test";
import {
  GatewayClient, GatewayError, GatewayProtocolError, GatewayStreamError, GatewayTransportError,
} from "../dist/index.js";

const request = { prompt: "Previous connector issue?", user_id: "user-7", feature_tag: "support" };
const final = { response: "Refresh the cursor. ☀", meta: { call_id: "call-1", durable: true } };
const makeClient = (fetch) => new GatewayClient({ apiKey: "server-test-key", baseUrl: "https://gateway.test/prefix/", fetch });
const collect = async (events) => { const out = []; for await (const e of events) out.push(e); return out; };

function streaming(payload, chunkSize = 1) {
  const bytes = new TextEncoder().encode(payload);
  let offset = 0;
  const state = { cancelled: false };
  const body = new ReadableStream({
    pull(controller) {
      if (offset === bytes.length) { controller.close(); return; }
      controller.enqueue(bytes.slice(offset, offset + chunkSize));
      offset = Math.min(offset + chunkSize, bytes.length);
    },
    cancel() { state.cancelled = true; },
  });
  return { state, response: new Response(body, {
    headers: { "content-type": "text/event-stream; charset=utf-8", "x-request-id": "req-1" },
  }) };
}

test("chat uses scoped native body and a server bearer key", async () => {
  const client = makeClient(async (url, init) => {
    assert.equal(url.pathname, "/prefix/v1/chat");
    assert.equal(init.method, "POST");
    assert.equal(init.redirect, "error");
    assert.equal(init.headers.Authorization, "Bearer server-test-key");
    assert.deepEqual(JSON.parse(init.body), request);
    return Response.json(final);
  });
  assert.deepEqual(await client.chat(request), final);
});

for (const status of [401, 409, 422, 429, 503]) {
  test(`HTTP ${status} preserves metadata and never retries`, async () => {
    let calls = 0;
    const detail = status === 422 ? [{ msg: "Required field" }] : "Gateway rejected request";
    const client = makeClient(async () => {
      calls++;
      return Response.json({ detail }, { status, headers: { "x-request-id": "req-error", "retry-after": "3" } });
    });
    await assert.rejects(client.chat(request), (error) => {
      assert.ok(error instanceof GatewayError);
      assert.equal(error.statusCode, status);
      assert.equal(error.requestId, "req-error");
      assert.equal(error.retryAfter, "3");
      assert.deepEqual(error.details, { detail });
      return true;
    });
    assert.equal(calls, 1);
  });
}

test("network failure is not retried", async () => {
  let calls = 0;
  const client = makeClient(async () => { calls++; throw new TypeError("connection failed"); });
  await assert.rejects(client.chat(request), GatewayTransportError);
  assert.equal(calls, 1);
});

test("invalid JSON and wrong chat shapes fail explicitly", async () => {
  for (const body of ["bad json", "[]", '{"response":3,"meta":{}}']) {
    await assert.rejects(makeClient(async () => new Response(body)).chat(request), GatewayProtocolError);
  }
});

test("SSE preserves byte-split UTF-8, CRLF, comments, and multiline JSON", async () => {
  const payload = '\uFEFF: comment\r\nevent: delta\r\ndata: {"delta":\r\ndata: "☀"}\r\n\r\n' +
    `event: final\r\ndata: ${JSON.stringify(final)}\r\n\r\n`;
  for (const size of [1, 2, 7, 8192]) {
    const { response } = streaming(payload, size);
    const client = makeClient(async (url, init) => {
      assert.equal(url.pathname, "/prefix/v1/chat/stream");
      assert.equal(init.headers.Accept, "text/event-stream");
      return response;
    });
    assert.deepEqual(await collect(client.streamChat(request)), [
      { type: "delta", delta: "☀" }, { type: "final", response: final },
    ]);
  }
});

test("SSE accepts CR-only delimiters across byte chunks", async () => {
  const { response } = streaming(`event: final\rdata: ${JSON.stringify(final)}\r\r`);
  assert.deepEqual(await collect(makeClient(async () => response).streamChat(request)), [
    { type: "final", response: final },
  ]);
});

test("extended native fields are transmitted unchanged", async () => {
  const extended = {
    ...request, retrieval_mode: "vector", system_prompt: "Use supplied evidence.",
    history: [{ role: "user", content: "hi" }, { role: "assistant", content: "hello" }],
    model: "configured-model",
  };
  const client = makeClient(async (_url, init) => {
    assert.deepEqual(JSON.parse(init.body), extended);
    return Response.json(final);
  });
  await client.chat(extended);
});

test("stream errors retain code, partial state and request ID", async () => {
  const { response } = streaming('event: delta\ndata: {"delta":"partial"}\n\n' +
    'event: error\ndata: {"error":{"code":"storage_unavailable","message":"Storage unavailable"},"partial":true,"durable":false}\n\n');
  const iterator = makeClient(async () => response).streamChat(request);
  assert.equal((await iterator.next()).value.delta, "partial");
  await assert.rejects(iterator.next(), (error) => {
    assert.ok(error instanceof GatewayStreamError);
    assert.equal(error.code, "storage_unavailable");
    assert.equal(error.partial, true);
    assert.equal(error.durable, false);
    assert.equal(error.requestId, "req-1");
    return true;
  });
});

test("truncated, malformed, and unconfirmed streams never look successful", async () => {
  for (const payload of [
    'event: delta\ndata: {"delta":"unfinished"}\n\n',
    'event: delta\ndata: not-json\n\n',
    'event: delta\ndata: {"delta":17}\n\n',
    'event: final\ndata: {"response":"x","meta":{}}\n\n',
    'event: final\ndata: {"response":"x","meta":{"durable":true}}\n',
  ]) {
    const { response } = streaming(payload, 7);
    await assert.rejects(collect(makeClient(async () => response).streamChat(request)), GatewayProtocolError);
  }
});

test("HTTP stream errors and wrong content type fail before yielding", async () => {
  await assert.rejects(collect(makeClient(async () => Response.json({ detail: "Unauthorized" }, { status: 401 }))
    .streamChat(request)), GatewayError);
  await assert.rejects(collect(makeClient(async () => Response.json(final)).streamChat(request)), GatewayProtocolError);
});

test("breaking stream iteration cancels the reader", async () => {
  const { response, state } = streaming('event: delta\ndata: {"delta":"start"}\n\n: keepalive\n\n');
  for await (const event of makeClient(async () => response).streamChat(request)) {
    assert.equal(event.type, "delta");
    break;
  }
  assert.equal(state.cancelled, true);
});

test("AbortSignal cancels a pending read and preserves AbortError", async () => {
  const abort = new AbortController();
  let reading;
  const started = new Promise((resolve) => { reading = resolve; });
  const client = makeClient(async (_url, init) => new Response(new ReadableStream({
    start(controller) {
      init.signal.addEventListener("abort", () => controller.error(init.signal.reason), { once: true });
    },
    pull() { reading(); },
  }), { headers: { "content-type": "text/event-stream" } }));
  const run = collect(client.streamChat(request, { signal: abort.signal }));
  await started;
  abort.abort();
  await assert.rejects(run, { name: "AbortError" });
});

test("AbortSignal cancels initial request without retrying", async () => {
  const abort = new AbortController();
  abort.abort();
  const client = makeClient(async (_url, init) => { init.signal.throwIfAborted(); });
  await assert.rejects(client.chat(request, { signal: abort.signal }), { name: "AbortError" });
});

test("a failed response body is a transport error", async () => {
  const client = makeClient(async () => new Response(new ReadableStream({
    start(controller) { controller.error(new TypeError("read failed")); },
  })));
  await assert.rejects(client.chat(request), GatewayTransportError);
});

test("memory operations send exact scope, pagination and revision guards", async () => {
  const seen = [];
  const client = makeClient(async (url, init) => {
    seen.push({ url, ...init });
    return Response.json({ items: [], next_cursor: null, scope_revision: 4 });
  });
  const scope = { user_id: "user-7", feature_tag: "support" };
  await client.listMemories({ ...scope, limit: 7, cursor: "cursor+with/slash=" });
  await client.getMemory("memory-1", scope);
  await client.createMemory({ ...scope, prompt: "rule", response: "fact", expected_scope_revision: 4 });
  await client.correctMemory("memory-1", { ...scope, prompt: "rule", response: "new", expected_revision: 2 });
  await client.deleteMemory("memory-1", { ...scope, expected_revision: 2 });
  await client.deleteScope({ ...scope, expected_scope_revision: 4 });
  assert.deepEqual(seen.map(({ method, url }) => [method, url.pathname]), [
    ["GET", "/prefix/v1/memories"], ["GET", "/prefix/v1/memories/memory-1"],
    ["POST", "/prefix/v1/memories"], ["PATCH", "/prefix/v1/memories/memory-1"],
    ["DELETE", "/prefix/v1/memories/memory-1"], ["DELETE", "/prefix/v1/memories"],
  ]);
  assert.equal(seen[0].url.searchParams.get("cursor"), "cursor+with/slash=");
  assert.equal(seen[0].url.searchParams.get("limit"), "7");
  assert.deepEqual(JSON.parse(seen[3].body), { ...scope, prompt: "rule", response: "new", expected_revision: 2 });
  assert.equal(seen[4].url.searchParams.get("expected_revision"), "2");
  assert.equal(seen[5].url.searchParams.get("expected_scope_revision"), "4");
  for (const item of seen) {
    const content = item.body ? JSON.parse(item.body) : Object.fromEntries(item.url.searchParams);
    assert.equal(content.user_id, "user-7");
    assert.equal(content.feature_tag, "support");
  }
});

test("empty scopes and credential-bearing URLs fail locally", async () => {
  const client = makeClient(async () => assert.fail("No request expected"));
  assert.throws(() => client.listMemories({ user_id: "", feature_tag: "support" }), TypeError);
  for (const baseUrl of ["file:///tmp/demo", "https://u:p@example.test", "https://a.test?token=x"]) {
    assert.throws(() => new GatewayClient({ apiKey: "key", baseUrl }), TypeError);
  }
});

test("browser construction is rejected to avoid exposing tenant keys", () => {
  globalThis.window = { document: {} };
  try { assert.throws(() => new GatewayClient({ apiKey: "key" }), /trusted server/); }
  finally { delete globalThis.window; }
});
