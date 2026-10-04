import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../../app/static/studio.js", import.meta.url), "utf8");
const html = readFileSync(new URL("../../app/static/index.html", import.meta.url), "utf8");

// Only the DOM operations exercised by the actual inspector script are modeled.
// These are behavior regressions, not a substitute for visual/browser verification.
class Element {
  constructor(tag = "div") {
    this.tagName = tag;
    this.children = [];
    this.listeners = new Map();
    this.value = "";
    this.checked = false;
    this.hidden = false;
    this.disabled = false;
    this.className = "";
    this.classes = new Set();
    this.classList = {
      add: (...values) => values.forEach((value) => this.classes.add(value)),
      remove: (...values) => values.forEach((value) => this.classes.delete(value)),
    };
    this.text = "";
  }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return this.text + this.children.map((child) => child.textContent).join(""); }
  set innerHTML(_value) { throw new Error("Untrusted content must not use innerHTML"); }
  append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } }
  replaceChildren(...children) { this.text = ""; this.children = []; this.append(...children); }
  addEventListener(type, fn) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(fn);
  }
  dispatch(type) { for (const fn of this.listeners.get(type) ?? []) fn({ preventDefault() {} }); }
  querySelector(selector) {
    const name = selector.replace(/^\./, "");
    for (const child of this.children) {
      if (child.className.split(" ").includes(name)) return child;
      const nested = child.querySelector(selector);
      if (nested) return nested;
    }
    return null;
  }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((child) => child !== this); }
  focus() {}
  close() { this.open = false; }
  showModal() { this.open = true; }
}

function inspector(fetch) {
  const elements = new Map([...html.matchAll(/\bid="([^"]+)"/g)].map((match) => [match[1], new Element()]));
  const created = [];
  const get = (id) => { assert.ok(elements.has(id), `Unknown inspector element: ${id}`); return elements.get(id); };
  const context = vm.createContext({
    document: {
      getElementById: get,
      createElement: (tag) => { created.push(tag); return new Element(tag); },
      body: new Element("body"),
    },
    window: { addEventListener() {} },
    fetch, AbortController, DOMException, TextDecoder, URLSearchParams, console,
  });
  vm.runInContext(source + "\nglobalThis.testApi = { state, clear, inspect };", context);
  const { state } = context.testApi;
  state.token = "operator-test-token";
  state.scope = { user_id: "customer-a", feature_tag: "support" };
  get("retrieval").value = "graph";
  get("remember").checked = true;
  return { ...context.testApi, get, created };
}

const completed = {
  response: "Reviewed support answer",
  meta: { durable: true, model: "synthetic", latency_ms: 1, tokens_in: 2, tokens_out: 3,
    cache_hit: false, context_used: [], memory_write: "queued", degraded: [] },
};

function sse(final = true) {
  let payload = 'event: delta\ndata: {"delta":"Provisional text"}\n\n';
  if (final) payload += `event: final\ndata: ${JSON.stringify(completed)}\n\n`;
  return new Response(payload, { headers: { "content-type": "text/event-stream" } });
}

async function finishChat(ui) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (ui.state.stream === null) return;
    await new Promise(setImmediate);
  }
  assert.fail("Inspector chat handler did not settle");
}

test("disconnect removes the credential and unsent content before another scope connects", () => {
  const ui = inspector(async () => assert.fail("Disconnect must not send a request"));
  ui.state.editing = { response: "Old customer's private answer" };
  ui.state.deleting = { prompt: "Old customer's private prompt" };
  for (const id of ["prompt", "edit-prompt", "edit-response", "token"]) ui.get(id).value = "private draft";
  ui.get("conversation").append(new Element("article"));
  ui.get("disconnect").dispatch("click");
  assert.equal(ui.state.token, "");
  assert.equal(ui.state.scope, null);
  assert.equal(ui.state.editing, null);
  assert.equal(ui.state.deleting, null);
  for (const id of ["prompt", "edit-prompt", "edit-response", "token"]) assert.equal(ui.get(id).value, "");
  assert.equal(ui.get("conversation").children.length, 0);
  assert.equal(ui.get("send").disabled, true);
});

test("a failed timeline refresh cannot relabel a committed answer as incomplete", async () => {
  const paths = [];
  const ui = inspector(async (path, init) => {
    paths.push(path);
    assert.equal(init.headers.Authorization, "Bearer operator-test-token");
    if (path === "/v1/chat/stream") return sse();
    return Response.json({ detail: "Storage unavailable" }, { status: 503 });
  });
  ui.get("prompt").value = "What did we discuss?";
  ui.get("chat-form").dispatch("submit");
  await finishChat(ui);
  assert.equal(paths.length, 2);
  assert.match(ui.get("conversation").textContent, /Reviewed support answer/);
  assert.doesNotMatch(ui.get("conversation").textContent, /Incomplete|No durable completion/);
  assert.match(ui.get("notice").textContent, /completed and committed.*refresh failed/i);
  assert.equal(ui.get("stream-state").textContent, "Answer complete");
  assert.equal(ui.get("send").disabled, false);
});

test("a stream without final remains visibly incomplete and does not refresh memory", async () => {
  let calls = 0;
  const ui = inspector(async () => { calls++; return sse(false); });
  ui.get("prompt").value = "What did we discuss?";
  ui.get("chat-form").dispatch("submit");
  await finishChat(ui);
  assert.equal(calls, 1);
  assert.match(ui.get("conversation").textContent, /Provisional text/);
  assert.match(ui.get("conversation").textContent, /Incomplete answer/);
  assert.match(ui.get("notice").textContent, /before completion/);
  assert.notEqual(ui.get("stream-state").textContent, "Answer complete");
});

test("memory content renders as literal text rather than executable markup", async () => {
  const hostile = '<img src=x onerror="fetch(\'/steal\')">';
  const ui = inspector(async () => Response.json({
    id: "memory-1", created_at: "2026-10-04T10:00:00Z", revision: 1, status: "active",
    prompt: hostile, response: "<script>alert(1)</script>", source_ids: [], retrieval: {},
  }));
  await ui.inspect("memory-1");
  assert.ok(ui.get("detail-content").textContent.includes(hostile));
  assert.ok(ui.get("detail-content").textContent.includes("<script>alert(1)</script>"));
  assert.equal(ui.created.includes("img"), false);
  assert.equal(ui.created.includes("script"), false);
});
