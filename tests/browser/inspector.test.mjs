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
    this.tagName = tag.toUpperCase(); this.children = []; this.listeners = new Map();
    this._value = undefined; this.checked = false; this.hidden = false; this.disabled = false;
    this.className = ""; this.dataset = {}; this.attributes = new Map(); this.style = {};
    this.text = ""; this.clientWidth = 960; this.clientHeight = 480; this.scrollTop = 0;
    const classes = () => new Set(this.className.split(/\s+/).filter(Boolean));
    this.classList = {
      add: (...values) => { const next = classes(); values.forEach(value => next.add(value)); this.className = [...next].join(" "); },
      remove: (...values) => { const next = classes(); values.forEach(value => next.delete(value)); this.className = [...next].join(" "); },
      contains: value => classes().has(value),
      toggle: (value, force) => {
        const enabled = force ?? !classes().has(value);
        this.classList[enabled ? "add" : "remove"](value); return enabled;
      },
    };
  }
  set value(value) { this._value = String(value); }
  get value() {
    if (this.tagName === "OPTION") return this._value ?? this.textContent;
    if (this.tagName === "SELECT") {
      const options = this.children.filter(child => child.tagName === "OPTION");
      if (this._value !== undefined) return options.some(option => option.value === this._value) ? this._value : "";
      return options[0]?.value ?? "";
    }
    return this._value ?? "";
  }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return this.text + this.children.map(child => child.textContent).join(""); }
  set innerHTML(_value) { throw new Error("Untrusted content must not use innerHTML"); }
  append(...children) {
    for (let child of children) {
      if (typeof child === "string") { const node = new Element("#text"); node.textContent = child; child = node; }
      child.parent = child.parentElement = this; this.children.push(child);
    }
  }
  appendChild(child) { this.append(child); return child; }
  replaceChildren(...children) { this.text = ""; this.children = []; if (this.tagName === "SELECT") this._value = undefined; this.append(...children); }
  setAttribute(name, value) {
    value = String(value); this.attributes.set(name, value);
    if (name === "class") this.className = value;
    if (name === "id") this.id = value;
    if (name === "value") this.value = value;
    if (["hidden", "disabled", "checked"].includes(name)) this[name] = true;
    if (name.startsWith("data-")) this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = value;
  }
  setAttributeNS(_namespace, name, value) { this.setAttribute(name, value); }
  getAttribute(name) { return name === "class" ? this.className : this.attributes.get(name) ?? null; }
  removeAttribute(name) {
    this.attributes.delete(name);
    if (["hidden", "disabled", "checked"].includes(name)) this[name] = false;
  }
  addEventListener(type, fn) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(fn);
  }
  dispatch(type, fields = {}) {
    const event = { target: this, currentTarget: this, preventDefault() {}, stopPropagation() {}, ...fields };
    for (const fn of this.listeners.get(type) ?? []) fn(event);
  }
  click() { this.dispatch("click"); }
  matches(selector) {
    return selector.split(",").some(part => {
      const expression = part.trim();
      if (expression.startsWith(".")) return this.classList.contains(expression.slice(1));
      if (expression.startsWith("#")) return this.id === expression.slice(1);
      const attr = expression.match(/^\[([\w-]+)(?:=["']?([^"'\]]+)["']?)?\]$/);
      if (attr) return this.attributes.has(attr[1]) && (attr[2] === undefined || this.getAttribute(attr[1]) === attr[2]);
      assert.match(expression, /^[\w-]+$/, `Add explicit harness support for selector ${expression}`);
      return this.tagName === expression.toUpperCase();
    });
  }
  querySelectorAll(selector) {
    return this.children.flatMap(child => [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)]);
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
  closest(selector) { return this.matches(selector) ? this : this.parent?.closest(selector) ?? null; }
  contains(node) { return this === node || this.children.some(child => child.contains(node)); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter(child => child !== this); }
  setPointerCapture(pointerId) { this.capturedPointer = pointerId; }
  getBoundingClientRect() { return { width: this.clientWidth, height: this.clientHeight, x: 0, y: 0, top: 0, left: 0 }; }
  focus() { this.focused = true; }
  close() { this.open = false; }
  showModal() { this.open = true; }
}

// Parse the trusted repository fixture into a small tree so navigation selectors,
// datasets and nested nodes are actually exercised. This is not an HTML renderer.
function fixtureDOM(html) {
  const root = new Element("document"), stack = [root], elements = new Map();
  const voidTags = new Set(["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"]);
  for (const token of html.matchAll(/<!--[\s\S]*?-->|<[^>]+>|[^<]+/g)) {
    const text = token[0];
    if (text.startsWith("<!")) continue;
    if (text.startsWith("</")) { if (stack.length > 1) stack.pop(); continue; }
    if (text.startsWith("<")) {
      const tag = /^<([\w-]+)/.exec(text)?.[1]; if (!tag) continue;
      const node = new Element(tag);
      for (const match of text.slice(tag.length + 1).matchAll(/([\w:-]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g)) {
        node.setAttribute(match[1], match[2] ?? match[3] ?? match[4] ?? "");
      }
      stack.at(-1).append(node); if (node.id) elements.set(node.id, node);
      if (!voidTags.has(tag) && !text.endsWith("/>")) stack.push(node);
    } else if (text.trim()) {
      const node = new Element("#text"); node.textContent = text; stack.at(-1).append(node);
    }
  }
  return { root, elements };
}

function inspector(fetch) {
  const { root, elements } = fixtureDOM(html), created = [], windowListeners = new Map();
  const get = id => { assert.ok(elements.has(id), `Unknown inspector element: ${id}`); return elements.get(id); };
  const createElement = tag => { created.push(tag); return new Element(tag); };
  const context = vm.createContext({
    document: { getElementById: get, createElement,
      createElementNS: (_namespace, tag) => createElement(tag),
      querySelector: selector => root.querySelector(selector),
      querySelectorAll: selector => root.querySelectorAll(selector),
      body: root.querySelector("body"), documentElement: root.querySelector("html"),
    },
    window: { addEventListener: (type, fn) => windowListeners.set(type, fn) },
    fetch, AbortController, DOMException, TextDecoder, URLSearchParams, URL, console, setTimeout, clearTimeout,
  });
  vm.runInContext(source + "\nglobalThis.testApi = { state, clear, inspect, refresh, refreshGraph, refreshOverview, setView, invalidateReads };", context);
  const { state } = context.testApi;
  state.token = "operator-test-token";
  state.scope = { user_id: "customer-a", feature_tag: "support" };
  get("retrieval").value = "graph"; get("remember").checked = true;
  get("delivery").value = "stream";
  return { ...context.testApi, get, created, root, windowListeners };
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
  assert.equal(paths.filter(path => path === "/v1/chat/stream").length, 1);
  assert.ok(paths.some(path => path.startsWith("/v1/memories?")));
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

const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const turn = () => new Promise(setImmediate);
async function settled(predicate, reason = 'UI operation did not settle') {
  for (let attempt = 0; attempt < 100; attempt++) { if (predicate()) return; await turn(); }
  assert.fail(reason);
}
const memory = (id, changes = {}) => ({ id, created_at: '2026-10-04T10:00:00Z', revision: 1,
  status: 'active', prompt: `Situation ${id}`, response: `Answer ${id}`, source_ids: [], retrieval: {}, ...changes });
const listing = (items = []) => ({ items, scope_revision: 1, next_cursor: null });
const overview = (changes = {}) => ({
  totals: { calls: 0, cache_hits: 0, p95_latency_ms: null, mean_latency_ms: null, known_cost: 0,
    unpriced_calls: 0, tokens_in: 0, tokens_out: 0 },
  memory: { active_count: 0, curated_active_count: 0, retired_count: 0 }, daily: [], recent_requests: [],
  health: { postgres: true, redis: true, graph: true, embedding: true },
  runtime: { generation: { backend: 'openai_compatible', simple_model: 'served-model', complex_model: 'served-model', max_concurrency: 1, default_max_tokens: 256 },
    embedding: { backend: 'local', model: 'MiniLM', device: 'cpu', dimensions: 384, batch_size: 32, workers: 2, queue_size: 512 },
    requests: { max_concurrent: 32, timeout_seconds: 120 }, docs_available: false }, ...changes,
});
const graphData = (nodes = [], edges = []) => ({ nodes, edges, scope_revision: 1,
  truncated: { nodes: false, edges: false }, degraded: false });
const graphNode = id => ({ id, prompt_preview: `Fact ${id}`, memory_kind: 'curated' });
const answer = (changes = {}) => ({ response: 'Answer using actual provider output',
  meta: { ...completed.meta, provider: 'test-provider', memory_epoch: 1, usage_available: true,
    cost: null, timings_ms: {}, ...changes } });
function defaultResponse(path) {
  if (path.startsWith('/v1/console/overview')) return Response.json(overview());
  if (path.startsWith('/v1/console/graph')) return Response.json(graphData());
  return Response.json(listing());
}
function connect(ui, user, token = `token-${user}`) {
  ui.get('token').value = token; ui.get('user').value = user; ui.get('feature').value = 'support';
  ui.get('connect-form').dispatch('submit');
}

test('late detail A cannot overwrite the newer selected record B', async () => {
  const late = deferred();
  const ui = inspector(async path => path.includes('/A?') ? late.promise : Response.json(memory('B')));
  const pending = ui.inspect('A'); await ui.inspect('B');
  late.resolve(Response.json(memory('A'))); await pending;
  assert.equal(ui.state.selected.id, 'B');
  assert.match(ui.get('detail-content').textContent, /Answer B/);
  assert.doesNotMatch(ui.get('detail-content').textContent, /Answer A/);
});

test('late connection A cannot clear an already connected scope B', async () => {
  const late = deferred();
  const ui = inspector(async path => path.includes('user_id=A') ? late.promise : defaultResponse(path));
  connect(ui, 'A'); connect(ui, 'B'); await turn();
  assert.equal(ui.state.scope.user_id, 'B');
  late.resolve(Response.json(listing())); await turn();
  assert.equal(ui.state.scope.user_id, 'B'); assert.equal(ui.state.token, 'token-B');
  assert.equal(ui.get('scope-pill').textContent, 'B / support');
});

test('stale HTTP errors cannot affect a new scope', async () => {
  const late = deferred(), paths = [];
  const ui = inspector(async path => { paths.push(path); return path.includes('/A?') ? late.promise : defaultResponse(path); });
  const pending = ui.inspect('A'); ui.clear();
  ui.state.token = 'token-B'; ui.state.scope = { user_id: 'B', feature_tag: 'support' };
  ui.get('notice').textContent = 'Scope B remains ready';
  late.resolve(Response.json({ detail: 'Old scope failed' }, { status: 503 })); await pending;
  assert.equal(ui.get('notice').textContent, 'Scope B remains ready');
  assert.equal(ui.state.selected, null); assert.equal(paths.length, 1);
});

test('mutation invalidation blocks an older detail response from reopening removed content', async () => {
  const late = deferred();
  const ui = inspector(async () => late.promise);
  const pending = ui.inspect('A'); ui.invalidateReads();
  late.resolve(Response.json(memory('A', { response: 'Old content before deletion' }))); await pending;
  assert.equal(ui.state.selected, null); assert.notEqual(ui.get('detail-dialog').open, true);
  assert.doesNotMatch(ui.get('detail-content').textContent, /Old content before deletion/);
});

test('navigation selects the requested panel and calls only its scoped data endpoint', async () => {
  const paths = [];
  const ui = inspector(async path => { paths.push(path); return defaultResponse(path); });
  const navigation = ui.root.querySelectorAll('[data-view]');
  navigation.find(node => node.dataset.view === 'graph').click(); await turn();
  assert.equal(ui.state.view, 'graph'); assert.equal(ui.get('view-graph').hidden, false);
  assert.equal(ui.get('view-overview').hidden, true);
  assert.equal(navigation.find(node => node.dataset.view === 'graph').getAttribute('aria-current'), 'page');
  assert.ok(paths[0].startsWith('/v1/console/graph?'));
  assert.match(paths[0], /user_id=customer-a/); assert.match(paths[0], /feature_tag=support/);
  navigation.find(node => node.dataset.view === 'connections').click(); await turn();
  assert.equal(ui.get('view-connections').hidden, false); assert.equal(ui.get('view-graph').hidden, true);
  assert.ok(paths[1].startsWith('/v1/console/overview?'));
});

test('graph draws only returned edges and selecting a node loads its authoritative record', async () => {
  const paths = [];
  const data = graphData([graphNode('A'), graphNode('B')], [{ source: 'A', target: 'B', type: 'INFORMED_BY', similarity: null }]);
  const ui = inspector(async path => { paths.push(path); return Response.json(path.startsWith('/v1/console/graph') ? data : memory('B')); });
  await ui.refreshGraph();
  assert.equal(ui.get('memory-graph').querySelectorAll('.graph-edge').length, 1);
  const nodes = ui.get('memory-graph').querySelectorAll('[data-node-id]');
  assert.equal(nodes.length, 2);
  nodes.find(node => node.dataset.nodeId === 'B').dispatch('keydown', { key: 'Enter' });
  await settled(() => ui.state.selected?.id === 'B');
  assert.match(paths[1], /^\/v1\/memories\/B\?user_id=customer-a&feature_tag=support$/);
  assert.ok(nodes.find(node => node.dataset.nodeId === 'B').classList.contains('selected'));
  assert.match(ui.get('detail-content').textContent, /Answer B/);
});

test('graph late responses and old node handlers are discarded after switching scopes', async () => {
  const late = deferred(), paths = [];
  let delay = false;
  const ui = inspector(async path => { paths.push(path); return delay ? late.promise : Response.json(graphData([graphNode('A')])); });
  await ui.refreshGraph();
  const oldNode = ui.get('memory-graph').querySelector('[data-node-id]');
  delay = true; const pending = ui.refreshGraph();
  ui.clear(); ui.state.token = 'token-B'; ui.state.scope = { user_id: 'B', feature_tag: 'support' };
  oldNode.click();
  late.resolve(Response.json(graphData([graphNode('A')]))); await pending;
  assert.equal(paths.length, 2); assert.equal(ui.state.graph, null);
  assert.equal(ui.get('memory-graph').children.length, 0);
});

test('complete delivery uses one normal chat response and records actual completion metadata', async () => {
  const paths = [], bodies = [];
  const ui = inspector(async (path, init) => { paths.push(path); if (path === '/v1/chat') { bodies.push(JSON.parse(init.body)); return Response.json(answer()); } return defaultResponse(path); });
  ui.get('delivery').value = 'complete'; ui.get('prompt').value = 'Use my remembered support history';
  ui.get('chat-form').dispatch('submit'); await finishChat(ui);
  assert.equal(paths.filter(path => path === '/v1/chat').length, 1);
  assert.ok(!paths.includes('/v1/chat/stream'));
  assert.equal(bodies[0].user_id, 'customer-a'); assert.equal(bodies[0].retrieval_mode, 'graph');
  assert.match(ui.get('conversation').textContent, /actual provider output/);
  assert.equal(ui.get('stream-state').textContent, 'Answer complete'); assert.equal(ui.state.history.length, 2);
});

test('unknown token usage remains unavailable while a real zero remains zero', async () => {
  for (const available of [false, true]) {
    const ui = inspector(async path => path === '/v1/chat' ? Response.json(answer({ usage_available: available, tokens_in: 0, tokens_out: 0 })) : defaultResponse(path));
    ui.get('delivery').value = 'complete'; ui.get('prompt').value = 'What happened?';
    ui.get('chat-form').dispatch('submit'); await finishChat(ui);
    assert.match(ui.get('conversation').textContent, available ? /0 reported tokens/ : /usage unavailable/);
    if (!available) assert.doesNotMatch(ui.get('conversation').textContent, /0 reported tokens/);
  }
});

test('stopping a complete request aborts transport and never records a late answer as completed', async () => {
  const late = deferred(); let signal;
  const ui = inspector(async (_path, init) => { signal = init.signal; return late.promise; });
  ui.get('delivery').value = 'complete'; ui.get('prompt').value = 'Long answer'; ui.get('chat-form').dispatch('submit');
  ui.get('stop').click(); assert.equal(signal.aborted, true);
  late.resolve(Response.json(answer())); await finishChat(ui);
  assert.notEqual(ui.get('stream-state').textContent, 'Answer complete'); assert.equal(ui.state.history.length, 0);
  assert.match(ui.get('conversation').textContent, /Stopped/);
  assert.doesNotMatch(ui.get('conversation').textContent, /actual provider output/);
});

test('disconnect clears provisional stream text and blocks subsequent late events', async () => {
  let transport, signal;
  const response = new Response(new ReadableStream({ start(controller) { transport = controller; } }));
  const ui = inspector(async (_path, init) => { signal = init.signal; return response; });
  ui.get('prompt').value = 'Scope A secret'; ui.get('chat-form').dispatch('submit');
  transport.enqueue(new TextEncoder().encode('event: delta\ndata: {"delta":"Provisional secret"}\n\n'));
  await turn(); assert.match(ui.get('conversation').textContent, /Provisional secret/);
  ui.clear(); assert.equal(signal.aborted, true);
  transport.enqueue(new TextEncoder().encode(`event: final\ndata: ${JSON.stringify(answer())}\n\n`)); transport.close();
  await turn(); assert.equal(ui.get('conversation').children.length, 0); assert.equal(ui.state.history.length, 0);
});

test('comparison failures remain explicit and never create an invented success score', async () => {
  const modes = [];
  const ui = inspector(async (_path, init) => {
    const body = JSON.parse(init.body); modes.push(body.retrieval_mode);
    assert.equal(body.store, false); assert.equal(body.use_cache, false); assert.equal(body.prompt, 'Compare the same question');
    if (body.retrieval_mode === 'vector') return Response.json({ detail: 'unavailable' }, { status: 503 });
    return Response.json(answer());
  });
  ui.get('compare-prompt').value = 'Compare the same question'; ui.get('compare-form').dispatch('submit');
  await settled(() => ui.state.comparison === null);
  assert.deepEqual(modes, ['none', 'vector', 'graph']);
  assert.equal(ui.state.comparisonReport.complete, false); assert.equal(ui.state.comparisonReport.results.length, 3);
  assert.ok(ui.state.comparisonReport.results.find(row => row.mode === 'vector').error);
  assert.match(ui.get('comparison-results').textContent, /FAILED/); assert.match(ui.get('comparison-status').textContent, /2 of 3/);
  assert.equal('score' in ui.state.comparisonReport, false);
});

test('comparison stop cancels the current mode and leaves later modes unrun', async () => {
  const late = deferred(); let signal, calls = 0;
  const ui = inspector(async (_path, init) => { calls++; signal = init.signal; return late.promise; });
  ui.get('compare-prompt').value = 'Compare'; ui.get('compare-form').dispatch('submit');
  ui.get('stop-comparison').click(); assert.equal(signal.aborted, true);
  late.resolve(Response.json(answer())); await settled(() => ui.state.comparison === null);
  assert.equal(calls, 1); assert.equal(ui.state.comparisonReport.complete, false);
  assert.match(ui.get('comparison-results').textContent, /STOPPED/); assert.match(ui.get('comparison-results').textContent, /NOT RUN/);
});

test('comparison identifies model or revision mismatch rather than claiming controlled conditions', async () => {
  const ui = inspector(async (_path, init) => { const mode = JSON.parse(init.body).retrieval_mode;
    return Response.json(answer({ model: mode === 'graph' ? 'different-model' : 'served-model', memory_epoch: mode === 'vector' ? 2 : 1 })); });
  ui.get('compare-prompt').value = 'Compare'; ui.get('compare-form').dispatch('submit'); await settled(() => ui.state.comparison === null);
  assert.equal(ui.state.comparisonReport.complete, true); assert.equal(ui.state.comparisonReport.model_matched, false);
  assert.equal(ui.state.comparisonReport.memory_revision_matched, false);
  assert.match(ui.get('comparison-status').textContent, /models or memory revisions differ/);
});

test('disconnect during comparison leaves no old results, prompts or late report in a new scope', async () => {
  const late = deferred(); let signal;
  const ui = inspector(async (_path, init) => { signal = init.signal; return late.promise; });
  ui.get('compare-prompt').value = 'Private scope A comparison'; ui.get('compare-form').dispatch('submit');
  ui.clear(); ui.state.token = 'token-B'; ui.state.scope = { user_id: 'B', feature_tag: 'support' };
  assert.equal(signal.aborted, true); late.resolve(Response.json(answer())); await turn();
  assert.equal(ui.state.comparisonReport, null); assert.equal(ui.get('comparison-results').children.length, 0);
  assert.equal(ui.get('compare-prompt').value, ''); assert.equal(ui.state.scope.user_id, 'B');
});

test('overview labels incomplete pricing as a known subtotal and never embeds the active credential', async () => {
  const data = overview({ totals: { calls: 3, cache_hits: 0, p95_latency_ms: 1, mean_latency_ms: 1, known_cost: 0,
    unpriced_calls: 3, tokens_in: 0, tokens_out: 0 } });
  const ui = inspector(async () => Response.json(data)); await ui.refreshOverview();
  assert.match(ui.get('accounting-cost').textContent, /Known price subtotal: \$0\.00000.*3 unpriced requests/);
  assert.match(ui.get('accounting-tokens').textContent, /Reported tokens/);
  assert.doesNotMatch(ui.get('integration-code').textContent, /operator-test-token/);
  assert.match(ui.get('integration-code').textContent, /<your-gateway-token>/);
});

test('automatic model routing remains an empty value after disconnect resets the selectors', () => {
  const ui = inspector(async () => assert.fail('Reset must not fetch'));
  ui.clear();
  assert.equal(ui.get('model').value, ''); assert.equal(ui.get('compare-model').value, '');
});

test('post-mutation refresh from an old scope cannot alter the new scope after disconnect', async () => {
  const late = deferred(), paths = [];
  const ui = inspector(async (path, init) => {
    paths.push(path);
    if (init.method === 'DELETE') return Response.json({ deleted_count: 1 });
    if (path.startsWith('/v1/memories?') && path.includes('user_id=customer-a')) return late.promise;
    return defaultResponse(path);
  });
  ui.get('forget-scope').click(); ui.get('delete-form').dispatch('submit');
  await settled(() => paths.some(path => path.startsWith('/v1/memories?')) && paths.length === 2);
  ui.clear(); ui.state.token = 'token-B'; ui.state.scope = { user_id: 'B', feature_tag: 'support' };
  ui.get('notice').textContent = 'Scope B remains ready';
  late.resolve(Response.json(listing())); await turn(); await turn();
  assert.equal(ui.get('notice').textContent, 'Scope B remains ready');
  assert.equal(paths.length, 2, 'An old mutation must not initiate new-scope refreshes');
});

test('overview with no observations does not present an invented zero-millisecond latency', async () => {
  const empty = overview(); empty.totals.p95_latency_ms = 0; empty.totals.mean_latency_ms = 0;
  const ui = inspector(async () => Response.json(empty)); await ui.refreshOverview();
  assert.equal(ui.get('metric-latency').textContent, '—');
  assert.match(ui.get('metric-latency-note').textContent, /No completed requests/);
});

test('comparison export is inspectable without credentials and is erased when the scope disconnects', async () => {
  const ui = inspector(async (_path, init) => {
    assert.equal(init.headers.Authorization, 'Bearer operator-test-token');
    return Response.json(answer());
  });
  ui.get('compare-prompt').value = 'Private scope A comparison question';
  ui.get('compare-form').dispatch('submit'); await settled(() => ui.state.comparison === null);
  ui.get('export-comparison').click();
  assert.equal(ui.get('export-dialog').open, true);
  assert.notEqual(ui.get('export-json').getAttribute('readonly'), null);
  const serialized = ui.get('export-json').value, exported = JSON.parse(serialized);
  assert.deepEqual(exported.scope, { user_id: 'customer-a', feature_tag: 'support' });
  assert.equal(exported.prompt, 'Private scope A comparison question');
  assert.equal(exported.results.length, 3); assert.equal(exported.complete, true);
  assert.equal(exported.results[0].response, 'Answer using actual provider output');
  assert.doesNotMatch(serialized, /operator-test-token|Authorization|Bearer/);
  ui.get('disconnect').click();
  assert.equal(ui.get('export-dialog').open, false); assert.equal(ui.get('export-json').value, '');
  assert.equal(ui.state.comparisonReport, null); assert.equal(ui.get('export-comparison').disabled, true);
  ui.state.token = 'new-scope-token'; ui.state.scope = { user_id: 'B', feature_tag: 'support' };
  ui.get('export-comparison').click();
  assert.equal(ui.get('export-dialog').open, false); assert.equal(ui.get('export-json').value, '');
});

test('completed provider truncation and filtering remain visible without being mislabeled as transport failure', async () => {
  for (const [reason, explanation] of [
    ['length', /Output token limit reached\. Increase the limit/],
    ['content_filter', /The provider filtered part of this response/],
  ]) {
    const ui = inspector(async path => path === '/v1/chat'
      ? Response.json(answer({ finish_reason: reason })) : defaultResponse(path));
    ui.get('delivery').value = 'complete'; ui.get('prompt').value = 'Give a long answer';
    ui.get('chat-form').dispatch('submit'); await finishChat(ui);
    assert.match(ui.get('conversation').textContent, explanation);
    assert.doesNotMatch(ui.get('conversation').textContent, /Incomplete answer|No durable completion/);
    assert.equal(ui.get('stream-state').textContent, 'Answer complete');
    assert.equal(ui.state.history.length, 2);
  }
});
