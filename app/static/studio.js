const $ = (id) => document.getElementById(id);
const viewNames = { overview: 'Overview', playground: 'Playground', graph: 'Memory graph', library: 'Memory library', compare: 'Compare retrieval', connections: 'Connections' };
const state = {
  token: '', scope: null, revision: 0, cursor: null, items: [], selected: null,
  session: 0, requests: new Set(), readRequests: new Set(), stream: null, comparison: null,
  editing: null, deleting: null, busy: false, view: 'overview', history: [], overview: null,
  graph: null, graphPositions: new Map(), graphBox: [0, 0, 1000, 570], comparisonReport: null,
  sequences: { list: 0, detail: 0, overview: 0, graph: 0 },
};
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
};
const svg = (tag, attrs = {}, text) => {
  const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
  if (text !== undefined) node.textContent = text;
  return node;
};
const scopeQuery = (extra = {}) => new URLSearchParams({ ...state.scope, ...extra }).toString();
const number = (value) => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 1 }) : '—';
const date = (value) => value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : '—';
const busy = () => state.busy || !!state.stream || !!state.comparison;
function notice(text, error = false) {
  $('notice').textContent = text;
  $('notice').className = `notice${error ? ' error' : ''}`;
  $('notice').hidden = !text;
}
function fail(error) { if (error.name !== 'AbortError') notice(error.message, true); }
function guard(fn) {
  return (event) => {
    event?.preventDefault();
    const session = state.session;
    Promise.resolve(fn(event)).catch((error) => { if (session === state.session) fail(error); });
  };
}
function controls() {
  const enabled = !!state.token && !busy();
  for (const id of ['refresh', 'global-refresh', 'graph-refresh', 'new-memory', 'seed', 'forget-scope', 'send', 'run-comparison']) $(id).disabled = !enabled;
  $('clear-chat').disabled = !!state.stream;
  $('export-comparison').disabled = !state.comparisonReport || !!state.comparison;
}
function ticket(kind) { return { session: state.session, kind, sequence: ++state.sequences[kind] }; }
function current(read) { return read.session === state.session && state.sequences[read.kind] === read.sequence; }
function invalidateReads() {
  for (const key of Object.keys(state.sequences)) state.sequences[key]++;
  for (const controller of state.readRequests) controller.abort();
}
async function api(path, { method = 'GET', body, signal } = {}) {
  const session = state.session, controller = new AbortController();
  state.requests.add(controller);
  if (method === 'GET') state.readRequests.add(controller);
  const abort = () => controller.abort();
  signal?.addEventListener('abort', abort, { once: true });
  try {
    if (signal?.aborted) controller.abort();
    const response = await fetch(path, {
      method, headers: { Authorization: `Bearer ${state.token}`, ...(body ? { 'Content-Type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined, signal: controller.signal,
      cache: 'no-store', credentials: 'omit', redirect: 'error',
    });
    if (!response.ok) {
      const messages = { 401: 'Token rejected. Disconnect and check your credentials.', 404: 'This record is no longer available in this scope.', 409: 'Memory changed during the request. Refresh before trying again.', 422: 'The provider or gateway rejected these settings. Check the token limit and supported response delivery.', 429: 'Request limit reached. Try again shortly.', 503: 'Gateway capacity or a backing service is unavailable. Check Connections and retry.' };
      throw new Error(messages[response.status] || `Request failed (${response.status}). Check gateway health.`);
    }
    const data = await response.json();
    if (session !== state.session || controller.signal.aborted) throw new DOMException('Workspace changed', 'AbortError');
    return data;
  } catch (error) {
    if (session !== state.session || controller.signal.aborted) throw new DOMException('Workspace changed or request stopped', 'AbortError');
    throw error;
  } finally {
    state.requests.delete(controller); state.readRequests.delete(controller);
    signal?.removeEventListener('abort', abort);
  }
}
async function readApi(read, path) {
  try { return await api(path); } catch (error) { if (current(read)) throw error; return null; }
}
function setView(view, load = true) {
  if (!Object.hasOwn(viewNames, view)) return;
  state.view = view; notice('');
  window.scrollTo?.({ top: 0, behavior: 'instant' });
  for (const panel of document.querySelectorAll('[data-panel]')) panel.hidden = panel.dataset.panel !== view;
  for (const link of document.querySelectorAll('[data-view]')) {
    link.classList.toggle('active', link.dataset.view === view);
    if (link.dataset.view === view) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  }
  $('page-name').textContent = viewNames[view];
  if (load && state.token) { const session = state.session; loadView().catch(error => { if (session === state.session && view === state.view) fail(error); }); }
}
async function loadView() {
  if (!state.token) return;
  if (state.view === 'graph') return refreshGraph();
  if (state.view === 'library') return refresh();
  if (['overview', 'connections'].includes(state.view)) return refreshOverview();
}
for (const link of document.querySelectorAll('[data-view]')) link.addEventListener('click', () => setView(link.dataset.view));
$('overview-chat').addEventListener('click', () => setView('playground'));
$('overview-library').addEventListener('click', () => setView('library'));
$('global-refresh').addEventListener('click', guard(() => loadView()));
$('graph-refresh').addEventListener('click', guard(() => refreshGraph()));
for (const id of ['connect-workspace', 'welcome-connect']) $(id).addEventListener('click', () => $('connection-dialog').showModal());
$('close-connection').addEventListener('click', () => $('connection-dialog').close());
$('close-detail').addEventListener('click', () => { state.sequences.detail++; $('detail-dialog').close(); });
function resetConversation() {
  state.history = [];
  $('conversation').replaceChildren(el('p', 'empty', 'Start a fresh conversation using the current memory.'));
}
function clear() {
  state.session++;
  invalidateReads();
  state.stream?.abort(); state.comparison?.abort();
  for (const request of state.requests) request.abort();
  state.stream = null; state.comparison = null; state.token = ''; state.scope = null;
  state.items = []; state.selected = null; state.cursor = null; state.revision = 0;
  state.busy = false; state.editing = null; state.deleting = null; state.history = [];
  state.overview = null; state.graph = null; state.graphPositions.clear(); state.comparisonReport = null;
  for (const id of ['token', 'prompt', 'edit-prompt', 'edit-response', 'system-prompt', 'compare-prompt', 'memory-search', 'graph-search', 'export-json']) $(id).value = '';
  for (const id of ['memory-list', 'detail-content', 'conversation', 'memory-graph', 'graph-node-list', 'comparison-results', 'recent-requests', 'health-services', 'runtime-cards']) $(id).replaceChildren();
  for (const id of ['metric-requests', 'metric-memories', 'metric-latency', 'metric-cache', 'nav-count']) $(id).textContent = '—';
  $('detail-content').hidden = true; $('detail-empty').hidden = false; $('empty').hidden = false;
  $('empty').textContent = 'Connect a workspace to explore its history.';
  $('count').textContent = '0 loaded'; $('more').hidden = true;
  $('connection-status').textContent = 'No workspace connected';
  document.body.classList.remove('connected');
  $('sidebar-scope').textContent = 'Connect to begin'; $('scope-pill').textContent = 'No scope selected';
  $('scope-revision').textContent = 'No workspace selected'; $('system-status').textContent = 'Gateway disconnected';
  $('stream-state').textContent = 'Ready when you are'; $('comparison-status').textContent = 'Connect a scope to compare its retrieval modes.';
  $('graph-empty').hidden = false; $('graph-count').textContent = '0 NODES'; $('graph-note').textContent = 'Graph data is loaded from your gateway.';
  $('requests-empty').hidden = false; $('welcome-banner').hidden = false;
  $('activity-chart').replaceChildren(); $('activity-chart').textContent = 'Connect a scope to see its activity.'; $('activity-chart').className = 'chart-empty';
  $('activity-summary').textContent = 'No data loaded'; $('health-label').textContent = 'Offline'; $('health-label').className = 'status-badge muted';
  $('active-model').textContent = 'No model connected'; $('model-note').textContent = 'Configured model details appear here.';
  $('chat-model-label').textContent = 'Gateway playground'; $('chat-backend-label').textContent = 'Connect a gateway to begin';
  $('connect-workspace').textContent = 'Connect gateway ↗';
  $('connect').hidden = false; $('disconnect').hidden = true; $('stop').hidden = true; $('stop-comparison').hidden = true;
  $('api-docs').hidden = true; $('memory-filter').value = 'all';
  for (const id of ['token', 'user', 'feature']) $(id).disabled = false;
  for (const id of ['metric-request-note', 'metric-memory-note', 'metric-latency-note', 'metric-cache-note', 'accounting-cost', 'accounting-tokens', 'last-updated']) $(id).textContent = 'Connect to load scoped data.';
  for (const id of ['model', 'compare-model']) { const option = el('option', '', 'Automatic routing'); option.value = ''; $(id).replaceChildren(option); }
  for (const id of ['edit-dialog', 'delete-dialog', 'detail-dialog', 'export-dialog']) $(id).close();
  updateCode(); controls(); notice('');
}
$('connect-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const token = $('token').value.trim(), user = $('user').value, feature = $('feature').value;
  clear(); state.token = token; state.scope = { user_id: user, feature_tag: feature };
  const session = state.session;
  try {
    await refresh();
    if (session !== state.session) return;
    $('connection-status').textContent = `${user} / ${feature}`;
    $('sidebar-scope').textContent = user; $('scope-pill').textContent = `${user} / ${feature}`;
    document.body.classList.add('connected'); $('connect').hidden = true; $('disconnect').hidden = false;
    $('welcome-banner').hidden = true; $('connect-workspace').textContent = 'Workspace settings';
    for (const id of ['token', 'user', 'feature']) $(id).disabled = true;
    $('connection-dialog').close(); controls(); updateCode();
    await refreshOverview();
    if (session === state.session && state.view === 'graph') await refreshGraph();
  } catch (error) {
    if (session !== state.session) return;
    if (!$('connect').hidden) clear();
    fail(error);
  }
});
$('disconnect').addEventListener('click', clear);
function renderList() {
  const query = $('memory-search').value.toLowerCase(), status = $('memory-filter').value || 'all';
  const items = state.items.filter((item) => (status === 'all' || item.status === status) && `${item.prompt || ''} ${item.id}`.toLowerCase().includes(query));
  $('count').textContent = `${items.length} / ${state.items.length} loaded`;
  $('memory-list').replaceChildren(); $('empty').hidden = items.length > 0;
  $('empty').textContent = state.items.length ? 'No loaded memories match this filter.' : 'No memories yet. Add a verified fact or load the fictional support example.';
  for (const memory of items) {
    const li = el('li'), button = el('button', `memory-item${state.selected?.id === memory.id ? ' selected' : ''}`);
    button.type = 'button';
    const description = el('div'); description.append(el('p', '', memory.prompt || 'Live content removed'), el('small', '', `${memory.id.slice(0, 8)} · revision ${memory.revision}`));
    button.append(description, el('span', 'cell', String(memory.source_ids?.length || 0)), el('span', 'cell', date(memory.created_at)), el('span', `tag status${memory.status === 'active' ? '' : ' muted'}`, memory.status || 'active'), el('span', 'cell', '↗'));
    button.addEventListener('click', guard(() => inspect(memory.id))); li.append(button); $('memory-list').append(li);
  }
  $('more').hidden = !state.cursor; $('scope-revision').textContent = `Scope revision ${state.revision}`;
}
async function refresh(append = false) {
  const read = ticket('list');
  const data = await readApi(read, `/v1/memories?${scopeQuery({ limit: 50, ...(append && state.cursor ? { cursor: state.cursor } : {}) })}`);
  if (!current(read)) return;
  state.revision = data.scope_revision; state.cursor = data.next_cursor;
  state.items = [...new Map((append ? [...state.items, ...data.items] : data.items).map(item => [item.id, item])).values()];
  renderList();
}
$('refresh').addEventListener('click', guard(() => refresh()));
$('more').addEventListener('click', guard(() => refresh(true)));
$('memory-search').addEventListener('input', renderList);
$('memory-filter').addEventListener('change', renderList);
async function inspect(id) {
  const read = ticket('detail');
  const result = await readApi(read, `/v1/memories/${encodeURIComponent(id)}?${scopeQuery()}`);
  if (!current(read)) return;
  const memory = result.memory || result;
  state.selected = memory; renderList();
  $('detail-empty').hidden = true; $('detail-content').hidden = false;
  const body = el('div', 'detail-body');
  body.append(el('p', 'identifier', `${memory.id}\n${date(memory.created_at)} · ${memory.status} · revision ${memory.revision}`));
  for (const [title, value] of [['Situation', memory.prompt || 'Live content removed'], ['Recorded answer', memory.response || 'No retained answer']]) {
    const section = el('section'); section.append(el('label', '', title), el('pre', '', value)); body.append(section);
  }
  const sources = el('section'); sources.append(el('label', '', 'Context supplied to this memory'), sourceChips(memory.source_ids || []));
  if (!memory.source_ids?.length) sources.append(el('p', 'identifier', 'No source memories recorded.'));
  if (memory.supersedes_id) sources.append(el('p', 'identifier', `Replaces ${memory.supersedes_id}`));
  body.append(sources);
  const explanation = el('section'); explanation.append(el('label', '', 'About this record'), el('p', 'identifier', memory.retrieval?.source || 'Relationships record retrieved context and similarity, not verified correctness.')); body.append(explanation);
  if (memory.status === 'active') {
    const actions = el('div', 'actions'), correct = el('button', 'quiet', 'Correct memory'), remove = el('button', 'quiet', 'Forget');
    correct.addEventListener('click', () => openEdit(memory)); remove.addEventListener('click', () => openDelete(memory));
    actions.append(correct, remove); body.append(actions);
  }
  $('detail-content').replaceChildren(body);
  if (!$('detail-dialog').open) $('detail-dialog').showModal();
  highlightGraph();
}
function sourceChips(ids) {
  const chips = el('div', 'source-chips'), session = state.session;
  for (const id of ids) {
    const button = el('button', '', `Source ${id.slice(0, 8)}`);
    button.addEventListener('click', guard(() => session === state.session ? inspect(id) : undefined));
    chips.append(button);
  }
  return chips;
}
function openEdit(memory = null) {
  if (busy()) return;
  state.editing = memory; $('detail-dialog').close();
  $('edit-title').textContent = memory ? 'Correct this memory' : 'Add a memory';
  $('edit-help').textContent = memory ? 'The replacement becomes a new record. Old content and dependent answers are retired, and scoped caches are invalidated.' : 'Save a verified exchange without generating a model answer.';
  $('edit-prompt').value = memory?.prompt || ''; $('edit-response').value = memory?.response || '';
  $('edit-dialog').showModal(); $('edit-prompt').focus();
}
function clearDerivedViews() {
  invalidateReads(); resetConversation(); state.selected = null; state.graph = null; state.comparisonReport = null;
  $('detail-dialog').close(); $('detail-content').replaceChildren(); $('detail-content').hidden = true; $('detail-empty').hidden = false;
  $('memory-graph').replaceChildren(); $('graph-node-list').replaceChildren(); $('graph-empty').hidden = false;
  $('comparison-results').replaceChildren(); $('comparison-status').textContent = 'Memory changed. Run a fresh comparison.';
  $('export-json').value = ''; $('export-dialog').close();
  state.items = []; state.cursor = null; renderList();
}
async function afterMutation(session) {
  const check = () => { if (session !== state.session) throw new DOMException('Workspace changed', 'AbortError'); };
  check(); clearDerivedViews(); await refresh(); check();
  await refreshOverview(); check();
  if (state.view === 'graph') { await refreshGraph(); check(); }
}
$('new-memory').addEventListener('click', () => openEdit());
$('cancel-edit').addEventListener('click', () => $('edit-dialog').close());
$('edit-form').addEventListener('submit', guard(async () => {
  if (busy()) return;
  const session = state.session, memory = state.editing;
  state.busy = true; invalidateReads(); controls(); $('save-edit').disabled = true;
  let committed = false;
  try {
    const result = await api(memory ? `/v1/memories/${encodeURIComponent(memory.id)}` : '/v1/memories', {
      method: memory ? 'PATCH' : 'POST', body: { ...state.scope, prompt: $('edit-prompt').value, response: $('edit-response').value,
        ...(memory ? { expected_revision: memory.revision } : { expected_scope_revision: state.revision }) },
    });
    committed = true; $('edit-dialog').close(); await afterMutation(session); await inspect(result.memory.id);
    notice(`Memory ${memory ? 'corrected' : 'saved'}. ${result.invalidated_count || 0} dependent records retired. Graph update queued.`);
  } catch (error) {
    if (session !== state.session) return;
    if (committed) { clearDerivedViews(); notice('Memory saved. Refresh to reload the current records.', true); }
    else throw error;
  } finally { if (session === state.session) { state.busy = false; $('save-edit').disabled = false; controls(); } }
}));
function openDelete(memory) {
  if (busy()) return;
  state.deleting = memory; $('detail-dialog').close();
  $('delete-title').textContent = memory ? 'Forget this memory?' : 'Forget all memories in this scope?';
  $('delete-dialog').showModal(); $('cancel-delete').focus();
}
$('forget-scope').addEventListener('click', () => openDelete(null));
$('cancel-delete').addEventListener('click', () => $('delete-dialog').close());
$('delete-form').addEventListener('submit', guard(async () => {
  if (busy()) return;
  const session = state.session, memory = state.deleting;
  state.busy = true; invalidateReads(); controls(); $('confirm-delete').disabled = true;
  let committed = false;
  try {
    await api(memory ? `/v1/memories/${encodeURIComponent(memory.id)}?${scopeQuery({ expected_revision: memory.revision })}` : `/v1/memories?${scopeQuery({ expected_scope_revision: state.revision })}`, { method: 'DELETE' });
    committed = true; $('delete-dialog').close(); await afterMutation(session);
    notice('Live content removed. Dependent records retired; graph cleanup queued.');
  } catch (error) {
    if (session !== state.session) return;
    if (committed) { clearDerivedViews(); notice('Memory removed. Refresh to reload the remaining records.', true); }
    else throw error;
  } finally { if (session === state.session) { state.busy = false; $('confirm-delete').disabled = false; controls(); } }
}));
const seeds = [
  ['Cedar support: connector fails with CDR-409 after workspace migration.', 'For CDR-409 after migration, refresh the connector cursor and run a dry-run reconciliation before resuming sync.'],
  ['Cedar support: what is the safe reconciliation sequence for CDR-409?', 'Verify workspace ownership, refresh the connector cursor, and run dry-run reconciliation. Do not reset or delete the workspace.'],
  ['Cedar support: when should a persistent CDR-409 connector failure escalate?', 'After two failed reconciliation attempts, escalate to the integration team with the workspace ID, connector version, and dry-run logs.'],
  ['Cedar support: what changed after the customer migrated workspaces?', 'The migrated workspace kept an outdated connector cursor. Preserve existing records and verify ownership before retrying reconciliation.'],
];
$('seed').addEventListener('click', guard(async () => {
  if (busy()) return;
  const session = state.session;
  state.busy = true; invalidateReads(); controls(); let saved = 0;
  try {
    for (const [prompt, response] of seeds) {
      const result = await api('/v1/memories', { method: 'POST', body: { ...state.scope, prompt, response, expected_scope_revision: state.revision } });
      state.revision = result.scope_revision; saved++;
    }
    await afterMutation(session);
    $('prompt').value = 'The connector still fails after migration. What should I try before escalating?';
    $('compare-prompt').value = 'What is the correct escalation procedure for a persistent CDR-409 error?';
    notice('Four fictional support memories saved. Graph relationships appear after indexing. Open the playground to ask your real configured model.');
  } catch (error) {
    if (session !== state.session) return;
    clearDerivedViews(); await refresh().catch(() => {});
    throw new Error(`${saved} of 4 memories saved. ${error.message}`);
  } finally { if (session === state.session) { state.busy = false; controls(); } }
}));
$('suggestion').addEventListener('click', () => { $('prompt').value = 'The connector still fails after migration. What should I try before escalating?'; $('prompt').focus(); });
$('clear-chat').addEventListener('click', () => { if (!state.stream) { resetConversation(); $('prompt').value = ''; } });
$('delivery').addEventListener('change', () => { $('delivery-label').textContent = $('delivery').value === 'stream' ? 'LIVE STREAM' : 'COMPLETE RESPONSE'; });
function message(role, text) {
  const node = el('article', `message ${role}`), body = el('div', 'body', text);
  node.append(el('div', 'speaker', role === 'user' ? 'You' : 'Assistant'), body); $('conversation').append(node);
  return { node, body };
}
function scrollChat() { $('conversation').scrollTop = $('conversation').scrollHeight; }
function responseMetadata(meta) {
  return `${meta.model || 'Exact cache'} · ${number(meta.latency_ms)} ms · ${meta.usage_available === false ? 'usage unavailable' : `${number(meta.tokens_in + meta.tokens_out)} reported tokens`} · ${meta.cache_hit ? 'cache hit' : 'generated'} · ${meta.memory_write === 'queued' ? 'memory queued' : 'not retained'}`;
}
function appendAnswerData(node, meta) {
  node.append(el('div', 'response-meta', responseMetadata(meta)), sourceChips(meta.context_used || []));
  if (meta.finish_reason === 'length') node.append(el('div', 'response-meta', 'Output token limit reached. Increase the limit for a longer answer.'));
  if (meta.finish_reason === 'content_filter') node.append(el('div', 'response-meta', 'The provider filtered part of this response.'));
  if (meta.degraded?.length) node.append(el('div', 'response-meta', `Reduced service: ${meta.degraded.join(', ')}`));
  if (Object.keys(meta.timings_ms || {}).length) {
    const details = el('details', 'response-meta'), timings = el('div', 'timing-list');
    details.append(el('summary', '', 'Inspect request timing'));
    for (const [stage, elapsed] of Object.entries(meta.timings_ms)) timings.append(el('span', '', `${stage}: ${number(elapsed)} ms`));
    details.append(timings); node.append(details);
  }
}
async function readStream(response, onEvent) {
  const reader = response.body.getReader(), decoder = new TextDecoder();
  let buffer = '', event = '', lines = [], eventBytes = 0;
  const dispatch = () => { if (lines.length) onEvent(event || 'message', JSON.parse(lines.join('\n'))); event = ''; lines = []; eventBytes = 0; };
  const line = (value) => {
    eventBytes += value.length;
    if (eventBytes > 2_000_000) throw new Error('Streaming event exceeds the console limit.');
    if (value === '') dispatch();
    else if (value.startsWith('event:')) event = value.slice(6).trim();
    else if (value.startsWith('data:')) lines.push(value.slice(5).replace(/^ /, ''));
  };
  try {
    while (true) {
      const { value, done } = await reader.read(); buffer += decoder.decode(value, { stream: !done });
      if (buffer.length > 2_000_000) throw new Error('Streaming event exceeds the console limit.');
      let match;
      while ((match = /\r\n|\n|\r(?!$)/.exec(buffer))) { const text = buffer.slice(0, match.index); buffer = buffer.slice(match.index + match[0].length); line(text); }
      if (done) break;
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
$('chat-form').addEventListener('submit', guard(async () => {
  if (!state.token || busy()) return;
  const prompt = $('prompt').value; if (!prompt.trim()) return;
  const controller = new AbortController(), session = state.session;
  state.stream = controller; controls(); $('stop').hidden = false; $('stream-state').textContent = 'Retrieving context and generating…'; notice('');
  $('conversation').querySelector('.welcome')?.remove();
  message('user', prompt); const reply = message('assistant', '');
  reply.node.classList.add('pending'); let final = false; $('prompt').value = '';
  const body = { ...state.scope, prompt, retrieval_mode: $('retrieval').value, store: $('remember').checked, use_cache: $('cache').checked, max_tokens: Number($('max-tokens').value) || 256,
    ...($('model').value ? { model: $('model').value } : {}), ...($('system-prompt').value.trim() ? { system_prompt: $('system-prompt').value } : {}), ...($('history').checked ? { history: state.history.slice(-8) } : {}) };
  const complete = (data) => {
    if (session !== state.session || controller.signal.aborted) return;
    if (data.meta?.durable !== true || typeof data.response !== 'string') throw new Error('The gateway did not confirm durable completion.');
    final = true; reply.body.textContent = data.response; appendAnswerData(reply.node, data.meta);
    state.history.push({ role: 'user', content: prompt }, { role: 'assistant', content: data.response });
    state.history = state.history.slice(-8);
  };
  try {
    if ($('delivery').value === 'complete') {
      complete(await api('/v1/chat', { method: 'POST', body, signal: controller.signal }));
    } else {
      const response = await fetch('/v1/chat/stream', { method: 'POST', credentials: 'omit', redirect: 'error', cache: 'no-store', signal: controller.signal,
        headers: { Authorization: `Bearer ${state.token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if (!response.ok) throw new Error(`Streaming unavailable (${response.status}). Select Complete response if your provider does not support streaming.`);
      await readStream(response, (event, data) => {
        if (session !== state.session || controller.signal.aborted) return;
        if (event === 'delta' && !final) { reply.body.textContent += data.delta; $('stream-state').textContent = 'Writing answer…'; }
        if (event === 'error') throw new Error(data.error?.message || 'The stream ended before durable completion.');
        if (event === 'final' && !final) complete(data);
        scrollChat();
      });
    }
    if (session !== state.session) return;
    if (!final) throw new Error('Connection ended before completion. This partial answer is not confirmed as saved.');
    await Promise.all([refresh(), refreshOverview()]);
  } catch (error) {
    if (session !== state.session) return;
    if (final) { notice('Answer completed and committed. Memory refresh failed; use Refresh to update the timeline.', true); return; }
    reply.node.append(el('div', 'response-meta', error.name === 'AbortError' ? 'Stopped. Partial content is not a completed answer; a request already committed may still appear in memory.' : 'Incomplete answer. No durable completion received.'));
    if (error.name !== 'AbortError') notice(error.message, true);
  } finally {
    if (session === state.session) { reply.node.classList.remove('pending'); state.stream = null; $('stop').hidden = true; $('stream-state').textContent = final ? 'Answer complete' : 'Ready when you are'; controls(); scrollChat(); }
  }
}));
$('stop').addEventListener('click', () => state.stream?.abort());
async function refreshOverview() {
  const read = ticket('overview');
  const data = await readApi(read, `/v1/console/overview?${scopeQuery()}`);
  if (!current(read)) return;
  state.overview = data;
  const totals = data.totals, memory = data.memory;
  $('metric-requests').textContent = number(totals.calls);
  $('metric-request-note').textContent = `${number(totals.cache_hits)} cached · ${number(totals.calls - totals.cache_hits)} generated`;
  $('metric-memories').textContent = number(memory.active_count); $('nav-count').textContent = number(memory.active_count);
  $('metric-memory-note').textContent = `${number(memory.curated_active_count)} curated · ${number(memory.retired_count)} retired`;
  $('metric-latency').textContent = !totals.calls || totals.p95_latency_ms === null ? '—' : `${number(totals.p95_latency_ms)} ms`;
  $('metric-latency-note').textContent = totals.calls ? `Mean ${number(totals.mean_latency_ms)} ms · recorded completions` : 'No completed requests in this window';
  $('metric-cache').textContent = totals.calls ? `${number(100 * totals.cache_hits / totals.calls)}%` : '—';
  $('metric-cache-note').textContent = totals.calls ? `${number(totals.cache_hits)} of ${number(totals.calls)} recorded requests` : 'No completed requests in this window';
  $('accounting-cost').textContent = `Known price subtotal: $${Number(totals.known_cost || 0).toFixed(5)} · ${number(totals.unpriced_calls)} unpriced requests`;
  $('accounting-tokens').textContent = `Reported tokens: ${number(totals.tokens_in + totals.tokens_out)}`;
  $('welcome-banner').hidden = true;
  drawActivity(data.daily);
  $('activity-summary').textContent = `${number(totals.calls)} recorded requests`;
  $('recent-requests').replaceChildren(); $('requests-empty').hidden = data.recent_requests.length > 0;
  const session = state.session;
  for (const record of data.recent_requests) {
    const row = el('tr'), identity = el('td');
    const id = el(record.retained_content ? 'button' : 'span', '', record.id.slice(0, 8));
    if (record.retained_content) id.addEventListener('click', guard(() => session === state.session ? inspect(record.id) : undefined));
    identity.append(id, el('small', '', date(record.created_at)));
    const model = el('td', '', record.model || '—'); model.append(el('small', '', record.provider || 'Cache / accounting'));
    const route = el('td'); route.append(el('span', `tag${record.cache_hit ? '' : ' muted'}`, record.cache_hit ? 'Cached' : record.fallback_used ? 'Fallback' : 'Generated'));
    row.append(identity, model, el('td', '', `${number(record.latency_ms)} ms`), el('td', '', number(record.tokens_in + record.tokens_out)), route, el('td', '', record.retained_content ? record.memory_status : 'Not retained'));
    $('recent-requests').append(row);
  }
  $('health-services').replaceChildren();
  const labels = { postgres: 'PostgreSQL / vectors', redis: 'Redis / rate limits', graph: 'Neo4j / relationships', embedding: 'Embedding worker' };
  let healthy = true;
  for (const [key, label] of Object.entries(labels)) {
    const okay = data.health[key] === true; healthy = healthy && okay;
    const row = el('div', `health-row${okay ? '' : ' unavailable'}`);
    row.append(el('span', '', label), el('span', '', okay ? '● HEALTHY' : '● UNAVAILABLE')); $('health-services').append(row);
  }
  $('health-label').textContent = healthy ? 'Operational' : 'Degraded'; $('health-label').className = `status-badge${healthy ? '' : ' error'}`;
  $('system-status').textContent = healthy ? 'Gateway connected' : 'Connected · degraded service';
  const generation = data.runtime.generation;
  $('active-model').textContent = generation.simple_model;
  $('model-note').textContent = `${generation.backend} · ${generation.max_concurrency} configured provider slots`;
  $('chat-model-label').textContent = generation.simple_model;
  $('chat-backend-label').textContent = generation.backend === 'synthetic-demo' ? 'Synthetic demo · fixture replies, not model generation' : `${generation.backend} · configured generation backend`;
  for (const id of ['model', 'compare-model']) {
    const chosen = $(id).value, automatic = el('option', '', 'Automatic routing'); automatic.value = '';
    $(id).replaceChildren(automatic);
    for (const model of new Set([generation.simple_model, generation.complex_model])) { const option = el('option', '', model); option.value = model; $(id).append(option); }
    if ([generation.simple_model, generation.complex_model].includes(chosen)) $(id).value = chosen;
  }
  $('api-docs').hidden = !data.runtime.docs_available;
  renderRuntime(data.runtime);
  $('last-updated').textContent = `Updated ${new Date().toLocaleTimeString()} · ${state.scope.user_id} / ${state.scope.feature_tag}`;
}
function drawActivity(days) {
  const chart = svg('svg', { viewBox: '0 0 640 190', role: 'img', 'aria-label': 'Recorded daily requests, generated and cached' });
  const maximum = Math.max(1, ...days.map(day => day.calls));
  for (let step = 0; step <= 3; step++) {
    const y = 15 + step * 45;
    chart.append(svg('line', { x1: 33, x2: 627, y1: y, y2: y, class: 'chart-grid' }));
    chart.append(svg('text', { x: 25, y: y + 3, 'text-anchor': 'end', class: 'chart-label' }, number(maximum * (3 - step) / 3)));
  }
  days.forEach((day, index) => {
    const x = 51 + index * 82, cached = day.cache_hits / maximum * 135, generated = (day.calls - day.cache_hits) / maximum * 135;
    const group = svg('g');
    group.append(svg('title', {}, `${day.day}: ${day.calls} requests, ${day.cache_hits} cache hits`));
    group.append(svg('rect', { x, y: 150 - generated, width: 41, height: generated, rx: 2, class: 'chart-bar' }));
    group.append(svg('rect', { x, y: 150 - generated - cached, width: 41, height: cached, rx: 2, class: 'chart-cached' }));
    group.append(svg('text', { x: x + 20, y: 178, 'text-anchor': 'middle', class: 'chart-label' }, day.day.slice(5)));
    chart.append(group);
  });
  $('activity-chart').className = ''; $('activity-chart').replaceChildren(chart);
}
function renderRuntime(runtime) {
  $('runtime-cards').replaceChildren();
  const gen = runtime.generation, embedding = runtime.embedding, request = runtime.requests;
  const groups = [
    ['Generation', [['Backend', gen.backend], ['Primary route', gen.simple_model], ['Complex route', gen.complex_model], ['Provider concurrency limit', gen.max_concurrency], ['Default token budget', gen.default_max_tokens]]],
    ['Embedding engine', [['Backend', embedding.backend], ['Model', embedding.model], ['Configured device', embedding.device], ['Dimensions', embedding.dimensions], ['Maximum batch size', embedding.batch_size], ['Workers / queue limit', `${embedding.workers} / ${embedding.queue_size}`]]],
    ['Request controls', [['Concurrent request limit', request.max_concurrent], ['Deadline', `${request.timeout_seconds} seconds`], ['Scope', `${state.scope.user_id} / ${state.scope.feature_tag}`], ['Authentication', 'Tenant bearer token'], ['Memory projection', 'PostgreSQL → durable outbox → Neo4j']]],
  ];
  for (const [name, fields] of groups) {
    const card = el('article', 'panel runtime-card'); card.append(el('h2', '', name));
    const list = el('dl');
    for (const [label, value] of fields) { const row = el('div', 'runtime-row'); row.append(el('dt', '', label), el('dd', '', String(value ?? 'Not configured'))); list.append(row); }
    card.append(list); $('runtime-cards').append(card);
  }
  updateCode();
}
function updateCode() {
  const origin = window.location?.origin || 'http://127.0.0.1:8001';
  const body = { user_id: state.scope?.user_id || 'your-user', feature_tag: state.scope?.feature_tag || 'your-feature', prompt: 'What should we try next?', retrieval_mode: 'graph', store: false };
  $('integration-code').textContent = `POST ${origin}/v1/chat\nAuthorization: Bearer <your-gateway-token>\nContent-Type: application/json\n\n${JSON.stringify(body, null, 2)}`;
}
$('copy-code').addEventListener('click', guard(async () => {
  if (!navigator.clipboard) throw new Error('Clipboard is unavailable. Select the request text to copy it.');
  await navigator.clipboard.writeText($('integration-code').textContent); notice('Request copied. Replace the placeholder with your server-side credential.');
}));
async function refreshGraph() {
  const read = ticket('graph');
  const data = await readApi(read, `/v1/console/graph?${scopeQuery({ limit: 60 })}`);
  if (!current(read)) return;
  state.graph = data; state.revision = data.scope_revision;
  $('graph-count').textContent = `${data.nodes.length} NODES · ${data.edges.length} EDGES`;
  $('graph-note').textContent = `${data.degraded ? 'Graph projection unavailable; showing live nodes without relationships. ' : 'Actual graph relationships among live scoped memories. '}${data.truncated.nodes || data.truncated.edges ? 'Bounded view; some nodes or edges are omitted. ' : ''}Revision ${data.scope_revision}.`;
  $('graph-empty').hidden = data.nodes.length > 0;
  drawGraph(data);
}
function layoutGraph(nodes, edges) {
  const positions = new Map(nodes.map((node, index) => [node.id, { x: 500 + 200 * Math.cos(index / Math.max(1, nodes.length) * Math.PI * 2), y: 280 + 190 * Math.sin(index / Math.max(1, nodes.length) * Math.PI * 2) }]));
  for (let step = 0; step < 100; step++) {
    const forces = new Map(nodes.map(node => [node.id, { x: 0, y: 0 }]));
    for (let i = 0; i < nodes.length; i++) for (let j = i + 1; j < nodes.length; j++) {
      const a = positions.get(nodes[i].id), b = positions.get(nodes[j].id), dx = a.x - b.x || .1, dy = a.y - b.y || .1;
      const d2 = Math.max(100, dx * dx + dy * dy), force = 4500 / d2;
      const x = dx / Math.sqrt(d2) * force, y = dy / Math.sqrt(d2) * force;
      forces.get(nodes[i].id).x += x; forces.get(nodes[i].id).y += y; forces.get(nodes[j].id).x -= x; forces.get(nodes[j].id).y -= y;
    }
    for (const edge of edges) {
      const a = positions.get(edge.source), b = positions.get(edge.target); if (!a || !b) continue;
      const dx = b.x - a.x, dy = b.y - a.y, d = Math.max(1, Math.hypot(dx, dy)), force = (d - 180) * .012;
      forces.get(edge.source).x += dx / d * force; forces.get(edge.source).y += dy / d * force;
      forces.get(edge.target).x -= dx / d * force; forces.get(edge.target).y -= dy / d * force;
    }
    for (const node of nodes) {
      const p = positions.get(node.id), f = forces.get(node.id);
      p.x = Math.max(100, Math.min(900, p.x + f.x + (500 - p.x) * .002));
      p.y = Math.max(75, Math.min(460, p.y + f.y + (265 - p.y) * .002));
    }
  }
  if (nodes.length === 1) positions.set(nodes[0].id, { x: 500, y: 255 });
  return positions;
}
function drawGraph(data) {
  state.graphPositions = layoutGraph(data.nodes, data.edges); $('memory-graph').replaceChildren(); $('graph-node-list').replaceChildren();
  const session = state.session;
  for (const edge of data.edges) {
    const a = state.graphPositions.get(edge.source), b = state.graphPositions.get(edge.target); if (!a || !b) continue;
    const line = svg('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, class: `graph-edge${edge.type === 'INFORMED_BY' ? ' informed' : ''}` });
    line.append(svg('title', {}, `${edge.type}${edge.similarity == null ? '' : ` · similarity ${edge.similarity.toFixed(3)}`}`)); $('memory-graph').append(line);
  }
  for (const node of data.nodes) {
    const p = state.graphPositions.get(node.id), curated = node.memory_kind === 'curated';
    const group = svg('g', { class: `graph-node${curated ? ' curated' : ''}`, transform: `translate(${p.x} ${p.y})`, tabindex: 0, role: 'button', 'aria-label': `Inspect memory: ${node.prompt_preview}`, 'data-node-id': node.id });
    group.append(svg('circle', { r: 30, class: 'halo' }), svg('circle', { r: 17, class: 'core' }));
    group.append(svg('text', { y: 49, 'text-anchor': 'middle' }, node.prompt_preview.length > 42 ? `${node.prompt_preview.slice(0, 39)}…` : node.prompt_preview));
    group.append(svg('text', { y: 66, 'text-anchor': 'middle', class: 'node-id' }, `${node.id.slice(0, 8)} · ${curated ? 'curated' : 'answer'}`));
    const select = () => session === state.session ? inspect(node.id) : undefined;
    group.addEventListener('click', guard(select));
    group.addEventListener('keydown', (event) => { if (['Enter', ' '].includes(event.key)) { event.preventDefault(); Promise.resolve(select()).catch(fail); } });
    $('memory-graph').append(group);
    const accessible = el('button', '', node.prompt_preview); accessible.addEventListener('click', guard(select)); $('graph-node-list').append(accessible);
  }
  resetGraphZoom(); highlightGraph();
}
function highlightGraph() {
  const query = $('graph-search').value.toLowerCase();
  for (const node of document.querySelectorAll('[data-node-id]')) {
    const data = state.graph?.nodes.find(item => item.id === node.dataset.nodeId);
    node.classList.toggle('selected', state.selected?.id === node.dataset.nodeId);
    node.classList.toggle('faded', !!query && !`${data?.prompt_preview} ${data?.id}`.toLowerCase().includes(query));
  }
}
$('graph-search').addEventListener('input', highlightGraph);
function graphBox() { $('memory-graph').setAttribute('viewBox', state.graphBox.join(' ')); }
function resetGraphZoom() { state.graphBox = [0, 0, 1000, 570]; graphBox(); }
function zoomGraph(factor) {
  const [x, y, width, height] = state.graphBox, next = Math.max(300, Math.min(2000, width * factor)), scale = next / width;
  state.graphBox = [x + width * (1 - scale) / 2, y + height * (1 - scale) / 2, next, height * scale]; graphBox();
}
$('zoom-in').addEventListener('click', () => zoomGraph(.8)); $('zoom-out').addEventListener('click', () => zoomGraph(1.25)); $('zoom-reset').addEventListener('click', resetGraphZoom);
let graphDrag = null;
$('memory-graph').addEventListener('pointerdown', (event) => { if (event.target.closest('[data-node-id]')) return; graphDrag = { x: event.clientX, y: event.clientY, box: [...state.graphBox] }; $('memory-graph').setPointerCapture(event.pointerId); });
$('memory-graph').addEventListener('pointermove', (event) => { if (!graphDrag) return; const bounds = $('memory-graph').getBoundingClientRect(); state.graphBox = [graphDrag.box[0] - (event.clientX - graphDrag.x) / bounds.width * graphDrag.box[2], graphDrag.box[1] - (event.clientY - graphDrag.y) / bounds.height * graphDrag.box[3], graphDrag.box[2], graphDrag.box[3]]; graphBox(); });
$('memory-graph').addEventListener('pointerup', () => { graphDrag = null; }); $('memory-graph').addEventListener('pointercancel', () => { graphDrag = null; });
const comparisonModes = [['none', 'No memory'], ['vector', 'Vector retrieval'], ['graph', 'Graph retrieval']];
$('compare-form').addEventListener('submit', guard(async () => {
  if (!state.token || busy() || !$('compare-prompt').value.trim()) return;
  const controller = new AbortController(), session = state.session, scope = { ...state.scope };
  const prompt = $('compare-prompt').value, model = $('compare-model').value, maxTokens = Number($('compare-tokens').value) || 256;
  state.comparison = controller; state.comparisonReport = null; controls(); $('stop-comparison').hidden = false; $('comparison-results').replaceChildren();
  const report = { created_at: new Date().toISOString(), scope, prompt, requested_model: model || 'automatic', max_tokens: maxTokens, caching: false, store: false, results: [], complete: false };
  const cards = new Map();
  for (const [mode, label] of comparisonModes) {
    const card = el('article', 'panel comparison-card'), heading = el('div', 'panel-heading'), status = el('span', 'tag muted', 'WAITING'), answer = el('div', 'comparison-answer', 'Waiting to run…'), details = el('div', 'comparison-data');
    heading.append(el('h2', '', label), status); card.append(heading, answer, details); cards.set(mode, { status, answer, details }); $('comparison-results').append(card);
  }
  try {
    for (const [mode, label] of comparisonModes) {
      if (controller.signal.aborted) break;
      const card = cards.get(mode); card.status.textContent = 'RUNNING'; card.answer.textContent = 'Retrieving context and generating…'; $('comparison-status').textContent = `Running ${label.toLowerCase()} through the gateway…`;
      try {
        const result = await api('/v1/chat', { method: 'POST', signal: controller.signal, body: { ...scope, prompt, retrieval_mode: mode, max_tokens: maxTokens, store: false, use_cache: false, ...(model ? { model } : {}) } });
        if (session !== state.session || controller.signal.aborted) break;
        if (result.meta?.durable !== true || typeof result.response !== 'string') throw new Error('No durable completion returned.');
        card.answer.textContent = result.response; card.status.textContent = 'COMPLETE'; card.status.className = 'tag'; appendAnswerData(card.details, result.meta);
        report.results.push({ mode, ...result });
      } catch (error) {
        if (session !== state.session) return;
        card.status.textContent = error.name === 'AbortError' ? 'STOPPED' : 'FAILED'; card.answer.textContent = error.name === 'AbortError' ? 'Stopped before confirmed completion.' : error.message;
        report.results.push({ mode, error: error.name === 'AbortError' ? 'cancelled' : error.message });
        if (controller.signal.aborted) break;
      }
    }
    if (session !== state.session) return;
    for (const [mode] of comparisonModes) if (!report.results.some(item => item.mode === mode)) { cards.get(mode).status.textContent = 'NOT RUN'; cards.get(mode).answer.textContent = 'Comparison stopped before this mode ran.'; }
    const successful = report.results.filter(item => item.meta);
    report.complete = successful.length === 3;
    report.model_matched = new Set(successful.map(item => `${item.meta.provider}/${item.meta.model}`)).size === 1 && report.complete;
    report.memory_revision_matched = new Set(successful.map(item => item.meta.memory_epoch)).size === 1 && report.complete;
    state.comparisonReport = report;
    $('comparison-status').textContent = !report.complete ? `${successful.length} of 3 modes completed. Failed or stopped modes are shown explicitly.` : !report.model_matched || !report.memory_revision_matched ? 'All modes returned, but models or memory revisions differ. This is not a controlled retrieval comparison.' : 'Three modes completed using the same actual model and memory revision. Inspect answers and sources; no quality score is inferred.';
  } finally { if (session === state.session) { state.comparison = null; $('stop-comparison').hidden = true; controls(); } }
}));
$('stop-comparison').addEventListener('click', () => state.comparison?.abort());
$('export-comparison').addEventListener('click', () => {
  if (!state.comparisonReport) return;
  $('export-json').value = JSON.stringify(state.comparisonReport, null, 2); $('export-dialog').showModal();
});
$('close-export').addEventListener('click', () => $('export-dialog').close());
$('copy-export').addEventListener('click', guard(async () => {
  if (!navigator.clipboard) throw new Error('Select the JSON text to copy it. Clipboard access is unavailable.');
  await navigator.clipboard.writeText($('export-json').value); notice('Comparison JSON copied.');
}));
window.addEventListener('pagehide', clear);
updateCode(); controls();
