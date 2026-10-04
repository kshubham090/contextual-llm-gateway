const $ = (id) => document.getElementById(id);
const state = { token: '', scope: null, revision: 0, cursor: null, items: [], selected: null,
  session: 0, requests: new Set(), stream: null, editing: null, deleting: null, busy: false };
const el = (tag, cls, text) => { const node = document.createElement(tag); if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text; return node; };
const scopeQuery = (extra = {}) => new URLSearchParams({ ...state.scope, ...extra }).toString();
function notice(text, error = false) { $('notice').textContent = text; $('notice').className = `notice${error ? ' error' : ''}`; $('notice').hidden = !text; }
function controls() { const enabled = !!state.token && !state.busy;
  for (const id of ['refresh', 'new-memory', 'seed', 'forget-scope']) $(id).disabled = !enabled || !!state.stream;
  $('send').disabled = !enabled || !!state.stream;
}
async function api(path, { method = 'GET', body, signal } = {}) {
  const session = state.session, controller = new AbortController();
  state.requests.add(controller);
  const abort = () => controller.abort(); signal?.addEventListener('abort', abort, { once: true });
  try {
    if (signal?.aborted) controller.abort();
    const response = await fetch(path, { method, headers: { Authorization: `Bearer ${state.token}`,
      ...(body ? { 'Content-Type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined,
    signal: controller.signal, cache: 'no-store', credentials: 'omit', redirect: 'error' });
    if (!response.ok) {
      const messages = { 401: 'Token rejected. Disconnect and check your credentials.',
        404: 'This memory is no longer available in the selected scope.',
        409: 'The memory changed while you were working. Refresh and try again.',
        422: 'Check the scope, text, and revision fields.', 429: 'Request limit reached. Try again shortly.' };
      throw new Error(messages[response.status] || `Request failed (${response.status}). Check gateway health.`);
    }
    const data = await response.json();
    if (session !== state.session) throw new DOMException('Workspace changed', 'AbortError');
    return data;
  } finally { state.requests.delete(controller); signal?.removeEventListener('abort', abort); }
}
function resetConversation() { $('conversation').replaceChildren(el('p', 'empty', 'Memory changed. Start a fresh conversation to use the current record.')); }
function fail(error) { if (error.name !== 'AbortError') notice(error.message, true); }
function guard(fn) { return (event) => { event?.preventDefault(); Promise.resolve(fn(event)).catch(fail); }; }
function clear() {
  state.session++; state.stream?.abort(); state.stream = null;
  for (const request of state.requests) request.abort(); state.requests.clear();
  state.token = ''; state.scope = null; state.items = []; state.selected = null; state.cursor = null;
  state.busy = false; state.editing = null; state.deleting = null;
  for (const id of ['prompt', 'edit-prompt', 'edit-response']) $(id).value = '';
  $('token').value = ''; $('memory-list').replaceChildren(); $('detail-content').replaceChildren();
  $('detail-content').hidden = true; $('detail-empty').hidden = false; $('empty').hidden = false;
  $('empty').textContent = 'Connect a workspace to explore its history.';
  $('conversation').replaceChildren(); $('count').textContent = '0'; $('more').hidden = true;
  $('connection-status').textContent = 'Connect to your workspace'; document.body.classList.remove('connected');
  $('scope-revision').textContent = 'No workspace selected'; $('stream-state').textContent = 'Ready when you are';
  $('connect').hidden = false; $('disconnect').hidden = true; $('stop').hidden = true;
  for (const id of ['token', 'user', 'feature']) $(id).disabled = false;
  $('edit-dialog').close(); $('delete-dialog').close(); controls(); notice('');
}
$('connect-form').addEventListener('submit', guard(async () => {
  const token = $('token').value.trim(), user = $('user').value, feature = $('feature').value;
  clear(); state.token = token; state.scope = { user_id: user, feature_tag: feature };
  try { await refresh(); } catch (error) { clear(); throw error; }
  $('connection-status').textContent = `${user} / ${feature}`; document.body.classList.add('connected');
  $('connect').hidden = true; $('disconnect').hidden = false;
  for (const id of ['token', 'user', 'feature']) $(id).disabled = true;
  controls(); notice('Connected. This view only shows the selected user and feature.');
}));
$('disconnect').addEventListener('click', clear);
function date(value) { return value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : 'Just now'; }
function renderList() {
  $('count').textContent = String(state.items.length); $('memory-list').replaceChildren();
  $('empty').hidden = state.items.length > 0; $('empty').textContent = 'No memories yet. Add a verified fact or load the fictional support example.';
  for (const memory of state.items) {
    const li = el('li'), button = el('button', `memory-item${state.selected?.id === memory.id ? ' selected' : ''}`);
    button.type = 'button'; button.append(el('small', '', date(memory.created_at)), el('span', 'status', memory.status || 'active'),
      el('p', '', memory.prompt || 'Content removed'), el('small', '', `${memory.id.slice(0, 8)} · revision ${memory.revision}`));
    button.addEventListener('click', guard(() => inspect(memory.id))); li.append(button); $('memory-list').append(li);
  }
  $('more').hidden = !state.cursor; $('scope-revision').textContent = `Scope revision ${state.revision}`;
}
async function refresh(append = false) {
  const data = await api(`/v1/memories?${scopeQuery({ limit: 30, ...(append && state.cursor ? { cursor: state.cursor } : {}) })}`);
  state.revision = data.scope_revision; state.cursor = data.next_cursor;
  const combined = append ? [...state.items, ...data.items] : data.items;
  state.items = [...new Map(combined.map(item => [item.id, item])).values()]; renderList();
}
$('refresh').addEventListener('click', guard(() => refresh()));
$('more').addEventListener('click', guard(() => refresh(true)));
async function inspect(id) {
  const result = await api(`/v1/memories/${encodeURIComponent(id)}?${scopeQuery()}`);
  const memory = result.memory || result; state.selected = memory;
  if (result.scope_revision !== undefined) state.revision = result.scope_revision;
  renderList(); $('detail-empty').hidden = true; $('detail-content').hidden = false;
  const body = el('div', 'detail-body'); body.append(el('p', 'identifier', `${memory.id}\n${date(memory.created_at)} · ${memory.status} · revision ${memory.revision}`));
  for (const [title, value] of [['Situation', memory.prompt || 'Live content removed'], ['Recorded answer', memory.response || 'No retained answer']]) {
    const section = el('section'); section.append(el('label', '', title), el('pre', '', value)); body.append(section);
  }
  const sources = el('section'); sources.append(el('label', '', 'Context supplied to this memory'));
  const chips = el('div', 'source-chips');
  for (const source of memory.source_ids || []) { const button = el('button', '', source.slice(0, 8));
    button.addEventListener('click', guard(() => inspect(source))); chips.append(button); }
  sources.append(chips); if (!(memory.source_ids || []).length) sources.append(el('p', 'identifier', 'No source memories recorded.'));
  if (memory.supersedes_id) sources.append(el('p', 'identifier', `Replaces ${memory.supersedes_id}`));
  body.append(sources);
  const explanation = el('section'); explanation.append(el('label', '', 'About this record'), el('p', 'identifier',
    memory.retrieval?.source || 'Connections record retrieved context and similarity. They are not proof that an answer is correct.'));
  body.append(explanation);
  if (memory.status === 'active') {
    const actions = el('div', 'actions'), correct = el('button', 'quiet', 'Correct'), remove = el('button', 'quiet', 'Forget');
    correct.addEventListener('click', () => openEdit(memory)); remove.addEventListener('click', () => openDelete(memory));
    actions.append(correct, remove); body.append(actions);
  }
  $('detail-content').replaceChildren(body);
}
function openEdit(memory = null) {
  if (state.busy || state.stream) return;
  state.editing = memory; $('edit-title').textContent = memory ? 'Correct this memory' : 'Add a memory';
  $('edit-help').textContent = memory ? 'The replacement becomes a new record. Old content and dependent answers are retired, and this scope’s response cache is invalidated.' : 'Save a factual exchange without asking the model to generate one.';
  $('edit-prompt').value = memory?.prompt || ''; $('edit-response').value = memory?.response || '';
  $('edit-dialog').showModal(); $('edit-prompt').focus();
}
$('new-memory').addEventListener('click', () => openEdit());
$('cancel-edit').addEventListener('click', () => $('edit-dialog').close());
$('edit-form').addEventListener('submit', guard(async () => {
  if (state.busy) return; state.busy = true; controls(); $('save-edit').disabled = true;
  try {
    const memory = state.editing;
    const result = await api(memory ? `/v1/memories/${encodeURIComponent(memory.id)}` : '/v1/memories', {
      method: memory ? 'PATCH' : 'POST', body: { ...state.scope, prompt: $('edit-prompt').value, response: $('edit-response').value,
        ...(memory ? { expected_revision: memory.revision } : { expected_scope_revision: state.revision }) } });
    $('edit-dialog').close(); resetConversation(); await refresh(); await inspect(result.memory.id);
    notice(`Memory ${memory ? 'corrected' : 'saved'}. ${result.invalidated_count || 0} dependent records retired. Graph update queued.`);
  } finally { state.busy = false; $('save-edit').disabled = false; controls(); }
}));
function openDelete(memory) { if (state.busy || state.stream) return; state.deleting = memory;
  $('delete-title').textContent = memory ? 'Forget this memory?' : 'Forget all memories in this scope?';
  $('delete-dialog').showModal(); $('cancel-delete').focus();
}
$('forget-scope').addEventListener('click', () => openDelete(null));
$('cancel-delete').addEventListener('click', () => $('delete-dialog').close());
$('delete-form').addEventListener('submit', guard(async () => {
  if (state.busy) return; state.busy = true; controls(); $('confirm-delete').disabled = true;
  try {
    const memory = state.deleting;
    await api(memory ? `/v1/memories/${encodeURIComponent(memory.id)}?${scopeQuery({ expected_revision: memory.revision })}` : `/v1/memories?${scopeQuery({ expected_scope_revision: state.revision })}`, { method: 'DELETE' });
    $('delete-dialog').close(); resetConversation(); state.selected = null; $('detail-content').replaceChildren(); $('detail-content').hidden = true;
    $('detail-empty').hidden = false; await refresh(); notice('Live memory removed. Dependent records retired; graph cleanup queued.');
  } finally { state.busy = false; $('confirm-delete').disabled = false; controls(); }
}));
const seeds = [
  ['Cedar support: connector fails with CDR-409 after workspace migration.', 'For CDR-409 after migration, refresh the connector cursor and run a dry-run reconciliation before resuming sync.'],
  ['Cedar support: what is the safe reconciliation sequence for CDR-409?', 'Verify workspace ownership, refresh the connector cursor, and run dry-run reconciliation. Do not reset or delete the workspace.'],
  ['Cedar support: when should a persistent CDR-409 connector failure escalate?', 'After two failed reconciliation attempts, escalate to the integration team with the workspace ID, connector version, and dry-run logs.'],
  ['Cedar support: what changed after the customer migrated workspaces?', 'The migrated workspace kept an outdated connector cursor. Preserve existing records and verify ownership before retrying reconciliation.'],
];
$('seed').addEventListener('click', guard(async () => {
  state.busy = true; controls(); let saved = 0;
  try { for (const [prompt, response] of seeds) {
    const result = await api('/v1/memories', { method: 'POST', body: { ...state.scope, prompt, response, expected_scope_revision: state.revision } });
    state.revision = result.scope_revision; saved++;
  } await refresh(); $('prompt').value = 'The connector still fails after migration. What should I try before escalating?';
  notice('Four fictional support memories added. Graph indexing follows asynchronously; refresh before comparing connected memory.');
  } catch (error) { await refresh(); throw new Error(`${saved} of 4 example memories saved. ${error.message}`); }
  finally { state.busy = false; controls(); }
}));
$('suggestion').addEventListener('click', () => { $('prompt').value = 'The connector still fails after migration. What should I try before escalating?'; $('prompt').focus(); });
function message(role, text) { const node = el('article', `message ${role}`), body = el('div', 'body', text);
  node.append(el('div', 'speaker', role === 'user' ? 'You' : 'Assistant'), body); $('conversation').append(node); return { node, body }; }
function scrollChat() { $('conversation').scrollTop = $('conversation').scrollHeight; }
async function readStream(response, onEvent) {
  const reader = response.body.getReader(), decoder = new TextDecoder(); let buffer = '', event = '', lines = [];
  const dispatch = () => { if (lines.length) onEvent(event || 'message', JSON.parse(lines.join('\n'))); event = ''; lines = []; };
  const line = (value) => { if (value === '') dispatch(); else if (value.startsWith('event:')) event = value.slice(6).trim(); else if (value.startsWith('data:')) lines.push(value.slice(5).replace(/^ /, '')); };
  try { while (true) { const { value, done } = await reader.read(); buffer += decoder.decode(value, { stream: !done });
    if (buffer.length > 2_000_000) throw new Error('Streaming event exceeds the inspector limit.');
    let match; while ((match = /\r\n|\n|\r(?!$)/.exec(buffer))) { const text = buffer.slice(0, match.index); buffer = buffer.slice(match.index + match[0].length); line(text); }
    if (done) break;
  } } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
$('chat-form').addEventListener('submit', guard(async () => {
  if (!state.token || state.stream || state.busy) return;
  const prompt = $('prompt').value; if (!prompt.trim()) return;
  const controller = new AbortController(), session = state.session; state.stream = controller;
  controls(); $('stop').hidden = false; $('stream-state').textContent = 'Retrieving context…'; notice('');
  $('conversation').querySelector('.welcome')?.remove(); message('user', prompt); const reply = message('assistant', '');
  reply.node.classList.add('pending'); let final = false; $('prompt').value = '';
  try {
    const response = await fetch('/v1/chat/stream', { method: 'POST', credentials: 'omit', redirect: 'error', cache: 'no-store', signal: controller.signal,
      headers: { Authorization: `Bearer ${state.token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...state.scope, prompt, retrieval_mode: $('retrieval').value, store: $('remember').checked, use_cache: $('cache').checked, max_tokens: 600 }) });
    if (!response.ok) throw new Error(`Chat unavailable (${response.status}). Check the token, scope and gateway health.`);
    await readStream(response, (event, data) => {
      if (session !== state.session) return;
      if (event === 'delta') { reply.body.textContent += data.delta; $('stream-state').textContent = 'Writing answer…'; }
      if (event === 'error') throw new Error(data.error?.message || 'The stream ended before durable completion.');
      if (event === 'final') {
        if (data.meta?.durable !== true) throw new Error('The gateway did not confirm durable completion.');
        final = true; reply.body.textContent = data.response; const meta = data.meta;
        reply.node.append(el('div', 'response-meta', `${meta.model || 'Exact cache'} · ${meta.latency_ms} ms · ${meta.usage_available === false ? 'usage unavailable' : `${meta.tokens_in + meta.tokens_out} tokens`} · ${meta.cache_hit ? 'cache hit' : 'generated'} · ${meta.memory_write === 'queued' ? 'memory queued' : 'not retained'}`));
        const chips = el('div', 'source-chips'); for (const id of meta.context_used || []) {
          const button = el('button', '', `Source ${id.slice(0, 8)}`); button.addEventListener('click', guard(() => inspect(id))); chips.append(button);
        } reply.node.append(chips);
        if (meta.degraded?.length) reply.node.append(el('div', 'response-meta', `Reduced service: ${meta.degraded.join(', ')}`));
      } scrollChat();
    });
    if (!final) throw new Error('Connection ended before completion. This partial answer is not confirmed as saved.');
    if (session === state.session) await refresh();
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
window.addEventListener('pagehide', () => { for (const request of state.requests) request.abort(); state.stream?.abort(); state.token = ''; });
