/** Native gateway API. Keep this client and its tenant credential on a trusted server. */
export interface ChatRequest {
  prompt: string;
  user_id: string;
  feature_tag: string;
  max_tokens?: number;
  use_graph?: boolean;
  retrieval_mode?: "none" | "vector" | "graph";
  system_prompt?: string;
  history?: Array<{ role: "user" | "assistant"; content: string }>;
  model?: string;
  use_cache?: boolean;
  cache_mode?: "exact" | "semantic";
  store?: boolean;
}

export interface ChatMetadata {
  call_id?: string | null;
  context_used?: string[];
  cache_hit?: boolean;
  model?: string | null;
  provider?: string | null;
  durable?: boolean;
  timings_ms?: Record<string, number>;
  [key: string]: unknown;
}

export interface ChatResponse { response: string; meta: ChatMetadata }
export type StreamEvent =
  | { type: "delta"; delta: string; model?: string; provider?: string }
  | { type: "final"; response: ChatResponse };
export interface Scope { user_id: string; feature_tag: string }
export interface Memory extends Scope {
  id: string;
  prompt: string | null;
  response: string | null;
  status: "active" | "deleted" | "superseded" | "invalidated" | "expired";
  memory_kind: "generated" | "curated";
  revision: number;
  created_at: string;
  expires_at: string | null;
  source_ids: string[];
  supersedes_id: string | null;
  cache_eligible: boolean;
  retrieval: { sources?: Array<{ id: string; similarity?: number; [key: string]: unknown }> };
}
export interface MemoryPage { items: Memory[]; next_cursor: string | null; scope_revision: number }
export interface MemoryMutation {
  memory: Memory; scope_revision: number; invalidated_count: number; graph_write: "queued";
}
export interface MemoryDeletion {
  deleted_ids: string[]; deleted_count: number; deleted_ids_truncated: boolean;
  invalidated_count: number; scope_revision: number; graph_write: "queued";
}
export interface RequestOptions { signal?: AbortSignal }
export interface GatewayOptions {
  apiKey: string;
  baseUrl?: string;
  /** Total timeout, including stream consumption. Default: 120 seconds. */
  timeoutMs?: number;
  /** Supply a Fetch-compatible transport for tests or server instrumentation. */
  fetch?: typeof globalThis.fetch;
}

export class GatewayError extends Error {
  readonly name = "GatewayError";
  constructor(
    message: string,
    readonly statusCode: number,
    readonly requestId: string | null,
    readonly retryAfter: string | null,
    readonly details: unknown,
  ) { super(message); }
}
export class GatewayProtocolError extends Error { readonly name = "GatewayProtocolError"; }
export class GatewayTransportError extends Error { readonly name = "GatewayTransportError"; }
export class GatewayStreamError extends Error {
  readonly name = "GatewayStreamError";
  readonly durable = false;
  constructor(message: string, readonly code: string, readonly partial: boolean,
    readonly requestId: string | null) { super(message); }
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new GatewayProtocolError("Expected a JSON object from the gateway");
  }
  return value as Record<string, unknown>;
}

function chatResponse(value: unknown): ChatResponse {
  const data = object(value);
  if (typeof data.response !== "string") {
    throw new GatewayProtocolError("Invalid chat response from the gateway");
  }
  object(data.meta);
  return data as unknown as ChatResponse;
}

function scope(value: Scope): void {
  if (!value.user_id || !value.feature_tag) {
    throw new TypeError("user_id and feature_tag are required");
  }
}

interface WireEvent { event: string; data: string }

/** Incremental SSE line framing; a CRLF may itself straddle byte chunks. */
class EventFramer {
  private buffer = "";
  private event = "";
  private data: string[] = [];
  private size = 0;
  private firstLine = true;

  push(text: string, end = false): WireEvent[] {
    this.buffer += text;
    const events: WireEvent[] = [];
    while (true) {
      const at = this.buffer.search(/[\r\n]/);
      if (at < 0 || (!end && this.buffer[at] === "\r" && at === this.buffer.length - 1)) break;
      let line = this.buffer.slice(0, at);
      const width = this.buffer[at] === "\r" && this.buffer[at + 1] === "\n" ? 2 : 1;
      this.buffer = this.buffer.slice(at + width);
      if (this.firstLine) { line = line.replace(/^\uFEFF/, ""); this.firstLine = false; }
      this.size += line.length;
      this.checkSize();
      if (line === "") {
        if (this.data.length) events.push({ event: this.event, data: this.data.join("\n") });
        this.event = ""; this.data = []; this.size = 0;
      } else if (!line.startsWith(":")) {
        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") this.event = value;
        if (field === "data") this.data.push(value);
      }
    }
    this.checkSize();
    return events;
  }

  private checkSize(): void {
    if (this.size + this.buffer.length > 2 * 1024 * 1024) {
      throw new GatewayProtocolError("Gateway SSE event exceeds the client size limit");
    }
  }
}

/** Reuse this client on your application server. Requests are never automatically retried. */
export class GatewayClient {
  private readonly baseUrl: URL;
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly transport: typeof globalThis.fetch;

  constructor(options: GatewayOptions) {
    if (typeof window !== "undefined" && typeof window.document !== "undefined") {
      throw new TypeError("GatewayClient requires a trusted server; never expose a tenant key in a browser");
    }
    this.baseUrl = new URL((options.baseUrl ?? "http://127.0.0.1:8000").replace(/\/+$/, "") + "/");
    if (!["http:", "https:"].includes(this.baseUrl.protocol) ||
        this.baseUrl.username || this.baseUrl.password || this.baseUrl.search || this.baseUrl.hash) {
      throw new TypeError("baseUrl must be an HTTP(S) URL without credentials, query, or fragment");
    }
    if (!options.apiKey || /[\r\n]/.test(options.apiKey)) throw new TypeError("apiKey must be a nonempty bearer token");
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs ?? 120_000;
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1) {
      throw new TypeError("timeoutMs must be a positive integer");
    }
    this.transport = options.fetch ?? globalThis.fetch;
  }

  private signal(options?: RequestOptions): AbortSignal {
    const timeout = AbortSignal.timeout(this.timeoutMs);
    return options?.signal ? AbortSignal.any([timeout, options.signal]) : timeout;
  }

  private async send(method: string, path: string, signal: AbortSignal,
    body?: unknown, query?: Record<string, string | number | undefined>, stream = false): Promise<Response> {
    const url = new URL(path, this.baseUrl);
    for (const [key, value] of Object.entries(query ?? {})) {
      if (value !== undefined) url.searchParams.set(key, String(value));
    }
    try {
      return await this.transport(url, {
        method, signal, redirect: "error",
        headers: {
          Authorization: `Bearer ${this.apiKey}`,
          Accept: stream ? "text/event-stream" : "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
    } catch (error) {
      if (signal.aborted) throw signal.reason;
      throw new GatewayTransportError("Gateway request transport failed; delivery may be uncertain", { cause: error });
    }
  }

  private async checkStatus(response: Response): Promise<void> {
    if (response.ok) return;
    let details: unknown = null;
    try { details = await response.json(); } catch { /* Do not include proxy HTML in errors. */ }
    const detail = details && typeof details === "object" && "detail" in details ? details.detail : null;
    throw new GatewayError(
      typeof detail === "string" ? detail : `Gateway returned HTTP ${response.status}`,
      response.status, response.headers.get("x-request-id"), response.headers.get("retry-after"), details,
    );
  }

  private async request<T>(method: string, path: string, body?: unknown,
    query?: Record<string, string | number | undefined>, options?: RequestOptions): Promise<T> {
    const signal = this.signal(options);
    const response = await this.send(method, path, signal, body, query);
    await this.checkStatus(response);
    try { return object(await response.json()) as T; }
    catch (error) {
      if (signal.aborted) throw signal.reason;
      if (error instanceof GatewayProtocolError) throw error;
      if (error instanceof SyntaxError) throw new GatewayProtocolError("Invalid JSON from the gateway", { cause: error });
      throw new GatewayTransportError("Gateway response transport failed; delivery may be uncertain", { cause: error });
    }
  }

  async chat(request: ChatRequest, options?: RequestOptions): Promise<ChatResponse> {
    scope(request);
    return chatResponse(await this.request("POST", "v1/chat", request, undefined, options));
  }

  /** Partial deltas are provisional. Only a final event confirms durable completion.
   * Breaking the loop cancels the response reader; AbortSignal cancels a pending read.
   */
  async *streamChat(request: ChatRequest, options?: RequestOptions): AsyncGenerator<StreamEvent> {
    scope(request);
    const signal = this.signal(options);
    const response = await this.send("POST", "v1/chat/stream", signal, request, undefined, true);
    await this.checkStatus(response);
    if (response.headers.get("content-type")?.split(";")[0]?.trim() !== "text/event-stream" || !response.body) {
      await response.body?.cancel();
      throw new GatewayProtocolError("Expected text/event-stream from the gateway");
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const framer = new EventFramer();
    try {
      while (true) {
        signal.throwIfAborted();
        const { done, value } = await reader.read();
        let text: string;
        try { text = decoder.decode(value, { stream: !done }); }
        catch (error) { throw new GatewayProtocolError("Invalid UTF-8 in gateway stream", { cause: error }); }
        for (const event of framer.push(text, done)) {
          if (!["delta", "final", "error"].includes(event.event)) continue;
          let data: Record<string, unknown>;
          try { data = object(JSON.parse(event.data)); }
          catch (error) { throw new GatewayProtocolError("Invalid JSON in gateway SSE event", { cause: error }); }
          if (event.event === "error") {
            const error = object(data.error);
            if (typeof error.message !== "string" || typeof error.code !== "string") {
              throw new GatewayProtocolError("Invalid gateway SSE error");
            }
            throw new GatewayStreamError(error.message, error.code, Boolean(data.partial), response.headers.get("x-request-id"));
          }
          if (event.event === "delta") {
            if (typeof data.delta !== "string") throw new GatewayProtocolError("Invalid gateway text delta");
            yield { ...data, type: "delta" } as StreamEvent;
          } else {
            const final = chatResponse(data);
            if (final.meta.durable !== true) {
              throw new GatewayProtocolError("Gateway final event did not confirm durable completion");
            }
            yield { type: "final", response: final };
            return;
          }
        }
        if (done) throw new GatewayProtocolError("Gateway stream ended before a final event");
      }
    } catch (error) {
      if (signal.aborted) throw signal.reason;
      if (error instanceof GatewayProtocolError || error instanceof GatewayStreamError) throw error;
      throw new GatewayTransportError("Gateway stream transport failed; delivery may be uncertain", { cause: error });
    } finally {
      try { await reader.cancel(); } catch { /* Preserve the original failure or cancellation. */ }
      reader.releaseLock();
    }
  }

  listMemories(request: Scope & { limit?: number; cursor?: string }, options?: RequestOptions): Promise<MemoryPage> {
    scope(request);
    return this.request("GET", "v1/memories", undefined, { ...request }, options);
  }

  getMemory(id: string, request: Scope, options?: RequestOptions): Promise<Memory> {
    scope(request);
    return this.request("GET", `v1/memories/${encodeURIComponent(id)}`, undefined, { ...request }, options);
  }

  createMemory(request: Scope & { prompt: string; response: string; expected_scope_revision: number },
    options?: RequestOptions): Promise<MemoryMutation> {
    scope(request);
    return this.request("POST", "v1/memories", request, undefined, options);
  }

  correctMemory(id: string, request: Scope & { prompt: string; response: string; expected_revision: number },
    options?: RequestOptions): Promise<MemoryMutation> {
    scope(request);
    return this.request("PATCH", `v1/memories/${encodeURIComponent(id)}`, request, undefined, options);
  }

  deleteMemory(id: string, request: Scope & { expected_revision: number }, options?: RequestOptions): Promise<MemoryDeletion> {
    scope(request);
    return this.request("DELETE", `v1/memories/${encodeURIComponent(id)}`, undefined, { ...request }, options);
  }

  deleteScope(request: Scope & { expected_scope_revision: number }, options?: RequestOptions): Promise<MemoryDeletion> {
    scope(request);
    return this.request("DELETE", "v1/memories", undefined, { ...request }, options);
  }
}
