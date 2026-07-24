import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { z } from "zod";
import type { PlatformAdapter, UniversalWebSocket } from "../src/platform/types";
import { WS_READY_STATE } from "../src/platform/types";

interface MockWs extends UniversalWebSocket {
  url: string;
}

const mockWsInstances: MockWs[] = [];

function createMockWs(url: string): MockWs {
  const ws: MockWs = {
    readyState: WS_READY_STATE.CONNECTING,
    onopen: null,
    onclose: null,
    onerror: null,
    onmessage: null,
    close: vi.fn(),
    send: vi.fn(),
    url,
  };
  mockWsInstances.push(ws);
  return ws;
}

const mockAdapter: PlatformAdapter = {
  createWebSocket: (url: string) => createMockWs(url),
  dataToString: (data: unknown) => {
    if (typeof data === "string") return data;
    if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
    if (data instanceof Uint8Array) return new TextDecoder().decode(data);
    if (Buffer.isBuffer(data)) return data.toString("utf-8");
    return null;
  },
  supportsPing: false,
  ping: vi.fn(),
  removeAllListeners: (ws: UniversalWebSocket) => {
    ws.onopen = null;
    ws.onclose = null;
    ws.onerror = null;
    ws.onmessage = null;
  },
};

vi.mock("../src/platform/index", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/platform/index")>();
  return {
    ...actual,
    getPlatformAdapter: () => mockAdapter,
  };
});

import { WebSocketClient } from "../src/WebSocketClient";

describe("WebSocketClient", () => {
  beforeEach(() => {
    mockWsInstances.length = 0;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function createClient(
    options?: Partial<ConstructorParameters<typeof WebSocketClient>[0]>
  ): WebSocketClient {
    return new WebSocketClient({
      service: "wss://test.example.com",
      logger: { enabled: false },
      ...options,
    });
  }

  function lastWs(): MockWs {
    return mockWsInstances[mockWsInstances.length - 1];
  }

  it("connects and emits open event", () => {
    const client = createClient();
    const onOpen = vi.fn();
    client.on("open", onOpen);

    client.connect();
    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    expect(onOpen).toHaveBeenCalled();
  });

  it("emits close event with code and reason", () => {
    const client = createClient();
    const onClose = vi.fn();
    client.on("close", onClose);

    client.connect();
    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});
    ws.readyState = WS_READY_STATE.CLOSED;
    ws.onclose!({ code: 1000, reason: "normal" });

    expect(onClose).toHaveBeenCalledWith({ code: 1000, reason: "normal" });
  });

  it("emits error event on connection error", () => {
    const client = createClient();
    const onError = vi.fn();
    client.on("error", onError);

    client.connect();
    const ws = lastWs();
    ws.onerror!(new Error("connection failed"));

    expect(onError).toHaveBeenCalledWith(expect.any(Error));
  });

  it("routes messages through handlers", async () => {
    const client = createClient();
    const schema = z.object({ type: z.literal("trade"), price: z.number() });
    const handler = vi.fn();

    client.handle(schema, handler);
    client.connect();

    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    // Simulate incoming message — adapter extracts data from MessageEvent-like object
    ws.onmessage!({ data: JSON.stringify({ type: "trade", price: 42.5 }) });

    await vi.waitFor(() => {
      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({
          data: { type: "trade", price: 42.5 },
        })
      );
    });
  });

  it("send() serializes objects as JSON", () => {
    const client = createClient();
    client.connect();

    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    const result = client.send({ action: "subscribe", channel: "trades" });

    expect(result).toBe(true);
    expect(ws.send).toHaveBeenCalledWith(
      JSON.stringify({ action: "subscribe", channel: "trades" })
    );
  });

  it("send() passes strings through unchanged", () => {
    const client = createClient();
    client.connect();

    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    client.send("raw string");
    expect(ws.send).toHaveBeenCalledWith("raw string");
  });

  it("send() normalizes typed arrays to browser-compatible ArrayBuffers", () => {
    const client = createClient();
    client.connect();

    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    const payload = new Uint8Array([1, 2, 3]);
    client.send(payload);

    const sent = vi.mocked(ws.send).mock.calls[0][0];
    expect(sent).toBeInstanceOf(ArrayBuffer);
    expect(new Uint8Array(sent as ArrayBuffer)).toEqual(payload);
  });

  it("send() returns false when not connected", () => {
    const client = createClient();
    const result = client.send("data");
    expect(result).toBe(false);
  });

  it("close() stops the connection", () => {
    const client = createClient();
    client.connect();

    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    client.close();
    expect(ws.close).toHaveBeenCalled();
  });

  it("getConnectionInfo() returns current state", () => {
    const client = createClient();
    const info = client.getConnectionInfo();
    expect(info.state).toBe("closed");
    expect(info.currentService).toBe("wss://test.example.com");
  });

  it("handle() returns this for chaining", () => {
    const client = createClient();
    const schema = z.object({ type: z.string() });
    const result = client.handle(schema, () => {});
    expect(result).toBe(client);
  });

  it("emits router errors as error events", async () => {
    const client = createClient();
    const onError = vi.fn();
    client.on("error", onError);

    client.connect();
    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    // Send invalid JSON to trigger router error
    ws.onmessage!({ data: "not json!" });

    await vi.waitFor(() => {
      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.any(Error),
          rawData: "not json!",
        })
      );
    });
  });

  it("reconnects exactly maxAttempts times, then emits exhausted", () => {
    vi.useFakeTimers();
    const client = createClient({
      reconnect: { maxAttempts: 3, initialDelay: 10, backoffFactor: 1, maxServiceCycles: 2 },
    });
    const onReconnecting = vi.fn();
    const onExhausted = vi.fn();
    client.on("reconnecting", onReconnecting);
    client.on("exhausted", onExhausted);

    client.connect();
    let ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});

    const drop = () => {
      ws.readyState = WS_READY_STATE.CLOSED;
      ws.onclose!({ code: 1006, reason: "" });
    };

    drop(); // schedules reconnect attempt 1
    for (let i = 0; i < 3; i++) {
      vi.advanceTimersByTime(10); // fire the reconnect timer → new socket
      ws = lastWs();
      drop(); // and fail that attempt
    }

    // maxAttempts: 3 must yield 3 real reconnect attempts (not 2 — regression guard)
    expect(onReconnecting).toHaveBeenCalledTimes(3);
    expect(onExhausted).toHaveBeenCalledTimes(1);
    expect(onExhausted).toHaveBeenCalledWith(expect.objectContaining({ attempts: 3 }));

    vi.useRealTimers();
  });

  it("updateParams({ immediate: false }) defers without reconnecting", () => {
    const client = createClient();
    client.connect();
    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});
    const countBefore = mockWsInstances.length;

    client.updateParams({ cursor: "123" }, { immediate: false });

    expect(mockWsInstances.length).toBe(countBefore); // no new socket
    expect(ws.close).not.toHaveBeenCalled();
    expect(client.getConnectionInfo().currentService).toContain("cursor=123");

    client.updateParams({ cursor: "456" }); // default immediate: true reconnects
    expect(mockWsInstances.length).toBe(countBefore + 1);
  });

  it("setParams is an alias for updateParams({ immediate: false })", () => {
    const client = createClient();
    client.connect();
    const ws = lastWs();
    ws.readyState = WS_READY_STATE.OPEN;
    ws.onopen!({});
    const countBefore = mockWsInstances.length;

    client.setParams({ cursor: "abc" });

    expect(mockWsInstances.length).toBe(countBefore); // no reconnect
    expect(client.getConnectionInfo().currentService).toContain("cursor=abc");
  });
});
