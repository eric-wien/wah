import type { PlatformAdapter, UniversalWebSocket } from "./types";

const decoder = new TextDecoder();

/**
 * Browser platform adapter.
 * Uses the native `WebSocket` and `TextDecoder` for binary conversion.
 */
export function createBrowserAdapter(): PlatformAdapter {
  return {
    createWebSocket(url: string): UniversalWebSocket {
      // WebSocketClient normalizes typed arrays to ArrayBuffer before sending.
      // Keep the wider shared interface for Node adapters and API compatibility.
      return new WebSocket(url) as unknown as UniversalWebSocket;
    },

    dataToString(data: unknown): string | null {
      if (typeof data === "string") return data;
      if (data instanceof ArrayBuffer) return decoder.decode(data);
      if (data instanceof Uint8Array) return decoder.decode(data);
      return null;
    },

    supportsPing: false,

    ping(): void {
      // Browser engines handle WebSocket keepalive at the protocol level.
    },

    removeAllListeners(ws: UniversalWebSocket): void {
      ws.onopen = null;
      ws.onclose = null;
      ws.onerror = null;
      ws.onmessage = null;
    },
  };
}
