import { Emitter } from "../platform/Emitter";
import { getPlatformAdapter, WS_READY_STATE } from "../platform/index";
import type { PlatformAdapter, UniversalWebSocket } from "../platform/types";
import { ConnectionOptions, ConnectionInfo, ConnectionState, ReconnectOptions } from "./types";
import { Logger } from "../logger/Logger";

/**
 * Manages the WebSocket connection lifecycle including connecting, reconnecting
 * with exponential backoff, multi-service failover, heartbeat pings, and
 * dynamic query parameter updates.
 *
 * Events emitted:
 * - `"open"` — connection established
 * - `"close"` — connection closed (emits `{ code: number, reason: string }`)
 * - `"error"` — connection error (emits the `Error` object)
 * - `"message"` — message received (emits the raw message data)
 * - `"reconnecting"` — about to attempt reconnection (emits `{ attempt, maxAttempts, delay, service }`)
 * - `"serviceSwitched"` — failed over to a different service URL (emits `{ from, to, cycle }`)
 * - `"exhausted"` — gave up reconnecting after all attempts/cycles (emits `{ services, attempts, cycles }`)
 */
export class WebSocketConnection extends Emitter {
  private services: string[];
  private queryParams: Record<string, string | number | boolean>;
  private reconnectConfig: Required<ReconnectOptions>;
  private pingInterval: number;

  private adapter: PlatformAdapter;
  private ws: UniversalWebSocket | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  private serviceIndex = 0;
  private reconnectAttempts = 0;
  private serviceCycles = 0;
  private isConnecting = false;
  private shouldReconnect = true;
  private messageCount = 0;
  private lastMessageTime = 0;

  private logger: Logger;

  constructor(options: ConnectionOptions, logger: Logger) {
    super();
    this.adapter = getPlatformAdapter();
    this.services = Array.isArray(options.service) ? options.service : [options.service];
    this.queryParams = { ...options.queryParams };
    this.pingInterval = options.pingInterval ?? 10000;
    this.logger = logger;

    const rc = options.reconnect ?? {};
    this.reconnectConfig = {
      initialDelay: rc.initialDelay ?? 5000,
      maxDelay: rc.maxDelay ?? 30000,
      backoffFactor: rc.backoffFactor ?? 1.5,
      maxAttempts: rc.maxAttempts ?? 3,
      maxServiceCycles: rc.maxServiceCycles ?? 2,
    };
  }

  /**
   * Opens the WebSocket connection. Does nothing if already connecting or connected.
   */
  connect(): void {
    if (this.isConnecting || this.getState() === "connected") {
      this.logger.debug("Connect called but already connecting/connected");
      return;
    }
    this.shouldReconnect = true;
    this.run();
  }

  /**
   * Closes the WebSocket connection gracefully and stops all reconnection attempts.
   */
  close(): void {
    this.shouldReconnect = false;
    this.stopHeartbeat();
    this.clearReconnectTimer();

    if (this.ws) {
      try {
        this.ws.close();
      } catch (error) {
        this.logger.error("Error closing WebSocket", error);
      }
    }
  }

  /**
   * Sends raw data through the WebSocket.
   * @returns `true` if the data was sent, `false` if the connection is not open.
   */
  send(data: string | ArrayBuffer | Uint8Array): boolean {
    if (this.ws && this.ws.readyState === WS_READY_STATE.OPEN) {
      try {
        this.ws.send(data);
        return true;
      } catch (error) {
        this.logger.error("Error sending data", error);
        return false;
      }
    }
    this.logger.debug("Cannot send: WebSocket not connected");
    return false;
  }

  /**
   * Merges new query parameters into the current set.
   *
   * When `immediate` is `true` (the default), the current connection is closed
   * and reopened so the updated URL takes effect now. When `false`, the params
   * are stored and take effect on the next connection attempt (e.g. after a
   * disconnect triggers the built-in reconnection) without tearing down a
   * healthy socket.
   */
  updateParams(
    params: Record<string, string | number | boolean>,
    options: { immediate?: boolean } = {}
  ): void {
    this.queryParams = { ...this.queryParams, ...params };

    if (options.immediate === false) {
      this.logger.debug("Query params set (deferred to next reconnect)", {
        params: this.queryParams,
      });
      return;
    }

    this.logger.info("Query params updated, reconnecting", { params: this.queryParams });

    // Graceful reconnect: close current, then re-run
    this.cleanup();
    this.reconnectAttempts = 0;
    this.serviceCycles = 0;
    this.run();
  }

  /**
   * Merges new query parameters without triggering a reconnection.
   *
   * @deprecated Use `updateParams(params, { immediate: false })`. Kept as an alias.
   */
  setParams(params: Record<string, string | number | boolean>): void {
    this.updateParams(params, { immediate: false });
  }

  /**
   * Returns a read-only snapshot of the current connection state.
   */
  getConnectionInfo(): ConnectionInfo {
    return {
      state: this.getState(),
      currentService: this.getCurrentServiceUrl(),
      serviceIndex: this.serviceIndex,
      allServices: [...this.services],
      reconnectAttempts: this.reconnectAttempts,
      serviceCycles: this.serviceCycles,
      messageCount: this.messageCount,
      lastMessageTime: this.lastMessageTime,
    };
  }

  // ---------------------------------------------------------------------------
  // Private
  // ---------------------------------------------------------------------------

  private getState(): ConnectionState {
    if (!this.ws) return "closed";
    switch (this.ws.readyState) {
      case WS_READY_STATE.CONNECTING:
        return "connecting";
      case WS_READY_STATE.OPEN:
        return "connected";
      case WS_READY_STATE.CLOSING:
        return "closing";
      default:
        return "closed";
    }
  }

  private getCurrentServiceUrl(): string {
    return this.buildUrl(this.services[this.serviceIndex]);
  }

  private buildUrl(base: string): string {
    if (Object.keys(this.queryParams).length === 0) {
      return base;
    }

    const url = new URL(base);
    for (const [key, value] of Object.entries(this.queryParams)) {
      url.searchParams.set(key, String(value));
    }
    return url.toString();
  }

  private run(): void {
    if (this.isConnecting) {
      return;
    }

    this.isConnecting = true;
    const serviceUrl = this.getCurrentServiceUrl();

    try {
      this.logger.info("Connecting to WebSocket", { service: serviceUrl });
      const ws = this.adapter.createWebSocket(serviceUrl);
      this.ws = ws;

      ws.onopen = () => {
        this.isConnecting = false;
        this.reconnectAttempts = 0;
        this.serviceCycles = 0;
        this.startHeartbeat();
        this.logger.info("WebSocket connected", { service: serviceUrl });
        this.emit("open");
      };

      ws.onmessage = (event: unknown) => {
        this.messageCount++;
        this.lastMessageTime = Date.now();
        // Node ws passes the data directly; browser wraps in MessageEvent
        const data =
          event && typeof event === "object" && "data" in event
            ? (event as { data: unknown }).data
            : event;
        this.emit("message", data);
      };

      ws.onerror = (event: unknown) => {
        const error = event instanceof Error ? event : new Error("WebSocket error");
        this.logger.error("WebSocket error", error);
        this.isConnecting = false;
        this.emit("error", error);
      };

      ws.onclose = (event: unknown) => {
        const code =
          event && typeof event === "object" && "code" in event
            ? (event as { code: number }).code
            : 1006;
        const reason =
          event && typeof event === "object" && "reason" in event
            ? String((event as { reason: unknown }).reason)
            : "";
        this.logger.info("WebSocket disconnected", { code, reason });
        this.isConnecting = false;
        this.stopHeartbeat();
        this.emit("close", { code, reason });

        if (this.shouldReconnect) {
          this.scheduleReconnect();
        }
      };
    } catch (error) {
      this.logger.error("Error creating WebSocket", error);
      this.isConnecting = false;
      if (this.shouldReconnect) {
        this.scheduleReconnect();
      }
    }
  }

  private scheduleReconnect(): void {
    // Have we already used all attempts for the current service?
    if (this.reconnectAttempts >= this.reconnectConfig.maxAttempts) {
      if (this.canTryNextService()) {
        this.moveToNextService();
        return;
      }
      this.logger.error("All services exhausted", {
        totalServices: this.services.length,
        maxCycles: this.reconnectConfig.maxServiceCycles,
        completedCycles: this.serviceCycles,
      });
      this.emit("exhausted", {
        services: [...this.services],
        attempts: this.reconnectAttempts,
        cycles: this.serviceCycles,
      });
      return;
    }

    this.reconnectAttempts++;

    const delay = Math.min(
      this.reconnectConfig.initialDelay *
        Math.pow(this.reconnectConfig.backoffFactor, this.reconnectAttempts - 1),
      this.reconnectConfig.maxDelay
    );

    this.logger.info("Scheduling reconnection", {
      attempt: this.reconnectAttempts,
      maxAttempts: this.reconnectConfig.maxAttempts,
      delay: `${Math.round(delay)}ms`,
      service: this.getCurrentServiceUrl(),
    });

    this.emit("reconnecting", {
      attempt: this.reconnectAttempts,
      maxAttempts: this.reconnectConfig.maxAttempts,
      delay: Math.round(delay),
      service: this.getCurrentServiceUrl(),
    });

    this.clearReconnectTimer();
    this.reconnectTimer = setTimeout(() => {
      this.cleanup();
      this.run();
    }, delay);
  }

  private canTryNextService(): boolean {
    return this.services.length > 1 && this.serviceCycles < this.reconnectConfig.maxServiceCycles;
  }

  private moveToNextService(): void {
    const previousIndex = this.serviceIndex;
    this.serviceIndex = (this.serviceIndex + 1) % this.services.length;

    if (this.serviceIndex === 0) {
      this.serviceCycles++;
    }

    this.reconnectAttempts = 0;

    const from = this.services[previousIndex];
    const to = this.services[this.serviceIndex];
    this.logger.info("Switching service", { from, to, cycle: this.serviceCycles });
    this.emit("serviceSwitched", { from, to, cycle: this.serviceCycles });

    this.cleanup();
    this.run();
  }

  private cleanup(): void {
    if (this.ws) {
      this.adapter.removeAllListeners(this.ws);
      if (
        this.ws.readyState === WS_READY_STATE.OPEN ||
        this.ws.readyState === WS_READY_STATE.CONNECTING
      ) {
        try {
          this.ws.close();
        } catch {
          // Swallow — we're cleaning up anyway
        }
      }
      this.ws = null;
    }
    this.clearReconnectTimer();
  }

  private startHeartbeat(): void {
    if (!this.adapter.supportsPing) return;
    this.stopHeartbeat();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WS_READY_STATE.OPEN) {
        try {
          this.adapter.ping(this.ws);
        } catch (error) {
          this.logger.error("Error sending ping", error);
        }
      }
    }, this.pingInterval);
  }

  private stopHeartbeat(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
