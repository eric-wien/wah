import { z } from "zod";
import { Emitter } from "./platform/Emitter";
import { getPlatformAdapter } from "./platform/index";
import type { PlatformAdapter } from "./platform/types";
import { WebSocketConnection } from "./connection/WebSocketConnection";
import { WebSocketRouter } from "./router/WebSocketRouter";
import { Logger } from "./logger/Logger";
import { WebSocketClientOptions } from "./types";
import { ConnectionInfo } from "./connection/types";
import { MessageHandler, HandlerError } from "./router/types";

/**
 * Main entry point for the wah library. Connects to a WebSocket, validates
 * incoming messages against Zod schemas, and dispatches to typed handlers.
 *
 * Composes {@link WebSocketConnection} (lifecycle, reconnect, heartbeat) with
 * {@link WebSocketRouter} (schema matching, handler dispatch) behind a single API.
 *
 * @example
 * ```typescript
 * import { WebSocketClient, LogLevel } from "wah";
 * import { z } from "zod";
 *
 * const tradeSchema = z.object({
 *   type: z.literal("trade"),
 *   symbol: z.string(),
 *   price: z.number(),
 * });
 *
 * const client = new WebSocketClient({
 *   service: "wss://stream.example.com/v1",
 *   queryParams: { apiKey: "abc" },
 *   logger: { enabled: true, level: LogLevel.DEBUG },
 * });
 *
 * client.handle(tradeSchema, async ({ data, send }) => {
 *   console.log(data.symbol, data.price);
 *   send({ ack: true });
 * });
 *
 * client.on("error", err => console.error(err));
 * client.connect();
 * ```
 */
export class WebSocketClient extends Emitter {
  private connection: WebSocketConnection;
  private router: WebSocketRouter;
  private logger: Logger;
  private adapter: PlatformAdapter;

  constructor(options: WebSocketClientOptions) {
    super();
    this.adapter = getPlatformAdapter();
    this.logger = new Logger(options.logger);
    this.connection = new WebSocketConnection(options, this.logger);
    this.router = new WebSocketRouter(this.logger);

    this.wireEvents();
  }

  /**
   * Registers a message handler that is invoked when an incoming message
   * successfully validates against the given Zod schema.
   *
   * Multiple handlers can be registered. All handlers whose schema matches
   * a message will be invoked concurrently.
   *
   * @typeParam T - The type inferred from the Zod schema.
   * @param schema - Zod schema to validate incoming messages against.
   * @param handler - Callback receiving the typed data and a context object.
   * @returns `this` for chaining.
   *
   * @example
   * ```typescript
   * const pingSchema = z.object({ type: z.literal("ping"), ts: z.number() });
   *
   * client.handle(pingSchema, ({ data, send }) => {
   *   send({ type: "pong", ts: data.ts });
   * });
   * ```
   */
  handle<T>(schema: z.ZodSchema<T>, handler: MessageHandler<T>): this {
    this.router.register(schema, handler);
    return this;
  }

  /**
   * Opens the WebSocket connection. The client will automatically reconnect
   * on disconnection according to the configured reconnection strategy.
   */
  connect(): void {
    this.connection.connect();
  }

  /**
   * Closes the WebSocket connection and stops all reconnection attempts.
   */
  close(): void {
    this.connection.close();
  }

  /**
   * Sends data through the WebSocket. Objects are JSON-serialized automatically.
   *
   * @param data - Data to send. Objects/arrays are JSON.stringified; strings and binary data are sent as-is.
   * @returns `true` if sent, `false` if the connection is not open.
   */
  send(data: unknown): boolean {
    const serialized =
      data instanceof Uint8Array
        ? new Uint8Array(data).buffer
        : typeof data === "string" || data instanceof ArrayBuffer
          ? data
          : JSON.stringify(data);
    return this.connection.send(serialized);
  }

  /**
   * Merges new query parameters into the connection URL.
   * Existing parameters not present in the update are preserved.
   *
   * By default the connection reconnects immediately so the new URL takes
   * effect. Pass `{ immediate: false }` to defer the change to the next
   * reconnection instead of tearing down a healthy socket.
   *
   * @param params - Key-value pairs to merge into the current query parameters.
   * @param options - `immediate` (default `true`): reconnect now vs. on next reconnect.
   *
   * @example
   * ```typescript
   * // Initial connection to wss://example.com?channel=lobby
   * client.updateParams({ channel: "game-1" });
   * // Reconnects to wss://example.com?channel=game-1
   *
   * // Update cursor on disconnect so reconnection resumes from last position
   * client.on("close", () => {
   *   client.updateParams({ cursor: lastEventId }, { immediate: false });
   * });
   * ```
   */
  updateParams(
    params: Record<string, string | number | boolean>,
    options: { immediate?: boolean } = {}
  ): void {
    this.connection.updateParams(params, options);
  }

  /**
   * Merges new query parameters without triggering a reconnection.
   *
   * @deprecated Use `updateParams(params, { immediate: false })`. Kept as an alias.
   * @param params - Key-value pairs to merge into the current query parameters.
   */
  setParams(params: Record<string, string | number | boolean>): void {
    this.connection.setParams(params);
  }

  /**
   * Returns a read-only snapshot of the current connection state.
   */
  getConnectionInfo(): ConnectionInfo {
    return this.connection.getConnectionInfo();
  }

  private wireEvents(): void {
    // Forward connection lifecycle events
    this.connection.on("open", () => this.emit("open"));
    this.connection.on("close", (info: { code: number; reason: string }) =>
      this.emit("close", info)
    );
    this.connection.on("reconnecting", (info: unknown) => this.emit("reconnecting", info));
    this.connection.on("serviceSwitched", (info: unknown) => this.emit("serviceSwitched", info));
    this.connection.on("exhausted", (info: unknown) => this.emit("exhausted", info));

    // Connection errors → unified "error" event
    this.connection.on("error", (error: Error) => this.emit("error", error));

    // Router errors → unified "error" event
    this.router.on("error", (handlerError: HandlerError) => this.emit("error", handlerError));

    // Connection messages → router
    this.connection.on("message", (data: unknown) => {
      const raw = this.adapter.dataToString(data);
      if (raw !== null) {
        const sendFn = (payload: unknown): boolean => this.send(payload);
        const info = this.connection.getConnectionInfo();
        // Fire and forget — errors are emitted via the router's error event
        this.router.route(raw, sendFn, info).catch((err: unknown) => {
          this.logger.error("Unexpected error in router.route", err);
        });
      }
    });
  }
}
