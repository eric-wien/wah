import { z } from "zod";
import { Emitter } from "../platform/Emitter";
import { ConnectionInfo } from "../connection/types";
import { HandlerRegistration, HandlerError, MessageHandler } from "./types";
import { Logger } from "../logger/Logger";

/**
 * Validates incoming WebSocket messages against registered Zod schemas
 * and dispatches to matching handlers.
 *
 * All handlers whose schema matches the incoming message are invoked
 * concurrently via `Promise.allSettled`. Handler errors are caught and
 * emitted as `"error"` events rather than propagating.
 *
 * Events emitted:
 * - `"error"` — a handler threw or JSON parsing failed (emits {@link HandlerError})
 */
export class WebSocketRouter extends Emitter {
  private handlers: HandlerRegistration[] = [];
  private logger: Logger;

  constructor(logger: Logger) {
    super();
    this.logger = logger;
  }

  /**
   * Registers a handler that will be invoked for every message matching the given schema.
   *
   * @typeParam T - The type inferred from the Zod schema.
   * @param schema - Zod schema to validate messages against.
   * @param handler - Function to call with the validated data.
   */
  register<T>(schema: z.ZodSchema<T>, handler: MessageHandler<T>): void {
    this.handlers.push({
      schema,
      handler: handler as MessageHandler<unknown>,
    });
    this.logger.debug("Handler registered", { totalHandlers: this.handlers.length });
  }

  /**
   * Processes a raw WebSocket message: parses JSON, validates against all
   * registered schemas, and invokes matching handlers.
   *
   * @param rawData - The raw message string from the WebSocket.
   * @param sendFn - Function to send data back through the WebSocket.
   * @param connectionInfo - Current connection state snapshot.
   */
  async route(
    rawData: string,
    sendFn: (data: unknown) => boolean,
    connectionInfo: ConnectionInfo
  ): Promise<void> {
    let parsed: unknown;
    try {
      parsed = JSON.parse(rawData);
    } catch {
      this.logger.debug("Failed to parse message as JSON", { rawData: rawData.slice(0, 200) });
      this.emit("error", {
        error: new Error("Failed to parse WebSocket message as JSON"),
        rawData,
        timestamp: Date.now(),
      } satisfies HandlerError);
      return;
    }

    const matched: Promise<void>[] = [];

    for (const registration of this.handlers) {
      const result = registration.schema.safeParse(parsed);
      if (result.success) {
        matched.push(
          this.invokeHandler(registration, result.data, rawData, sendFn, connectionInfo)
        );
      }
    }

    if (matched.length === 0) {
      this.logger.debug("No handlers matched message");
      return;
    }

    this.logger.debug("Dispatching to matched handlers", { count: matched.length });
    await Promise.allSettled(matched);
  }

  private async invokeHandler(
    registration: HandlerRegistration,
    data: unknown,
    rawData: string,
    sendFn: (data: unknown) => boolean,
    connectionInfo: ConnectionInfo
  ): Promise<void> {
    try {
      await registration.handler({ data, rawData, send: sendFn, connection: connectionInfo });
    } catch (error) {
      this.logger.error("Handler threw an error", error);
      this.emit("error", {
        error,
        rawData,
        timestamp: Date.now(),
      } satisfies HandlerError);
    }
  }
}
