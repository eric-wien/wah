# AGENTS.md

## What this is

`@vinerima/wah` — a generic WebSocket action handler library for TypeScript. Connect to a WebSocket, register Zod schemas paired with handlers, and incoming messages are validated and dispatched to all matching handlers with full type inference. Runs in both Node.js (`ws` peer dependency) and browsers (native `WebSocket`) from one codebase; the runtime is detected at load time.

This is a published npm package — the public surface is whatever `src/index.ts` re-exports. Treat that file as the API contract.

## Commands

Uses **pnpm** (`packageManager: pnpm@9.15.0`).

```bash
pnpm build          # bundle via tsup → dist/ (cjs + esm + .d.ts)
pnpm test           # vitest run (one-shot)
pnpm test:watch     # vitest watch
pnpm typecheck      # tsc --noEmit
pnpm lint           # eslint src   (note: lints src only, not tests/)
pnpm lint:fix
pnpm format         # prettier --write src/**/*.ts
```

Run a single test file or test by name:

```bash
pnpm vitest run tests/router.test.ts
pnpm vitest run -t "exponential backoff"
```

There is no preconfigured runner for `test-jetstream.ts` — it is a manual scratch/integration script that connects to the live Bluesky Jetstream firehose and imports directly from `./src`. Run it with a TS runner (e.g. `pnpm dlx tsx test-jetstream.ts`); it is not part of the build, the test suite, or lint.

## Architecture

`WebSocketClient` (`src/WebSocketClient.ts`) is a thin facade that **composes three independent units** and wires their events together. It owns no connection or routing logic itself — understand the three pieces and the wiring:

1. **`WebSocketConnection`** (`src/connection/`) — owns the socket lifecycle: connect, exponential-backoff reconnect, multi-service failover, heartbeat pings, dynamic query-param updates. Emits `open` / `close` / `error` / `message` / `reconnecting` / `serviceSwitched`. Knows nothing about schemas.
2. **`WebSocketRouter`** (`src/router/`) — owns dispatch: JSON-parses each raw message, runs `schema.safeParse` against every registered handler, and invokes **all** matches concurrently via `Promise.allSettled`. Handler throws and JSON parse failures become `error` events (`HandlerError`) — they never crash the connection. Knows nothing about sockets.
3. **`Logger`** (`src/logger/`) — shared, passed into both units.

The wiring lives in `WebSocketClient.wireEvents()`: connection `message` events are converted to a string via the platform adapter and pushed into `router.route()`; both connection and router `error` events are funneled into a single unified `error` event. This composition-over-inheritance split is the core design — keep connection logic out of the router and vice versa.

### Platform abstraction (the cross-platform mechanism)

Everything environment-specific is isolated behind **`PlatformAdapter`** (`src/platform/types.ts`). `getPlatformAdapter()` (`src/platform/index.ts`) detects Node vs browser **once** (via `process.versions.node`) and returns a **cached** singleton — either `createNodeAdapter()` or `createBrowserAdapter()`. The adapter abstracts: socket construction, binary→string decoding (`Buffer` vs `TextDecoder`), ping support (`supportsPing` — false in browser, where the engine handles keepalive), and listener teardown. **No other file should branch on the runtime** — if you need platform-specific behavior, add it to the adapter interface and implement it in both `node.ts` and `browser.ts`.

The Node adapter `require("ws")` lazily inside `createNodeAdapter()` so browsers never touch the optional `ws` peer dependency. tsup marks `ws` as `external`.

### Custom Emitter

`src/platform/Emitter.ts` is a minimal hand-rolled event emitter (`on`/`off`/`emit`/`removeAllListeners`). It exists specifically so the library does **not** depend on Node's `EventEmitter` and thus runs in browsers. `WebSocketClient`, `WebSocketConnection`, and `WebSocketRouter` all extend it. Do not reintroduce `events`/`EventEmitter`.

### Reconnect / failover state machine

In `WebSocketConnection`: on close, `scheduleReconnect()` retries the current service up to `maxAttempts` with exponential backoff (`initialDelay * backoffFactor^(n-1)`, capped at `maxDelay`). When attempts are exhausted, `moveToNextService()` advances `serviceIndex` round-robin; completing a full loop back to index 0 increments `serviceCycles`. After `maxServiceCycles` full cycles it gives up. `updateParams()` merges params and forces an immediate graceful reconnect; `setParams()` merges params **without** reconnecting (takes effect on the next reconnect) — these two are deliberately distinct, don't collapse them.

## Conventions

- **Type-only exports** in `src/index.ts` use `export type { ... }` — preserve this; the build emits separate `.d.ts` and the distinction matters for `verbatimModuleSyntax`-style consumers.
- Public classes/methods carry **TSDoc with `@example` blocks**. New public API should follow suit.
- tsconfig is strict and includes `noUnusedLocals` / `noUnusedParameters` / `noImplicitReturns` — unused symbols are build errors.
- Tests live in `tests/` (not under `src/`) and are excluded from lint. Each unit is tested in isolation (`client`, `router`, `emitter`, `logger`, `platform`).
- When adding a public symbol, re-export it from `src/index.ts` or it won't ship.
