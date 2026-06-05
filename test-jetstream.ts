import { WebSocketClient, LogLevel } from "./src";
import { z } from "zod";

// ---------------------------------------------------------------------------
// Jetstream message schemas
//
// Each event is keyed on a `kind` literal ("commit" | "identity" | "account").
// Instead of registering three separate schemas — which makes the router run a
// full Zod validation per schema per message (O(handlers) on a firehose) — we
// combine them into a single `z.discriminatedUnion`. Zod dispatches on `kind`
// in O(1), and the whole stream is handled by one `handle()` registration.
// ---------------------------------------------------------------------------

const commitCreateSchema = z.object({
  did: z.string(),
  time_us: z.number(),
  kind: z.literal("commit"),
  commit: z.object({
    rev: z.string(),
    operation: z.literal("create"),
    collection: z.literal("app.bsky.feed.post"),
    rkey: z.string(),
    record: z.object({
      $type: z.literal("app.bsky.feed.post"),
      text: z.string(),
      createdAt: z.string(),
      langs: z.array(z.string()).optional(),
      reply: z
        .object({
          root: z.object({ cid: z.string(), uri: z.string() }),
          parent: z.object({ cid: z.string(), uri: z.string() }),
        })
        .optional(),
    }),
    cid: z.string(),
  }),
});

const identitySchema = z.object({
  did: z.string(),
  time_us: z.number(),
  kind: z.literal("identity"),
  identity: z.object({
    did: z.string(),
    handle: z.string(),
    seq: z.number(),
    time: z.string(),
  }),
});

const accountSchema = z.object({
  did: z.string(),
  time_us: z.number(),
  kind: z.literal("account"),
  account: z.object({
    active: z.boolean(),
    did: z.string(),
    seq: z.number(),
    time: z.string(),
  }),
});

// One union, dispatched on `kind` — the fast path for high-throughput streams.
const eventSchema = z.discriminatedUnion("kind", [
  commitCreateSchema,
  identitySchema,
  accountSchema,
]);

// ---------------------------------------------------------------------------
// Counters for summary
// ---------------------------------------------------------------------------

let postCount = 0;
let identityCount = 0;
let accountCount = 0;

// ---------------------------------------------------------------------------
// Create client — subscribe only to posts, identities, and accounts
// ---------------------------------------------------------------------------

const client = new WebSocketClient({
  service: [
    "wss://jetstream1.us-east.bsky.network/subscribe",
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-west.bsky.network/subscribe",
    "wss://jetstream2.us-west.bsky.network/subscribe",
  ],
  queryParams: {
    wantedCollections: "app.bsky.feed.post",
  },
  reconnect: {
    maxAttempts: 3,
    initialDelay: 3000,
    backoffFactor: 1.5,
    maxServiceCycles: 2,
  },
  pingInterval: 15000,
  logger: { enabled: true, level: LogLevel.INFO },
});

// ---------------------------------------------------------------------------
// Register a single handler for the whole stream — `data.kind` narrows the type
// ---------------------------------------------------------------------------

client.handle(eventSchema, ({ data }) => {
  switch (data.kind) {
    case "commit": {
      postCount++;
      const text = data.commit.record.text.slice(0, 80).replace(/\n/g, " ");
      const langs = data.commit.record.langs?.join(",") ?? "?";
      const isReply = data.commit.record.reply ? " [reply]" : "";
      console.log(`[POST #${postCount}] (${langs})${isReply} ${text}`);
      break;
    }
    case "identity": {
      identityCount++;
      console.log(`[IDENTITY] ${data.identity.handle} (${data.identity.did})`);
      break;
    }
    case "account": {
      accountCount++;
      const status = data.account.active ? "active" : "inactive";
      console.log(`[ACCOUNT] ${data.account.did} → ${status}`);
      break;
    }
  }
});

// ---------------------------------------------------------------------------
// Connection events
// ---------------------------------------------------------------------------

client.on("open", () => {
  console.log("\n--- Connected to Jetstream ---\n");
});

client.on("close", (info) => {
  console.log("\n--- Disconnected ---", info);
});

client.on("error", (err) => {
  console.error("[ERROR]", err);
});

client.on("reconnecting", (info) => {
  console.log("[RECONNECTING]", info);
});

client.on("serviceSwitched", (info) => {
  console.log("[SERVICE SWITCHED]", info);
});

client.on("exhausted", (info) => {
  console.error("[EXHAUSTED] gave up reconnecting", info);
});

// ---------------------------------------------------------------------------
// Connect and auto-close after 15 seconds
// ---------------------------------------------------------------------------

console.log("Starting Jetstream test — will run for 15 seconds...\n");
client.connect();

setTimeout(() => {
  client.close();
  console.log("\n--- Test complete ---");
  console.log(`Posts: ${postCount}, Identities: ${identityCount}, Accounts: ${accountCount}`);
  console.log(`Total messages received: ${client.getConnectionInfo().messageCount}`);
  process.exit(0);
}, 15000);
