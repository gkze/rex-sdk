/**
 * A local Rex server for tests: the real control and data protocols on a temporary Unix socket.
 *
 * It enforces the wire contract in both directions. Every request payload is validated against
 * the method's `input_schema` from `schema/rex-api.json`, and every result a test handler
 * returns is validated against its `output_schema`. Violations are recorded and answered with
 * `bad_request` (requests) or reported by {@link IRexTestServer.violations} (results), so a test
 * fails when the SDK or a fixture drifts from the server's contract.
 *
 * Tests script behavior per method with {@link IRexTestServer.handle}, push events with
 * {@link IRexTestServer.emit}, and serve block output with {@link IRexTestServer.serveData}.
 *
 * @since 0.1.0
 */
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer, type IncomingMessage } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import { Ajv2020, type ValidateFunction } from "ajv/dist/2020.js";
import { Effect, Schema } from "effect";
import { WebSocket, WebSocketServer, type RawData } from "ws";

function rawDataBytes(data: RawData): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data);
}

/** An error reply a handler sends instead of a result. */
export class RexTestServerErrorReply {
  /** The documented error code, such as `not_found`. */
  readonly code: string;
  /** The message people read. */
  readonly message: string;
  /** The HTTP-like status that goes with the code. */
  readonly status: number;

  /** Creates a reply with a documented error code, message, and HTTP-like status. */
  constructor(code: string, message: string, status: number) {
    this.code = code;
    this.message = message;
    this.status = status;
  }
}

/** Builds an error reply for a documented code with its documented status. */
export function rexTestServerError(code: string, message = code): RexTestServerErrorReply {
  const statuses: Readonly<Record<string, number>> = {
    bad_request: 400,
    not_found: 404,
    method_not_found: 404,
    session_not_found: 404,
    conflict: 409,
    not_supported: 400,
    refused: 403,
    client_unavailable: 409,
    too_large: 413,
    revision_too_low: 400,
    timeout: 504,
    internal: 500,
  };
  return new RexTestServerErrorReply(code, message, statuses[code] ?? 500);
}

/** One request the server received on a control connection. */
export interface RexTestServerRequest {
  readonly connectionId: number;
  readonly id: string;
  readonly method: string;
  readonly payload: unknown;
}

/** One control connection, as the server sees it. */
export interface RexTestServerConnection {
  readonly id: number;
  /** The hello message the client sent. */
  readonly hello: unknown;
  /** Sends an event on this connection with the next `seq`. */
  readonly emit: (event: { readonly type: string; readonly payload?: unknown }) => void;
  /** Sends any raw text frame, for malformed-message tests. */
  readonly sendRaw: (text: string) => void;
  /** Closes the connection with a close frame. */
  readonly close: (code: number, reason: string) => void;
  /** Drops the connection without a close frame. */
  readonly terminate: () => void;
}

/** Handles one request; return a result or a {@link RexTestServerErrorReply}. */
export type RexTestServerHandler = (
  payload: unknown,
  connection: RexTestServerConnection,
) => Effect.Effect<Schema.Json | RexTestServerErrorReply>;

/** A scripted block output stream served on `/api/data`. */
export interface RexTestServerDataStream {
  readonly sessionId: string;
  readonly blockId: string;
  /** Stream bytes sent right after the WELCOME. */
  readonly chunks: ReadonlyArray<Uint8Array>;
  /** Channel nonce sent in the WELCOME. */
  readonly nonce: bigint;
  /** Close code and reason sent after the chunks; leave out to keep the stream open. */
  readonly closeWith?: { readonly code: number; readonly reason: string } | undefined;
  /** REJECT reason to answer the HELLO with instead of a WELCOME. */
  readonly rejectWith?: number | undefined;
}

/**
 * The server's side of one byte stream (`rex.data.bytes.v1`), for scripting it.
 *
 * Send an empty chunk to half-close, as Rex does at the end of a download.
 */
export interface RexTestServerBytesConnection {
  /** The data reference the stream was opened for, such as `download:<uuid>`. */
  readonly ref: string;
  readonly send: (chunk: Uint8Array) => void;
  readonly close: (code: number, reason?: string) => void;
  /** Calls `listener` for each binary message the client sends, empty half-close included. */
  readonly onMessage: (listener: (chunk: Uint8Array) => void) => void;
}

/** What one byte stream carried, for assertions after it ended. */
export interface RexTestServerBytesRecord {
  readonly ref: string;
  /** Every binary message the client sent, in order; an empty one is its half-close. */
  readonly received: ReadonlyArray<Uint8Array>;
  /** The close code and reason, once the connection closed. */
  readonly closed: { readonly code: number; readonly reason: string } | undefined;
}

/** Scripts the server's side of a byte stream once its WELCOME has been sent. */
export type RexTestServerBytesPeer = (connection: RexTestServerBytesConnection) => void;

/** Options for the server's welcome. */
export interface RexTestServerOptions {
  /** API revision the server reports; defaults to 7. */
  readonly apiRevision?: number | undefined;
  /** Close every control connection right after its hello with this code and reason. */
  readonly closeAfterHello?: { readonly code: number; readonly reason: string } | undefined;
  /** Message limit announced in the welcome; defaults to 1 MiB. */
  readonly maxMessageBytes?: number | undefined;
  /** Protocol name in the welcome; defaults to `rex.control.v1`. */
  readonly protocol?: string | undefined;
}

/** The running test server. */
export interface IRexTestServer {
  /** Path of the server's Unix socket; pass it as the SDK's `server` option. */
  readonly socketPath: string;
  /** Sets the handler for a method, replacing any earlier one. */
  readonly handle: (method: string, handler: RexTestServerHandler) => void;
  /** Every request received so far, in arrival order. */
  readonly requests: () => ReadonlyArray<RexTestServerRequest>;
  /** Requests received for one method. */
  readonly requestsFor: (method: string) => ReadonlyArray<RexTestServerRequest>;
  /** Open control connections, oldest first. */
  readonly connections: () => ReadonlyArray<RexTestServerConnection>;
  /** Every control connection ever opened, oldest first. */
  readonly connectionCount: () => number;
  /** Sends an event on every open control connection. */
  readonly emit: (event: { readonly type: string; readonly payload?: unknown }) => void;
  /** Contract violations: requests or scripted results that don't match the schemas. */
  readonly violations: () => ReadonlyArray<string>;
  /** Serves a data stream for a block; `data.connect` issues tickets for it. */
  readonly serveData: (stream: RexTestServerDataStream) => void;
  /** Data handshakes received, as raw bytes, in arrival order. */
  readonly dataHandshakes: () => ReadonlyArray<Uint8Array>;
  /** Sends more bytes on every open data connection of a block. */
  readonly sendData: (blockId: string, chunk: Uint8Array) => void;
  /** Closes every open data connection of a block. */
  readonly closeData: (blockId: string, code: number, reason: string) => void;
  /** Drops every open data connection of a block without a close frame. */
  readonly terminateData: (blockId: string) => void;
  /**
   * Issues a byte-stream ticket for `ref`, as `download_begin` or `upload_begin` would; a scripted
   * handler returns it. `peer` runs when the client opens the stream.
   */
  readonly issueBytesTicket: (ref: string, peer: RexTestServerBytesPeer) => string;
  /**
   * Serves byte streams for a block through `data.connect` with `rex.data.bytes.v1`, as a tunnel
   * does: `peerFor` gets each requested data reference and returns its peer.
   */
  readonly serveBytes: (blockId: string, peerFor: (ref: string) => RexTestServerBytesPeer) => void;
  /** Every byte stream opened so far, oldest first. */
  readonly bytesStreams: () => ReadonlyArray<RexTestServerBytesRecord>;
}

const JsonSchemaObject = Schema.Record(Schema.String, Schema.Json);

const ApiDocument = Schema.Struct({
  methods: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      input_schema: Schema.optionalKey(JsonSchemaObject),
      output_schema: Schema.optionalKey(JsonSchemaObject),
    }),
  ),
  block_types: Schema.Array(
    Schema.Struct({
      creator: Schema.String,
      methods: Schema.Array(
        Schema.Struct({
          invoke: Schema.Struct({ method: Schema.String }),
          args_schema: JsonSchemaObject,
          output_schema: JsonSchemaObject,
        }),
      ),
    }),
  ),
});

const decodeApiDocument = Schema.decodeUnknownSync(Schema.fromJsonString(ApiDocument));

/**
 * Gives a schema its own `$id` and keeps only the first of identical repeated `$anchor`s.
 *
 * The server inlines the same anchored definition (such as `view-layout-node`) at several
 * places in one schema; JSON Schema requires an anchor to be unique within its resource.
 */
function standaloneJsonSchema(id: string, schema: { readonly [key: string]: Schema.Json }) {
  const seen = new Set<string>();
  const strip = (value: Schema.Json): Schema.Json => {
    if (Array.isArray(value)) return value.map(strip);
    if (value === null || typeof value !== "object") return value;
    const entries = Object.entries(value).flatMap(([key, child]): Array<[string, Schema.Json]> => {
      if (key === "$anchor" && typeof child === "string") {
        if (seen.has(child)) return [];
        seen.add(child);
      }
      return [[key, strip(child)]];
    });
    return Object.fromEntries(entries);
  };
  return {
    ...Object.fromEntries(Object.entries(schema).map(([key, value]) => [key, strip(value)])),
    $id: id,
  };
}

interface MethodValidators {
  readonly input: ValidateFunction | undefined;
  readonly output: ValidateFunction | undefined;
}

function loadValidators(): ReadonlyMap<string, MethodValidators> {
  const document = decodeApiDocument(
    readFileSync(join(import.meta.dirname, "../../schema/rex-api.json"), "utf8"),
  );
  const ajv = new Ajv2020({ strict: false, allErrors: true });
  ajv.addFormat("date-time", true);
  const validators = new Map<string, MethodValidators>();
  for (const method of document.methods) {
    validators.set(method.name, {
      input:
        method.input_schema === undefined
          ? undefined
          : ajv.compile(standaloneJsonSchema(`rex:${method.name}:input`, method.input_schema)),
      output:
        method.output_schema === undefined
          ? undefined
          : ajv.compile(standaloneJsonSchema(`rex:${method.name}:output`, method.output_schema)),
    });
  }
  for (const blockType of document.block_types) {
    for (const method of blockType.methods) {
      validators.set(method.invoke.method, {
        input: ajv.compile({
          $id: `rex:${method.invoke.method}:input`,
          type: "object",
          properties: {
            session_id: { type: "string" },
            block_id: { type: "string" },
            args: method.args_schema,
          },
          required: ["session_id", "block_id"],
          additionalProperties: false,
        }),
        output: ajv.compile(
          standaloneJsonSchema(`rex:${method.invoke.method}:output`, method.output_schema),
        ),
      });
    }
  }
  return validators;
}

let cachedValidators: ReadonlyMap<string, MethodValidators> | undefined;

const ControlMessage = Schema.Struct({
  type: Schema.String,
  id: Schema.optionalKey(Schema.String),
  method: Schema.optionalKey(Schema.String),
  payload: Schema.optionalKey(Schema.Json),
});
const decodeControlMessage = Schema.decodeUnknownOption(Schema.fromJsonString(ControlMessage));

const decodeDataConnectPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    block_id: Schema.String,
    data_ref_id: Schema.String,
    subprotocol: Schema.optionalKey(Schema.String),
  }),
);

interface BytesStreamRecord {
  readonly ref: string;
  readonly received: Array<Uint8Array>;
  closed: { readonly code: number; readonly reason: string } | undefined;
}

interface DataConnectionState {
  readonly stream: RexTestServerDataStream;
  readonly sockets: Set<WebSocket>;
  readonly sent: Array<Uint8Array>;
  readonly token: Uint8Array;
}

/**
 * Starts a test server for the current Scope and stops it when the Scope closes.
 *
 * @category constructors
 * @since 0.1.0
 */
export function startRexTestServer(options: RexTestServerOptions = {}) {
  return Effect.acquireRelease(
    Effect.callback<IRexTestServer & { readonly stop: () => Promise<void> }>((resume) => {
      const validators = (cachedValidators ??= loadValidators());
      const directory = mkdtempSync("/tmp/rex-sdk-");
      const socketPath = join(directory, "server.sock");
      const handlers = new Map<string, RexTestServerHandler>();
      const requests: Array<RexTestServerRequest> = [];
      const violations: Array<string> = [];
      const connections = new Map<number, RexTestServerConnection>();
      const dataStreams = new Map<string, DataConnectionState>();
      const tickets = new Map<
        string,
        | { readonly kind: "terminal"; readonly blockId: string }
        | { readonly kind: "bytes"; readonly ref: string; readonly peer: RexTestServerBytesPeer }
      >();
      const bytesBlocks = new Map<string, (ref: string) => RexTestServerBytesPeer>();
      const bytesStreams: Array<BytesStreamRecord> = [];
      const newTicket = () => {
        const ticketBytes = Buffer.alloc(16);
        ticketBytes.writeUInt32LE(nextTicket++, 0);
        return ticketBytes.toString("base64url");
      };
      const dataHandshakes: Array<Uint8Array> = [];
      let nextConnectionId = 1;
      let nextTicket = 1;

      const httpServer = createServer((_request, response) => {
        response.writeHead(404).end("404 page not found");
      });
      const controlServer = new WebSocketServer({
        noServer: true,
        handleProtocols: (protocols) =>
          protocols.has("rex.control.v1") ? "rex.control.v1" : false,
      });
      const dataServer = new WebSocketServer({
        noServer: true,
        handleProtocols: (protocols) => {
          const [first] = protocols;
          return first ?? false;
        },
      });

      httpServer.on("upgrade", (request: IncomingMessage, socket: Duplex, head: Buffer) => {
        if (request.url === "/api/control") {
          controlServer.handleUpgrade(request, socket, head, (ws) => acceptControl(ws));
        } else if (request.url === "/api/data") {
          dataServer.handleUpgrade(request, socket, head, (ws) => acceptData(ws));
        } else {
          socket.destroy();
        }
      });

      function validate(
        method: string,
        side: "input" | "output",
        value: unknown,
      ): string | undefined {
        const validator = validators.get(method)?.[side];
        if (validator === undefined || validator(value)) return undefined;
        return `${method} ${side}: ${JSON.stringify(validator.errors)}`;
      }

      function acceptControl(ws: WebSocket) {
        if (ws.protocol !== "rex.control.v1") {
          ws.close(1002, "protocol error");
          return;
        }
        const id = nextConnectionId++;
        let seq = 0;
        let hello: unknown;
        const connection: RexTestServerConnection = {
          id,
          get hello() {
            return hello;
          },
          emit: (event) => {
            seq += 1;
            ws.send(JSON.stringify({ type: "event", seq, payload: event }));
          },
          sendRaw: (text) => ws.send(text),
          close: (code, reason) => ws.close(code, reason),
          terminate: () => ws.terminate(),
        };
        ws.on("close", () => connections.delete(id));
        ws.on("message", (data, isBinary) => {
          if (isBinary) {
            ws.close(1003, "protocol error");
            return;
          }
          const text = rawDataBytes(data).toString("utf8");
          const message = decodeControlMessage(text);
          if (message._tag === "None") {
            ws.close(1002, "protocol error");
            return;
          }
          const value = message.value;
          if (hello === undefined) {
            if (value.type !== "hello") {
              ws.close(1002, "protocol error");
              return;
            }
            hello = JSON.parse(text);
            connections.set(id, connection);
            if (options.closeAfterHello !== undefined) {
              ws.close(options.closeAfterHello.code, options.closeAfterHello.reason);
              return;
            }
            const apiRevision = options.apiRevision ?? 19;
            ws.send(
              JSON.stringify({
                type: "welcome",
                protocol: options.protocol ?? "rex.control.v1",
                limits: { max_message_bytes: options.maxMessageBytes ?? 1_048_576 },
                client: {
                  id: `client:test-${id}`,
                  reconnect_token: `token-${id}`,
                  resumed: false,
                },
                api_revision: apiRevision,
                api_server_revision: apiRevision,
              }),
            );
            return;
          }
          if (value.type !== "request" || value.id === undefined || value.method === undefined) {
            if (value.type === "request") ws.close(1002, "protocol error");
            return;
          }
          const request: RexTestServerRequest = {
            connectionId: id,
            id: value.id,
            method: value.method,
            payload: value.payload,
          };
          requests.push(request);
          const reply = (body: object) =>
            ws.send(JSON.stringify({ type: "response", id: request.id, seq, ...body }));
          const inputViolation = validators.has(request.method)
            ? validate(request.method, "input", request.payload ?? {})
            : undefined;
          if (inputViolation !== undefined) {
            violations.push(inputViolation);
            reply({ error: { code: "bad_request", message: inputViolation, status: 400 } });
            return;
          }
          const handler = handlers.get(request.method) ?? builtInHandler(request.method);
          if (handler === undefined) {
            reply({
              error: {
                code: "method_not_found",
                message: `no handler for method "${request.method}"`,
                status: 404,
              },
            });
            return;
          }
          void Effect.runPromise(handler(request.payload, connection)).then((result) => {
            if (result instanceof RexTestServerErrorReply) {
              reply({
                error: { code: result.code, message: result.message, status: result.status },
              });
              return;
            }
            const outputViolation = validate(request.method, "output", result);
            if (outputViolation !== undefined) violations.push(outputViolation);
            reply({ payload: result });
          });
        });
      }

      function builtInHandler(method: string): RexTestServerHandler | undefined {
        if (method !== "data.connect") return undefined;
        return (payload) =>
          Effect.sync(() => {
            const request = decodeDataConnectPayload(payload);
            if (request._tag === "None") return rexTestServerError("bad_request", "bad payload");
            const blockId = request.value.block_id;
            if (request.value.subprotocol === "rex.data.bytes.v1") {
              const peerFor = bytesBlocks.get(blockId);
              if (peerFor === undefined) return rexTestServerError("not_found", "block not found");
              const ref = request.value.data_ref_id;
              const ticket = newTicket();
              tickets.set(ticket, { kind: "bytes", ref, peer: peerFor(ref) });
              return { ticket, expires_in_ms: 60_000 };
            }
            if (!dataStreams.has(blockId))
              return rexTestServerError("not_found", "block not found");
            const ticket = newTicket();
            tickets.set(ticket, { kind: "terminal", blockId });
            return { ticket, expires_in_ms: 60_000 };
          });
      }

      function acceptData(ws: WebSocket) {
        ws.once("message", (data) => {
          const bytes = new Uint8Array(rawDataBytes(data));
          dataHandshakes.push(bytes);
          const reject = (reason: number) => {
            ws.send(Uint8Array.of(0x82, reason));
            ws.close(1008, "data handshake rejected");
          };
          if (bytes[0] === 0x01 && bytes.length >= 17) {
            const ticket = Buffer.from(bytes.slice(1, 17)).toString("base64url");
            const issued = tickets.get(ticket);
            tickets.delete(ticket);
            const wantsBytes = ws.protocol === "rex.data.bytes.v1";
            if (issued !== undefined && (issued.kind === "bytes") !== wantsBytes)
              return reject(0x07);
            if (issued?.kind === "bytes") return openBytes(ws, issued.ref, issued.peer);
            const state = issued === undefined ? undefined : dataStreams.get(issued.blockId);
            if (state === undefined) return reject(0x01);
            if (state.stream.rejectWith !== undefined) return reject(state.stream.rejectWith);
            // Rex allows one data connection per client and block: a second is `already_connected`.
            if (state.sockets.size > 0) return reject(0x02);
            state.sent.length = 0;
            welcome(ws, state, 0);
            for (const chunk of state.stream.chunks) send(ws, state, chunk);
            if (state.stream.closeWith !== undefined) {
              ws.close(state.stream.closeWith.code, state.stream.closeWith.reason);
            }
            return;
          }
          if (bytes[0] === 0x02 && bytes.length >= 25) {
            const token = bytes.slice(1, 17);
            const received = Number(Buffer.from(bytes.slice(17, 25)).readBigUInt64LE(0));
            const state = [...dataStreams.values()].find((candidate) =>
              Buffer.from(candidate.token).equals(Buffer.from(token)),
            );
            if (state === undefined) return reject(0x01);
            const all = Buffer.concat(state.sent);
            if (received > all.length) return reject(0x05);
            welcome(ws, state, 0);
            const rest = all.subarray(received);
            if (rest.length > 0) ws.send(rest);
            return;
          }
          reject(0x05);
        });
      }

      function openBytes(ws: WebSocket, ref: string, peer: RexTestServerBytesPeer) {
        const message = Buffer.alloc(32);
        message[0] = 0x81;
        message.writeUInt32LE(60_000, 20);
        message.writeBigUInt64LE(1n, 24);
        ws.send(message);
        const record: BytesStreamRecord = { ref, received: [], closed: undefined };
        bytesStreams.push(record);
        ws.on("message", (data) => record.received.push(new Uint8Array(rawDataBytes(data))));
        ws.on("close", (code, reason) => {
          record.closed = { code, reason: reason.toString() };
        });
        peer({
          ref,
          send: (chunk) => ws.send(chunk, { binary: true }),
          close: (code, reason) => ws.close(code, reason),
          onMessage: (listener) =>
            ws.on("message", (data) => listener(new Uint8Array(rawDataBytes(data)))),
        });
      }

      function welcome(ws: WebSocket, state: DataConnectionState, _offset: number) {
        const message = Buffer.alloc(32);
        message[0] = 0x81;
        Buffer.from(state.token).copy(message, 1);
        message.writeUInt32LE(60_000, 20);
        message.writeBigUInt64LE(state.stream.nonce, 24);
        ws.send(message);
        state.sockets.add(ws);
        ws.on("close", () => state.sockets.delete(ws));
      }

      function send(ws: WebSocket, state: DataConnectionState, chunk: Uint8Array) {
        state.sent.push(chunk);
        ws.send(chunk);
      }

      const server: IRexTestServer & { readonly stop: () => Promise<void> } = {
        socketPath,
        handle: (method, handler) => {
          handlers.set(method, handler);
        },
        requests: () => [...requests],
        requestsFor: (method) => requests.filter((request) => request.method === method),
        connections: () => [...connections.values()],
        connectionCount: () => nextConnectionId - 1,
        emit: (event) => {
          for (const connection of connections.values()) connection.emit(event);
        },
        violations: () => [...violations],
        serveData: (stream) => {
          dataStreams.set(stream.blockId, {
            stream,
            sockets: new Set(),
            sent: [],
            token: new Uint8Array(randomBytes(16)),
          });
        },
        dataHandshakes: () => [...dataHandshakes],
        sendData: (blockId, chunk) => {
          const state = dataStreams.get(blockId);
          if (state === undefined) return;
          state.sent.push(chunk);
          for (const socket of state.sockets) socket.send(chunk);
        },
        closeData: (blockId, code, reason) => {
          for (const socket of dataStreams.get(blockId)?.sockets ?? []) socket.close(code, reason);
        },
        terminateData: (blockId) => {
          for (const socket of dataStreams.get(blockId)?.sockets ?? []) socket.terminate();
        },
        issueBytesTicket: (ref, peer) => {
          const ticket = newTicket();
          tickets.set(ticket, { kind: "bytes", ref, peer });
          return ticket;
        },
        serveBytes: (blockId, peerFor) => {
          bytesBlocks.set(blockId, peerFor);
        },
        bytesStreams: () =>
          bytesStreams.map((record) => ({
            ref: record.ref,
            received: [...record.received],
            closed: record.closed,
          })),
        stop: () =>
          new Promise<void>((resolve) => {
            for (const client of controlServer.clients) client.terminate();
            for (const client of dataServer.clients) client.terminate();
            controlServer.close();
            dataServer.close();
            httpServer.close(() => {
              rmSync(directory, { recursive: true, force: true });
              resolve();
            });
          }),
      };

      httpServer.listen(socketPath, () => resume(Effect.succeed(server)));
    }),
    (server) => Effect.promise(() => server.stop()),
  );
}
