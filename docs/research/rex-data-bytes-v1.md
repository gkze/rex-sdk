# `rex.data.bytes.v1` (observed contract)

Rex's protocol guide (`rex-api-revision-19.md`) names this subprotocol but never defines it. This
page is assembled from three sources and is **evidence, not a server snapshot**: recheck it on
every Rex upgrade.

Rechecked against Rex `0.1.0-preview.22` (API 19): the guide's text about this subprotocol is
unchanged from revision 18, and the live suite's download, paste, and forward tests pass.

| Source                                                                                                                       | Covers                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| **Probe**: a raw WebSocket client against Rex `0.1.0-preview.21` (API 18, macOS), Unix socket, scratch session, 2026-11-23     | Download, upload, batch upload, dial and listen tunnels, half-close, abort |
| **Changelog**: the API changelog embedded in the `rex` binary (same build)                                                   | The tunnel contract, quoted below                                          |
| **rex-rune D0**: [shuv1337/rex-rune `docs/server-api13.md`](https://github.com/shuv1337/rex-rune/blob/master/docs/server-api13.md), Linux, API 13 | Exhaustive download edge cases: limits, tickets, path errors, files that change   |

## One stream, three uses

A bytes stream is a byte pipe over the ordinary data connection: `GET /api/data`, subprotocol
`rex.data.bytes.v1`, the same HELLO (`0x01` + 16 ticket bytes), WELCOME (32 bytes) and REJECT
(`0x82` + reason) as the terminal stream. **The data messages carry raw bytes, with no header or
length prefix.** The only framing is:

- **An empty binary message is a half-close**: "no more bytes from me." It is how a download
  ends, how an upload ends, and a TCP half-close on a tunnel. (Server message: "server sent
  bytes after its half-close.")
- **Close 1000 after both directions have ended is success.** Any other close, or a connection
  that ends without a close, is failure, whatever bytes arrived.
- **A client may abort** by closing with any code; on a tunnel this resets the TCP connection.
- **Streams cannot be resumed.** RESUME gets REJECT `0x03` (rex-rune). There is no reconnect.

| Use       | Ref (`data_ref_id`)             | Ticket from                                   | Direction                       |
| --------- | ------------------------------- | --------------------------------------------- | ------------------------------- |
| Download  | `download:<uuid>` (client picks) | `com.superlogical.terminal.download_begin`    | server → client                 |
| Upload    | `upload:<uuid>` (client picks)   | `com.superlogical.terminal.upload_begin`      | client → server                 |
| Dial      | `conn:<name>` (client picks)     | `data.connect` on a `tunnel.dial` block       | both ways, one TCP connection   |
| Listen    | `conn:<…>` (server picks)        | `data.connect` with the ref from `connection_requested` | both ways, one TCP connection |

UUIDs must be lower-case canonical; `ref "download:NOT-A-UUID" does not end with a UUID in
canonical form` is `bad_request`.

## Download

```text
client → HELLO        01 <16 ticket bytes>
server → WELCOME      81 …                      (32 bytes)
server → data         ≤ 32,768 bytes each       (probe: 32768 ×3, 1696 for a 100,000-byte file)
server → end          empty binary message
server → close        1000, empty reason
```

- `download_begin` answers `{ticket, expires_in_ms: 60000, path, name, size, modified_at, version?}`.
  With `if_none_match` equal to the current `version`, it answers `not_modified: true` and **no
  ticket** (probe). An empty file is WELCOME, one empty message, close 1000 (probe).
- **Complete only with the empty message and then close 1000.** A client that closes early also
  gets back a 1000 close (its own), with no empty message (probe), so "1000" alone never means complete.
- `size` is a hint: the server reads to end of file at read time, so a file that grows or shrinks
  ends with 1000 and a different length; one appended to faster than it is read never ends
  (rex-rune). A client needs its own byte cap and stall timer; the server never pings.
- Relative paths resolve against the foreground process's working directory. When the terminal
  hasn't reported one (for example `shell: "none"`), they fail: `bad_request` "working directory
  is unknown, so give an absolute path" (probe).
- Errors: missing file `not_found`; every other refusal (directory, permission, FIFO, device)
  `bad_request` with an English message (rex-rune).
- Limits (rex-rune): 16 reserved or running downloads per terminal (the 17th is `conflict`).
  An unopened ticket holds the open file and a slot for 60 s; opening it and closing at once
  frees both. Closing the control connection that issued a ticket revokes it, and ends a running
  stream with 1001 `data provider canceled`. Block closed mid-stream: 4000 `block closed`.
  Read error: 1011 `data provider failed`.
- `name` comes back verbatim, control characters and all (rex-rune): never trust it as a file name.

## Upload

```text
client → HELLO
server → WELCOME
client → data         exactly `size` bytes, in messages of up to 64 KiB
client → end          empty binary message
server → close        1000 on success
```

- `upload_begin {ref, name, size, batch?, index?, count?}` answers `{ticket, expires_in_ms}`.
- Sending fewer bytes than `size` before the empty message: close **4006 `data stream broke a
  rule`** (probe).
- **Side effect: the server types the stored file's path into the terminal**, shell-escaped, as
  if the user had pasted it, once every file of the batch has arrived. Probe: a terminal running
  `cat` received `/Users/…/Library/Application\ Support/rex/uploads/<id>/<block_id>/<uuid>/probe\ upload.bin`.
  Upload is built for drag-and-drop into a shell prompt, not for silently placing a file.
- Each file in a batch closes 1000 as soon as its own bytes arrive; the paste waits for the batch
  (probe).
- Stored files live in the server's upload directory, under the block. **Destroying the session
  deleted them** (probe). There is no method that returns the stored path; the paste is the only
  report of it.

## Tunnels

From the embedded changelog:

> The new block creator `com.superlogical.tunnel` forwards TCP connections between the machine
> running the block and its owner, one connection per data stream. Each stream uses subprotocol
> `rex.data.bytes.v1` and a ref of the form `conn:<name>`. Only the block's owner may open its
> streams. A half-close on the stream is a TCP half-close, and a stream that ends with an error
> resets the connection. … `dial {host, port, client_address}` works like `ssh -L`: for each
> stream the owner opens, with a name it chooses, the block connects to `host:port`.
> `listen {address, port, client_target}` works like `ssh -R`: the block listens … and for each
> connection it accepts it sends a `connection_requested {ref, peer, client}` event, where
> `client` is the owner. Every client in the session sees the event, so only the one it names
> should answer. The owner opens that ref with `data.connect` within 30 seconds and connects the
> stream onward itself.

Observed (probe):

- **Dial**: `data.connect {data_ref_id: "conn:probe-1", subprotocol: "rex.data.bytes.v1"}`, send
  `hello`, then an empty message. The target received the bytes, then a FIN; its reply
  `echo:hello` came back, then an empty message, then close 1000.
- **Dial to a closed port**: WELCOME, then close **1001 `data provider canceled`**, no data.
- **A ref without `conn:`**: WELCOME, then close **1008 `data provider unavailable`**.
- **Listen**: a TCP connection to the tunnel's address produced `connection_requested` with
  `ref: "conn:E53Z…"`, `peer`, and `client` (our client ID). Bytes flowed both ways; the outside
  peer's FIN arrived as an empty message, and our empty message reached it as a FIN; then close 1000.
- **Abort**: closing the stream with 4001 reset the outside connection (`ECONNRESET`).
- A tunnel loses its owner as soon as the owner's control connection drops (`tunnel.info`
  description), so streams depend on that one connection staying up.

## Handshake failures

| Case                                              | Result                                  | Source  |
| ------------------------------------------------- | --------------------------------------- | ------- |
| Terminal ticket (`data.connect … terminal`) on `rex.data.bytes.v1` | REJECT `0x07`, close 1008 `data handshake rejected` | probe |
| Unknown, used, expired, or revoked ticket         | REJECT `0x01`                           | rex-rune |
| RESUME                                             | REJECT `0x03`                           | rex-rune |
| Malformed HELLO, or none within 5 s               | REJECT `0x05`, close 1002               | rex-rune |

## Not verified

- HTTPS, Tailscale, and TCP endpoints (only Unix sockets were probed, here and by rex-rune).
- Back-pressure: whether the server slows a tunnel target when the client stops reading.
- Upload limits (size cap, free-space check: the binary has "reading free space for uploads"),
  and how long stored uploads live while the block runs.
- How many tunnel streams a block allows at once.
