# ZMTP Handshake Lifecycle - Complete Implementation Guide

This document provides a detailed specification of the ZeroMQ Message Transport Protocol (ZMTP) handshake lifecycle as implemented in libzmq. It contains all the information necessary to reimplement the handshake from scratch.

## Table of Contents

1. [Overview](#overview)
2. [ZMTP Greeting Exchange](#zmtp-greeting-exchange)
3. [ZMTP Frame Encoding](#zmtp-frame-encoding)
4. [Security Mechanisms](#security-mechanisms)
   - [NULL Mechanism](#null-mechanism)
   - [PLAIN Mechanism](#plain-mechanism)
   - [CURVE Mechanism](#curve-mechanism)
5. [ZAP Authentication Protocol](#zap-authentication-protocol)
6. [Metadata Property Format](#metadata-property-format)
7. [Post-Handshake Message Encryption](#post-handshake-message-encryption-curve)
8. [State Machine Diagrams](#state-machine-diagrams)
9. [Error Handling](#error-handling)

---

## Overview

The ZMTP handshake consists of two phases:

1. **Greeting Exchange**: Version negotiation and mechanism selection
2. **Mechanism Handshake**: Security handshake specific to the chosen mechanism

After successful handshake completion, the connection enters the normal message flow phase.

### Protocol Versions

| Version | Major | Minor | Description |
|---------|-------|-------|-------------|
| ZMTP/1.0 | 0 | N/A | Legacy unversioned |
| ZMTP/2.0 | 1 | N/A | Basic versioning |
| ZMTP/3.0 | 3 | 0 | Security mechanisms |
| ZMTP/3.1 | 3 | 1 | Current version |

---

## ZMTP Greeting Exchange

### Greeting Structure

The greeting is exchanged simultaneously by both peers. The structure differs based on protocol version.

#### ZMTP/3.x Greeting (64 bytes)

```
Offset  Size  Field                 Description
------  ----  -----                 -----------
0       1     signature[0]          Always 0xFF
1       8     signature[1-8]        Padding (for ZMTP/1.0 compatibility)
9       1     signature[9]          Flags: bit 0 set = versioned protocol
10      1     major version         Protocol major version (3 for ZMTP/3.x)
11      1     minor version         Protocol minor version (0 or 1)
12      20    mechanism             ASCII mechanism name, null-padded
32      1     as-server             Server role flag (1=server, 0=client)
33      31    filler                Reserved, must be zeros
------  ----
Total:  64 bytes
```

#### Initial Signature (sent before version detection)

Both peers begin by sending the first 10 bytes simultaneously:

```
Bytes 0-9 (signature):
  [0]     = 0xFF
  [1-8]   = Padding (encoding: 8-byte big-endian length for ZMTP/1.0 routing ID frame)
  [9]     = 0x7F (flags: indicates versioned protocol when bit 0 is set)
```

This signature is designed to be distinguishable from:
- ZMTP/1.0 unversioned messages (first byte != 0xFF)
- ZMTP/1.0 routing ID frame headers (byte 9, bit 0 not set)

#### Version Detection Algorithm

```
1. Read first byte from peer
   - If byte != 0xFF: peer is using ZMTP/1.0 unversioned
   - If byte == 0xFF: continue reading

2. Read bytes 1-9 from peer
   - Check byte 9, bit 0 (0x01)
   - If bit 0 not set: peer is using ZMTP/1.0 with routing ID frame
   - If bit 0 set: peer is using versioned protocol, continue

3. Send major version number (byte 10)
   - Send value 3 for ZMTP/3.x

4. Read peer's major version (byte 10)
   - Value 0: ZMTP/1.0
   - Value 1: ZMTP/2.0
   - Value 3: ZMTP/3.x

5. For ZMTP/3.x, send remaining greeting:
   - Minor version (byte 11): 0 or 1
   - Mechanism name (bytes 12-31): "NULL", "PLAIN", "CURVE", or "GSSAPI"
   - As-server flag (byte 32): role indicator
   - Filler (bytes 33-63): zeros
```

#### Mechanism Names (20-byte field, null-padded)

| Mechanism | Bytes (hex) |
|-----------|-------------|
| NULL | `4E 55 4C 4C 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00` |
| PLAIN | `50 4C 41 49 4E 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00` |
| CURVE | `43 55 52 56 45 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00` |
| GSSAPI | `47 53 53 41 50 49 00 00 00 00 00 00 00 00 00 00 00 00 00 00` |

---

## ZMTP Frame Encoding

### ZMTP/2.0+ Frame Format

All handshake commands and application messages are sent as frames with this structure:

```
Offset  Size        Field       Description
------  ----        -----       -----------
0       1           flags       Frame flags (see below)
1       1 or 8      size        Message size
1+size  variable    body        Frame payload
```

#### Flags Byte

```
Bit 0 (0x01): MORE    - More frames follow in this message
Bit 1 (0x02): LARGE   - Size field is 8 bytes (otherwise 1 byte)
Bit 2 (0x04): COMMAND - Frame is a command (vs. application message)
```

#### Size Encoding

- If size <= 255: Use 1-byte size encoding (flags bit 1 = 0)
- If size > 255: Use 8-byte big-endian size encoding (flags bit 1 = 1)

### Encoding Example

```
To send a 100-byte READY command:

Flags: 0x04 (COMMAND flag set, LARGE flag clear)
Size:  0x64 (100 in decimal)
Body:  [100 bytes of command data]

Wire bytes: 04 64 [100 bytes...]

To send a 300-byte message:

Flags: 0x02 (LARGE flag set)
Size:  00 00 00 00 00 00 01 2C (300 in big-endian)
Body:  [300 bytes of data]

Wire bytes: 02 00 00 00 00 00 00 01 2C [300 bytes...]
```

---

## Security Mechanisms

### Command Name Encoding

All mechanism commands begin with a length-prefixed name:

```
[name_length: 1 byte][name: name_length bytes]

Examples:
  HELLO    = "\x05HELLO"    (0x05 0x48 0x45 0x4C 0x4C 0x4F)
  WELCOME  = "\x07WELCOME"  (0x07 0x57 0x45 0x4C 0x43 0x4F 0x4D 0x45)
  INITIATE = "\x08INITIATE" (0x08 0x49 0x4E 0x49 0x54 0x49 0x41 0x54 0x45)
  READY    = "\x05READY"    (0x05 0x52 0x45 0x41 0x44 0x59)
  ERROR    = "\x05ERROR"    (0x05 0x45 0x52 0x52 0x4F 0x52)
```

---

### NULL Mechanism

The NULL mechanism provides no authentication or encryption. Both peers exchange READY commands.

#### State Machine

```
                     [Start]
                        |
                        v
         +----[ZAP Required?]----+
         |                       |
        Yes                      No
         |                       |
         v                       |
   Send ZAP Request              |
         |                       |
         v                       |
   Wait for ZAP Reply            |
         |                       |
         v                       |
   [ZAP Status 200?]             |
         |                       |
    No   |   Yes                 |
    |    |                       |
    v    +-------+---------------+
Send ERROR       |
    |            v
    |      Send READY
    |            |
    v            v
   [Error]  Wait for READY
                 |
        +--------+--------+
        |                 |
   Received           Received
     READY              ERROR
        |                 |
        v                 v
     [Ready]          [Error]
```

#### READY Command Format

```
Offset  Size      Field           Description
------  ----      -----           -----------
0       6         command name    "\x05READY"
6       variable  metadata        Property list (see Metadata section)
```

#### ERROR Command Format

```
Offset  Size          Field         Description
------  ----          -----         -----------
0       6             command name  "\x05ERROR"
6       1             reason_len    Length of error reason
7       reason_len    reason        ASCII status code ("300", "400", "500")
```

#### Wire Protocol Sequence (NULL)

**Without ZAP:**
```
Client                              Server
   |                                   |
   |-------- Greeting --------------->|
   |<-------- Greeting ---------------|
   |                                   |
   |-------- READY ------------------>|
   |<-------- READY ------------------|
   |                                   |
   [Handshake Complete]
```

**With ZAP (server side):**
```
Server                              ZAP Handler
   |                                   |
   |-------- ZAP Request ------------->|
   |<-------- ZAP Reply ---------------|
   |                                   |
   [Then send READY or ERROR to client]
```

---

### PLAIN Mechanism

The PLAIN mechanism provides username/password authentication without encryption.

#### Client States

```
enum client_state {
    sending_hello,           // Initial state
    waiting_for_welcome,     // After sending HELLO
    sending_initiate,        // After receiving WELCOME
    waiting_for_ready,       // After sending INITIATE
    error_command_received,  // Received ERROR
    ready                    // Handshake complete
};
```

#### Server States

```
enum server_state {
    waiting_for_hello,       // Initial state
    sending_welcome,         // After validating HELLO
    waiting_for_initiate,    // After sending WELCOME
    waiting_for_zap_reply,   // After sending ZAP request
    sending_ready,           // After ZAP success
    sending_error,           // After ZAP failure
    error_sent,              // After sending ERROR
    ready                    // Handshake complete
};
```

#### HELLO Command (Client -> Server)

```
Offset  Size            Field         Description
------  ----            -----         -----------
0       6               command name  "\x05HELLO"
6       1               username_len  Length of username (0-255)
7       username_len    username      Username bytes
7+N     1               password_len  Length of password (0-255)
8+N     password_len    password      Password bytes
```

#### WELCOME Command (Server -> Client)

```
Offset  Size  Field         Description
------  ----  -----         -----------
0       8     command name  "\x07WELCOME"
```

Note: WELCOME is exactly 8 bytes with no additional data.

#### INITIATE Command (Client -> Server)

```
Offset  Size      Field         Description
------  ----      -----         -----------
0       9         command name  "\x08INITIATE"
9       variable  metadata      Property list (Socket-Type, Identity, etc.)
```

#### READY Command (Server -> Client)

```
Offset  Size      Field         Description
------  ----      -----         -----------
0       6         command name  "\x05READY"
6       variable  metadata      Property list (Socket-Type, User-Id, etc.)
```

#### ERROR Command

Same format as NULL mechanism ERROR command.

#### Wire Protocol Sequence (PLAIN)

```
Client                              Server                              ZAP Handler
   |                                   |                                   |
   |-------- Greeting --------------->|                                   |
   |<-------- Greeting ---------------|                                   |
   |                                   |                                   |
   |-------- HELLO ------------------>|                                   |
   |          (username+password)      |-------- ZAP Request ------------->|
   |                                   |<-------- ZAP Reply ---------------|
   |<-------- WELCOME ----------------|                                   |
   |                                   |                                   |
   |-------- INITIATE --------------->|                                   |
   |          (metadata)               |                                   |
   |<-------- READY ------------------|                                   |
   |          (metadata)               |                                   |
   |                                   |                                   |
   [Handshake Complete]
```

---

### CURVE Mechanism

The CURVE mechanism provides strong encryption and authentication using elliptic curve cryptography (Curve25519).

#### Cryptographic Constants

```c
crypto_box_PUBLICKEYBYTES  = 32   // Public key size
crypto_box_SECRETKEYBYTES  = 32   // Secret key size
crypto_box_NONCEBYTES      = 24   // Nonce size
crypto_box_ZEROBYTES       = 32   // Plaintext padding
crypto_box_BOXZEROBYTES    = 16   // Ciphertext padding
crypto_box_MACBYTES        = 16   // Authentication tag size
crypto_secretbox_KEYBYTES  = 32   // Symmetric key size
crypto_secretbox_NONCEBYTES = 24  // Symmetric nonce size
```

#### Key Notation

- **C**: Client's permanent public key
- **c**: Client's permanent secret key
- **S**: Server's permanent public key
- **s**: Server's permanent secret key
- **C'**: Client's ephemeral (short-term) public key
- **c'**: Client's ephemeral secret key
- **S'**: Server's ephemeral public key
- **s'**: Server's ephemeral secret key

#### Client States

```
enum client_state {
    send_hello,      // Initial state
    expect_welcome,  // After sending HELLO
    send_initiate,   // After processing WELCOME
    expect_ready,    // After sending INITIATE
    error_received,  // Received ERROR
    connected        // Handshake complete
};
```

#### Server States

```
enum server_state {
    waiting_for_hello,       // Initial state
    sending_welcome,         // After validating HELLO
    waiting_for_initiate,    // After sending WELCOME
    waiting_for_zap_reply,   // After sending ZAP request
    sending_ready,           // After ZAP success
    sending_error,           // After ZAP failure
    error_sent,              // After sending ERROR
    ready                    // Handshake complete
};
```

#### HELLO Command (200 bytes)

```
Offset  Size  Field                 Description
------  ----  -----                 -----------
0       6     command name          "\x05HELLO"
6       1     version_major         CurveZMQ major version (1)
7       1     version_minor         CurveZMQ minor version (0)
8       72    anti_amplification    Zeros (anti-amplification padding)
80      32    client_public_key     C' (client's ephemeral public key)
112     8     nonce                 Short nonce (lower 8 bytes)
120     80    box                   Box [64 zeros](C'->S)
------  ----
Total: 200 bytes
```

**HELLO Box Encryption:**
```
Nonce:      "CurveZMQHELLO---" + 8-byte short nonce
Plaintext:  64 bytes of zeros (32 bytes zero padding + 32 bytes zeros)
Key pair:   Client's ephemeral secret (c') -> Server's permanent public (S)
```

#### WELCOME Command (168 bytes)

```
Offset  Size  Field           Description
------  ----  -----           -----------
0       8     command name    "\x07WELCOME"
8       16    nonce           Random nonce (short part)
24      144   box             Box [S' + cookie](S->C')
------  ----
Total: 168 bytes
```

**WELCOME Box Contents (128 bytes plaintext):**
```
Offset  Size  Field         Description
------  ----  -----         -----------
0       32    S'            Server's ephemeral public key
32      16    cookie_nonce  Nonce used to encrypt cookie
48      80    cookie        Encrypted cookie: Box [C' + s'](t)
```

**Cookie Encryption:**
```
Nonce:      "COOKIE--" + 16-byte random
Plaintext:  C' (32) + s' (32) = 64 bytes (with 32-byte zero padding)
Key:        Random symmetric key 't' (cookie key, ephemeral)
```

**WELCOME Box Encryption:**
```
Nonce:      "WELCOME-" + 16-byte random nonce
Plaintext:  S' (32) + cookie_nonce (16) + encrypted_cookie (80) = 128 bytes
Key pair:   Server's permanent secret (s) -> Client's ephemeral public (C')
```

#### INITIATE Command (variable size, minimum 257 bytes)

```
Offset  Size          Field           Description
------  ----          -----           -----------
0       9             command name    "\x08INITIATE"
9       16            cookie_nonce    Cookie nonce from WELCOME
25      80            cookie          Encrypted cookie from WELCOME
105     8             nonce           Short nonce
113     128+N+16      box             Box [C + vouch + metadata](C'->S')
------  ----
Minimum: 257 bytes (with empty metadata)
```

**INITIATE Box Contents (128 + N bytes plaintext):**
```
Offset  Size  Field         Description
------  ----  -----         -----------
0       32    C             Client's permanent public key
32      16    vouch_nonce   Nonce used for vouch
48      80    vouch         Box [C' + S](C->S')
128     N     metadata      Property list
```

**Vouch Box Encryption:**
```
Nonce:      "VOUCH---" + 16-byte random
Plaintext:  C' (32) + S (32) = 64 bytes (with zero padding)
Key pair:   Client's permanent secret (c) -> Server's ephemeral public (S')
```

This vouch proves the client knows the private key for C and binds C' to C.

**INITIATE Box Encryption:**
```
Nonce:      "CurveZMQINITIATE" + 8-byte short nonce
Plaintext:  C (32) + vouch_nonce (16) + vouch_box (80) + metadata (N)
Key pair:   Client's ephemeral secret (c') -> Server's ephemeral public (S')
```

#### READY Command (variable size, minimum 30 bytes)

```
Offset  Size      Field           Description
------  ----      -----           -----------
0       6         command name    "\x05READY"
6       8         nonce           Short nonce
14      16+N      box             Box [metadata](S'->C')
------  ----
Minimum: 30 bytes (with empty metadata)
```

**READY Box Encryption:**
```
Nonce:      "CurveZMQREADY---" + 8-byte short nonce
Plaintext:  Metadata property list (N bytes)
Key:        Precomputed shared secret from S' and c'
```

#### Wire Protocol Sequence (CURVE)

```
Client                              Server                              ZAP Handler
   |                                   |                                   |
   |-------- Greeting --------------->|                                   |
   |<-------- Greeting ---------------|                                   |
   |                                   |                                   |
   |-------- HELLO ------------------>|                                   |
   |    (C', Box[64 zeros](C'->S))     |                                   |
   |                                   |                                   |
   |<-------- WELCOME ----------------|                                   |
   |    (Box[S' + cookie](S->C'))      |                                   |
   |                                   |                                   |
   |-------- INITIATE --------------->|                                   |
   |    (cookie, Box[C+vouch+meta])    |-------- ZAP Request ------------->|
   |                                   |         (C = client pub key)       |
   |                                   |<-------- ZAP Reply ---------------|
   |                                   |                                   |
   |<-------- READY ------------------|                                   |
   |    (Box[metadata](S'->C'))        |                                   |
   |                                   |                                   |
   [Handshake Complete - Messages now encrypted with (C'<->S') shared secret]
```

---

## ZAP Authentication Protocol

The ZeroMQ Authentication Protocol (ZAP) allows external handlers to validate connections.

### ZAP Request (7+ frames, multipart message)

```
Frame  Content               Description
-----  -------               -----------
0      ""                    Empty delimiter frame
1      "1.0"                 ZAP version
2      "1"                   Request ID (for matching replies)
3      domain                ZAP domain (from socket option)
4      address               Peer's IP address/credentials
5      routing_id            Socket's routing ID
6      mechanism             Mechanism name: "NULL", "PLAIN", "CURVE"
7+     credentials           Mechanism-specific credentials
```

#### Credentials by Mechanism

| Mechanism | Frame 7 | Frame 8 |
|-----------|---------|---------|
| NULL | (none) | (none) |
| PLAIN | username | password |
| CURVE | client_public_key (32 bytes) | (none) |

### ZAP Reply (7 frames, multipart message)

```
Frame  Content               Description
-----  -------               -----------
0      ""                    Empty delimiter frame
1      "1.0"                 ZAP version
2      "1"                   Request ID (matching request)
3      status_code           "200", "300", "400", or "500"
4      status_text           Human-readable status (optional)
5      user_id               Authenticated user ID (optional)
6      metadata              Additional metadata (optional)
```

### ZAP Status Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | Success | Continue handshake |
| 300 | Temporary failure | Disconnect silently (no ERROR sent) |
| 400 | Authentication failure | Send ERROR command |
| 500 | Internal error | Send ERROR command |

---

## Metadata Property Format

Metadata is exchanged in READY, INITIATE commands as a list of properties.

### Property Entry Structure

```
[name_length: 1 byte]
[name: name_length bytes]
[value_length: 4 bytes, big-endian]
[value: value_length bytes]
```

### Standard Properties

| Property Name | Description | When Sent |
|--------------|-------------|-----------|
| `Socket-Type` | Socket type string (REQ, REP, DEALER, etc.) | Always |
| `Identity` | Routing ID bytes | REQ, DEALER, ROUTER only |
| `User-Id` | Authenticated user identifier | From ZAP reply |

### Socket Type Compatibility Matrix

| This Socket | Compatible With |
|-------------|-----------------|
| REQ | REP, ROUTER |
| REP | REQ, DEALER |
| DEALER | REP, DEALER, ROUTER |
| ROUTER | REQ, DEALER, ROUTER |
| PUB | SUB, XSUB |
| SUB | PUB, XPUB |
| XPUB | SUB, XSUB |
| XSUB | PUB, XPUB |
| PUSH | PULL |
| PULL | PUSH |
| PAIR | PAIR |

### Metadata Parsing Algorithm

```python
def parse_metadata(data):
    properties = {}
    pos = 0
    while pos < len(data):
        # Read property name
        name_len = data[pos]
        pos += 1
        name = data[pos:pos + name_len].decode('ascii')
        pos += name_len

        # Read property value
        value_len = int.from_bytes(data[pos:pos + 4], 'big')
        pos += 4
        value = data[pos:pos + value_len]
        pos += value_len

        properties[name] = value

    return properties
```

---

## Post-Handshake Message Encryption (CURVE)

After CURVE handshake completes, all messages are encrypted using the established shared secret.

### MESSAGE Command Structure

```
Offset  Size      Field           Description
------  ----      -----           -----------
0       8         command name    "\x07MESSAGE"
8       8         nonce           Short nonce (8 bytes)
16      variable  box             Encrypted message content
```

### Message Encryption

```
Nonce prefix (client->server): "CurveZMQMESSAGEC"
Nonce prefix (server->client): "CurveZMQMESSAGES"

Full nonce: [16-byte prefix] + [8-byte short nonce]

Plaintext structure:
  [flags: 1 byte]
  [message data: N bytes]

Flags byte:
  Bit 0: MORE flag
  Bit 2: COMMAND flag
```

### Nonce Management

- Both peers maintain independent nonce counters starting at 1
- Client uses "CurveZMQMESSAGEC" prefix for sending
- Server uses "CurveZMQMESSAGES" prefix for sending
- Each message increments the sender's nonce counter
- Receivers track peer nonce and reject any nonce <= last seen (replay protection)

### Encryption Algorithm

```python
def encrypt_message(plaintext_msg, flags, shared_secret, nonce_counter, prefix):
    # Build nonce
    nonce = prefix + struct.pack('>Q', nonce_counter)

    # Build plaintext: flags + message
    plaintext = bytes([flags]) + plaintext_msg

    # Encrypt using crypto_box_afternm (precomputed shared secret)
    ciphertext = crypto_box_easy_afternm(plaintext, nonce, shared_secret)

    # Build MESSAGE command
    message = b'\x07MESSAGE' + struct.pack('>Q', nonce_counter) + ciphertext

    return message
```

---

## State Machine Diagrams

### Complete ZMTP Engine State Machine

```
                            [Socket Created]
                                   |
                                   v
                        [TCP Connection Established]
                                   |
                                   v
                          +----------------+
                          | PLUG_INTERNAL  |
                          | (start timer)  |
                          +----------------+
                                   |
                                   v
                          +----------------+
                          | HANDSHAKING    |<--------+
                          | receive_greeting|         |
                          +----------------+         |
                                   |                 |
                          [Greeting Complete]        |
                                   |                 |
                    +--------------+--------------+  |
                    |              |              |  |
                    v              v              v  |
              [ZMTP/1.0]     [ZMTP/2.0]     [ZMTP/3.x]
                    |              |              |  |
                    v              v              v  |
              [No security] [No security]  [Create Mechanism]
                    |              |              |  |
                    |              |              v  |
                    |              |     +------------------+
                    |              |     | MECHANISM        |
                    |              |     | HANDSHAKING      |
                    |              |     | (NULL/PLAIN/CURVE)|
                    |              |     +------------------+
                    |              |              |
                    |              |        [mechanism_ready()]
                    |              |              |
                    +--------------+--------------+
                                   |
                                   v
                          +----------------+
                          | READY          |
                          | (normal flow)  |
                          +----------------+
                                   |
                          [encode/decode messages]
                                   |
                    +--------------+--------------+
                    |                             |
                    v                             v
              [Error/Disconnect]           [Continue messaging]
                    |
                    v
               [TERMINATED]
```

### Mechanism Ready Transition

When `mechanism->status()` returns `ready`:

```python
def mechanism_ready():
    # 1. Start heartbeat timer if configured
    if heartbeat_interval > 0:
        add_timer(heartbeat_interval)

    # 2. Notify session that engine is ready
    if has_handshake_stage:
        session.engine_ready()

    # 3. Send peer routing ID to socket if configured
    if recv_routing_id:
        routing_id = mechanism.peer_routing_id()
        session.push_msg(routing_id)

    # 4. Build metadata from mechanism properties
    metadata = {}
    metadata.update(get_basic_properties())   # Peer address, FD
    metadata.update(mechanism.zap_properties) # From ZAP reply
    metadata.update(mechanism.zmtp_properties) # From peer READY/INITIATE

    # 5. Cancel handshake timer
    cancel_timer(handshake_timer_id)

    # 6. Switch to normal message flow
    next_msg = pull_and_encode
    process_msg = decode_and_push

    # 7. Fire handshake succeeded event
    socket.event_handshake_succeeded()
```

---

## Error Handling

### Protocol Error Codes

| Code | Constant | Description |
|------|----------|-------------|
| 0x10000001 | ZMQ_PROTOCOL_ERROR_ZMTP_UNSPECIFIED | Unspecified ZMTP error |
| 0x10000002 | ZMQ_PROTOCOL_ERROR_ZMTP_UNEXPECTED_COMMAND | Unexpected command received |
| 0x10000003 | ZMQ_PROTOCOL_ERROR_ZMTP_INVALID_SEQUENCE | Invalid nonce/sequence |
| 0x10000004 | ZMQ_PROTOCOL_ERROR_ZMTP_KEY_EXCHANGE | Key exchange failure |
| 0x10000005 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_UNSPECIFIED | Malformed command |
| 0x10000011 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_MESSAGE | Malformed MESSAGE |
| 0x10000012 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_HELLO | Malformed HELLO |
| 0x10000013 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_INITIATE | Malformed INITIATE |
| 0x10000014 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_ERROR | Malformed ERROR |
| 0x10000015 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_READY | Malformed READY |
| 0x10000016 | ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_WELCOME | Malformed WELCOME |
| 0x10000030 | ZMQ_PROTOCOL_ERROR_ZMTP_INVALID_METADATA | Invalid metadata |
| 0x10000031 | ZMQ_PROTOCOL_ERROR_ZMTP_CRYPTOGRAPHIC | Cryptographic error |
| 0x10000032 | ZMQ_PROTOCOL_ERROR_ZMTP_MECHANISM_MISMATCH | Mechanism mismatch |
| 0x20000001 | ZMQ_PROTOCOL_ERROR_ZAP_UNSPECIFIED | Unspecified ZAP error |
| 0x20000002 | ZMQ_PROTOCOL_ERROR_ZAP_MALFORMED_REPLY | Malformed ZAP reply |
| 0x20000003 | ZMQ_PROTOCOL_ERROR_ZAP_BAD_REQUEST_ID | Bad ZAP request ID |
| 0x20000004 | ZMQ_PROTOCOL_ERROR_ZAP_BAD_VERSION | Bad ZAP version |
| 0x20000005 | ZMQ_PROTOCOL_ERROR_ZAP_INVALID_STATUS_CODE | Invalid ZAP status |
| 0x20000006 | ZMQ_PROTOCOL_ERROR_ZAP_INVALID_METADATA | Invalid ZAP metadata |

### Error Events

```c
// Fired when protocol error occurs during handshake
void event_handshake_failed_protocol(endpoint, error_code);

// Fired when authentication fails (ZAP returns 300/400/500)
void event_handshake_failed_auth(endpoint, zap_status_code);

// Fired when handshake fails for unspecified reason
void event_handshake_failed_no_detail(endpoint, errno);

// Fired when handshake succeeds
void event_handshake_succeeded(endpoint, 0);
```

### Handshake Timeout

- Configurable via `ZMQ_HANDSHAKE_IVL` socket option
- Default: 30000ms (30 seconds)
- Value of 0 disables timeout
- On timeout: `error(timeout_error)` triggers disconnect

---

## Implementation Checklist

To implement ZMTP handshake from scratch:

### Phase 1: Greeting Exchange
- [ ] Send initial signature (10 bytes)
- [ ] Detect peer protocol version
- [ ] Exchange full greeting for ZMTP/3.x
- [ ] Validate mechanism match
- [ ] Handle version negotiation downgrade

### Phase 2: Frame Encoding/Decoding
- [ ] Implement ZMTP/2.0+ frame format
- [ ] Handle short and long size encoding
- [ ] Set/parse frame flags correctly

### Phase 3: NULL Mechanism
- [ ] Implement READY command creation/parsing
- [ ] Implement ERROR command creation/parsing
- [ ] Implement metadata serialization/parsing
- [ ] Socket type compatibility checking

### Phase 4: PLAIN Mechanism
- [ ] HELLO command with username/password
- [ ] WELCOME command (empty)
- [ ] INITIATE command with metadata
- [ ] READY command with metadata
- [ ] State machine transitions

### Phase 5: CURVE Mechanism
- [ ] Generate ephemeral keypairs
- [ ] HELLO: crypto_box with zeros
- [ ] WELCOME: cookie encryption, box creation
- [ ] INITIATE: cookie echo, vouch creation
- [ ] READY: encrypted metadata
- [ ] Precompute shared secrets
- [ ] Nonce management

### Phase 6: ZAP Integration
- [ ] ZAP request frame construction
- [ ] ZAP reply parsing
- [ ] Status code handling
- [ ] User ID extraction

### Phase 7: Post-Handshake
- [ ] MESSAGE command encryption (CURVE)
- [ ] MESSAGE command decryption (CURVE)
- [ ] Nonce sequence validation
- [ ] Heartbeat PING/PONG

---

## References

- [ZMTP/3.1 Specification (RFC 23)](https://rfc.zeromq.org/spec/23/)
- [CurveZMQ Specification (RFC 26)](https://rfc.zeromq.org/spec/26/)
- [ZAP Specification (RFC 27)](https://rfc.zeromq.org/spec/27/)
- [libsodium documentation](https://doc.libsodium.org/)

---

## Appendix: Code Reference

Key source files in libzmq:

| File | Purpose |
|------|---------|
| `src/zmtp_engine.cpp` | ZMTP greeting and version negotiation |
| `src/stream_engine_base.cpp` | Handshake orchestration |
| `src/mechanism.cpp` | Base mechanism and metadata parsing |
| `src/null_mechanism.cpp` | NULL mechanism implementation |
| `src/plain_client.cpp` | PLAIN client state machine |
| `src/plain_server.cpp` | PLAIN server state machine |
| `src/curve_client.cpp` | CURVE client state machine |
| `src/curve_server.cpp` | CURVE server state machine |
| `src/curve_client_tools.hpp` | CURVE crypto operations |
| `src/curve_mechanism_base.cpp` | CURVE message encryption |
| `src/zap_client.cpp` | ZAP protocol implementation |
| `src/v2_encoder.cpp` | ZMTP frame encoding |
| `src/v2_decoder.hpp` | ZMTP frame decoding |
