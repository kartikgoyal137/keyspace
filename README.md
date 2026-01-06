# FluxKV

**FluxKV** is a multi-threaded, Redis-compatible key-value store implemented in **C++23**.  
It implements the **RESP (Redis Serialization Protocol)** to communicate with standard Redis clients and supports multiple data structures, transactions, and master–replica replication.

---

## Technical Architecture

### Core Design

- **Networking Model**  
  Implements a synchronous, **thread-per-client** architecture using standard BSD sockets (`sys/socket`).  
  Each incoming connection spawns a detached `std::thread` to handle the request lifecycle.

- **Protocol**  
  Features a custom **RESP parser** capable of handling **Bulk Strings** and **Arrays** for command deserialization.

---

### Data Storage & Concurrency

The system uses granular locking mechanisms to ensure thread safety across global data structures.

#### Storage Backend

- **Key-Value Store**  
  `std::unordered_map` for O(1) string storage.

- **Lists**  
  `std::unordered_map` mapping to `std::deque` for efficient head/tail operations.

- **Streams**  
  `std::map` within a `Stream` struct to maintain strict ID ordering.

#### Synchronization

- Uses `std::shared_mutex`:
  - `std::shared_lock` for multiple concurrent readers
  - `std::unique_lock` for exclusive writers

- Global locks are segregated by data type:
  - `sm_store`
  - `sm_list`
  - `sm_stream`  
  This reduces lock contention across unrelated operations.

#### Blocking Operations

- **BLPOP** and **XREAD** use `std::condition_variable_any` to suspend worker threads efficiently until:
  - Data becomes available, or
  - A timeout occurs.

---

## Features

### 1. Supported Commands

**Basic**
- `PING`
- `ECHO`

**Strings**
- `SET` (supports `EX` / `PX` expiry)
- `GET`
- `INCR`

**Lists**
- `LPUSH`
- `RPUSH`
- `LPOP`
- `BLPOP` (blocking)
- `LLEN`
- `LRANGE`

**Streams**
- `XADD` (auto-ID generation)
- `XRANGE`
- `XREAD` (blocking)

**System**
- `CONFIG`
- `INFO`
- `TYPE`

---

### 2. Transactions

Implements optimistic execution via:

- `MULTI`
- `EXEC`
- `DISCARD`

Commands issued inside a transaction block are:
- Queued in a **thread-local buffer**
- Stored in a `pending_cmd` map keyed by file descriptor
- Executed atomically upon `EXEC`

---

### 3. Replication

Supports a **Master–Replica** architecture.

- **Handshake**
  - Full replication handshake:
    ```
    PING → REPLCONF → PSYNC
    ```

- **Propagation**
  - Write commands (`SET`, `LPUSH`, etc.) are propagated from the Master to all connected Replicas.

- **Offset Tracking**
  - Maintains `master_repl_offset` to handle synchronization states correctly.

---

## Build and Run

### Prerequisites

- C++ compiler supporting **C++23**
- **pthread** library

---

### Setup

```bash
mkdir build && cd build
cmake ..
make

