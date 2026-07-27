# CoCreate Server Container

An isolated V8 execution engine designed to execute dynamic multi-tenant scripts within hardened sandboxes. Featuring untamperable host-side metering, single-object `container.send(data)` messaging, and deterministic resource boundaries, this module allows CoCreate applications to run custom user code safely without risking host server stability or cross-tenant data leaks.

---

## Documentation

For full API references, security isolation specifications, execution hooks, and examples, visit the CoCreate Server Container documentation:

https://cocreatejs.com/docs/server-container

---

## Table of Contents

- [Hardened Two-Layer Execution Engine](#hardened-two-layer-execution-engine)
- [Why CoCreate Server Container?](#why-cocreate-server-container)
- [Unified Immutable Runtime Interface](#unified-immutable-runtime-interface)
- [Untamperable Bandwidth Metering](#untamperable-bandwidth-metering)
- [Host RPC Delegation & Child Isolate Spawning](#host-rpc-delegation--child-isolate-spawning)
- [Configuration](#configuration)
- [Installation](#installation)
- [Announcements](#announcements)
- [Roadmap](#roadmap)
- [How to Contribute](#how-to-contribute)
- [About](#about)

---

## Hardened Two-Layer Execution Engine

Running untrusted, tenant-written code on a shared server presents significant risks, from event loop starvation and memory exhaustion to global scope tampering. `@cocreate/server-container` solves this by strictly separating host control logic from tenant script execution into two distinct architectural layers.

```text
┌────────────────────────────────────────────────────────────────────────────┐
│ LAYER 1: HOST APPLICATION BRIDGE (Main Node.js Server Process)             │
│                                                                            │
│ • Manages worker thread lifecycles & resource boundaries                   │
│ • Calculates untamperable usage metrics on the host thread                 │
│ • Intercepts & routes database / system RPC calls (RPC_SEND)               │
│ • Enforces hard memory ceilings & execution timeout timers                 │
└─────────────────────────────────────┬──────────────────────────────────────┘
                                      │
                     In-Memory IPC Bridge (parentPort)
                                      │
┌─────────────────────────────────────▼──────────────────────────────────────┐
│ LAYER 2: ISOLATED EXECUTION ENGINE (V8 Worker Thread Sandbox)              │
│                                                                            │
│ • Pure V8 execution thread evaluated directly from memory                  │
│ • Exposes frozen, immutable global container namespace                     │
│ • Encapsulates messaging bridges inside private closure scope              │
│ • Overrides globalThis.fetch for network bandwidth metering                │
└────────────────────────────────────────────────────────────────────────────┘
```

### Private IIFE Encapsulation

All host communication primitives (`parentPort`), active request maps, and event listeners are encapsulated inside a private closure before user code is evaluated. Tenant scripts executed via `AsyncFunction("data", script)` have no access to the outer module scope, preventing interception or tampering with IPC bridges.

---

## Why CoCreate Server Container?

Traditional containerization solutions such as Docker or full virtual machines introduce significant cold-start latency and memory overhead, making them impractical for lightweight, high-frequency tenant scripts.

Conversely, executing user code directly inside the primary Node.js process exposes global variables, unrestricted `process.env` access, and the possibility of blocking the host event loop.

`@cocreate/server-container` bridges this gap using lightweight V8 worker thread isolates.

### Features

- **Sub-10ms Cold Starts:** Isolates are instantiated directly from in-memory source without disk access.
- **Hard Memory Limits:** Configurable V8 heap limits prevent runaway scripts from impacting the host.
- **Execution Timeouts:** Host-managed timers terminate stalled or infinite-loop executions automatically.
- **Single Object API:** Every operation follows the CoCreate standard of accepting a single data object.

---

## Unified Immutable Runtime Interface

Scripts communicate with the host exclusively through a locked-down, read-only `container` interface.

The global namespace is created using `Object.defineProperty()` with `writable: false` and `configurable: false`, then frozen using `Object.freeze()`.

### Available Interface

```javascript
container.send(data);      // Execute RPC requests
container.on(event, fn);   // Listen for host events
```

### Executing a Script

```javascript
const { organization_id, filter } = data;

const result = await container.send({
	method: "object.read",
	array: "products",
	object: {
		category: filter
	},
	organization_id
});

return {
	status: "success",
	count: result.object.length
};
```

---

## Untamperable Bandwidth Metering

Execution billing is calculated entirely by the host process, ensuring tenant code cannot manipulate usage accounting.

Billable usage combines:

- Base isolate allocation cost
- Input payload size
- Output payload size
- External network traffic performed through `fetch`

### Billable Usage Formula

\[
B_{total}=B_{base}+\frac{D_{input}+D_{output}+D_{fetch}}{1024\times1024}
\]

Where:

- **Base allocation:** 2 MB
- **Input:** serialized request size
- **Output:** serialized response size
- **Fetch:** downloaded network payloads

Every execution automatically records an immutable usage entry in the container ledger.

---

## Host RPC Delegation & Child Isolate Spawning

Since V8 worker isolates cannot directly create nested isolates, sub-container execution is delegated back to the host.

```javascript
const result = await container.send({
	method: "container.send",
	script: "return data.a + data.b;",
	data: {
		a: 15,
		b: 25
	},
	organization_id: data.organization_id
});
```

The host validates the tenant context, creates a sibling isolate, executes the request, and returns the result to the parent container.

---

## Configuration

Initialize the container engine during server startup.

```javascript
import serverContainer from "@cocreate/server-container";

serverContainer.init(server);

const response = await serverContainer.send({
	script: `
		const orders = await container.send({
			method: "object.read",
			array: "orders"
		});

		return {
			total: orders.length
		};
	`,
	organization_id: "org_654321",
	memoryLimitMB: 128,
	timeoutMs: 5000
});

console.log(response.result);
console.log(response.usage);
```

---

## Installation

### NPM

```bash
npm install @cocreate/server-container
```

### Yarn

```bash
yarn add @cocreate/server-container
```

---

## Announcements

Release notes, security improvements, execution engine enhancements, and performance optimizations are available on GitHub:

- https://github.com/CoCreate-app/CoCreate-server-container/releases

---

## Roadmap

Upcoming capabilities include:

- **V8 Heap Snapshot Warm-Up:** Faster startup using precompiled heap snapshots.
- **WebAssembly (Wasm) Isolation:** Secure execution of WebAssembly modules within isolated runtimes.
- **Shared Runtime Pools:** Reusable worker pools to reduce initialization overhead.
- **Execution Profiling:** Built-in CPU, memory, and execution statistics for sandbox workloads.

---

## How to Contribute

We welcome pull requests for performance improvements, security enhancements, documentation, and new adapters.

Please review the contribution guide before submitting changes:

- https://github.com/CoCreate-app/CoCreate-server-container/blob/master/CONTRIBUTING.md

To report bugs or request new features, please use GitHub Issues:

- https://github.com/CoCreate-app/CoCreate-server-container/issues

---

## About

`@cocreate/server-container` is designed, built, and maintained by the CoCreate Developer Experience Team.

> [!NOTE]
> Questions about sandbox execution, runtime isolation, usage metering, or custom execution environments can be discussed on GitHub Discussions or in the CoCreate Discord community.
>
> GitHub Discussions:
> https://github.com/CoCreate-app/CoCreate-server-container/discussions

`@cocreate/server-container` is open source and funded by CoCreate. CoCreate names and logos are trademarks of CoCreate, LLC.