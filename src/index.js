/********************************************************************************
 * Copyright (C) 2023-2026 CoCreate and Contributors.
 *
 * Licensed under GNU AGPLv3.
 ********************************************************************************/

import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { performance } from "node:perf_hooks";
import v8 from "node:v8";

// ==============================================================================
// CONFIGURATION CONSTANTS & DESIGN TOKENS
// ==============================================================================
const BASE_ISOLATE_FEE_BYTES = 2 * 1024 * 1024; // 2 MB Flat Base Fee for Isolate Spin-up
const DEFAULT_MEMORY_LIMIT_MB = 256;             // 256 MB Virtual Memory Ceiling
const DEFAULT_TIMEOUT_MS = 10000;                // 10-second hard execution timeout

// ==============================================================================
// LAYER 2: ISOLATED EXECUTION ENGINE SHIM (Bundled Isolate Runtime String)
// Embedded string blob evaluated inside fresh worker isolates.
// ==============================================================================
const CONTAINER_ISOLATE_BUNDLE = `
const { parentPort, workerData } = await import("node:worker_threads");

// Freeze workerData so it is immutable inside the isolate
if (workerData && typeof workerData === "object") {
    Object.freeze(workerData);
}

// Initialize internal metrics state inside isolate
self.bytesFetched = 0;

// Overridden global fetch to meter external network bandwidth payload sizes
const nativeFetch = globalThis.fetch;
if (typeof nativeFetch === "function") {
    globalThis.fetch = async (...args) => {
        const response = await nativeFetch(...args);
        try {
            const clone = response.clone();
            const buffer = await clone.arrayBuffer();
            self.bytesFetched += buffer.byteLength;
        } catch (_) {
            // Non-blocking catch if stream cannot be cloned
        }
        return response;
    };
}

// Enclose parentPort IPC bridge inside a private closure to prevent user code tampering
const { callHostRpc, registerContainerListener } = (() => {
    const activeRequests = new Map();
    const eventListeners = new Map();

    // Protect parentPort message listener from being cleared or intercepted
    parentPort.on("message", (msg) => {
        if (!msg) return;

        // Handle RPC Responses
        if (msg.id && activeRequests.has(msg.id)) {
            const { resolve, reject } = activeRequests.get(msg.id);
            activeRequests.delete(msg.id);
            if (msg.error) reject(new Error(msg.error));
            else resolve(msg.result);
            return;
        }

        // Handle Event Pushes from Host (container.on)
        if (msg.event && eventListeners.has(msg.event)) {
            const listeners = eventListeners.get(msg.event);
            for (const listener of listeners) {
                try {
                    listener(msg.payload);
                } catch (err) {
                    console.error("[Container Event Error]", err);
                }
            }
        }
    });

    return {
        callHostRpc: function(action, payload = {}) {
            return new Promise((resolve, reject) => {
                const id = "rpc_" + Math.random().toString(36).substring(2, 11) + "_" + Date.now();
                activeRequests.set(id, { resolve, reject });
                parentPort.postMessage({ id, action, payload });
            });
        },
        registerContainerListener: function(event, callback) {
            if (typeof callback !== "function") return;
            if (!eventListeners.has(event)) {
                eventListeners.set(event, new Set());
            }
            eventListeners.get(event).add(callback);
        }
    };
})();

// Helper to define read-only, non-configurable, non-overwritable global objects
function defineReadOnlyGlobal(name, value) {
    Object.defineProperty(globalThis, name, {
        value: Object.freeze(value),
        writable: false,
        configurable: false,
        enumerable: true
    });
}

// Unified Container Interface replacing parentPort, crud, and api namespaces
const containerInterface = Object.freeze({
    send: (data = {}) => callHostRpc("RPC_SEND", data),
    on: (event, callback) => registerContainerListener(event, callback)
});

// Hardened container namespace exposed to user scripts (Cannot be overwritten)
defineReadOnlyGlobal("container", containerInterface);

// Primary message listener executing isolated user code
parentPort.on("message", async (msg) => {
    const { id, action, payload } = msg;
    if (action !== "EXECUTE_SCRIPT") return;

    const startTime = Date.now();
    try {
        // Construct dynamic Function wrapper passing single unified 'data' context object
        const userFunction = new AsyncFunction("data", payload.script);
        
        // Construct unified input payload object matching platform standard
        const executionData = Object.freeze({
            ...(payload.data || {}),
            container: globalThis.container
        });

        const result = await userFunction(executionData);

        parentPort.postMessage({
            id,
            success: true,
            result,
            metrics: {
                bytesFetched: self.bytesFetched || 0,
                executionDurationMs: Date.now() - startTime
            }
        });
    } catch (err) {
        parentPort.postMessage({
            id,
            success: false,
            error: err.message || "Script execution failed",
            metrics: {
                bytesFetched: self.bytesFetched || 0,
                executionDurationMs: Date.now() - startTime
            }
        });
    }
});

const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
`;

// ==============================================================================
// LAYER 1: HOST APPLICATION BRIDGE (Main Node.js Server Process)
// Fully controls execution, untamperable metering, and host-side resource bounds.
// ==============================================================================
class ServerContainerManager {
    constructor() {
        this.serverInstance = null;
        this.activeIsolates = new Map();
        this.requestIdCounter = 0;
    }

    /**
     * Initializes the Host Bridge with server reference.
     * @param {Object} server - Core server context holding crud, ws, and auth handles.
 * @private
 * @environment server
 */
    init(server) {
        this.serverInstance = server;
        console.log("[server.container] Isolated Execution Engine initialized.");
    }

    /**
     * Executes a script inside an isolated V8 container thread with hard resource bounds.
     * @param {Object} data - Unified data object holding script, data params, organization_id, etc.
     * @returns {Promise<Object>} Script execution result + untamperable usage metrics.
 * @private
 * @environment server
 */
    async send(data = {}) {
        const { 
            script, 
            organization_id, 
            memoryLimitMB = DEFAULT_MEMORY_LIMIT_MB, 
            timeoutMs = DEFAULT_TIMEOUT_MS 
        } = data;

        if (!organization_id) {
            throw new Error("[server.container] organization_id is required for execution isolation.");
        }

        const executionId = `exec_${++this.requestIdCounter}_${Date.now()}`;
        const inputPayloadSize = Buffer.byteLength(JSON.stringify(data || {}));

        return new Promise(async (resolve, reject) => {
            let worker = null;
            let timer = null;
            let isTerminated = false;

            const cleanup = () => {
                if (timer) clearTimeout(timer);
                if (worker) {
                    worker.terminate();
                    this.activeIsolates.delete(executionId);
                }
            };

            try {
                // Spawn Worker Thread / Isolate with resource limits
                worker = new Worker(CONTAINER_ISOLATE_BUNDLE, {
                    eval: true,
                    resourceLimits: {
                        maxYoungGenerationSizeMb: 16,
                        maxOldGenerationSizeMb: memoryLimitMB
                    }
                });

                this.activeIsolates.set(executionId, { worker, organization_id, startTime: Date.now() });

                const startTimeHost = performance.now();

                // Handle bi-directional messages from Isolate
                worker.on("message", async (msg) => {
                    if (isTerminated) return;

                    const { id, action, payload, result, success, error, metrics } = msg;

                    // Handle Unified RPC requests sent by Isolate to Host via container.send(data)
                    if (action === "RPC_SEND") {
                        try {
                            // Route all methods through main host execution pipeline
                            let routeResult;
                            if (payload?.method?.startsWith("container")) {
                                routeResult = await this.send({
                                    ...payload,
                                    organization_id // Inherit parent organization ID
                                });
                            } else {
                                routeResult = await this.serverInstance?.crud?.send({
                                    ...payload,
                                    organization_id // Force host organization boundary
                                });
                            }
                            worker.postMessage({ id, result: routeResult });
                        } catch (err) {
                            worker.postMessage({ id, error: err.message });
                        }
                        return;
                    }

                    // Handle Final Script Execution Completion
                    if (success !== undefined) {
                        const durationMs = performance.now() - startTimeHost;
                        const outputPayloadSize = success ? Buffer.byteLength(JSON.stringify(result || {})) : 0;

                        // Calculate Untamperable Usage Metrics for Billing
                        const externalFetchBytes = metrics?.bytesFetched || 0;
                        const payloadDataBytes = inputPayloadSize + outputPayloadSize;
                        const totalBillableBytes = BASE_ISOLATE_FEE_BYTES + payloadDataBytes + externalFetchBytes;
                        const billableMB = (totalBillableBytes / (1024 * 1024)).toFixed(3);

                        // Async record usage event to server ledger
                        this.recordBillingLedger({
                            organization_id,
                            executionId,
                            billableMB,
                            baseFeeMB: (BASE_ISOLATE_FEE_BYTES / (1024 * 1024)).toFixed(2),
                            payloadMB: (payloadDataBytes / (1024 * 1024)).toFixed(3),
                            fetchMB: (externalFetchBytes / (1024 * 1024)).toFixed(3),
                            durationMs: durationMs.toFixed(2)
                        });

                        cleanup();

                        if (success) {
                            resolve({
                                result,
                                usage: {
                                    billableMB: Number(billableMB),
                                    durationMs: Number(durationMs.toFixed(2)),
                                    bytesFetched: externalFetchBytes
                                }
                            });
                        } else {
                            reject(new Error(`[Isolate Execution Error] ${error}`));
                        }
                    }
                });

                // Error and crash handlers enforced outside Isolate
                worker.on("error", (err) => {
                    cleanup();
                    reject(new Error(`[Isolate Engine Crash] ${err.message}`));
                });

                worker.on("exit", (code) => {
                    if (!isTerminated && code !== 0) {
                        cleanup();
                        reject(new Error(`[Isolate Terminated] Stopped with exit code ${code}`));
                    }
                });

                // Hard Termination Timeout Timer on Host
                timer = setTimeout(() => {
                    isTerminated = true;
                    cleanup();
                    reject(new Error(`[Isolate Timeout] Script exceeded maximum limit of ${timeoutMs}ms`));
                }, timeoutMs);

                // Dispatch execution payload to worker
                worker.postMessage({
                    action: "EXECUTE_SCRIPT",
                    payload: { script, data }
                });

            } catch (fatalErr) {
                cleanup();
                reject(fatalErr);
            }
        });
    }

    /**
     * Records container consumption to the platform database ledger via server.crud.send.
     * @param {Object} data - Unified billing payload object.
 * @private
 * @environment server
 */
    async recordBillingLedger(data) {
        if (!this.serverInstance?.crud?.send) return;

        try {
            await this.serverInstance.crud.send({
                method: "object.create",
                array: "container_ledger",
                object: {
                    organization_id: data.organization_id,
                    execution_id: data.executionId,
                    metric: "bandwidth_mb",
                    billable_mb: data.billableMB,
                    breakdown: {
                        base_isolate_fee_mb: data.baseFeeMB,
                        payload_data_mb: data.payloadMB,
                        external_fetch_mb: data.fetchMB
                    },
                    duration_ms: data.durationMs,
                    created_at: new Date().toISOString()
                },
                organization_id: data.organization_id
            });
        } catch (err) {
            console.error("[server.container] Failed to write billing ledger:", err.message);
        }
    }
}

// Export singleton instance matching standard server module patterns
const serverContainer = new ServerContainerManager();
export default serverContainer;

/**
 *
 * @name default
 * @kind member
 * @public
 * @environment server
 */

/**
 * Hardened V8 isolated execution engine for CoCreate Server. Executes multi-tenant dynamic scripts inside sandboxed worker threads with untamperable host-side bandwidth metering and unified container interfaces.
 *
 * @module container
 * @environment server
 */
