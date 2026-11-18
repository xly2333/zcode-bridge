import { startBridge } from './gateway/bridge.mjs';
import { startEndpoint } from './gateway/llm-endpoint.mjs';

function interrupted() {
  const error = new Error('Gateway startup interrupted by stop');
  error.code = 'ABORT_ERR';
  return error;
}
function port(value, fallback) {
  const n = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(n) || n < 0 || n > 65535) throw new RangeError('Gateway port must be an integer from 0 to 65535');
  return n;
}
function validate(service, label) {
  if (!service || typeof service.close !== 'function' || typeof service.url !== 'string') {
    throw new TypeError(`${label} did not return a managed service`);
  }
}

/** Own both HTTP services in the DSH host process; never spawn detached daemons. */
export function createGatewayLifecycle(options = {}, deps = {}) {
  const settings = { ...options };
  const launchBridge = deps.startBridge ?? startBridge;
  const launchEndpoint = deps.startEndpoint ?? startEndpoint;
  const logger = deps.logger ?? settings.logger;
  const host = settings.host || '127.0.0.1';
  const bridgePort = port(settings.bridgePort, 47821);
  const endpointPort = port(settings.endpointPort, 47830);
  let state = 'stopped';
  let lastError;
  let bridge;
  let endpoint;
  let runtime;
  let startup;
  let stopping;
  let controller;
  let stopRequested = false;

  function status() {
    return {
      state,
      ...(bridge?.url ? { bridgeUrl: bridge.url } : {}),
      ...(endpoint?.url ? { endpointUrl: endpoint.url } : {}),
      ...(lastError ? { error: lastError } : {}),
    };
  }
  function notify() {
    try { logger?.({ kind: 'zcode.gateway.lifecycle', ...status() }); } catch { /* diagnostics are best-effort */ }
  }
  async function closeOwned() {
    // Transfer ownership once before any await so rollback and stop cannot close
    // another generation's server or run a close callback twice.
    const ownedEndpoint = endpoint;
    const ownedBridge = bridge;
    endpoint = undefined;
    bridge = undefined;
    runtime = undefined;
    const errors = [];
    for (const service of [ownedEndpoint, ownedBridge]) {
      if (typeof service?.close !== 'function') continue;
      try { await service.close(); } catch (error) { errors.push(error); }
    }
    if (errors.length) throw new AggregateError(errors, 'Gateway service cleanup failed');
  }

  function start() {
    if (state === 'running' && runtime) return Promise.resolve(runtime);
    if (startup) return startup;
    if (stopping || state === 'stopping') return Promise.reject(interrupted());
    state = 'starting';
    lastError = undefined;
    stopRequested = false;
    controller = new AbortController();
    notify();
    startup = Promise.resolve().then(async () => {
      try {
        bridge = await launchBridge({
          host, port: bridgePort, stateDir: settings.stateDir,
          token: settings.token, tokenFile: settings.tokenFile,
          persistState: settings.persistState !== false,
          signal: controller.signal, logger,
        });
        validate(bridge, 'Bridge');
        if (typeof bridge.token !== 'string' || bridge.token.length === 0) throw new TypeError('Bridge did not return its authentication token');
        if (stopRequested || controller.signal.aborted) throw interrupted();
        endpoint = await launchEndpoint({
          host, port: endpointPort, stateDir: settings.stateDir,
          persistState: settings.persistState !== false,
          bridgeUrl: bridge.url, bridgeToken: bridge.token,
          signal: controller.signal, logger,
        });
        validate(endpoint, 'Endpoint');
        if (stopRequested || controller.signal.aborted) throw interrupted();
        runtime = {
          bridge, endpoint, bridgeUrl: bridge.url,
          endpointUrl: endpoint.url, token: bridge.token,
        };
        state = 'running';
        notify();
        return runtime;
      } catch (error) {
        lastError = String(error?.code ? `${error.code}: ${error.message}` : error?.message ?? error);
        state = stopRequested ? 'stopping' : 'failed';
        try { await closeOwned(); }
        catch (cleanupError) {
          throw new AggregateError([error, cleanupError], 'Gateway startup and rollback failed');
        } finally { notify(); }
        throw error;
      } finally { startup = undefined; }
    });
    return startup;
  }

  function stop() {
    if (stopping) return stopping;
    if (state === 'stopped' && !startup && !bridge && !endpoint) return Promise.resolve();
    const wasStarting = state === 'starting';
    stopRequested = true;
    state = 'stopping';
    if (wasStarting) controller?.abort();
    notify();
    const pendingStartup = startup;
    stopping = Promise.resolve().then(async () => {
      try {
        if (pendingStartup) await pendingStartup.catch(() => undefined);
        await closeOwned();
        // For a running service, explicit endpoint-first cleanup leaves the
        // receiver available while endpoint cancellation notifications drain.
        controller?.abort();
        state = 'stopped';
      } catch (error) {
        lastError = String(error?.message ?? error);
        state = 'failed';
        throw error;
      } finally {
        notify();
        stopping = undefined;
      }
    });
    return stopping;
  }

  return { start, stop, get status() { return status(); } };
}
