// --- Configuration ---
const CACHE_TTL_SECONDS = 10 * 60; // 10 minutes (for KV expirationTtl)
const CACHE_TTL_MS = CACHE_TTL_SECONDS * 1000; // 10 minutes in milliseconds (for internal checking)
const PING_TIMEOUT_MS = 5000; // Timeout for pinging individual instances

/**
 * @typedef {object} Env
 * @property {KVNamespace} INSTANCE_CACHE_KV - Binding for the KV namespace.
 * @property {string} [TARGET_SERVICE] - Specific service to redirect (e.g., from path/query/header).
 * @property {string} INSTANCES_SOURCE_URL - URL to fetch the instance list JSON.
 * @property {string} DEFAULT_TARGET_SERVICE - Fallback service if TARGET_SERVICE isn't determined.
 */

/**
 * @typedef {object} ExecutionContext
 * @property {(promise: Promise<any>) => void} waitUntil - Execute tasks after response.
 * @property {() => void} passThroughOnException - Handle exceptions differently.
 */

/**
 * @typedef {object} CachedInstanceValue
 * @property {string} url - The cached instance URL.
 * @property {number} timestamp - Unix timestamp (ms) when the instance was verified.
 */


export default {
  /**
   * Handles incoming fetch requests.
   * @param {Request} request - The incoming request object.
   * @param {Env} env - Environment variables and bindings.
   * @param {ExecutionContext} ctx - Execution context.
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    // --- Determine Target Service ---
    // Example: Get from query param `?service=xxx`, header `X-Service`, or use default
    // This logic can be customized based on how you want to route requests.
    const url = new URL(request.url);
    const targetServiceQuery = url.searchParams.get('service');
    const targetServiceHeader = request.headers.get('X-Service');
    const TARGET_SERVICE = targetServiceQuery || targetServiceHeader || env.TARGET_SERVICE || env.DEFAULT_TARGET_SERVICE || "redlib.clearnet";
    const INSTANCES_SOURCE_URL = env.INSTANCES_SOURCE_URL || "https://raw.githubusercontent.com/libredirect/instances/main/data.json";

    // Use a service-specific cache key (KV keys are strings)
    const CACHE_KEY = `instance_cache:${TARGET_SERVICE}`;

    console.log(`[${TARGET_SERVICE}] Received request: ${request.method} ${request.url}`);

    try {
      // 1. Check Cache
      const cached = await getCachedInstance(env.INSTANCE_CACHE_KV, CACHE_KEY, TARGET_SERVICE);
      if (cached) {
        return createRedirectResponse(cached, request.url, TARGET_SERVICE);
      }

      // 2. Cache Miss/Expired: Find and Cache New Instance
      console.log(`[${TARGET_SERVICE}] Cache miss/expired. Finding new instance...`);
      const instances = await getInstanceList(TARGET_SERVICE, INSTANCES_SOURCE_URL);

      if (instances.length === 0) {
          console.error(`[${TARGET_SERVICE}] No instances found or fetched from ${INSTANCES_SOURCE_URL}. Cannot proceed.`);
          return new Response(`Service Unavailable: No instances configured or fetched for ${TARGET_SERVICE}`, { status: 503 });
      }

      // Find the fastest instance and cache it in the background
      const fastestUrl = await findAndCacheFastestInstance(
        instances,
        env.INSTANCE_CACHE_KV,
        CACHE_KEY,
        TARGET_SERVICE,
        ctx
      );

      if (fastestUrl) {
        return createRedirectResponse(fastestUrl, request.url, TARGET_SERVICE);
      } else {
        // Failed to find any working instance after trying
        console.error(`[${TARGET_SERVICE}] Failed to find any responsive instance.`);
        return new Response("Service Unavailable: No backend instances available or responding", { status: 503 });
      }

    } catch (error) {
      console.error(`[${TARGET_SERVICE}] Internal Server Error:`, error.message, error.stack);
      // Avoid exposing detailed errors to the client
      let errorMessage = "Internal Server Error";
      // If it's a known error type we want to signal differently, we could
      // if (error instanceof SpecificHandledError) errorMessage = error.message;
      return new Response(errorMessage, { status: 500 });
    }
  }
};

// --- Helper Functions ---

/**
 * Retrieves the cached instance URL if it exists and is not expired.
 * @param {KVNamespace} kvStore - The KV namespace binding.
 * @param {string} cacheKey - The key to retrieve from KV.
 * @param {string} targetService - For logging purposes.
 * @returns {Promise<string | null>} The cached URL or null.
 */
async function getCachedInstance(kvStore, cacheKey, targetService) {
  try {
    // Use type "json" for automatic parsing if your KV binding supports it,
    // otherwise parse manually. Let's assume manual parsing for broader compatibility.
    const cachedDataString = await kvStore.get(cacheKey);
    if (!cachedDataString) {
        console.log(`[${targetService}] Cache miss: No entry found for key ${cacheKey}.`);
        return null;
    }

    /** @type {CachedInstanceValue | null} */
    let cacheEntry = null;
    try {
        cacheEntry = JSON.parse(cachedDataString);
    } catch (e) {
        console.error(`[${targetService}] Error parsing cached data from KV for key ${cacheKey}:`, e);
        // Consider deleting the invalid entry
        // await kvStore.delete(cacheKey);
        return null;
    }


    if (cacheEntry && cacheEntry.url && cacheEntry.timestamp && (Date.now() - cacheEntry.timestamp < CACHE_TTL_MS)) {
      console.log(`[${targetService}] Cache hit: Using ${cacheEntry.url} (verified at ${new Date(cacheEntry.timestamp).toISOString()})`);
      return cacheEntry.url;
    }

    if (cacheEntry) {
        console.log(`[${targetService}] Cache expired: Entry from ${new Date(cacheEntry.timestamp).toISOString()} is older than ${CACHE_TTL_MS / 60000} minutes.`);
    }
    // If !cacheEntry (parsing failed) or expired, fall through to return null
  } catch (error) {
      console.error(`[<span class="math-inline">\{targetService\}\] Error reading from Cloudflare KV \[</span>{cacheKey}]:`, error);
  }
  return null;
}

/**
 * Finds the fastest instance, caches it asynchronously, and returns its URL.
 * @param {string[]} instances - List of instance URLs to check.
 * @param {KVNamespace} kvStore - The KV namespace binding.
 * @param {string} cacheKey - The key to store in KV.
 * @param {string} targetService - For logging purposes.
 * @param {ExecutionContext} ctx - Execution context for waitUntil.
 * @returns {Promise<string | null>} The fastest URL found, or null.
 */
async function findAndCacheFastestInstance(instances, kvStore, cacheKey, targetService, ctx) {
  const newFastestUrl = await autoPickInstance(instances, targetService);

  if (newFastestUrl) {
    console.log(`[${targetService}] Determined fastest instance: ${newFastestUrl}`);
    /** @type {CachedInstanceValue} */
    const cacheValue = {
      url: newFastestUrl,
      timestamp: Date.now()
    };

    // Cache in the background, don't block the response
    ctx.waitUntil(
        (async () => {
            try {
                await kvStore.put(cacheKey, JSON.stringify(cacheValue), { expirationTtl: CACHE_TTL_SECONDS });
                console.log(`[<span class="math-inline">\{targetService\}\] Cache updated in KV \[</span>{cacheKey}]: Stored ${newFastestUrl}.`);
            } catch (error) {
                // Log the error but don't fail the request if KV write fails
                console.error(`[<span class="math-inline">\{targetService\}\] Failed to write to Cloudflare KV \[</span>{cacheKey}]:`, error);
            }
        })()
    );
    return newFastestUrl;
  }

  console.warn(`[${targetService}] Could not determine a fastest instance from the list.`);
  return null;
}

/**
 * Creates the final 307 Redirect Response.
 * @param {string} instanceBaseUrl - The base URL of the chosen instance.
 * @param {string} originalRequestUrlString - The full URL of the original request.
 * @param {string} targetService - For logging purposes.
 * @returns {Response}
 */
function createRedirectResponse(instanceBaseUrl, originalRequestUrlString, targetService) {
    const finalRedirectUrl = constructRedirectUrl(instanceBaseUrl, originalRequestUrlString, targetService);
    if (finalRedirectUrl) {
        console.log(`[${targetService}] Redirecting client to: ${finalRedirectUrl}`);
        // Use 307 Temporary Redirect to preserve method and body
        return Response.redirect(finalRedirectUrl, 307);
    } else {
        // This case should ideally not happen if instanceBaseUrl is valid
        console.error(`[${targetService}] Failed to construct final redirect URL from base ${instanceBaseUrl} and original ${originalRequestUrlString}.`);
        return new Response("Internal Server Error: Failed to construct redirect URL", { status: 500 });
    }
}

/**
 * Constructs the final redirect URL.
 * @param {string} instanceBaseUrl - Base URL of the target instance (e.g., "https://nitter.net").
 * @param {string} originalRequestUrlString - Full original request URL.
 * @param {string} targetService - For logging.
 * @returns {string | null} The final URL or null on error.
 */
function constructRedirectUrl(instanceBaseUrl, originalRequestUrlString, targetService) {
    try {
        const base = new URL(instanceBaseUrl);
        let safeInstanceBaseUrl = instanceBaseUrl;
        // Ensure the base URL ends with a slash if it's just the origin,
        // for correct relative path resolution by the URL constructor.
        if (!base.pathname || base.pathname === '/') {
            safeInstanceBaseUrl = instanceBaseUrl.endsWith('/') ? instanceBaseUrl : instanceBaseUrl + '/';
        }

        const originalUrl = new URL(originalRequestUrlString);
        // Combine instance base with original path, search, and hash
        const targetUrl = new URL(originalUrl.pathname + originalUrl.search + originalUrl.hash, safeInstanceBaseUrl);
        return targetUrl.toString();
    } catch (error) {
        console.error(`[<span class="math-inline">\{targetService\}\] Error constructing redirect URL from base '</span>{instanceBaseUrl}' and original '${originalRequestUrlString}':`, error);
        return null;
    }
}

/**
 * Fetches and parses the instance list for a specific service.
 * @param {string} serviceIdentifier - e.g., "nitter" or "redlib.clearnet".
 * @param {string} sourceUrl - URL of the JSON data source.
 * @returns {Promise<string[]>} Array of valid instance URLs.
 */
async function getInstanceList(serviceIdentifier, sourceUrl) {
  // Note: targetService isn't passed here, use serviceIdentifier for logs
  console.log(`[${serviceIdentifier}] Fetching instance list from ${sourceUrl}`);
  try {
    // Use a short timeout for fetching the instance list itself
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout

    const response = await fetch(sourceUrl, { signal: controller.signal });
    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`Failed to fetch instance list: ${response.status} ${response.statusText} from ${sourceUrl}`);
    }
    const data = await response.json();

    let instanceListSource = null;
    const parts = serviceIdentifier.split('.');

    // Handle nested structure like "service.type" (e.g., "redlib.clearnet")
    if (parts.length === 2 && typeof data[parts[0]] === 'object' && data[parts[0]] !== null) {
        instanceListSource = data[parts[0]][parts[1]];
    } else if (parts.length === 1) { // Handle top-level service identifiers
        instanceListSource = data[serviceIdentifier];
    }

    if (Array.isArray(instanceListSource)) {
      const validUrls = instanceListSource
        .filter(url => typeof url === 'string' && (url.startsWith('http://') || url.startsWith('https://')))
        .map(url => url.replace(/\/$/, '')); // Remove trailing slash for consistency
      console.log(`[${serviceIdentifier}] Found ${validUrls.length} valid instances.`);
      return validUrls;
    } else {
      console.error(`[<span class="math-inline">\{serviceIdentifier\}\] Instance list for '</span>{serviceIdentifier}' not found or not an array in the fetched data from ${sourceUrl}.`);
      return [];
    }
  } catch (error) {
    console.error(`[${serviceIdentifier}] Error fetching/parsing instance list from ${sourceUrl}:`, error);
    return [];
  }
}

/**
 * Selects the fastest instance by pinging a random subset.
 * @param {string[]} instances - Full list of instance URLs.
 * @param {string} targetService - For logging.
 * @returns {Promise<string | null>} URL of the fastest instance or null.
 */
async function autoPickInstance(instances, targetService) {
  if (instances.length === 0) {
      console.warn(`[${targetService}] autoPickInstance called with empty instance list.`);
      return null;
  }

  // Ping a subset (e.g., up to 5)
  const subsetSize = Math.min(instances.length, 5);
  const randomSubset = randomInstances(instances, subsetSize);
  console.log(`[${targetService}] Pinging subset: ${randomSubset.join(', ')}`);

  const pingPromises = randomSubset.map(async (instanceUrl) => {
    const startTime = performance.now();
    const time = await ping(instanceUrl, targetService); // Ping returns avg time or null/Infinity
    const endTime = performance.now();
    // console.log(`[${targetService}] Ping result for ${instanceUrl}: ${time !== null ? time + 'ms' : 'FAIL'} (Total check time: ${(endTime - startTime).toFixed(1)}ms)`);
    return { instance: instanceUrl, time }; // time can be number, null, or Infinity
  });

  const results = await Promise.all(pingPromises);

  const successfulPings = results
   .filter(r => typeof r.time === 'number' && r.time < PING_TIMEOUT_MS) // Filter out null/Infinity/timeouts
   .sort((a, b) => a.time - b.time);

  if (successfulPings.length > 0) {
    const fastest = successfulPings[0];
    console.log(`[${targetService}] Fastest instance in subset: <span class="math-inline">\{fastest\.instance\} \(</span>{fastest.time}ms)`);
    return fastest.instance;
  } else {
    console.warn(`[${targetService}] No instances in the subset responded successfully within the timeout.`);
    // Potential enhancement: If subset fails, try pinging more/all instances?
    return null;
  }
}

/**
 * Pings a URL multiple times (e.g., 3) and calculates average latency.
 * Skips the first ping result. Returns average time in ms, or Infinity on failure/timeout.
 * @param {string} href - The URL to ping.
 * @param {string} targetService - For logging.
 * @returns {Promise<number | null>} Average ping time in ms, PING_TIMEOUT_MS if any timed out, or null on network error.
 */
async function ping(href, targetService) {
  let totalTime = 0;
  let successfulPings = 0;
  const NUM_PINGS = 3;

  for (let i = 0; i < NUM_PINGS; i++) {
    const time = await pingOnce(href, PING_TIMEOUT_MS, targetService);

    if (time === null) {
      // console.debug(`[${targetService}] Ping attempt <span class="math-inline">\{i \+ 1\}/</span>{NUM_PINGS} for ${href} failed (Network/DNS error).`);
      return null; // Network/DNS error is critical failure
    }

    if (time >= PING_TIMEOUT_MS) {
      // console.debug(`[${targetService}] Ping attempt <span class="math-inline">\{i \+ 1\}/</span>{NUM_PINGS} for <span class="math-inline">\{href\} timed out or failed validation \(</span>{time} >= ${PING_TIMEOUT_MS}).`);
      // Treat timeout or high error code status as a failure for this instance check
      return PING_TIMEOUT_MS; // Indicate timeout/unreliability
    }

    // Skip first ping for averaging (warm-up)
    if (i > 0) {
      totalTime += time;
      successfulPings++;
    }
    // console.debug(`[${targetService}] Ping attempt <span class="math-inline">\{i \+ 1\}/</span>{NUM_PINGS} for ${href}: ${time}ms`);
  }

  // Require at least two successful pings (after skipping the first)
  if (successfulPings < NUM_PINGS - 1) {
    // console.warn(`[${targetService}] Not enough successful pings for <span class="math-inline">\{href\} \(</span>{successfulPings}/${NUM_PINGS - 1}). Considering it slow/unreliable.`);
    return PING_TIMEOUT_MS; // Treat as unreliable
  }

  return Math.round(totalTime / successfulPings);
}

/**
 * Pings a URL once using a HEAD request.
 * Returns duration in ms, PING_TIMEOUT_MS on timeout/abort near timeout,
 * a value > PING_TIMEOUT_MS on HTTP error, or null on network/URL error.
 * @param {string} href - The URL to ping.
 * @param {number} timeoutMs - Timeout duration.
 * @param {string} targetService - For logging.
 * @returns {Promise<number | null>} Ping time in ms or status code.
 */
async function pingOnce(href, timeoutMs, targetService) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new DOMException("Timeout", "TimeoutError")), timeoutMs);
  const startTime = performance.now();
  let response = undefined; // Define response outside try

  try {
    // Add cache-busting query param
    const pingUrl = new URL(href);
    pingUrl.searchParams.set('_ Lcache', Date.now().toString());

    response = await fetch(pingUrl.toString(), {
      signal: controller.signal,
      method: "HEAD", // Use HEAD - less data
      redirect: "error", // Don't follow redirects during ping
      headers: {
        'User-Agent': 'InstanceRedirector/1.0 (Health Check; Cloudflare Worker)'
      }
    });
    clearTimeout(timeoutId); // Clear timeout if fetch resolved
    const endTime = performance.now();
    const duration = endTime - startTime;

    // Important: Ensure the body is consumed/cancelled even for HEAD to release resources
    await response.body?.cancel().catch(e => { /* Ignore cancellation errors */ });

    if (response.ok) { // Status 200-299
      return Math.round(duration);
    } else {
      // console.warn(`[${targetService}] Ping HEAD to ${href} failed with status ${response.status}.`);
      // Return a value indicating HTTP error, distinguishable from timeout
      return timeoutMs + response.status;
    }
  } catch (error) {
    clearTimeout(timeoutId); // Clear timeout on error too
    // Ensure body is closed if response was partially received before error
    await response?.body?.cancel().catch(e => { /* Ignore */ });

    if (error instanceof DOMException && error.name === "TimeoutError") {
      // console.debug(`[${targetService}] Ping ${href} timed out after ${timeoutMs}ms.`);
      return timeoutMs; // Explicit timeout
    } else if (error instanceof DOMException && error.name === "AbortError") {
      // If abort happened *very* close to the timeout, treat it as a timeout
      if (performance.now() - startTime >= timeoutMs - 50) { // 50ms buffer
         // console.debug(`[${targetService}] Ping ${href} aborted
