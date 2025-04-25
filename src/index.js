/**
 * Cloudflare Worker Script for Redirecting to the Fastest Instance
 *
 * This worker fetches a list of potential instances for a specified service
 * (like Nitter, Invidious, Libreddit etc.) from a JSON source. It pings a
 * subset of these instances to find the fastest responsive one, caches the result
 * in KV storage, and redirects incoming requests to that fastest instance.
 */

// --- Configuration ---
const CACHE_TTL_SECONDS = 10 * 60; // 10 minutes (for KV expirationTtl)
const CACHE_TTL_MS = CACHE_TTL_SECONDS * 1000; // 10 minutes in milliseconds (for internal checking)
const PING_TIMEOUT_MS = 8000; // 8 seconds timeout for pinging individual instances
const PING_METHOD = "GET"; // Use "GET" instead of "HEAD" as it might be more reliable for some instances

/**
 * @typedef {object} Env
 * @property {KVNamespace} ['redlib-instances'] - Binding for the KV namespace (MUST match wrangler.toml).
 * @property {string} [TARGET_SERVICE] - Specific service override (e.g., from path/query/header).
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
    const url = new URL(request.url);
    const targetServiceQuery = url.searchParams.get('service');
    const targetServiceHeader = request.headers.get('X-Service');
    // Use query param, header, worker env var, or default
    const TARGET_SERVICE = targetServiceQuery || targetServiceHeader || env.TARGET_SERVICE || env.DEFAULT_TARGET_SERVICE || "redlib.clearnet";
    // Use worker env var or hardcoded default
    const INSTANCES_SOURCE_URL = env.INSTANCES_SOURCE_URL || "https://raw.githubusercontent.com/libredirect/instances/main/data.json";

    // --- KV Setup ---
    // IMPORTANT: Ensure 'redlib-instances' exactly matches the binding name in your wrangler.toml
    const KV_BINDING_NAME = 'redlib-instances';
    console.log(`DEBUG: Available env keys: ${JSON.stringify(Object.keys(env))}`);
    const kvStore = env[KV_BINDING_NAME];
    console.log(`DEBUG: Type of env['${KV_BINDING_NAME}'] is: ${typeof kvStore}`);

    // Check if KV binding is valid before proceeding
    if (!kvStore || typeof kvStore.get !== 'function') {
      console.error(`[${TARGET_SERVICE}] CRITICAL ERROR: KV Namespace binding '${KV_BINDING_NAME}' is invalid or not configured correctly. Type: ${typeof kvStore}. Check wrangler.toml and deployment.`);
      console.log("DEBUG: Returning 500 due to invalid KV binding.");
      // Ensure the error response itself is valid
      return new Response(`Configuration Error: KV binding '${KV_BINDING_NAME}' is invalid or not found.`, { status: 500, headers: { 'Content-Type': 'text/plain' } });
    }
    console.log(`DEBUG: KV Binding '${KV_BINDING_NAME}' seems valid (has .get method).`);

    // Use a service-specific cache key
    const CACHE_KEY = `instance_cache:${TARGET_SERVICE}`;
    console.log(`[${TARGET_SERVICE}] Received request: ${request.method} ${request.url}`);

    try {
      // 1. Check Cache
      console.log(`DEBUG: Checking cache with key: ${CACHE_KEY}`);
      const cached = await getCachedInstance(kvStore, CACHE_KEY, TARGET_SERVICE);
      if (cached) {
        console.log(`DEBUG: Returning redirect response from cache to: ${cached}`);
        return createRedirectResponse(cached, request.url, TARGET_SERVICE);
      }
      console.log(`DEBUG: Cache miss or expired for key: ${CACHE_KEY}`);

      // 2. Cache Miss/Expired: Find and Cache New Instance
      console.log(`[${TARGET_SERVICE}] Cache miss/expired. Finding new instance...`);
      // Fetch the list of potential instances
      const instances = await getInstanceList(TARGET_SERVICE, INSTANCES_SOURCE_URL);

      // Handle case where fetching/finding instances failed
      if (!instances || instances.length === 0) {
          console.error(`[${TARGET_SERVICE}] No instances found or fetched from ${INSTANCES_SOURCE_URL}. Cannot proceed.`);
          console.log("DEBUG: Returning 503 due to no instances found/fetched.");
          return new Response(`Service Unavailable: No instances configured or fetched for ${TARGET_SERVICE}`, { status: 503, headers: { 'Content-Type': 'text/plain' } });
      }
      console.log(`DEBUG: Found ${instances.length} potential instances. Attempting to find fastest.`);

      // Find the fastest instance asynchronously
      const fastestUrl = await findAndCacheFastestInstance(
        instances,
        kvStore,
        CACHE_KEY,
        TARGET_SERVICE,
        ctx
      );

      // Redirect to the fastest instance if found
      if (fastestUrl) {
        console.log(`DEBUG: Found fastest instance ${fastestUrl}. Returning redirect response.`);
        return createRedirectResponse(fastestUrl, request.url, TARGET_SERVICE);
      } else {
        // Failed to find any working instance after trying
        console.error(`[${TARGET_SERVICE}] Failed to find any responsive instance after pinging subset.`);
        console.log("DEBUG: Returning 503 due to no responsive backend instances found.");
        return new Response("Service Unavailable: No backend instances available or responding", { status: 503, headers: { 'Content-Type': 'text/plain' } });
      }

    } catch (error) {
      // Catch unexpected errors in the main flow
      console.error(`[${TARGET_SERVICE}] Internal Server Error in main fetch handler:`, error.message, error.stack);
      console.log("DEBUG: Returning 500 due to caught error in main fetch handler.");
      // Try to return a standard error response
      try {
          return new Response("Internal Server Error", { status: 500, headers: { 'Content-Type': 'text/plain' } });
      } catch (responseError) {
          // Fallback if creating the error Response fails itself
          console.error("CRITICAL: Error creating the error response itself!", responseError);
          return new Response("Internal Server Error", { status: 500, headers: { 'Content-Type': 'text/plain' } });
      }
    }
  } // End of fetch handler
}; // End of export default

// =============================================================================
// --- Helper Functions ---
// =============================================================================

/**
 * Retrieves the cached instance URL if it exists and is not expired.
 * @param {KVNamespace} kvStore - The KV namespace binding.
 * @param {string} cacheKey - The key to retrieve from KV.
 * @param {string} targetService - For logging purposes.
 * @returns {Promise<string | null>} The cached URL or null.
 */
async function getCachedInstance(kvStore, cacheKey, targetService) {
  try {
    // Get the cached data as text first
    const cachedDataString = await kvStore.get(cacheKey);
    if (!cachedDataString) {
        console.log(`[${targetService}] Cache miss: No entry found for key ${cacheKey}.`);
        return null;
    }
    console.log(`DEBUG: Raw cache data for ${cacheKey}: ${cachedDataString}`);

    /** @type {CachedInstanceValue | null} */
    let cacheEntry = null;
    try {
        // Parse the JSON data
        cacheEntry = JSON.parse(cachedDataString);
    } catch (e) {
        console.error(`[${targetService}] Error parsing cached JSON data from KV for key ${cacheKey}:`, e);
        // Optionally delete the invalid entry
        // await kvStore.delete(cacheKey);
        return null; // Treat as cache miss if parsing fails
    }

    // Check if parsed data is valid and not expired
    if (cacheEntry && cacheEntry.url && typeof cacheEntry.timestamp === 'number') {
        const age = Date.now() - cacheEntry.timestamp;
        if (age < CACHE_TTL_MS) {
            console.log(`[${targetService}] Cache hit: Using ${cacheEntry.url} (verified at ${new Date(cacheEntry.timestamp).toISOString()}, age ${Math.round(age/1000)}s)`);
            return cacheEntry.url;
        } else {
            console.log(`[${targetService}] Cache expired: Entry from ${new Date(cacheEntry.timestamp).toISOString()} (age ${Math.round(age/1000)}s) is older than TTL (${CACHE_TTL_SECONDS}s).`);
            return null;
        }
    } else {
         console.warn(`[${targetService}] Invalid cache data structure for key ${cacheKey}.`);
         return null; // Treat as cache miss if data structure is wrong
    }

  } catch (error) {
      console.error(`[${targetService}] Error reading from Cloudflare KV [${cacheKey}]:`, error);
  }
  // Fallthrough case: error occurred during KV read
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
  // Find the fastest instance from the provided list
  const newFastestUrl = await autoPickInstance(instances, targetService);

  if (newFastestUrl) {
    console.log(`[${targetService}] Determined fastest instance: ${newFastestUrl}`);
    /** @type {CachedInstanceValue} */
    const cacheValue = {
      url: newFastestUrl,
      timestamp: Date.now()
    };

    // Cache the result in KV in the background (don't block the response)
    ctx.waitUntil(
        (async () => {
            try {
                // Store the data as a JSON string with expiration
                await kvStore.put(cacheKey, JSON.stringify(cacheValue), { expirationTtl: CACHE_TTL_SECONDS });
                console.log(`[${targetService}] Cache updated in KV [${cacheKey}]: Stored ${newFastestUrl} with TTL ${CACHE_TTL_SECONDS}s.`);
            } catch (error) {
                // Log the error but don't fail the main request if KV write fails
                console.error(`[${targetService}] Failed to write to Cloudflare KV [${cacheKey}]:`, error);
            }
        })()
    );
    return newFastestUrl; // Return the found URL immediately
  }

  // Only log warning if no instance could be determined
  console.warn(`[${targetService}] Could not determine a fastest instance from the list.`);
  return null;
}

/**
 * Creates the final 307 Redirect Response object.
 * @param {string} instanceBaseUrl - The base URL of the chosen instance.
 * @param {string} originalRequestUrlString - The full URL of the original request.
 * @param {string} targetService - For logging purposes.
 * @returns {Response} A Response object (either redirect or error).
 */
function createRedirectResponse(instanceBaseUrl, originalRequestUrlString, targetService) {
    // Construct the final URL (e.g., instance base + original path/query)
    const finalRedirectUrl = constructRedirectUrl(instanceBaseUrl, originalRequestUrlString, targetService);

    if (finalRedirectUrl) {
        console.log(`[${targetService}] Redirecting client to: ${finalRedirectUrl}`);
        // Use 307 Temporary Redirect to preserve method and body for non-GET requests
        return Response.redirect(finalRedirectUrl, 307);
    } else {
        // This should only happen if constructRedirectUrl fails unexpectedly
        console.error(`[${targetService}] Critical Error: Failed to construct final redirect URL from base ${instanceBaseUrl} and original ${originalRequestUrlString}.`);
        return new Response("Internal Server Error: Failed to construct redirect URL", { status: 500, headers: { 'Content-Type': 'text/plain' } });
    }
}

/**
 * Constructs the final redirect URL by combining the instance base URL
 * with the path and query parameters of the original request.
 * @param {string} instanceBaseUrl - Base URL of the target instance (e.g., "https://nitter.net").
 * @param {string} originalRequestUrlString - Full original request URL.
 * @param {string} targetService - For logging.
 * @returns {string | null} The final URL string or null on error.
 */
function constructRedirectUrl(instanceBaseUrl, originalRequestUrlString, targetService) {
    try {
        // Ensure instanceBaseUrl is a valid URL structure
        const base = new URL(instanceBaseUrl);
        let safeInstanceBaseUrl = instanceBaseUrl;

        // Ensure the base URL ends with a slash if it's just the origin,
        // for correct relative path resolution by the URL constructor.
        // Example: 'https://nitter.net' becomes 'https://nitter.net/'
        // Example: 'https://nitter.net/path' remains unchanged
        if (!base.pathname || base.pathname === '/') {
            safeInstanceBaseUrl = instanceBaseUrl.endsWith('/') ? instanceBaseUrl : instanceBaseUrl + '/';
        }

        // Parse the original request URL
        const originalUrl = new URL(originalRequestUrlString);

        // Combine instance base (scheme, host, port) with original path, search, and hash
        // Example: base='https://nitter.net/', original='/user?q=a' -> 'https://nitter.net/user?q=a'
        const targetUrl = new URL(originalUrl.pathname + originalUrl.search + originalUrl.hash, safeInstanceBaseUrl);

        return targetUrl.toString();
    } catch (error) {
        console.error(`[${targetService}] Error constructing redirect URL from base '${instanceBaseUrl}' and original '${originalRequestUrlString}':`, error);
        return null; // Return null if URL construction fails
    }
}

/**
 * Fetches and parses the instance list for a specific service from the source URL.
 * Returns an array of instance URLs (strings) or an empty array on failure.
 * @param {string} serviceIdentifier - e.g., "nitter" or "redlib.clearnet".
 * @param {string} sourceUrl - URL of the JSON data source.
 * @returns {Promise<string[]>} Array of valid instance URLs.
 */
async function getInstanceList(serviceIdentifier, sourceUrl) {
  console.log(`[${serviceIdentifier}] Fetching instance list from ${sourceUrl}`);
  try {
    // Use AbortController for fetch timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(new DOMException("Timeout", "TimeoutError")), 10000); // 10 second timeout

    const response = await fetch(sourceUrl, {
        signal: controller.signal,
        headers: { 'Accept': 'application/json' } // Be explicit about expected content type
    });
    clearTimeout(timeoutId); // Clear timeout if fetch resolved

    if (!response.ok) {
      // Log specific HTTP error status
      throw new Error(`HTTP error ${response.status} ${response.statusText} fetching instance list from ${sourceUrl}`);
    }

    // Check content type before parsing (optional but good practice)
    const contentType = response.headers.get("content-type");
    if (!contentType || !contentType.includes("application/json")) {
        // This warning is expected when fetching from raw.githubusercontent.com
        console.warn(`[${serviceIdentifier}] Unexpected content-type '${contentType}' received from ${sourceUrl}. Attempting to parse as JSON anyway.`);
    }

    // Parse the JSON response
    const data = await response.json();

    // Navigate the potentially nested JSON structure based on the serviceIdentifier
    let instanceListSource = null;
    const parts = serviceIdentifier.split('.');

    if (parts.length === 2 && typeof data[parts[0]] === 'object' && data[parts[0]] !== null) {
        // Handles identifiers like "redlib.clearnet" -> data['redlib']['clearnet']
        const baseService = data[parts[0]];
        if (typeof baseService === 'object' && baseService !== null && parts[1] in baseService) {
            instanceListSource = baseService[parts[1]];
        } else {
             console.warn(`[${serviceIdentifier}] Structure error: Base service '${parts[0]}' found, but sub-key '${parts[1]}' missing in data from ${sourceUrl}.`);
        }
    } else if (parts.length === 1) {
        // Handles top-level identifiers like "nitter" -> data['nitter']
         if (serviceIdentifier in data) {
            instanceListSource = data[serviceIdentifier];
        } else {
            console.warn(`[${serviceIdentifier}] Structure error: Top-level key '${serviceIdentifier}' missing in data from ${sourceUrl}.`);
        }
    } else {
        console.error(`[${serviceIdentifier}] Invalid service identifier format: '${serviceIdentifier}'. Expected 'service' or 'service.subservice'.`);
    }

    // Validate the extracted data is an array
    if (Array.isArray(instanceListSource)) {
        // Filter for valid http/https URLs and remove trailing slashes
        const validUrls = instanceListSource
            .filter(url => typeof url === 'string' && (url.startsWith('http://') || url.startsWith('https://')))
            .map(url => url.replace(/\/$/, '')); // Remove trailing slash for consistency

        console.log(`[${serviceIdentifier}] Found ${validUrls.length} valid instances.`);
        return validUrls;
    } else {
        console.error(`[${serviceIdentifier}] Instance list for '${serviceIdentifier}' not found or not an array in the fetched data from ${sourceUrl}. Structure might be incorrect or key missing.`);
        // Log the structure near the expected key for debugging
        if (parts.length === 2 && data[parts[0]]) {
             console.log(`DEBUG: Structure under '${parts[0]}': ${JSON.stringify(Object.keys(data[parts[0]]))}`);
        } else if (parts.length === 1) {
            console.log(`DEBUG: Available top-level keys: ${JSON.stringify(Object.keys(data))}`);
        }
        return []; // Return empty array if structure is wrong
    }
  } catch (error) {
    // Catch and log various types of errors during fetch/parse
     if (error instanceof DOMException && error.name === "TimeoutError") {
        console.error(`[${serviceIdentifier}] Timeout fetching instance list from ${sourceUrl}:`, error.message);
     } else if (error instanceof SyntaxError) {
         // JSON parsing error
         console.error(`[${serviceIdentifier}] Error parsing JSON instance list from ${sourceUrl}:`, error.message);
     } else if (error instanceof TypeError) {
         // Network error, DNS error, invalid URL error during fetch
         console.error(`[${serviceIdentifier}] Network/URL error fetching instance list from ${sourceUrl}:`, error.message);
     } else {
         // Handles the HTTP error thrown above or other unexpected errors
        console.error(`[${serviceIdentifier}] Error fetching/parsing instance list from ${sourceUrl}:`, error.message);
     }
    return []; // Return empty array on any error
  }
}

/**
 * Selects the fastest instance from a list by pinging a random subset.
 * Returns the URL of the fastest responsive instance, or null if none respond.
 * @param {string[]} instances - Full list of instance URLs.
 * @param {string} targetService - For logging purposes.
 * @returns {Promise<string | null>} URL of the fastest instance or null.
 */
async function autoPickInstance(instances, targetService) {
  if (!instances || instances.length === 0) {
      console.warn(`[${targetService}] autoPickInstance called with empty or invalid instance list.`);
      return null;
  }

  // Determine the size of the subset to ping (e.g., up to 5 instances)
  const subsetSize = Math.min(instances.length, 5);
  // Get a random selection of instances
  const randomSubset = randomInstances(instances, subsetSize);
  console.log(`[${targetService}] Pinging subset (${subsetSize} instances): ${randomSubset.join(', ')}`);

  // Ping each instance in the subset concurrently
  const pingPromises = randomSubset.map(async (instanceUrl) => {
    const startTime = performance.now();
    // 'ping' returns average time in ms, PING_TIMEOUT_MS on failure/timeout, or null on network error
    const time = await ping(instanceUrl, targetService);
    const endTime = performance.now();
    return { instance: instanceUrl, time }; // time can be number, null, or >= PING_TIMEOUT_MS
  });

  // Wait for all pings in the subset to complete
  const results = await Promise.all(pingPromises);
  console.log(`DEBUG: Ping results for subset: ${JSON.stringify(results)}`);

  // Filter out failures (null or timeout/error) and sort by time
  const successfulPings = results
   .filter(r => typeof r.time === 'number' && r.time < PING_TIMEOUT_MS) // Only include successful numeric times below timeout
   .sort((a, b) => a.time - b.time); // Sort ascending by time

  // If we have at least one successful ping, return the fastest one
  if (successfulPings.length > 0) {
    const fastest = successfulPings[0];
    console.log(`[${targetService}] Fastest instance found in subset: ${fastest.instance} (${fastest.time}ms)`);
    return fastest.instance; // Return the URL string
  } else {
    // Log failure if no instances in the subset responded successfully
    console.warn(`[${targetService}] No instances in the tested subset [${randomSubset.join(', ')}] responded successfully within the timeout (${PING_TIMEOUT_MS}ms).`);
    return null;
  }
}

/**
 * Pings a URL multiple times (currently 3) and calculates an average latency.
 * Skips the first ping result (warm-up).
 * Returns average time in ms, PING_TIMEOUT_MS if any ping times out or fails validation,
 * or null on critical network/DNS error during pings.
 * @param {string} href - The URL to ping.
 * @param {string} targetService - For logging.
 * @returns {Promise<number | null>} Average ping time (ms), PING_TIMEOUT_MS (error/timeout), or null (network error).
 */
async function ping(href, targetService) {
  let totalTime = 0;
  let successfulPings = 0;
  const NUM_PINGS = 3; // Number of pings to perform
  let isUnreliable = false; // Flag if any ping fails critically or unacceptably

  for (let i = 0; i < NUM_PINGS; i++) {
    const attemptNum = i + 1;
    // pingOnce returns time (ms), >= PING_TIMEOUT_MS (timeout/HTTP error), or null (network/URL error)
    const time = await pingOnce(href, PING_TIMEOUT_MS, targetService, attemptNum);

    if (time === null) {
      // Critical network/DNS/URL error - instance is likely unreachable
      console.warn(`[${targetService}] Ping attempt ${attemptNum}/${NUM_PINGS} for ${href} failed critically (Network/DNS/URL error). Aborting checks for this instance.`);
      isUnreliable = true; // Mark as unreliable
      return null; // Return null immediately for critical failure
    }

    if (time >= PING_TIMEOUT_MS) {
      // Timeout or HTTP error code returned by pingOnce
      console.warn(`[${targetService}] Ping attempt ${attemptNum}/${NUM_PINGS} for ${href} failed (Timeout or HTTP Error >= ${PING_TIMEOUT_MS}ms). Considering instance unreliable.`);
      isUnreliable = true; // Mark as unreliable
      // Stricter: if one ping times out/errors, disqualify immediately.
      break;
    }

    // If the ping was successful (time < PING_TIMEOUT_MS)
    // Skip the first successful ping for averaging (warm-up)
    if (i > 0) {
      totalTime += time;
      successfulPings++;
    } else {
        console.debug(`[${targetService}] Ping attempt ${attemptNum}/${NUM_PINGS} for ${href} (warm-up): ${time}ms`);
    }
  }

  // If any ping failed critically or timed out/errored
  if (isUnreliable) {
      return PING_TIMEOUT_MS; // Return a value indicating failure/unreliability
  }

  // Require at least N-1 successful pings (after skipping the first) for averaging
  const requiredSuccessfulPings = NUM_PINGS - 1;
  if (successfulPings < requiredSuccessfulPings) {
      console.warn(`[${targetService}] Not enough successful pings for ${href} (${successfulPings}/${requiredSuccessfulPings} after warm-up). Considering it unreliable.`);
      return PING_TIMEOUT_MS; // Treat as unreliable if not enough data points
  }

  // Calculate and return the average time
  const averageTime = Math.round(totalTime / successfulPings);
  console.debug(`[${targetService}] Average ping time for ${href} (after warm-up): ${averageTime}ms from ${successfulPings} pings.`);
  return averageTime;
}

/**
 * Pings a URL once using the configured PING_METHOD (GET or HEAD).
 * Measures latency until headers (or potentially first byte for GET) are received.
 * Returns duration in ms on success (HTTP 2xx).
 * Returns a value >= PING_TIMEOUT_MS on timeout or HTTP error (non-2xx).
 * Returns null on critical network/DNS/URL error.
 * @param {string} href - The URL to ping.
 * @param {number} timeoutMs - Timeout duration in milliseconds.
 * @param {string} targetService - For logging.
 * @param {number} attemptNum - The attempt number (for logging).
 * @returns {Promise<number | null>} Ping time (ms), PING_TIMEOUT_MS + status code (HTTP error), PING_TIMEOUT_MS (timeout), or null (network/URL error).
 */
async function pingOnce(href, timeoutMs, targetService, attemptNum) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new DOMException("Timeout", "TimeoutError")), timeoutMs);
  const startTime = performance.now();
  let response = undefined; // Define response outside try

  try {
    // Add cache-busting query parameter to avoid CDN/browser caches influencing ping
    const pingUrl = new URL(href);
    // Ensure cache bust param doesn't have a space
    pingUrl.searchParams.set('_Lcache', Date.now().toString());

    console.debug(`[${targetService}] Ping attempt ${attemptNum}: ${PING_METHOD} ${pingUrl.toString()}`);
    response = await fetch(pingUrl.toString(), {
      signal: controller.signal,
      method: PING_METHOD, // Use configured method (GET or HEAD)
      // *** FIX: Use "manual" instead of "error" for Cloudflare Workers ***
      redirect: "manual",
      headers: {
        'User-Agent': 'InstanceRedirector/1.0 (Health Check; Cloudflare Worker)',
        'Accept': '*/*' // Accept anything, less critical for ping
      }
    });
    // If fetch resolves (successfully or with HTTP error), clear the timeout
    clearTimeout(timeoutId);
    const endTime = performance.now();
    const duration = endTime - startTime;

    // IMPORTANT: Consume or cancel the body to release resources, even if HEAD or GET with no body.
    // Use .cancel() for efficiency if body content isn't needed.
    await response.body?.cancel().catch(e => { console.warn(`[${targetService}] Error cancelling response body for ${href}: ${e.message}`) });

    // Check if the response status is OK (200-299)
    if (response.ok) {
      console.debug(`[${targetService}] Ping attempt ${attemptNum} OK for ${href}: Status ${response.status}, Duration ${duration.toFixed(0)}ms`);
      return Math.round(duration); // Return measured duration on success
    } else {
      // Log non-OK status codes (including 3xx redirects now that redirect is "manual")
      console.warn(`[${targetService}] Ping attempt ${attemptNum} FAILED for ${href}: Received HTTP Status ${response.status}`);
      // Return a value indicating HTTP error, distinguishable from timeout
      // Adding status ensures it's >= timeoutMs but encodes the status
      return timeoutMs + response.status;
    }
  } catch (error) {
    // Ensure timeout is cleared if an error occurs
    clearTimeout(timeoutId);
    // Attempt to cancel body even if error occurred mid-flight
    await response?.body?.cancel().catch(e => { /* Ignore cancel errors here */ });

    let errorType = "Unknown Error";
    let returnValue = null; // Default to null for critical errors

    // Classify the error
    if (error instanceof DOMException && error.name === "TimeoutError") {
      errorType = "Timeout";
      returnValue = timeoutMs; // Return timeout value
    } else if (error instanceof DOMException && error.name === "AbortError") {
      errorType = "Abort";
      // Check if abort happened near timeout
      if (performance.now() - startTime >= timeoutMs - 150) { // Small buffer
         console.warn(`[${targetService}] Ping attempt ${attemptNum} for ${href} aborted near/at timeout threshold.`);
         returnValue = timeoutMs; // Treat as timeout
      } else {
         console.warn(`[${targetService}] Ping attempt ${attemptNum} for ${href} aborted unexpectedly: ${error.message}`);
         returnValue = null; // Treat unexpected aborts as network/config errors
      }
    } else if (error instanceof TypeError) {
      // TypeErrors often indicate network issues (DNS resolution failure, connection refused),
      // invalid URL, or CORS issues (less likely for backend ping).
      errorType = "Network/URL Error";
      if (error.message.includes('invalid URL')) {
        console.error(`[${targetService}] Ping attempt ${attemptNum} FAILED for '${href}': Invalid URL format.`);
      } else {
        // Log other TypeErrors which are likely network-related
        console.warn(`[${targetService}] Ping attempt ${attemptNum} FAILED for ${href}: ${errorType} - ${error.message}`);
      }
      returnValue = null; // Treat as critical failure
    } else {
      // Catch any other unexpected errors
      console.error(`[${targetService}] Ping attempt ${attemptNum} FAILED unexpectedly for ${href}:`, error);
      returnValue = null; // Treat as critical failure
    }

    console.warn(`[${targetService}] Ping attempt ${attemptNum} FAILED for ${href} with ${errorType}.`);
    return returnValue;

  } finally {
      // Final safety net to clear timeout, although should be cleared in try/catch blocks.
      clearTimeout(timeoutId);
  }
}

/**
 * Selects N random instances from a given list.
 * Returns a new array containing the random selection.
 * @param {string[]} sourceList - The list to select from.
 * @param {number} n - The number of items to select.
 * @returns {string[]} A new array with N random items.
 */
function randomInstances(sourceList, n) {
  // Create a shuffled copy of the source list using Fisher-Yates
  const shuffled = [...sourceList]; // Create a mutable copy
  for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]]; // Swap elements
  }
  // Return the first N elements from the shuffled list
  // Ensure N is not greater than the list length
  return shuffled.slice(0, Math.min(n, shuffled.length));
}
