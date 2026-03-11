/**
 * SASL authentication mechanisms for the Kafka client.
 *
 * Implements the authentication token exchange for each supported SASL
 * mechanism. Each mechanism provides a function to produce the initial
 * client message and an async step function to process server challenges.
 *
 * Supported mechanisms:
 * - **PLAIN** — Single-step; sends username and password in one message (RFC 4616).
 * - **SCRAM-SHA-256** — Multi-step challenge–response with SHA-256 (RFC 5802).
 * - **SCRAM-SHA-512** — Multi-step challenge–response with SHA-512 (RFC 5802).
 *
 * @packageDocumentation
 */

import type { SaslConfig } from "../config.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * A SASL authenticator that produces auth tokens for the SaslAuthenticate API.
 *
 * All mechanisms use the async interface — single-step mechanisms like PLAIN
 * simply resolve immediately in `stepAsync`.
 */
export type SaslAuthenticator = {
  /** The mechanism name (e.g. "PLAIN", "SCRAM-SHA-256"). */
  readonly mechanism: string
  /**
   * Create the initial client auth bytes.
   * For single-step mechanisms (PLAIN) this is the only call needed.
   * For multi-step mechanisms (SCRAM) this returns the client-first message.
   */
  readonly initialAuthBytes: () => Uint8Array
  /**
   * Process a server challenge and produce the next client response.
   * Returns `null` when authentication is complete (final server message validated).
   * @throws {Error} If the server challenge is invalid.
   */
  readonly stepAsync: (serverBytes: Uint8Array) => Promise<Uint8Array | null>
}

// ---------------------------------------------------------------------------
// PLAIN mechanism (RFC 4616)
// ---------------------------------------------------------------------------

const TEXT_ENCODER = new TextEncoder()

/**
 * Create a SASL/PLAIN authenticator.
 *
 * The PLAIN mechanism sends a single message containing:
 * `\0{username}\0{password}` (UTF-8 encoded).
 *
 * @see https://tools.ietf.org/html/rfc4616
 */
export function createPlainAuthenticator(config: SaslConfig): SaslAuthenticator {
  return {
    mechanism: "PLAIN",

    initialAuthBytes(): Uint8Array {
      // PLAIN format: [authzid] NUL authcid NUL passwd
      // Kafka uses empty authzid, username as authcid, password as passwd
      const usernameBytes = TEXT_ENCODER.encode(config.username)
      const passwordBytes = TEXT_ENCODER.encode(config.password)
      const buf = new Uint8Array(1 + usernameBytes.length + 1 + passwordBytes.length)
      // buf[0] = 0x00 (empty authzid — already zero-initialised)
      buf.set(usernameBytes, 1)
      // buf[1 + usernameBytes.length] = 0x00 (separator — already zero-initialised)
      buf.set(passwordBytes, 1 + usernameBytes.length + 1)
      return buf
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async stepAsync(_serverBytes: Uint8Array): Promise<Uint8Array | null> {
      // PLAIN is single-step; the server response is an empty success message
      return null
    }
  }
}

// ---------------------------------------------------------------------------
// SCRAM mechanism (RFC 5802)
// ---------------------------------------------------------------------------

const TEXT_DECODER = new TextDecoder()

/**
 * Hash algorithm descriptor for SCRAM variants.
 */
export type ScramAlgorithm = {
  readonly name: "SHA-256" | "SHA-512"
  readonly hashName: "SHA-256" | "SHA-512"
  readonly hashLength: number
}

const SCRAM_SHA_256: ScramAlgorithm = {
  name: "SHA-256",
  hashName: "SHA-256",
  hashLength: 32
}

const SCRAM_SHA_512: ScramAlgorithm = {
  name: "SHA-512",
  hashName: "SHA-512",
  hashLength: 64
}

/**
 * Generate a random nonce for SCRAM authentication.
 */
function generateNonce(): string {
  const bytes = new Uint8Array(24)
  crypto.getRandomValues(bytes)
  return uint8ArrayToBase64(bytes)
}

/**
 * Extract a plain ArrayBuffer from a Uint8Array. Needed because
 * Uint8Array.buffer may be SharedArrayBuffer, which Web Crypto rejects.
 */
function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}

/**
 * HMAC using Web Crypto API.
 */
async function hmac(
  algorithm: ScramAlgorithm,
  key: Uint8Array,
  data: Uint8Array
): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    toArrayBuffer(key),
    { name: "HMAC", hash: algorithm.hashName },
    false,
    ["sign"]
  )
  const sig = await crypto.subtle.sign("HMAC", cryptoKey, toArrayBuffer(data))
  return new Uint8Array(sig)
}

/**
 * Hash using Web Crypto API.
 */
async function hash(algorithm: ScramAlgorithm, data: Uint8Array): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest(algorithm.hashName, toArrayBuffer(data))
  return new Uint8Array(digest)
}

/**
 * SCRAM Hi() function — PBKDF2 key derivation.
 */
async function hi(
  algorithm: ScramAlgorithm,
  password: Uint8Array,
  salt: Uint8Array,
  iterations: number
): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", toArrayBuffer(password), "PBKDF2", false, [
    "deriveBits"
  ])
  const bits = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      hash: algorithm.hashName,
      salt: toArrayBuffer(salt),
      iterations
    },
    key,
    algorithm.hashLength * 8
  )
  return new Uint8Array(bits)
}

/**
 * XOR two equal-length Uint8Arrays.
 */
function xorBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length)
  for (let i = 0; i < a.length; i++) {
    result[i] = a[i] ^ b[i]
  }
  return result
}

/**
 * Encode Uint8Array to base64 string.
 */
function uint8ArrayToBase64(bytes: Uint8Array): string {
  let binary = ""
  for (const b of bytes) {
    binary += String.fromCharCode(b)
  }
  return btoa(binary)
}

/**
 * Decode base64 string to Uint8Array.
 */
function base64ToUint8Array(b64: string): Uint8Array {
  const binary = atob(b64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

/**
 * Parse SCRAM server attributes from a comma-separated string.
 * Returns a map of single-character keys to values.
 */
function parseScramAttributes(message: string): Map<string, string> {
  const attrs = new Map<string, string>()
  for (const part of message.split(",")) {
    const eq = part.indexOf("=")
    if (eq > 0) {
      attrs.set(part.slice(0, eq), part.slice(eq + 1))
    }
  }
  return attrs
}

/**
 * Escape SCRAM username per RFC 5802: '=' → '=3D', ',' → '=2C'.
 */
function escapeUsername(username: string): string {
  return username.replace(/=/g, "=3D").replace(/,/g, "=2C")
}

/**
 * Constant-time comparison of two byte arrays to prevent timing attacks.
 */
function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false
  }
  let diff = 0
  for (let i = 0; i < a.length; i++) {
    diff |= a[i] ^ b[i]
  }
  return diff === 0
}

/**
 * Create a SASL/SCRAM authenticator.
 *
 * SCRAM is a multi-step challenge–response mechanism:
 * 1. Client sends client-first message (nonce, username)
 * 2. Server responds with server-first message (salt, iterations, combined nonce)
 * 3. Client sends client-final message (proof)
 * 4. Server responds with server-final message (verifier)
 *
 * Uses Web Crypto API for PBKDF2 and HMAC operations.
 *
 * @param config - SASL configuration with username and password.
 * @param algorithm - The SCRAM hash algorithm to use.
 *
 * @see https://tools.ietf.org/html/rfc5802
 */
export function createScramAuthenticator(
  config: SaslConfig,
  algorithm: ScramAlgorithm
): SaslAuthenticator {
  const clientNonce = generateNonce()
  const gs2Header = "n,,"
  const clientFirstBare = `n=${escapeUsername(config.username)},r=${clientNonce}`

  let savedSaltedPassword: Uint8Array | null = null
  let savedAuthMessage: string | null = null
  let stage: "server-first" | "server-final" | "done" = "server-first"

  return {
    mechanism: `SCRAM-${algorithm.name}`,

    initialAuthBytes(): Uint8Array {
      return TEXT_ENCODER.encode(`${gs2Header}${clientFirstBare}`)
    },

    async stepAsync(serverBytes: Uint8Array): Promise<Uint8Array | null> {
      if (stage === "server-first") {
        const serverFirstMessage = TEXT_DECODER.decode(serverBytes)
        const attrs = parseScramAttributes(serverFirstMessage)

        const combinedNonce = attrs.get("r")
        const saltB64 = attrs.get("s")
        const iterationsStr = attrs.get("i")

        if (combinedNonce === undefined || saltB64 === undefined || iterationsStr === undefined) {
          throw new Error("invalid server-first-message: missing required attributes")
        }

        if (!combinedNonce.startsWith(clientNonce)) {
          throw new Error(
            "invalid server-first-message: server nonce does not start with client nonce"
          )
        }

        const salt = base64ToUint8Array(saltB64)
        const iterations = Number.parseInt(iterationsStr, 10)

        if (Number.isNaN(iterations) || iterations < 1) {
          throw new Error("invalid server-first-message: invalid iteration count")
        }

        // SaltedPassword = Hi(password, salt, iterations)
        const passwordBytes = TEXT_ENCODER.encode(config.password)
        const saltedPassword = await hi(algorithm, passwordBytes, salt, iterations)

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        const clientKey = await hmac(algorithm, saltedPassword, TEXT_ENCODER.encode("Client Key"))

        // StoredKey = H(ClientKey)
        const storedKey = await hash(algorithm, clientKey)

        // Build client-final-message-without-proof
        const channelBinding = uint8ArrayToBase64(TEXT_ENCODER.encode(gs2Header))
        const clientFinalWithoutProof = `c=${channelBinding},r=${combinedNonce}`
        const authMessage = `${clientFirstBare},${serverFirstMessage},${clientFinalWithoutProof}`

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        const clientSignature = await hmac(algorithm, storedKey, TEXT_ENCODER.encode(authMessage))

        // ClientProof = ClientKey XOR ClientSignature
        const clientProof = xorBytes(clientKey, clientSignature)

        // client-final-message = client-final-message-without-proof "," proof
        const clientFinalMessage = `${clientFinalWithoutProof},p=${uint8ArrayToBase64(clientProof)}`

        savedSaltedPassword = saltedPassword
        savedAuthMessage = authMessage
        stage = "server-final"

        return TEXT_ENCODER.encode(clientFinalMessage)
      }

      if (stage === "server-final") {
        const serverFinalMessage = TEXT_DECODER.decode(serverBytes)
        const attrs = parseScramAttributes(serverFinalMessage)

        const err = attrs.get("e")
        if (err !== undefined && err !== "") {
          throw new Error(`SCRAM authentication failed: ${err}`)
        }

        const verifierB64 = attrs.get("v")
        if (verifierB64 === undefined || verifierB64 === "") {
          throw new Error("invalid server-final-message: missing verifier")
        }

        // Verify: ServerKey = HMAC(SaltedPassword, "Server Key")
        //         ServerSignature = HMAC(ServerKey, AuthMessage)
        if (savedSaltedPassword !== null && savedAuthMessage !== null) {
          const serverKey = await hmac(
            algorithm,
            savedSaltedPassword,
            TEXT_ENCODER.encode("Server Key")
          )
          const expectedSignature = await hmac(
            algorithm,
            serverKey,
            TEXT_ENCODER.encode(savedAuthMessage)
          )
          const actualSignature = base64ToUint8Array(verifierB64)

          if (!constantTimeEqual(expectedSignature, actualSignature)) {
            throw new Error("SCRAM server verification failed: server signature mismatch")
          }
        }

        stage = "done"
        return null
      }

      return null
    }
  }
}

/**
 * Create a SASL authenticator based on the mechanism in the config.
 *
 * @param config - SASL configuration specifying mechanism, username, and password.
 * @returns A SASL authenticator for the configured mechanism.
 */
export function createSaslAuthenticator(config: SaslConfig): SaslAuthenticator {
  switch (config.mechanism) {
    case "PLAIN":
      return createPlainAuthenticator(config)
    case "SCRAM-SHA-256":
      return createScramAuthenticator(config, SCRAM_SHA_256)
    case "SCRAM-SHA-512":
      return createScramAuthenticator(config, SCRAM_SHA_512)
  }
}
