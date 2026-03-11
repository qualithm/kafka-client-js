import { describe, expect, it } from "vitest"

import {
  createPlainAuthenticator,
  createSaslAuthenticator,
  type SaslAuthenticator
} from "../../../network/sasl"

const TEXT_ENCODER = new TextEncoder()
const TEXT_DECODER = new TextDecoder()

// ---------------------------------------------------------------------------
// Helpers for SCRAM server-final stage tests
// ---------------------------------------------------------------------------

function uint8ArrayToBase64(bytes: Uint8Array): string {
  let binary = ""
  for (const b of bytes) {
    binary += String.fromCharCode(b)
  }
  return btoa(binary)
}

function base64ToUint8Array(b64: string): Uint8Array {
  const binary = atob(b64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

/**
 * Drive a SCRAM authenticator through the server-first stage and return
 * the computed auth message (needed to build a valid server-final).
 */
async function completeServerFirstStage(
  auth: SaslAuthenticator,
  _password: string
): Promise<{
  serverNonce: string
  saltB64: string
  iterations: number
  serverFirstMessage: string
  authMessage: string
}> {
  const clientFirst = TEXT_DECODER.decode(auth.initialAuthBytes())
  const clientFirstBare = clientFirst.slice(3) // strip "n,,"
  const nonceMatch = /r=([A-Za-z0-9+/=]+)/.exec(clientFirstBare)
  if (!nonceMatch) {
    throw new Error("failed to extract client nonce")
  }
  const clientNonce = nonceMatch[1]

  const serverNonce = `${clientNonce}serverpart`
  const saltB64 = btoa("salt1234")
  const iterations = 4096
  const serverFirstMessage = `r=${serverNonce},s=${saltB64},i=${String(iterations)}`

  const clientFinalBytes = await auth.stepAsync(TEXT_ENCODER.encode(serverFirstMessage))
  if (!clientFinalBytes) {
    throw new Error("expected client-final bytes")
  }

  const clientFinal = TEXT_DECODER.decode(clientFinalBytes)
  const pIndex = clientFinal.lastIndexOf(",p=")
  const clientFinalWithoutProof = clientFinal.slice(0, pIndex)
  const authMessage = `${clientFirstBare},${serverFirstMessage},${clientFinalWithoutProof}`

  return { serverNonce, saltB64, iterations, serverFirstMessage, authMessage }
}

/**
 * Compute the SCRAM server signature for verification in the server-final message.
 */
async function computeServerSignature(
  password: string,
  saltB64: string,
  iterations: number,
  authMessage: string,
  hashName: "SHA-256" | "SHA-512",
  hashBits: number
): Promise<string> {
  const passwordBytes = TEXT_ENCODER.encode(password)
  const salt = base64ToUint8Array(saltB64)
  const saltBuffer = salt.buffer.slice(
    salt.byteOffset,
    salt.byteOffset + salt.byteLength
  ) as ArrayBuffer

  // SaltedPassword = PBKDF2(password, salt, iterations)
  const pbkdfKey = await crypto.subtle.importKey("raw", passwordBytes, "PBKDF2", false, [
    "deriveBits"
  ])
  const saltedPassword = new Uint8Array(
    await crypto.subtle.deriveBits(
      { name: "PBKDF2", hash: hashName, salt: saltBuffer, iterations },
      pbkdfKey,
      hashBits
    )
  )

  // ServerKey = HMAC(SaltedPassword, "Server Key")
  const skKey = await crypto.subtle.importKey(
    "raw",
    saltedPassword,
    { name: "HMAC", hash: hashName },
    false,
    ["sign"]
  )
  const serverKey = new Uint8Array(
    await crypto.subtle.sign("HMAC", skKey, TEXT_ENCODER.encode("Server Key"))
  )

  // ServerSignature = HMAC(ServerKey, AuthMessage)
  const sigKey = await crypto.subtle.importKey(
    "raw",
    serverKey,
    { name: "HMAC", hash: hashName },
    false,
    ["sign"]
  )
  const serverSignature = new Uint8Array(
    await crypto.subtle.sign("HMAC", sigKey, TEXT_ENCODER.encode(authMessage))
  )

  return uint8ArrayToBase64(serverSignature)
}

// ---------------------------------------------------------------------------
// PLAIN mechanism
// ---------------------------------------------------------------------------

describe("createPlainAuthenticator", () => {
  it("produces correct auth bytes format", () => {
    const auth = createPlainAuthenticator({
      mechanism: "PLAIN",
      username: "user",
      password: "pass"
    })

    expect(auth.mechanism).toBe("PLAIN")
    const bytes = auth.initialAuthBytes()

    // PLAIN format: \0user\0pass
    expect(bytes[0]).toBe(0)
    expect(TEXT_DECODER.decode(bytes.subarray(1, 5))).toBe("user")
    expect(bytes[5]).toBe(0)
    expect(TEXT_DECODER.decode(bytes.subarray(6))).toBe("pass")
    expect(bytes.byteLength).toBe(1 + 4 + 1 + 4) // \0 + "user" + \0 + "pass"
  })

  it("handles empty username and password", () => {
    const auth = createPlainAuthenticator({
      mechanism: "PLAIN",
      username: "",
      password: ""
    })

    const bytes = auth.initialAuthBytes()
    // \0\0\0 — empty authzid, empty username, empty password
    expect(bytes.byteLength).toBe(2) // \0 + "" + \0 + ""
    expect(bytes[0]).toBe(0)
    expect(bytes[1]).toBe(0)
  })

  it("handles unicode username and password", () => {
    const auth = createPlainAuthenticator({
      mechanism: "PLAIN",
      username: "café",
      password: "naïve"
    })

    const bytes = auth.initialAuthBytes()
    const usernameBytes = TEXT_ENCODER.encode("café")
    const passwordBytes = TEXT_ENCODER.encode("naïve")

    expect(bytes.byteLength).toBe(1 + usernameBytes.length + 1 + passwordBytes.length)
  })

  it("step returns null (single-step mechanism)", async () => {
    const auth = createPlainAuthenticator({
      mechanism: "PLAIN",
      username: "u",
      password: "p"
    })

    const result = await auth.stepAsync(new Uint8Array(0))
    expect(result).toBe(null)
  })
})

// ---------------------------------------------------------------------------
// SCRAM mechanisms
// ---------------------------------------------------------------------------

describe("createSaslAuthenticator", () => {
  it("creates a PLAIN authenticator", () => {
    const auth = createSaslAuthenticator({
      mechanism: "PLAIN",
      username: "user",
      password: "pass"
    })

    expect(auth.mechanism).toBe("PLAIN")
    const bytes = auth.initialAuthBytes()
    expect(bytes[0]).toBe(0)
  })

  it("creates a SCRAM-SHA-256 authenticator", () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password: "pass"
    })

    expect(auth.mechanism).toBe("SCRAM-SHA-256")
    const bytes = auth.initialAuthBytes()
    const message = TEXT_DECODER.decode(bytes)
    expect(message).toMatch(/^n,,n=user,r=/)
  })

  it("creates a SCRAM-SHA-512 authenticator", () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-512",
      username: "user",
      password: "pass"
    })

    expect(auth.mechanism).toBe("SCRAM-SHA-512")
    const bytes = auth.initialAuthBytes()
    const message = TEXT_DECODER.decode(bytes)
    expect(message).toMatch(/^n,,n=user,r=/)
  })
})

describe("SCRAM-SHA-256 authenticator", () => {
  it("sends correct client-first message format", () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "testuser",
      password: "testpass"
    })

    const bytes = auth.initialAuthBytes()
    const message = TEXT_DECODER.decode(bytes)

    // Must start with gs2-header "n,,"
    expect(message.startsWith("n,,")).toBe(true)

    // Must contain n=username
    expect(message).toContain("n=testuser")

    // Must contain r=nonce
    expect(message).toMatch(/r=[A-Za-z0-9+/=]+/)
  })

  it("escapes special characters in username", () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user=name,test",
      password: "pass"
    })

    const bytes = auth.initialAuthBytes()
    const message = TEXT_DECODER.decode(bytes)

    // '=' must be escaped to '=3D', ',' to '=2C'
    expect(message).toContain("n=user=3Dname=2Ctest")
  })

  it("rejects invalid server-first-message", async () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password: "pass"
    })

    auth.initialAuthBytes()

    // Server sends garbage
    await expect(auth.stepAsync(TEXT_ENCODER.encode("garbage"))).rejects.toThrow(
      "missing required attributes"
    )
  })

  it("rejects server nonce that does not contain client nonce", async () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password: "pass"
    })

    auth.initialAuthBytes()

    // Server sends a valid-looking but wrong nonce
    const serverFirst = TEXT_ENCODER.encode("r=wrongnonce,s=c2FsdA==,i=4096")
    await expect(auth.stepAsync(serverFirst)).rejects.toThrow(
      "server nonce does not start with client nonce"
    )
  })

  it("completes full SCRAM exchange with valid server", async () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password: "pencil"
    })

    // Get client-first-message
    const clientFirst = TEXT_DECODER.decode(auth.initialAuthBytes())
    const clientFirstBare = clientFirst.slice(3) // Remove "n,,"

    // Extract client nonce
    const nonceMatch = /r=([A-Za-z0-9+/=]+)/.exec(clientFirstBare)
    expect(nonceMatch).not.toBe(null)
    const clientNonce = nonceMatch![1]

    // Simulate server-first-message
    const serverNonce = `${clientNonce}serverpart`
    // Use a known salt
    const salt = btoa("salt1234")
    const iterations = 4096

    const serverFirstMessage = `r=${serverNonce},s=${salt},i=${String(iterations)}`
    const clientFinalBytes = await auth.stepAsync(TEXT_ENCODER.encode(serverFirstMessage))

    expect(clientFinalBytes).not.toBe(null)
    const clientFinal = TEXT_DECODER.decode(clientFinalBytes!)

    // client-final must contain channel binding and nonce
    expect(clientFinal).toContain(`r=${serverNonce}`)
    expect(clientFinal).toMatch(/c=[A-Za-z0-9+/=]+/)
    expect(clientFinal).toMatch(/p=[A-Za-z0-9+/=]+/)
  })

  it("rejects invalid iteration count (NaN)", async () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password: "pass"
    })

    const clientFirst = TEXT_DECODER.decode(auth.initialAuthBytes())
    const clientFirstBare = clientFirst.slice(3)
    const clientNonce = /r=([A-Za-z0-9+/=]+)/.exec(clientFirstBare)![1]

    const serverFirst = `r=${clientNonce}srv,s=${btoa("salt")},i=notanumber`
    await expect(auth.stepAsync(TEXT_ENCODER.encode(serverFirst))).rejects.toThrow(
      "invalid iteration count"
    )
  })

  it("rejects invalid iteration count (zero)", async () => {
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password: "pass"
    })

    const clientFirst = TEXT_DECODER.decode(auth.initialAuthBytes())
    const clientFirstBare = clientFirst.slice(3)
    const clientNonce = /r=([A-Za-z0-9+/=]+)/.exec(clientFirstBare)![1]

    const serverFirst = `r=${clientNonce}srv,s=${btoa("salt")},i=0`
    await expect(auth.stepAsync(TEXT_ENCODER.encode(serverFirst))).rejects.toThrow(
      "invalid iteration count"
    )
  })

  it("completes full SCRAM exchange including server-final verification", async () => {
    const password = "pencil"
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password
    })

    const { saltB64, iterations, authMessage } = await completeServerFirstStage(auth, password)

    // Compute correct server signature
    const verifier = await computeServerSignature(
      password,
      saltB64,
      iterations,
      authMessage,
      "SHA-256",
      256
    )

    // server-final with valid verifier
    const result = await auth.stepAsync(TEXT_ENCODER.encode(`v=${verifier}`))
    expect(result).toBeNull()
  })

  it("rejects server-final with error attribute", async () => {
    const password = "pencil"
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password
    })

    await completeServerFirstStage(auth, password)

    await expect(auth.stepAsync(TEXT_ENCODER.encode("e=invalid-proof"))).rejects.toThrow(
      "SCRAM authentication failed: invalid-proof"
    )
  })

  it("rejects server-final with missing verifier", async () => {
    const password = "pencil"
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password
    })

    await completeServerFirstStage(auth, password)

    await expect(auth.stepAsync(TEXT_ENCODER.encode("x=something"))).rejects.toThrow(
      "missing verifier"
    )
  })

  it("rejects server-final with mismatched server signature", async () => {
    const password = "pencil"
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password
    })

    await completeServerFirstStage(auth, password)

    // Provide a wrong signature
    const bogusSignature = btoa("this-is-not-a-valid-signature!!")
    await expect(auth.stepAsync(TEXT_ENCODER.encode(`v=${bogusSignature}`))).rejects.toThrow(
      "server signature mismatch"
    )
  })

  it("returns null when stepped after done stage", async () => {
    const password = "pencil"
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-256",
      username: "user",
      password
    })

    const { saltB64, iterations, authMessage } = await completeServerFirstStage(auth, password)

    const verifier = await computeServerSignature(
      password,
      saltB64,
      iterations,
      authMessage,
      "SHA-256",
      256
    )

    // Complete server-final
    await auth.stepAsync(TEXT_ENCODER.encode(`v=${verifier}`))

    // Step again after done — should return null
    const result = await auth.stepAsync(new Uint8Array(0))
    expect(result).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// SCRAM-SHA-512
// ---------------------------------------------------------------------------

describe("SCRAM-SHA-512 authenticator", () => {
  it("completes full exchange including server-final verification", async () => {
    const password = "pencil"
    const auth = createSaslAuthenticator({
      mechanism: "SCRAM-SHA-512",
      username: "user",
      password
    })

    const { saltB64, iterations, authMessage } = await completeServerFirstStage(auth, password)

    const verifier = await computeServerSignature(
      password,
      saltB64,
      iterations,
      authMessage,
      "SHA-512",
      512
    )

    const result = await auth.stepAsync(TEXT_ENCODER.encode(`v=${verifier}`))
    expect(result).toBeNull()
  })
})
