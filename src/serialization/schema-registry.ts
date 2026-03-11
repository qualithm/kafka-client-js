/**
 * Confluent Schema Registry client.
 *
 * Provides HTTP-based schema management for Avro, Protobuf, and JSON Schema
 * formats. Implements schema caching by ID and subject for efficient
 * repeated lookups. Uses the `fetch` API for runtime-agnostic HTTP.
 *
 * Subject naming strategies control how topics map to Schema Registry
 * subjects:
 * - {@link topicNameStrategy} — `{topic}-key` / `{topic}-value` (default)
 * - {@link recordNameStrategy} — Uses a fixed record name
 * - {@link topicRecordNameStrategy} — `{topic}-{recordName}`
 *
 * @packageDocumentation
 */

import { KafkaError } from "../errors.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Schema type supported by the registry. */
export type SchemaType = "AVRO" | "PROTOBUF" | "JSON"

/** A schema registered in the Schema Registry. */
export type RegisteredSchema = {
  /** Global unique schema ID. */
  readonly id: number
  /** Schema definition string (JSON for Avro, proto for Protobuf, etc.). */
  readonly schema: string
  /** Schema type. Defaults to AVRO if not present. */
  readonly schemaType?: SchemaType
  /** Subject the schema is registered under, if known. */
  readonly subject?: string
  /** Version number within the subject, if known. */
  readonly version?: number
}

/**
 * Determines the subject name for a given topic and key/value position.
 *
 * @param topic - Kafka topic name.
 * @param isKey - Whether this is for the message key (true) or value (false).
 * @returns The subject name to use in the Schema Registry.
 */
export type SubjectNameStrategy = (topic: string, isKey: boolean) => string

/** Configuration for the Schema Registry client. */
export type SchemaRegistryConfig = {
  /** Base URL of the Schema Registry (e.g. `"http://localhost:8081"`). */
  readonly url: string
  /** Basic auth credentials for the Schema Registry. */
  readonly auth?: {
    readonly username: string
    readonly password: string
  }
}

// ---------------------------------------------------------------------------
// Subject Naming Strategies
// ---------------------------------------------------------------------------

/**
 * Default subject naming strategy: `{topic}-key` or `{topic}-value`.
 *
 * This is the standard Confluent naming convention.
 */
export const topicNameStrategy: SubjectNameStrategy = (topic, isKey) =>
  `${topic}-${isKey ? "key" : "value"}`

/**
 * Creates a strategy that always uses a fixed record name as the subject.
 *
 * Useful when the same schema is shared across multiple topics.
 *
 * @param recordName - The fixed subject name to use.
 */
export function recordNameStrategy(recordName: string): SubjectNameStrategy {
  return () => recordName
}

/**
 * Creates a strategy that combines topic and record name: `{topic}-{recordName}`.
 *
 * @param recordName - The record name to combine with the topic.
 */
export function topicRecordNameStrategy(recordName: string): SubjectNameStrategy {
  return (topic) => `${topic}-${recordName}`
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Error from the Schema Registry.
 *
 * Wraps HTTP and Schema Registry API errors with the registry error code
 * when available.
 */
export class SchemaRegistryError extends KafkaError {
  override readonly name = "SchemaRegistryError"

  /** Schema Registry error code (e.g. 40401 = subject not found). */
  readonly errorCode?: number

  /** HTTP status code from the response. */
  readonly statusCode?: number

  constructor(
    message: string,
    options?: { errorCode?: number; statusCode?: number; retriable?: boolean; cause?: unknown }
  ) {
    super(
      message,
      options?.retriable ?? false,
      options?.cause !== undefined ? { cause: options.cause } : undefined
    )
    this.errorCode = options?.errorCode
    this.statusCode = options?.statusCode
  }

  static override isError(error: unknown): error is SchemaRegistryError {
    return error instanceof SchemaRegistryError
  }
}

// ---------------------------------------------------------------------------
// Wire Format Constants
// ---------------------------------------------------------------------------

/** Confluent wire format magic byte. */
export const WIRE_FORMAT_MAGIC = 0x00

/** Size of the Confluent wire format header (magic byte + 4-byte schema ID). */
export const WIRE_FORMAT_HEADER_SIZE = 5

/**
 * Encode a Confluent wire format header.
 *
 * @param schemaId - Global schema ID.
 * @returns 5-byte header: magic byte + big-endian int32 schema ID.
 */
export function encodeWireFormatHeader(schemaId: number): Uint8Array {
  const header = new Uint8Array(WIRE_FORMAT_HEADER_SIZE)
  const view = new DataView(header.buffer)
  header[0] = WIRE_FORMAT_MAGIC
  view.setInt32(1, schemaId, false) // big-endian
  return header
}

/**
 * Decode a Confluent wire format header.
 *
 * @param data - Message bytes (must be at least 5 bytes).
 * @returns The schema ID from the header.
 * @throws {SchemaRegistryError} If the magic byte is invalid or data is too short.
 */
export function decodeWireFormatHeader(data: Uint8Array): { schemaId: number; offset: number } {
  if (data.length < WIRE_FORMAT_HEADER_SIZE) {
    throw new SchemaRegistryError(
      `message too short for wire format header: ${String(data.length)} bytes, expected at least ${String(WIRE_FORMAT_HEADER_SIZE)}`
    )
  }
  if (data[0] !== WIRE_FORMAT_MAGIC) {
    throw new SchemaRegistryError(
      `invalid wire format magic byte: 0x${data[0].toString(16).padStart(2, "0")}, expected 0x00`
    )
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const schemaId = view.getInt32(1, false) // big-endian
  return { schemaId, offset: WIRE_FORMAT_HEADER_SIZE }
}

// ---------------------------------------------------------------------------
// Schema Registry Client
// ---------------------------------------------------------------------------

/** @internal Retriable HTTP status codes. */
const RETRIABLE_STATUS_CODES = new Set([408, 429, 500, 502, 503, 504])

/**
 * Confluent Schema Registry client.
 *
 * Provides methods for registering and fetching schemas. Caches schemas
 * by ID and by subject to minimise HTTP round-trips.
 *
 * @example
 * ```ts
 * const registry = new SchemaRegistry({ url: "http://localhost:8081" })
 * const { id } = await registry.register("my-topic-value", '{"type":"record",...}', "AVRO")
 * const schema = await registry.getSchema(id)
 * ```
 */
export class SchemaRegistry {
  private readonly baseUrl: string
  private readonly authHeader?: string
  private readonly schemaCache = new Map<number, RegisteredSchema>()
  private readonly subjectVersionCache = new Map<string, RegisteredSchema>()

  constructor(config: SchemaRegistryConfig) {
    // Strip trailing slash
    this.baseUrl = config.url.replace(/\/+$/, "")
    if (config.auth) {
      const credentials = `${config.auth.username}:${config.auth.password}`
      this.authHeader = `Basic ${btoa(credentials)}`
    }
  }

  /**
   * Fetch a schema by its global ID.
   *
   * Results are cached so repeated lookups for the same ID avoid HTTP calls.
   *
   * @param id - Global schema ID.
   * @returns The registered schema.
   */
  async getSchema(id: number): Promise<RegisteredSchema> {
    const cached = this.schemaCache.get(id)
    if (cached) {
      return cached
    }

    const response = await this.request<{
      schema: string
      schemaType?: SchemaType
      subject?: string
      version?: number
    }>("GET", `/schemas/ids/${String(id)}`)

    const schema: RegisteredSchema = {
      id,
      schema: response.schema,
      schemaType: response.schemaType,
      subject: response.subject,
      version: response.version
    }

    this.schemaCache.set(id, schema)
    return schema
  }

  /**
   * Register a schema under a subject.
   *
   * If the schema already exists, the existing ID is returned.
   *
   * @param subject - Subject name (e.g. "my-topic-value").
   * @param schema - Schema definition string.
   * @param schemaType - Schema type (default: "AVRO").
   * @returns The global schema ID.
   */
  async register(
    subject: string,
    schema: string,
    schemaType: SchemaType = "AVRO"
  ): Promise<{ id: number }> {
    const body = schemaType === "AVRO" ? { schema } : { schema, schemaType }

    const response = await this.request<{ id: number }>(
      "POST",
      `/subjects/${encodeURIComponent(subject)}/versions`,
      body
    )

    // Cache the registered schema
    this.schemaCache.set(response.id, {
      id: response.id,
      schema,
      schemaType,
      subject
    })

    return { id: response.id }
  }

  /**
   * Get the latest schema version for a subject.
   *
   * @param subject - Subject name.
   * @returns The latest registered schema.
   */
  async getLatestSchema(subject: string): Promise<RegisteredSchema> {
    const cacheKey = `${subject}:latest`
    const cached = this.subjectVersionCache.get(cacheKey)
    if (cached) {
      return cached
    }

    const response = await this.request<{
      id: number
      schema: string
      schemaType?: SchemaType
      subject: string
      version: number
    }>("GET", `/subjects/${encodeURIComponent(subject)}/versions/latest`)

    const schema: RegisteredSchema = {
      id: response.id,
      schema: response.schema,
      schemaType: response.schemaType,
      subject: response.subject,
      version: response.version
    }

    this.schemaCache.set(response.id, schema)
    this.subjectVersionCache.set(cacheKey, schema)
    return schema
  }

  /**
   * Get a specific schema version for a subject.
   *
   * @param subject - Subject name.
   * @param version - Version number.
   * @returns The registered schema at the given version.
   */
  async getSchemaVersion(subject: string, version: number): Promise<RegisteredSchema> {
    const cacheKey = `${subject}:${String(version)}`
    const cached = this.subjectVersionCache.get(cacheKey)
    if (cached) {
      return cached
    }

    const response = await this.request<{
      id: number
      schema: string
      schemaType?: SchemaType
      subject: string
      version: number
    }>("GET", `/subjects/${encodeURIComponent(subject)}/versions/${String(version)}`)

    const schema: RegisteredSchema = {
      id: response.id,
      schema: response.schema,
      schemaType: response.schemaType,
      subject: response.subject,
      version: response.version
    }

    this.schemaCache.set(response.id, schema)
    this.subjectVersionCache.set(cacheKey, schema)
    return schema
  }

  /**
   * List all subjects in the registry.
   *
   * @returns Array of subject names.
   */
  async listSubjects(): Promise<readonly string[]> {
    return this.request<string[]>("GET", "/subjects")
  }

  /**
   * Clear the local schema cache.
   *
   * Useful when schemas may have been updated externally.
   */
  clearCache(): void {
    this.schemaCache.clear()
    this.subjectVersionCache.clear()
  }

  // -------------------------------------------------------------------------
  // Internal HTTP
  // -------------------------------------------------------------------------

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const url = `${this.baseUrl}${path}`
    const headers: Record<string, string> = {
      Accept: "application/vnd.schemaregistry.v1+json",
      "Content-Type": "application/vnd.schemaregistry.v1+json"
    }
    if (this.authHeader !== undefined) {
      headers.Authorization = this.authHeader
    }

    let response: Response
    try {
      response = await fetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined
      })
    } catch (error) {
      throw new SchemaRegistryError(`failed to connect to schema registry at ${this.baseUrl}`, {
        retriable: true,
        cause: error
      })
    }

    if (!response.ok) {
      let errorCode: number | undefined
      let errorMessage: string | undefined
      try {
        const errorBody = (await response.json()) as { error_code?: number; message?: string }
        errorCode = errorBody.error_code
        errorMessage = errorBody.message
      } catch {
        // Could not parse error body
      }

      throw new SchemaRegistryError(
        errorMessage ??
          `schema registry request failed: ${method} ${path} returned ${String(response.status)}`,
        {
          errorCode,
          statusCode: response.status,
          retriable: RETRIABLE_STATUS_CODES.has(response.status)
        }
      )
    }

    return (await response.json()) as T
  }
}
