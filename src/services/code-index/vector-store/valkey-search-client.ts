import Valkey, { Command, Redis } from "iovalkey"
import { createHash } from "crypto"
import * as path from "path"
import { IVectorStore, VectorStoreSearchResult } from "../interfaces"
import { DEFAULT_MAX_SEARCH_RESULTS } from "../constants"

export class ValkeySearchVectorStore implements IVectorStore {
	private readonly vectorSize: number
	private readonly DISTANCE_METRIC = "COSINE"
	private client: Redis | null = null
	private isInitializing = false
	private readonly indexName: string
	private readonly valkeyUrl: string

	constructor(workspacePath: string, url: string, vectorSize: number) {
		this.valkeyUrl = this.parseValkeyUrl(url)
		this.vectorSize = vectorSize

		const hash = createHash("sha256").update(workspacePath).digest("hex")
		this.indexName = `ws-${hash.substring(0, 16)}`
		this.initializeClient()
	}

	private parseValkeyUrl(url: string | undefined): string {
		if (!url || url.trim() === "") {
			return "http://localhost:6379"
		}
		return url.trim()
	}

	private initializeClient = async () => {
		if (this.isInitializing || (this.client && this.client.status === "ready")) {
			return
		}

		this.isInitializing = true

		try {
			if (this.client && this.client.disconnect) {
				this.client.disconnect()
			}

			const { hostname, port, password } = this.parseConnectionOptions()
			this.client = new Valkey({
				host: hostname,
				port,
				password,
				retryStrategy: (times) => Math.min(times * 100, 5000),
			})

			this.client.on("error", (err: Error) => {
				console.error("[ValkeySearch] Connection error:", err)
			})

			this.client.on("ready", () => {
				console.log("[ValkeySearch] Connection established")
				this.isInitializing = false
			})

			this.client.on("end", () => {
				console.log("[ValkeySearch] Connection closed")
				this.isInitializing = false
			})

			await this.client.connect()
		} catch (error) {
			console.error("[ValkeySearch] Failed to initialize client:", error)
			this.isInitializing = false
			setTimeout(async () => await this.initializeClient(), 5000)
		}
	}

	private parseConnectionOptions() {
		const url = new URL(this.valkeyUrl)
		return {
			hostname: url.hostname,
			port: Number(url.port) || 6379,
			password: url.password || undefined,
		}
	}

	private async ensureConnected() {
		if (!this.client || this.client.status !== "ready") {
			await this.initializeClient()
		}
	}

	private float32ToBuffer(vector: number[]): Buffer {
		const buffer = Buffer.alloc(vector.length * 4)
		for (let i = 0; i < vector.length; i++) {
			buffer.writeFloatLE(vector[i], i * 4)
		}
		return buffer
	}

	async initialize(): Promise<boolean> {
		await this.ensureConnected()

		try {
			const info = (await this.client?.sendCommand(new Command("FT.INFO", [this.indexName]))) as {
				attributes: Array<{ attribute: string; dimension?: number }>
			}

			const vectorAttr = info?.attributes?.find((attr) => attr.attribute === "vector")
			if (vectorAttr?.dimension === this.vectorSize) {
				return false
			}

			await this.client?.sendCommand(new Command("FT.DROPINDEX", [this.indexName, "DD"]))
		} catch (error) {
			if (!(error instanceof Error && error.message.includes("Unknown index name"))) {
				throw error
			}
		}

		await this.client?.sendCommand(
			new Command("FT.CREATE", [
				this.indexName,
				"ON",
				"JSON",
				"SCHEMA",
				"$.vector",
				"AS",
				"vector",
				"VECTOR",
				"FLAT",
				"6",
				"TYPE",
				"FLOAT32",
				"DIM",
				String(this.vectorSize),
				"DISTANCE_METRIC",
				this.DISTANCE_METRIC,
			]),
		)
		await this._createPayloadIndexes()
		return true
	}

	async upsertPoints(
		points: Array<{
			id: string
			vector: number[]
			payload: Record<string, any>
		}>,
	): Promise<void> {
		await this.ensureConnected()

		if (points.length === 0) return

		try {
			const pipeline = this.client?.pipeline()
			for (const point of points) {
				const docId = `${this.indexName}:${point.id}`
				const segments = point.payload.filePath?.split(path.sep) || []
				const indexedPayload: Record<string, string> = {}
				segments.forEach((seg: string, i: number) => {
					indexedPayload[`pathSegments_${i}`] = seg
				})

				pipeline?.hset(docId, {
					...point.payload,
					vector: this.float32ToBuffer(point.vector),
					...indexedPayload,
				})
			}
			await pipeline?.exec()
		} catch (error) {
			console.error("[ValkeySearch] Failed to upsert points:", error)
			throw error
		}
	}

	async search(queryVector: number[], directoryPrefix?: string): Promise<VectorStoreSearchResult[]> {
		await this.ensureConnected()

		const queryParts: string[] = []
		if (directoryPrefix) {
			const segments = directoryPrefix.split(path.sep).filter(Boolean)
			segments.forEach((seg: string, i: number) => {
				queryParts.push(`@pathSegments_${i}:{${seg}}`)
			})
		}

		const vectorBuffer = this.float32ToBuffer(queryVector)
		const results = await this.client?.sendCommand(
			new Command("FT.SEARCH", [
				this.indexName,
				queryParts.length > 0 ? `(${queryParts.join(" ")})` : "*",
				"PARAMS",
				"2",
				"vector",
				vectorBuffer.toString("base64"),
				"DIALECT",
				"2",
				"RETURN",
				"3",
				"payload",
				"vector",
				"id",
				"LIMIT",
				"0",
				String(DEFAULT_MAX_SEARCH_RESULTS),
			]),
		)

		if (!Array.isArray(results) || results.length < 2) return []

		const parsedResults: VectorStoreSearchResult[] = []
		for (let i = 1; i < results.length; i += 2) {
			const docId = results[i]
			const [payload, vector, id] = results[i + 1] as string[]

			const vectorBuffer = vector ? Buffer.from(vector, "base64") : Buffer.alloc(0)
			const vectorArray = vector ? Array.from(new Float32Array(vectorBuffer.buffer)) : []

			parsedResults.push({
				id: docId,
				vector: vectorArray,
				payload: payload ? JSON.parse(payload) : {},
				score: 0, // Valkey doesn't return score with this query format
			})
		}
		return parsedResults
	}

	async deletePointsByFilePath(filePath: string): Promise<void> {
		await this.deletePointsByMultipleFilePaths([filePath])
	}

	async deletePointsByMultipleFilePaths(filePaths: string[]): Promise<void> {
		if (filePaths.length === 0) return
		await this.ensureConnected()

		try {
			for (const filePath of filePaths) {
				const result = await this.client?.sendCommand(
					new Command("FT.SEARCH", [this.indexName, `@filePath:"${filePath}"`, "LIMIT", "0", "10000"]),
				)

				if (Array.isArray(result)) {
					for (let i = 1; i < result.length; i += 2) {
						await this.client?.sendCommand(new Command("DEL", [result[i]]))
					}
				}
			}
		} catch (error) {
			console.error("Failed to delete points by file paths:", error)
			throw error
		}
	}

	async deleteCollection(): Promise<void> {
		await this.ensureConnected()
		await this.client?.sendCommand(new Command("FT.DROPINDEX", [this.indexName, "DD"]))
	}

	async clearCollection(): Promise<void> {
		await this.ensureConnected()
		try {
			const keys = await this.client?.sendCommand(new Command("KEYS", [`${this.indexName}:*`]))
			if (Array.isArray(keys) && keys.length > 0) {
				await this.client?.sendCommand(new Command("DEL", keys))
			}
		} catch (error) {
			console.error("Failed to clear collection:", error)
			throw error
		}
	}

	async collectionExists(): Promise<boolean> {
		await this.ensureConnected()
		try {
			await this.client?.sendCommand(new Command("FT.INFO", [this.indexName]))
			return true
		} catch (error) {
			if (error instanceof Error && error.message.includes("Unknown index name")) {
				return false
			}
			throw error
		}
	}

	async destroy() {
		if (this.client && this.client.disconnect) {
			this.client.disconnect()
		}
	}

	private async _createPayloadIndexes(): Promise<void> {
		for (let i = 0; i <= 4; i++) {
			try {
				await this.client?.sendCommand(
					new Command("FT.ALTER", [
						this.indexName,
						"SCHEMA",
						"ADD",
						`$.pathSegments.${i}`,
						"AS",
						`pathSegments_${i}`,
						"TAG",
					]),
				)
			} catch (error) {
				if (!(error instanceof Error && error.message.includes("already exists"))) {
					console.warn(`[ValkeySearch] Failed to create index for pathSegments.${i}:`, error)
				}
			}
		}
	}
}
