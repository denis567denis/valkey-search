import Valkey, { Command, Redis } from "iovalkey"
import { createHash } from "crypto"
import * as path from "path"
import { IVectorStore } from "../interfaces/vector-store"
import { VectorStoreSearchResult } from "../interfaces"
import { DEFAULT_MAX_SEARCH_RESULTS } from "../constants"

interface ValkeyDocument {
	id: string
	name?: string
	age?: number
	coins?: number
	vector?: number[]
	filePath?: string
	pathSegments?: Record<string, string>
}

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
			console.log(`[ValkeySearch] Checking index ${this.indexName}`)

			let indexExists = true
			try {
				await this.client?.sendCommand(new Command("FT.INFO", [this.indexName]))
			} catch (error) {
				console.log("denis", error)
				if (error instanceof Error && error.message.includes("Unknown index name")) {
					indexExists = false
				} else {
					throw error
				}
			}

			if (indexExists) {
				console.log(`[ValkeySearch] Index exists, verifying configuration`)
				const info: any = await this.client?.sendCommand(new Command("FT.INFO", [this.indexName]))

				const vectorField = info.attributes?.find((attr: any) => attr.attribute === "vector")
				if (vectorField?.type === "VECTOR" && vectorField.dimension === this.vectorSize) {
					console.log(`[ValkeySearch] Index configuration is correct`)
					await this._createPayloadIndexes()
					return false
				}

				console.warn(`[ValkeySearch] Recreating index due to dimension mismatch`)
				await this.deleteCollection()
			}

			console.log(`[ValkeySearch] Creating new index`)
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
			console.log(`[ValkeySearch] Index created successfully`)
			return true
		} catch (error) {
			console.error(`[ValkeySearch] Initialization failed:`, error)
			throw new Error(`Index initialization failed: ${error instanceof Error ? error.message : String(error)}`)
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

	private async _recreateCollectionWithNewDimension(existingVectorSize: number): Promise<boolean> {
		try {
			await this.deleteCollection()
			await new Promise((resolve) => setTimeout(resolve, 100))
			return await this.initialize()
		} catch (error) {
			console.error(
				`[ValkeySearch] Failed to recreate index for dimension change (${existingVectorSize} -> ${this.vectorSize}):`,
				error,
			)
			throw new Error(`Dimension mismatch: ${existingVectorSize} -> ${this.vectorSize}`)
		}
	}

	private isPayloadValid(payload: Record<string, unknown> | null | undefined) {
		if (!payload) return false
		const validKeys = ["filePath", "codeChunk", "startLine", "endLine"]
		return validKeys.every((key) => key in payload)
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
			const pointIds = new Set<string>()

			for (const point of points) {
				if (!this.isPayloadValid(point.payload)) {
					throw new Error(`Invalid payload for point ${point.id}`)
				}

				const docId = `${this.indexName}:${point.id}`
				pointIds.add(docId)

				// Process path segments like Qdrant
				const payload = point.payload.filePath
					? {
							...point.payload,
							pathSegments: point.payload.filePath
								.split(path.sep)
								.filter(Boolean)
								.reduce(
									(acc: Record<string, string>, segment: string, index: number) => {
										acc[index] = segment
										return acc
									},
									{} as Record<string, string>,
								),
						}
					: point.payload

				pipeline?.hset(docId, {
					vector: this.float32ToBuffer(point.vector),
					payload: JSON.stringify(payload),
					id: point.id,
				})
			}

			await pipeline?.exec()
			console.log(`[ValkeySearch] Upserted ${pointIds.size} points to index ${this.indexName}`)
		} catch (error) {
			console.error(`[ValkeySearch] Failed to upsert ${points.length} points:`, error)
			throw new Error(`Failed to upsert points: ${error instanceof Error ? error.message : String(error)}`)
		}
	}

	async search(
		queryVector: number[],
		directoryPrefix?: string,
		minScore?: number,
		maxResults?: number,
	): Promise<VectorStoreSearchResult[]> {
		await this.ensureConnected()

		try {
			let filterQuery = ""
			if (directoryPrefix) {
				const segments = directoryPrefix.split(path.sep).filter(Boolean)
				filterQuery = segments.map((segment, index) => `@pathSegments_${index}:{${segment}}`).join(" ")
			}

			const vectorBlob = this.float32ToBuffer(queryVector).toString("binary")
			const params = [
				this.indexName,
				filterQuery
					? `(${filterQuery})=>[KNN ${maxResults ?? DEFAULT_MAX_SEARCH_RESULTS} @vector $BLOB]`
					: `(*)=>[KNN ${maxResults ?? DEFAULT_MAX_SEARCH_RESULTS} @vector $BLOB]`,
				"PARAMS",
				"2",
				"BLOB",
				vectorBlob,
				"DIALECT",
				"2",
				"RETURN",
				"3",
				"$.name",
				"$.age",
				"$.coins",
				"LIMIT",
				"0",
				String(maxResults ?? DEFAULT_MAX_SEARCH_RESULTS),
			]

			const result = await this.client?.sendCommand(new Command("FT.SEARCH", params))
			const [, , , ...documents] = result as [string, string, ...Array<[string, string]>]

			return documents.map(([docId, jsonData]) => {
				const data = JSON.parse(jsonData) as ValkeyDocument
				return {
					id: docId,
					score: 0, // Replace with actual score from result
					payload: {
						filePath: data.filePath || "",
						codeChunk: "",
						startLine: 0,
						endLine: 0,
						name: data.name,
						age: data.age,
						coins: data.coins,
					},
					vector: data.vector || [],
				}
			})
		} catch (error) {
			console.error("Failed to search points:", error)
			throw error
		}
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

				const [, , , ...documents] = result as [string, string, ...Array<[string, string]>]
				for (const [docId] of documents) {
					await this.client?.sendCommand(new Command("DEL", [docId]))
				}
			}
		} catch (error) {
			console.error("Failed to delete points by file paths:", error)
			throw error
		}
	}

	async deleteCollection(): Promise<void> {
		await this.ensureConnected()

		try {
			await this.client?.sendCommand(new Command("FT.DROPINDEX", [this.indexName, "DD"]))
			console.log(`[ValkeySearch] Deleted index ${this.indexName}`)
		} catch (error) {
			console.error(`[ValkeySearch] Failed to delete index:`, error)
			throw error
		}
	}

	async clearCollection(): Promise<void> {
		await this.ensureConnected()

		try {
			const keys = (await this.client?.sendCommand(new Command("KEYS", [`${this.indexName}`]))) as string[]
			if (keys.length > 0) {
				await this.client?.sendCommand(new Command("DEL", [...keys]))
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
			return false
		}
	}

	async destroy() {
		if (this.client && this.client.disconnect) {
			await this.client.disconnect()
		}
	}
}
