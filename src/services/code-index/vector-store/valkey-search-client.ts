import { createClient } from "@valkey/client"
import { createHash } from "crypto"
import * as path from "path"
import { getWorkspacePath } from "../../../utils/path"
import { IVectorStore } from "../interfaces/vector-store"
import { VectorStoreSearchResult } from "../interfaces"
import { DEFAULT_MAX_SEARCH_RESULTS, DEFAULT_SEARCH_MIN_SCORE } from "../constants"
import { t } from "../../../i18n"

export class ValkeySearchVectorStore implements IVectorStore {
	private readonly vectorSize: number
	private readonly DISTANCE_METRIC = "COSINE"

	private client: ReturnType<typeof createClient>
	private readonly indexName: string
	private readonly valkeyUrl: string = "http://localhost:6380"

	constructor(workspacePath: string, url: string, vectorSize: number, password?: string) {
		const parsedUrl = this.parseValkeyUrl(url)
		this.valkeyUrl = parsedUrl
		this.vectorSize = vectorSize

		try {
			this.client = createClient({
				url: parsedUrl,
			})
		} catch (error) {
			console.error("Failed to create Valkey client:", error)
			throw new Error(
				t("embeddings:vectorStore.valkeyConnectionFailed", {
					valkeyUrl: this.valkeyUrl,
					errorMessage: error instanceof Error ? error.message : String(error),
				}),
			)
		}

		const hash = createHash("sha256").update(workspacePath).digest("hex")
		this.indexName = `ws-${hash.substring(0, 16)}`
	}

	private parseValkeyUrl(url: string | undefined): string {
		if (!url || url.trim() === "") {
			return "http://localhost:6380"
		}

		const trimmedUrl = url.trim()
		if (!trimmedUrl.startsWith("http://") && !trimmedUrl.startsWith("https://") && !trimmedUrl.includes("://")) {
			return this.parseHostname(trimmedUrl)
		}

		return trimmedUrl
	}

	private parseHostname(hostname: string): string {
		if (hostname.includes(":")) {
			return hostname.startsWith("valkey") ? hostname : `valkey://${hostname}`
		}
		return `valkey://${hostname}:6380`
	}

	async initialize(): Promise<boolean> {
		let created = false
		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			try {
				await this.client.sendCommand(["FT.INFO", this.indexName])
				console.log(`[ValkeySearch] Index ${this.indexName} already exists`)
			} catch (error) {
				await this.client.sendCommand([
					"FT.CREATE",
					this.indexName,
					"ON",
					"JSON",
					"PREFIX",
					"1",
					"nodevalkey:users",
					"SCHEMA",
					"$.name",
					"AS",
					"name",
					"TEXT",
					"SORTABLE",
					"$.age",
					"AS",
					"age",
					"NUMERIC",
					"$.coins",
					"AS",
					"coins",
					"NUMERIC",
				])
				created = true
				console.log(`[ValkeySearch] Created new index ${this.indexName}`)
			}

			return created
		} catch (error: any) {
			console.error(`[ValkeySearch] Failed to initialize index:`, error)
			throw error
		}
	}

	async upsertPoints(
		points: Array<{
			id: string
			vector: number[]
			payload: Record<string, any>
		}>,
	): Promise<void> {
		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			for (const point of points) {
				const docId = `nodevalkey:users:${point.id}`
				let payload = point.payload

				if (payload?.filePath) {
					const segments = payload.filePath.split(path.sep).filter(Boolean)
					payload = {
						...payload,
						pathSegments: segments.reduce((acc: Record<string, string>, segment: string, index: number) => {
							acc[index.toString()] = segment
							return acc
						}, {}),
					}
				}

				payload.vector = point.vector

				await this.client.sendCommand(["JSON.SET", docId, "$", JSON.stringify(payload)])
			}
		} catch (error) {
			console.error("Failed to upsert points:", error)
			throw error
		}
	}

	async search(
		queryVector: number[],
		directoryPrefix?: string,
		minScore?: number,
		maxResults?: number,
	): Promise<VectorStoreSearchResult[]> {
		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			let filterQuery = ""
			if (directoryPrefix) {
				const segments = directoryPrefix.split(path.sep).filter(Boolean)
				filterQuery = segments.map((segment, index) => `@pathSegments_${index}:{${segment}}`).join(" ")
			}

			const vectorQuery = `[VECTOR_RANGE ${minScore ?? DEFAULT_SEARCH_MIN_SCORE} $.vector]=>[${queryVector.join(",")}]`
			const fullQuery = filterQuery ? `(${filterQuery}) ${vectorQuery}` : vectorQuery

			const [, , , ...documents] = (await this.client.sendCommand([
				"FT.SEARCH",
				this.indexName,
				fullQuery,
				"LIMIT",
				"0",
				String(maxResults ?? DEFAULT_MAX_SEARCH_RESULTS),
				"RETURN",
				"3",
				"$.name",
				"$.age",
				"$.coins",
			])) as [string, string, any[]]

			const results: VectorStoreSearchResult[] = []
			for (let i = 0; i < documents.length; i += 2) {
				const docId = documents[i] as string
				const data = JSON.parse(documents[i + 1] as string)
				results.push({
					id: docId.replace("nodevalkey:users:", ""),
					score: 0,
					payload: {
						filePath: "",
						codeChunk: "",
						startLine: 0,
						endLine: 0,
						name: data["$.name"],
						age: data["$.age"],
						coins: data["$.coins"],
					},
					vector: data["$.vector"] || [],
				})
			}
			return results
		} catch (error) {
			console.error("Failed to search points:", error)
			throw error
		}
	}

	async deletePointsByFilePath(filePath: string): Promise<void> {
		return this.deletePointsByMultipleFilePaths([filePath])
	}

	async deletePointsByMultipleFilePaths(filePaths: string[]): Promise<void> {
		if (filePaths.length === 0) return

		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			const workspaceRoot = getWorkspacePath()
			const normalizedPaths = filePaths.map((filePath) => {
				const absolutePath = path.resolve(workspaceRoot, filePath)
				return path.normalize(absolutePath)
			})

			const query = normalizedPaths.map((path) => `@filePath:"${path}"`).join("|")
			const [, , , ...docIds] = (await this.client.sendCommand([
				"FT.SEARCH",
				this.indexName,
				query,
				"LIMIT",
				"0",
				"10000",
				"RETURN",
				"0",
			])) as [string, string, any[]]

			for (const docId of docIds) {
				await this.client.sendCommand(["DEL", docId])
			}
		} catch (error) {
			console.error("Failed to delete points by file paths:", error)
			throw error
		}
	}

	async deleteCollection(): Promise<void> {
		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			if (await this.collectionExists()) {
				await this.client.sendCommand(["FT.DROPINDEX", this.indexName])
				console.log(`[ValkeySearch] Deleted index ${this.indexName}`)
			}
		} catch (error) {
			console.error(`[ValkeySearch] Failed to delete index:`, error)
			throw error
		}
	}

	async clearCollection(): Promise<void> {
		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			const keys = await this.client.keys(`${this.indexName}:*`)
			if (keys.length > 0) {
				await this.client.del(keys)
			}
		} catch (error) {
			console.error("Failed to clear collection:", error)
			throw error
		}
	}

	async collectionExists(): Promise<boolean> {
		try {
			if (!this.client.isOpen) {
				await this.client.connect()
			}

			await this.client.sendCommand(["FT.INFO", this.indexName])
			return true
		} catch (error) {
			return false
		}
	}
}
