import { ValkeySearchVectorStore } from "../valkey-search-client"
import { mockValkeyClient } from "./__mocks__/valkey"
import type { VectorStoreSearchResult } from "../../interfaces"

describe("ValkeySearchVectorStore", () => {
	const testWorkspacePath = "/test/workspace"
	const testUrl = "http://localhost:6379"
	const testVectorSize = 1536

	beforeEach(() => {
		mockValkeyClient.sendCommand = () => Promise.resolve()
		mockValkeyClient.pipeline = () => ({
			hset: () => {},
			exec: () => Promise.resolve(),
		})
	})

	describe("constructor", () => {
		it("should initialize with default parameters", () => {
			const store = new ValkeySearchVectorStore(testWorkspacePath, testUrl, testVectorSize)
			expect(store).toBeDefined()
			expect(store).toHaveProperty("workspacePath", testWorkspacePath)
		})

		it("should throw error if url is missing", () => {
			expect(() => new ValkeySearchVectorStore(testWorkspacePath, "", testVectorSize)).toThrow(
				"Valkey URL is required",
			)
		})
	})

	describe("search", () => {
		const testVector = [0.1, 0.2, 0.3]
		const testLimit = 5
		const testMinScore = 0.5

		it("should perform search with correct parameters", async () => {
			const store = new ValkeySearchVectorStore(testWorkspacePath, testUrl, testVectorSize)
			const mockResults: VectorStoreSearchResult[] = [
				{
					id: "doc1",
					score: 0.95,
					payload: {
						filePath: "file1.ts",
						codeChunk: "",
						startLine: 0,
						endLine: 0,
					},
					vector: [],
				},
			]

			mockValkeyClient.sendCommand = () =>
				Promise.resolve([
					"count",
					"1",
					"doc1",
					"payload",
					JSON.stringify({
						filePath: "file1.ts",
						name: "test",
						age: 10,
						coins: 100,
					}),
				])

			const results = await store.search(testVector, undefined, testMinScore, testLimit)

			expect(results[0]?.payload?.filePath).toEqual("file1.ts")
		})
	})

	describe("upsertPoints", () => {
		const testVectors = [
			{
				id: "doc1",
				vector: [0.1, 0.2, 0.3],
				payload: {
					filePath: "file1.ts",
					codeChunk: "test",
					startLine: 1,
					endLine: 2,
				},
			},
		]

		it("should save vectors with metadata", async () => {
			const store = new ValkeySearchVectorStore(testWorkspacePath, testUrl, testVectorSize)
			mockValkeyClient.pipeline = () => ({
				hset: () => {},
				exec: () => Promise.resolve(),
			})

			await store.upsertPoints(testVectors)

			expect(mockValkeyClient.pipeline).toHaveBeenCalled()
		})
	})

	describe("deletePointsByFilePath", () => {
		it("should delete vectors by file path", async () => {
			const store = new ValkeySearchVectorStore(testWorkspacePath, testUrl, testVectorSize)
			const responses = [["count", "1", "doc1", "payload", "{}"], 1]
			let callIndex = 0
			mockValkeyClient.sendCommand = () => Promise.resolve(responses[callIndex++])

			await store.deletePointsByFilePath("file1.ts")

			expect(mockValkeyClient.sendCommand).toHaveBeenCalledTimes(2)
		})
	})

	describe("clearCollection", () => {
		it("should clear all vectors", async () => {
			const store = new ValkeySearchVectorStore(testWorkspacePath, testUrl, testVectorSize)
			mockValkeyClient.sendCommand = () => Promise.resolve(1 as any)

			await store.clearCollection()

			expect(mockValkeyClient.sendCommand).toHaveBeenCalled()
		})
	})
})
