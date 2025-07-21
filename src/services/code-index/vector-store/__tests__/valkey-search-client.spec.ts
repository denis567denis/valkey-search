import { ValkeySearchVectorStore } from "../valkey-search-client"
import { describe, it, expect } from "vitest"

describe("ValkeySearchVectorStore", () => {
	it("should connect to Valkey server", async () => {
		const store = new ValkeySearchVectorStore(
			process.cwd(),
			process.env.VALKEY_URL || "valkey://localhost:6380",
			1536,
			process.env.VALKEY_PASSWORD,
		)

		await expect(store.initialize()).resolves.toBeDefined()
	})
})
