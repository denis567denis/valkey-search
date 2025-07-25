import type { Command, Redis } from "iovalkey"

// Minimal mock implementation without jest dependencies
export const mockValkeyClient = {
	sendCommand: () => Promise.resolve() as Promise<any>,
	pipeline: () => ({
		hset: () => {},
		exec: () => Promise.resolve() as Promise<any>,
	}),
	connect: () => Promise.resolve(),
	disconnect: () => Promise.resolve(),
	status: "ready",
	on: () => {},
}

const ValkeyClient = () => mockValkeyClient

export default ValkeyClient
