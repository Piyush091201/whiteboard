import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The React + Canvas client. It talks to the Go server's WebSocket endpoint
// directly (ws://<host>:8080/ws/<board>); no proxy needed for local dev.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173 },
});
