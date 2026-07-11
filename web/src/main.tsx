import ReactDOM from "react-dom/client";
import { App } from "./App";
import "./styles.css";

// Note: no <React.StrictMode> — its double-invoked effects would open the
// WebSocket twice in development.
ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
