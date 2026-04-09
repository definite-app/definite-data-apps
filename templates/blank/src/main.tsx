import React from "react";
import { createRoot } from "react-dom/client";

import App from "./App";

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error("Missing #root mount element");
}

createRoot(rootElement).render(<App />);
