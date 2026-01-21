import { defineConfig } from "vitest/config";
import { mergeConfig } from "vite";
import baseConfig from "./vite.config.js";

export default mergeConfig(
  baseConfig,
  defineConfig({
    test: {
      include: ["src/routes/**/*.svelte.spec.js"],
      exclude: ["e2e/**"],
      browser: {
        enabled: true,
        name: "chromium",
        headless: true,
      },
    },
  }),
);
