import { defineConfig } from "vitest/config";
import { mergeConfig } from "vite";
import baseConfig from "./vite.config.js";

export default mergeConfig(
  baseConfig,
  defineConfig({
    test: {
      include: ["src/**/*.spec.js"],
      exclude: ["e2e/**", "src/routes/**/*.svelte.spec.js"],
    },
  }),
);
