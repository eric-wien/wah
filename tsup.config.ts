import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  dts: {
    // tsup 8.5.1 injects a deprecated baseUrl while generating declarations.
    // Keep the workaround local so the project tsconfig stays TypeScript 7-compatible.
    compilerOptions: {
      ignoreDeprecations: "6.0",
    },
  },
  splitting: false,
  sourcemap: true,
  clean: true,
  target: "es2020",
  outDir: "dist",
  external: ["ws"],
  shims: true,
});
