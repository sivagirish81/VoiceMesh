import { FlatCompat } from "@eslint/eslintrc";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const currentDirectory = dirname(fileURLToPath(import.meta.url));
const compatibility = new FlatCompat({ baseDirectory: currentDirectory });

const config = [
  ...compatibility.extends("next/core-web-vitals", "next/typescript"),
  {
    ignores: [".next/**", "next-env.d.ts", "node_modules/**", "out/**"],
  },
];

export default config;
