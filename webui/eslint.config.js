import { defineConfig } from 'eslint/config';
import globals from 'globals';
import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import react from 'eslint-plugin-react';
import reactHooks from 'eslint-plugin-react-hooks';
import compat from 'eslint-plugin-compat';
import eslintConfigPrettier from 'eslint-config-prettier';
import { includeIgnoreFile, fixupConfigRules } from '@eslint/compat';
import { fileURLToPath } from 'node:url';

const gitignorePath = fileURLToPath(new URL('.gitignore', import.meta.url));

export default defineConfig([
  {
    name: 'ignore/defaults',
    ignores: ['node_modules', 'dist', 'pub'],
  },
  {
    name: 'ignore/gitignore',
    ...includeIgnoreFile(gitignorePath),
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    name: 'app/js-ts-jsx-tsx',
    files: ['**/*.{js,jsx,ts,tsx}'],
    plugins: {
      '@typescript-eslint': tseslint.plugin,
      react,
      'react-hooks': reactHooks,
      compat,
    },
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        ecmaFeatures: {
          jsx: true,
        },
      },
      globals: {
        ...globals.browser,
        process: 'readonly',
      },
    },
    settings: {
      react: {
        version: 'detect',
      },
    },
    rules: {
      'no-console': 'error',
      'eqeqeq': 'warn',
      'react/prop-types': 0,
      'react/jsx-key': 0,
      'react/display-name': 0,
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          caughtErrors: 'none',
        },
      ],
      '@typescript-eslint/no-unused-expressions': [
        'error',
        {
          allowShortCircuit: true,
          allowTernary: true,
        },
      ],
      'react-hooks/exhaustive-deps': [
        'error',
        {
          additionalHooks: '(useAPI|useAPIWithPagination)',
        },
      ],
    },
  },
  {
    name: 'prettier/config',
    ...eslintConfigPrettier,
  },
]);
