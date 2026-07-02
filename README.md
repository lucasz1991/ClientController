# Tauri + Vue + TypeScript

This template should help get you started developing with Vue 3 and TypeScript in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

## Signed updates

Updates are built by `.github/workflows/release.yml`. Configure the repository secrets
`TAURI_SIGNING_PRIVATE_KEY` and `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`, then keep the matching
public key in the AiUserFactory ClientController update settings.

For a release, update the version consistently in `package.json`, `src-tauri/Cargo.toml` and
`src-tauri/tauri.conf.json`, commit it, and push a matching tag such as `v0.2.0`. GitHub Actions
publishes the signed Windows installer and `latest.json`; nodes install it only after an explicit
update job from AiUserFactory.

## Recommended IDE Setup

- [VS Code](https://code.visualstudio.com/) + [Vue - Official](https://marketplace.visualstudio.com/items?itemName=Vue.volar) + [Tauri](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode) + [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
