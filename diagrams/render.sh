#!/bin/sh
# Re-render every .mmd in this folder to PNG. Needs Google Chrome installed; no Chromium download.
set -e
cd "$(dirname "$0")"
export PUPPETEER_SKIP_DOWNLOAD=true
for f in *.mmd; do
  npx -y @mermaid-js/mermaid-cli@11 -i "$f" -o "${f%.mmd}.png" -p puppeteer.json -b white -s 3 --iconPacks @iconify-json/logos
done
