#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${root_dir}/diagrams/src"
output_dir="${root_dir}/assets/diagrams"
mermaid_config="${root_dir}/diagrams/mermaid-config.json"
puppeteer_config="${root_dir}/diagrams/puppeteer-config.json"

mkdir -p "${output_dir}"

while IFS= read -r input_path; do
  relative_path="${input_path#${source_dir}/}"
  output_path="${output_dir}/${relative_path%.mmd}.svg"
  mkdir -p "$(dirname "${output_path}")"

  npm exec --prefix "${root_dir}" mmdc -- \
    -i "${input_path}" \
    -o "${output_path}" \
    -b white \
    -c "${mermaid_config}" \
    -p "${puppeteer_config}" \
    -q
done < <(find "${source_dir}" -type f -name '*.mmd' | sort)
