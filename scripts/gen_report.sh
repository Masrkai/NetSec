#!/usr/bin/env bash
# build_report.sh – Combine docs into a PDF in the order given.
# Run from the project root. Markdown sources are in docs/Current/.

set -euo pipefail

DOCS="Docs/Technical"

pandoc -o report.pdf --pdf-engine=xelatex --resource-path=$DOCS \
  -f markdown+tex_math_single_backslash+tex_math_double_backslash+tex_math_dollars+raw_tex+smart \
  -V geometry:"a4paper, margin=1.5cm" -V fontsize=12pt -V  mainfont="FreeSans" --filter pandoc-include --lua-filter="$PANDOC_DIAGRAM_FILTER" \
  "$DOCS/all2.md" \
  "$DOCS/all.md" \
  "$DOCS/all3.md"

echo "✅ PDF generated: report.pdf"