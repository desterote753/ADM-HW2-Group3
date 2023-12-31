#!/bin/bash

# Your JSON data file
data="series.json"

# Use jq to process and format the JSON data
jq -r '.id as $id | .title as $title | (.works | map(.books_count | tonumber) | add) as $total_books_count | "\($id)\t\($title)\t\($total_books_count)"' "$data" | sort -t$'\t' -k3,3nr | head -n 5 | column -t -s $'\t'
