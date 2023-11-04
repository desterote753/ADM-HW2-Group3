#!/bin/bash

data="series.json"

# Kullanıcıdan istenen üst sıra sayısı
top_count=5

# JSON verilerini bir kez okuyup uygun şekilde işleme
series=()
while read -r line; do
    id=$(echo "$line" | jq -r '.id')
    title=$(echo "$line" | jq -r '.title')
    book_count=$(echo "$line" | jq -r '.works | map(.books_count | tonumber) | add')

    if [ -n "$id" ] && [ -n "$title" ] && [ -n "$book_count" ]; then
        series+=("$id|$title|$book_count")
    fi
done < "$data"

# Verileri sırala (top_count kadar sıra)
sorted_series=($(echo "${series[@]}" | tr ' ' '\n' | sort -t "|" -k3,3nr | head -n $top_count))

# Başlık satırını yazdır
echo "id    title                                total_books_count"

# Sıralanmış verileri yazdır
for line in "${sorted_series[@]}"; do
    IFS="|" read -r id title book_count <<< "$line"
    printf "%-6s%-35s%-6s\n" "$id" "$title" "$book_count"
done
