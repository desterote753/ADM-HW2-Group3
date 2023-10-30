#!/bin/bash

data="series.json"

ids=()
titles=()
book_counts=()

while read -r line; do
    id=$(echo "$line" | jq -r '.id')
    title=$(echo "$line" | jq -r '.title')
    book_count=$(echo "$line" | jq -r '.works | map(.books_count | tonumber) | add')

    if [ -n "$id" ] && [ -n "$title" ] && [ -n "$book_count" ]; then
        ids+=("$id")
        titles+=("$title")
        book_counts+=("$book_count")
    fi
done < "$data"

for ((i = 0; i < ${#book_counts[@]}; i++)); do
    for ((j = i + 1; j < ${#book_counts[@]}; j++)); do
        if [ "${book_counts[i]}" -lt "${book_counts[j]}" ]; then
            tmp=${book_counts[i]}
            book_counts[i]=${book_counts[j]}
            book_counts[j]=$tmp

            tmp=${ids[i]}
            ids[i]=${ids[j]}
            ids[j]=$tmp

            tmp=${titles[i]}
            titles[i]=${titles[j]}
            titles[j]=$tmp
        fi
    done
done

echo "id    title                                total_books_count"
for ((i = 0; i < 5; i++)); do
    printf "%-6s%-35s%-6s\n" "${ids[i]}" "${titles[i]}" "${book_counts[i]}"
done
