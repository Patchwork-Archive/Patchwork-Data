#!/bin/bash

URL="https://cdn.pinapelz.com/VTuber%20Covers%20Archive/"
FILE_TYPE=".webm"
OUTPUT="radio.m3u"

while IFS= read -r video_id; do
    file_link="${URL}${video_id}${FILE_TYPE}"
    echo "$file_link" >> "$OUTPUT"
done < parsed.txt
