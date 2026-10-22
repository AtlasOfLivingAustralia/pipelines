#!/bin/bash

### Download latest release vocabularies and save the files into the current folder
### Exclude EventType vocab

function download() {
  echo "Downloading ${VOCABULARY_NAME} vocabulary"
  
  API_URL="https://api.gbif.org/v1/vocabularies/${VOCABULARY_NAME}/releases/latest"
  EXPORT_URL=$(curl -sS "$API_URL" | jq -r '.exportUrl // empty')

  # 1. Verify that EXPORT_URL is non-empty and not the string "null"
  if [ -z "$EXPORT_URL" ] || [ "$EXPORT_URL" = "null" ]; then
    echo "Error: No valid exportUrl found for ${VOCABULARY_NAME}."
    return 1
  fi

  # 2. Download to a temporary file first
  TEMP_FILE=$(mktemp)
  ## Remove any non standard whitespace
  curl -sS -L "$EXPORT_URL" | sed 's/\xc2\xa0/ /g' > "$TEMP_FILE"

  # 3. Check if the downloaded file actually contains data
  if [ -s "$TEMP_FILE" ]; then
    mv "$TEMP_FILE" "${VOCABULARY_NAME}.json"
    echo "Saved ${VOCABULARY_NAME}.json successfully."
  else
    echo "Error: Downloaded content was empty. No file created."
    rm -f "$TEMP_FILE"
    return 1
  fi
}

echo "Download vocabularies"
VOCABULARY_NAME=DegreeOfEstablishment
download
VOCABULARY_NAME=EstablishmentMeans
download
VOCABULARY_NAME=LifeStage
download
VOCABULARY_NAME=Pathway
download
VOCABULARY_NAME=TypeStatus
download
VOCABULARY_NAME=Sex
download
VOCABULARY_NAME=OccurrenceStatus
download
