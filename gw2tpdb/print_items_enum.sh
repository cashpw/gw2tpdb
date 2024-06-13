#!/usr/bin/env sh
# Print enum values for items
#
# Deprecated in favor of passing in an integer because names are not distinct (e.g. 81 and 99 are both 'Mighty Studded Boots' with different stats)

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ITEM_ID_JSON_PATH="${SCRIPT_DIR}/item_ids.json"
ITEM_ID_PY_PATH="${SCRIPT_DIR}/item_ids.py"

if [[ ! -x $(command -v "jq") ]]; then
    echo "[Sync notes] Error: jq not installed. Please install jq (e.g. sudo apt install jq)."
fi

if [[ ! -x $(command -v "wget") ]]; then
    echo "[Sync notes] Error: wget not installed."
fi

if [[ -f "${ITEM_ID_PY_PATH}" ]]; then
    echo "Error: Output file ('${ITEM_ID_PY_PATH}') already exists. Exiting rather than overwrite."
    exit 1
fi


if [[ -f "${ITEM_ID_JSON_PATH}" ]]; then
    echo "Items already present in '${ITEM_ID_JSON_PATH}'. Skipping download."
else
    echo "Downloading items ..."
    wget "https://api.datawars2.ie/gw2/v1/items/json" --output-document="${ITEM_ID_JSON_PATH}"
fi

    #| sed 's/[ -\/]/_/g' \
echo "Processing items ..."
eval "$(cat ${ITEM_ID_JSON_PATH} | jq -r '.[] | [ "printf", "%s~=~%s\n", .name, .id ] | @sh')" \
    | sed "s/^10/TEN/" \
    | sed "s/^12/TWELVE/" \
    | sed "s/^15/FIFTEEN/" \
    | sed "s/^18/EIGHTEEN/" \
    | sed "s/^20/TWENTY/" \
    | sed "s/^8/EIGHT/" \
    \
    | sed "s/[a-z]/\u&/g" \
    \
    | sed "s/://g" \
    | sed "s/'//g" \
    | sed 's/"//g' \
    | sed "s/(//g" \
    | sed "s/)//g" \
    \
    | sed "s/ /_/g" \
    | sed "s/-/_/g" \
    | sed "s/—/_/g" \
    | sed "s/\\//_/g" \
    \
    | sed "s/\~=\~/ = /g" \
    \
    | sed "s/^\+/PLUS_/g" \
    | sed "s/\+\([0-9]\)/PLUS_\1/g" \
    \
    | sed "s/^/    /" \
    > item_ids.py

if [[ ! -s "${ITEM_ID_PY_PATH}" ]]; then
    echo "Error: Something went wrong ('${ITEM_ID_PY_PATH}' is empty)"
    echo "Deleting empty '${ITEM_ID_PY_PATH}'"
    rm "${ITEM_ID_PY_PATH}"
    exit 1
fi

#echo "Cleaning up ..."
#rm "${ITEM_ID_JSON_PATH}"

echo "Done. The item ID enum values are in '${ITEM_ID_PY_PATH}'"

exit 0
