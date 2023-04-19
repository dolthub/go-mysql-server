#!/bin/bash

set -eou pipefail
SRC=$(dirname ${BASH_SOURCE[0]})

GEN_DIR="$SRC/../serial"

# cleanup old generated files
if [ ! -z "$(ls $GEN_DIR)" ]; then
    rm $GEN_DIR/*.go
fi

# Last generated with github.com/dolthub/flatbuffers v23.3.3-dh.2

FLATC=${FLATC:=flatc}

# generate golang (de)serialization package
"$FLATC" -o $GEN_DIR --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go mysql_db.fbs

# prefix files with copyright header
for FILE in $GEN_DIR/*.go;
do
  mv $FILE "tmp.go"
  cat "copyright.txt" "tmp.go" >> $FILE
  rm "tmp.go"
done

# format and remove unused imports
goimports -w $GEN_DIR
