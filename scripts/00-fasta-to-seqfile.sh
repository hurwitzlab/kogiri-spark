#!/bin/bash

set -u

IN_DIR="/home/kyclark/data/pov"
OUT_DIR="/user/metagenomics/pov/fasta-seq"
CWD=$PWD

COMMON="./common.sh"
if [ -e $COMMON ]; then
  source $COMMON
fi


TMP_FILES=$(mktemp)
find $IN_DIR -name \*.fa > $TMP_FILES

NUM_FILES=$(lc $TMP_FILES)

echo Found \"$NUM_FILES\" files in \"$IN_DIR\"

if [ $NUM_FILES -lt 1 ]; then
  echo Nothing to do
  exit 1
fi

i=0
while read FILE; do
  let i++
  BASENAME=$(basename $FILE)
  hdfs dfs -rm -r -f $OUT_DIR/$BASENAME
  printf "%5d: %s\n" $i $BASENAME
  spark-submit $CWD/workers/fasta-to-seqfile.py $FILE $OUT_DIR
  break
done < $TMP_FILES
