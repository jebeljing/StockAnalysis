#!/bin/sh

if [ -f tmpFile ]; then
    rm tmpFile
fi

for file in ./*Output$(date +%Y)-$(date +%j)/part-0000?; do
    #echo $file
    cat $file |cut -d "," -f 1 | cut -d "(" -f 2 >> tmpFile
done

cat tmpFile | sort > PopSymbol$(date +%Y)-$(date +%j).txt

rm tmpFile
