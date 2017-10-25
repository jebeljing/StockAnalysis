#/bin/sh
cat $1 | cut -d "," -f 1 | cut -d "(" -f 2 | sort > $2
