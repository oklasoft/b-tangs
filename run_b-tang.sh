#!/bin/sh

set -o verbose

ruby1.9 /tmp/b-tangs.rb --run=hadoop --input_format=qseq --range_start=00 --range_size=20 --endedness=paired ${1}_input ${1}_input_00_20 || exit 1
ruby1.9 /tmp/b-tangs.rb --run=hadoop --input_format=qseq --range_start=40 --range_size=20 --endedness=paired ${1}_input_00_20 ${1}_input_20_40 || exit 1
ruby1.9 /tmp/b-tangs.rb --run=hadoop --input_format=qseq --range_start=60 --range_size=20 --endedness=paired ${1}_input_20_40 ${1}_input_40_60 || exit 1

ruby1.9 /tmp/qseq_joiner.rb --run=hadoop ${1}_input_40_60/ ${1}_output || exit 1

hadoop fs -get ${1}_output . || exit 1

cd ${1}_output || exit 1

cut -f -11 part-* > ${1}_b_tangs_cleaned_qseq_1.txt || exit 1
cut -f -7,12-15 part-* > ${1}_b_tangs_cleaned_qseq_2.txt || exit 1

wc -l *.txt


