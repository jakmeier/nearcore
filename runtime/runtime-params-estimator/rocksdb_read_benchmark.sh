#!/bin/bash

# Run the estimator with several RocksDB configurations and plot the results using gnuplot

OUTPUT="rocksdb_read_benchmark-`date '+%Y-%m-%d-%H-%M'`.data"

declare -a OPTIONS=("--rdb-force-flush --rdb-force-compaction --rdb-block-cache"
"--rdb-block-cache"
"--rdb-force-flush --rdb-force-compaction"
" " )

for VSIZE in 100 500 1000 2000 4000 6000 8000 16000 32000 64000
do
	COUNT=$((2000000/VSIZE*100))
	SETUP_COUNT=$((2000000/VSIZE*1000))
	O=0
	for OPTION in "${OPTIONS[@]}"
	do
		((O++))
		echo "# ${OPTION}" >> $OUTPUT
		for i in {1..3}
		do
			COMBINDED_OPTIONS="--rdb-setup-insertions ${SETUP_COUNT} --rdb-value-size ${VSIZE} --rdb-op-count ${COUNT} ${OPTION}"
			echo "Running with: ${COMBINDED_OPTIONS}"
			echo -n "$COUNT $VSIZE $O " >> $OUTPUT
			cargo run --release -p runtime-params-estimator --features required -- \
			--metric time --vm-kind wasmer2 --costs RocksDbReadValueByte --home ~/.near \
			$COMBINDED_OPTIONS \
			2> >(awk '/ gas /{print $2}' >> $OUTPUT)
		done
	done
done

# Plot results

if ! command -v gnuplot &> /dev/null
then
    echo "gnuplot is not installed, exit without plotting"
    exit
fi

declare -a OPTION_NAMES=("cache+flush+compact"
"cache+default"
"nocache+flush+compaction"
"nocache+default" )

TEMP_FILE=$(mktemp)

echo "# repetitions value_bytes config gas" > $TEMP_FILE

# Post-process data for gnuplot:
#  1) Average results for identical runs
#  2) Group data series: Each series starts with a label followed by all values. Double empty lines between two series.
for CONFIG in 1 2 3 4
do
	echo ${OPTION_NAMES[((CONFIG-1))]} >> $TEMP_FILE
	awk "(\$3==${CONFIG})" $OUTPUT | tr -d _ \
	| awk '{k=$1 " " $2; config=$3; sum[k]+=$4;count[k]++}END{for (key in sum){print key, config, sum[key]/count[key]}}' \
	| sort -k 2 -n \
	>> $TEMP_FILE
	echo >> $TEMP_FILE
	echo >> $TEMP_FILE
done

# Produce two plots
gnuplot -e "inputfile='${TEMP_FILE}'" - <<-'EOF'
	# Layout
	set title "RocksDB worst-case read performance (All unique keys)"
	set xlabel "kB per value"
	set ylabel "Gas per value read"
	set key left top
	set key autotitle columnheader

	set terminal pdf size 10,7 lw 5
	
	# First plot over entire range
	set output 'rocksdb_read_benchmark.pdf'
	stats inputfile using 0 nooutput
	plot for [i=0:(STATS_blocks - 1)] inputfile using ($2/1000):($2*$4) index i with linespoints

	# Second plot only up to 6kB
	set output 'rocksdb_read_benchmark_up_to_6kb.pdf'
	set xrange [0:6]
	replot
EOF

rm $TEMP_FILE