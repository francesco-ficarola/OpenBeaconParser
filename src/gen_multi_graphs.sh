#!/bin/bash
PATH_FILES=/home/redcrow/Desktop/graphs
LOG_NAME=wsdm.txt
TIME=2

while [ $TIME -le 120 ]
do
	PERC=5
	echo "Generating graphs for TIME $TIME..."
	while [ $PERC -le 100 ]
	do
		echo "Percentage of messages: $PERC"
		java -cp ".:../lib/*" it.uniroma1.dis.wsngroup.parsing.LogParser -am -csv -it $TIME $PERC -f $PATH_FILES/$LOG_NAME
		GRAPH_RENAMING="graph_${TIME}_${PERC}.csv"
		mv $PATH_FILES/graph.csv $PATH_FILES/$GRAPH_RENAMING
		PERC=$(( $PERC + 5 ))
	done
	TIME=$(( $TIME + 1 ))
done
