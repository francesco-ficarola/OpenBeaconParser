#!/bin/bash

PATH_FILES=/home/redcrow/Desktop/graphs
LOG_NAME=log.txt
TIME=2

###############
### ENDLESS ###
###############
START_TS=0
END_TS=2147483647

#################################
### SOCIALDIAG - EXPERIMENT 1 ###
#################################
#START_TS=1368702550
#END_TS=1368703680

#################################
### SOCIALDIAG - EXPERIMENT 2 ###
#################################
#START_TS=1368704530
#END_TS=1368705795

if [ ! -f "$PATH_FILES/$LOG_NAME" ]
	then
	echo "$LOG_NAME does not exist. Exit!"
	return
fi

while [ $TIME -le 120 ]
do
	PERC=5
	echo "Generating graphs for TIME $TIME..."
	while [ $PERC -le 100 ]
	do
		echo "Percentage of messages: $PERC"
		mvn exec:java -Dexec.mainClass="it.uniroma1.dis.wsngroup.parsing.LogParser" -Dexec.args="-st $START_TS -et $END_TS -am -csv -it $TIME $PERC -f $PATH_FILES/$LOG_NAME"
		if [ -f $PATH_FILES/graph.csv ]
		then
			GRAPH_RENAMING="graph_${TIME}_${PERC}.csv"
			mv $PATH_FILES/graph.csv $PATH_FILES/$GRAPH_RENAMING
			PERC=$(( $PERC + 5 ))
		else
			PERC=101
		fi
	
	done
	TIME=$(( $TIME + 1 ))
done
