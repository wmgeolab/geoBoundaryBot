#!/bin/tcsh
until grep -Fxq "DONE" /sciclone/geograd/geoBoundaries/logs/gbBuilderCron/pullStat
do
	echo ""
	echo "----GIT PULL UNDERWAY----"
	date
	cat /sciclone/geograd/geoBoundaries/logs/gbBuilderCron/pullStat
	tail /sciclone/geograd/geoBoundaries/logs/gbBuilderCron/gitPullLog.log
	sleep 30
done

echo "GIT PULL COMPLETE, BEGINNING LFS SYNC"

until grep -Fxq "DONE" /sciclone/geograd/geoBoundaries/logs/gbBuilderCron/lfsStat
do
	echo ""
	echo "----GIT LFS PULL UNDERWAY----"
	date
	cat /sciclone/geograd/geoBoundaries/logs/gbBuilderCron/lfsStat
	tail /sciclone/geograd/geoBoundaries/logs/gbBuilderCron/gitLFSLog.log
	sleep 30
done
