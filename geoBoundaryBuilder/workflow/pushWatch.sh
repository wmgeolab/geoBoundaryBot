qstat | grep gbPush > /sciclone/geograd/geoBoundaries/logs/gbBuilderCommitCSV/pushJobStat


until grep "R" /sciclone/geograd/geoBoundaries/logs/gbBuilderCommitCSV/pushJobStat
do
    echo ""
	echo "----WAITING FOR PUSH JOB TO COMMENCE----"
	qstat | grep gbPush > /sciclone/geograd/geoBoundaries/logs/gbBuilderCommitCSV/pushJobStat
	date
	sleep 5
done

until grep -Fxq "PUSH IS COMPLETE." /sciclone/geograd/geoBoundaries/logs/gbBuilderCommitCSV/pushStat
do
	echo ""
	echo "----PUSH UNDERWAY----"
	date
	tail /sciclone/geograd/geoBoundaries/logs/gbBuilderCommitCSV/pushLog.log
	sleep 30
done

echo "PUSH COMPLETE."
