qstat | grep gbBuildCommit > /sciclone/geograd/geoBoundaries/logs/gbBuilder/commitJobStat


until grep "R" /sciclone/geograd/geoBoundaries/logs/gbBuilder/commitJobStat
do
    echo ""
	echo "----WAITING FOR CORE COMMIT JOB TO COMMENCE----"
	qstat | grep gbBuildCommit > /sciclone/geograd/geoBoundaries/logs/gbBuilder/commitJobStat
	date
	sleep 5
done

until grep -Fxq "CORE COMMIT IS COMPLETE." /sciclone/geograd/geoBoundaries/logs/gbBuilder/commitJobStat
do
	echo ""
	echo "----CORE COMMIT UNDERWAY----"
	date
	cat /sciclone/geograd/geoBoundaries/logs/gbBuilder/commitJobStat
	sleep 30
done

echo "CORE COMMIT COMPLETE."
