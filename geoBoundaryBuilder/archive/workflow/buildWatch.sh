qstat | grep gbBuilder > /sciclone/geograd/geoBoundaries/logs/gbBuilder/buildJobStat


until grep "R" /sciclone/geograd/geoBoundaries/logs/gbBuilder/buildJobStat
do
    echo ""
	echo "----WAITING FOR CORE BUILD JOB TO COMMENCE----"
	qstat | grep gbBuilder > /sciclone/geograd/geoBoundaries/logs/gbBuilder/buildJobStat
	date
	sleep 5
done

until grep -Fxq "BUILD IS COMPLETE." /sciclone/geograd/geoBoundaries/tmp/gbBuilderStage/buildStatus
do
	echo ""
	echo "----CORE BUILD UNDERWAY----"
	date
	cat /sciclone/geograd/geoBoundaries/tmp/gbBuilderStage/buildStatus
	sleep 30
done

echo ""
echo "CORE FILE BUILD COMPLETE."

