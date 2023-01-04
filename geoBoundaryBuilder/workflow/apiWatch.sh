qstat | grep gbAPIBuild > /sciclone/geograd/geoBoundaries/logs/gbWeb/apiJobStat


until grep "R" /sciclone/geograd/geoBoundaries/logs/gbWeb/apiJobStat
do
    echo ""
	echo "----WAITING FOR API BUILD JOB TO COMMENCE----"
	qstat | grep gbAPIBuild > /sciclone/geograd/geoBoundaries/logs/gbWeb/apiJobStat
	date
	sleep 5
done


until grep -Fxq "STATUS: API BUILD COMPLETE, PUSHING TO GIT" /sciclone/geograd/geoBoundaries/logs/gbWeb/apiStat
do
	echo ""
	echo "----API BUILD UNDERWAY----"
	date
	cat /sciclone/geograd/geoBoundaries/logs/gbWeb/apiStat
    cat /sciclone/geograd/geoBoundaries/logs/gbWeb/apiBuild.log
	sleep 30
done

until grep -Fxq "STATUS: GIT PUSH COMPLETE" /sciclone/geograd/geoBoundaries/logs/gbWeb/apiStat
do
	echo ""
	echo "----PUSHING API TO GITHUB----"
	date
	cat /sciclone/geograd/geoBoundaries/logs/gbWeb/apiStat
    cat /sciclone/geograd/geoBoundaries/logs/gbWeb/gitPush.log
	sleep 30
done

echo "CORE COMMIT & METADATA BUILD COMPLETE."


echo "STATUS: GIT PUSH COMPLETE" > /sciclone/geograd/geoBoundaries/logs/gbWeb/apiStat