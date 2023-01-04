qstat | grep gbCGAZ > /sciclone/geograd/geoBoundaries/logs/gbCGAZ/cgazJobStat


until grep "R" /sciclone/geograd/geoBoundaries/logs/gbCGAZ/cgazJobStat
do
    echo ""
	echo "----WAITING FOR CGAZ BUILD JOB TO COMMENCE----"
	qstat | grep gbCGAZ > /sciclone/geograd/geoBoundaries/logs/gbCGAZ/cgazJobStat
	date
	sleep 5
done

until grep -Fxq "CGAZ BUILD IS COMPLETE." /sciclone/geograd/geoBoundaries/logs/gbCGAZ/cgazStat
do
	echo ""
	echo "----CGAZ BUILD UNDERWAY----"
	date
	cat /sciclone/geograd/geoBoundaries/logs/gbCGAZ/cgazStat
	sleep 30
done

echo "CGAZ BUILD COMPLETE."
