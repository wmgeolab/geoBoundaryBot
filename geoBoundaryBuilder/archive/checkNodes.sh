#!bin/bash

vorCor=`showbf -f vortex | tail -n 4 | grep -E '[0-9]{2,3}' | grep -vE '[0-9]{4}' | grep -Eo '[0-9]{1,4}' | head -n 1`
borCor=`showbf -f bora | tail -n 4 | grep -E '[0-9]{2,3}' | grep -vE '[0-9]{4}' | grep -Eo '[0-9]{1,4}' | head -n 1`

vorNodes=`echo "scale=0 ; $vorCor / 12" | bc`
borNodes=`echo "scale=0 ; $borCor / 20" | bc`

echo "Vortex Node Availability: $vorNodes"
echo "Bora Node Availability: $borNodes"

launch=0

if [ $borNodes -ge 5 ]
then
    echo There are at least 5 bora nodes available.  Switching into Bora build scripts.
    qsub /sciclone/geograd/geoBoundaries/scripts/geoBoundaryBot/geoBoundaryBuilder/buildRunBora
    launch=1
fi

if [ $launch -eq 0 ]
then
if [ $vorNodes -ge 15 ]
then
    echo Insufficient Bora nodes were available for the build.
    echo There are at least 15 vortex nodes available. Switching into vortex build scripts.
    qsub /sciclone/geograd/geoBoundaries/scripts/geoBoundaryBot/geoBoundaryBuilder/buildRunVortex
    launch=1
fi
fi

if [ $launch -eq 0 ]
then
    echo There were insufficient nodes to launch the build.
fi