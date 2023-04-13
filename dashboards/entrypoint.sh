#!/bin/bash

python cron.py &

folders=(apps/*)
i=0
for p in ${folders[@]}; do
    i=$((i+1))
    cd $p
    streamlit run app.py --theme.base light --server.port 850$i --server.fileWatcherType none &
    cd ../../
    # echo "streamlit run ${p}/app.py --server.port 850$i &"
done
# execute with sudo to run in port 80
sudo python app.py
