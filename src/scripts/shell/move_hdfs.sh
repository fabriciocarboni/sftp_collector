#!/bin/bash
################################################################################################################
#
# AUTOR...: Fabricio Carboni
#
################################################################################################################

export HADOOP_CLIENT_OPTS="-XX:-UseGCOverheadLimit -Xmx4096m -XX:-UseSerialGC -Xms64M -Xmx64M"
source "${data_dir_conf}/${properties_file}"

P_ARRAY=(P00 P01 P02 P03 P04 P05 P06 P07 P08)

find ${WORK_FILES_TMP} -size 0 -delete

for i in "${P_ARRAY[@]}"
do
    LISTA_CMD=`ps -ef | grep moveFromLocal | grep ${data_dir} | grep $i`
   
    if [[ -z $LISTA_CMD ]];then
        hdfs dfs -moveFromLocal ${WORK_FILES_TMP}*$i*txt.gz ${HDFS_STAGE_DIR} &

    fi
done

