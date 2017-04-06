#!/bin/bash
################################################################################################################
#
# AUTOR...: Fabricio Carboni
#
################################################################################################################


source "${data_work_dir}/${properties_file}"

PROCESS_ID=$$
RETORNO=0
PROG_NAME="$(basename "$(test -L "$0" && readlink "$0" || echo "$0")")"

if [ ! -f ${DPI_MON_CTRL_EXEC_DESLIGADO} ]; then    
    echo 0 > ${DPI_MON_CTRL_EXEC_DESLIGADO}
fi

if [ ! -f ${DPI_MON_CTRL_EXEC_LIGADO} ]; then    
    echo 0 > ${DPI_MON_CTRL_EXEC_LIGADO}
fi


for host in `cat ${SFTP_HOSTS}`
do
    LOGFILE=${LOGS_DIR}"clt_dpi_sig_sftp_01_010_${host}_${PROCESS_ID}_${CTRL}.log"
    processo=`ps -ef | grep dpi_coleta | grep $host | grep -v "grep"` 
    NOHUP="${LOGS_DIR}dpi_coleta_sftp.py_${host}_${PROCESS_ID}_${CTRL}_nohup.out"
   
   
    if [[ -z $processo ]];then
        
        echoLog $LOGFILE "******************************************************************************************"
        echoLog $LOGFILE "Inicio da execucao do script $PROG_NAME. Ciclo de coleta em ${host}."
        echoLog $LOGFILE "ID Execucao: "$PROCESS_ID
        echoLog $LOGFILE "------------------------------------------------------------------------------------------"

        nohup /usr/hdp/anaconda2_2.4.0.0/bin/python ${WORK_DIR}Scripts/Python/Periodicos/dpi_coleta_sftp.py ${host} ${SFTP_USER} ${SFTP_PASSWORD} ${WORK_DIR} ${SFTP_HOME} ${CTRL} ${LOGS_DIR} ${PROCESS_ID} ${WORK_FILES_TMP} ${HDFS_STAGE_DIR} ${DPI_COLETA_SFTP_GET_SERIALIZED} > $NOHUP 2>&1 &
     
        # Expurgo KEEPING DAYS #
        echoLog $LOGFILE "Inicio da execucao do processo de exclusao de logs maiores que ${DPI_COLETA_KEEPING_DAYS} dias."

        #####################################################
        # remove logs mantendo somente de 2 horas
        
        if [ -d ${LOGS_DIR}${host}/  ]; then
            
            echoLog ${LOGFILE} "Removendo logs com data de criacao maior do que 2 horas."
            find ${LOGS_DIR} -maxdepth 1 -mmin +119 -type f -exec rm -f {} \;
            
            # Removendo s diretorios dos logs mantendo o atual e o dia anterior
            echoLog ${LOGFILE} "Removendo os diretorios dos logs mantendo o atual e o dia anterior"
            find ${LOGS_DIR}${host}/ -maxdepth 1 -type d -mtime +0 -name "*" -exec rm -fr {} \;
            
            # remove os arquivos de controles mantendo somente o valor de 7 dias
            echoLog ${LOGFILE} "Removendo os arquivos de controles mantendo somente o valor de 7 dias"
            find ${WORK_DIR}Controles/Coleta/ -maxdepth 2 -mindepth 2 -type d -mtime +5 -name "*" -exec rm -fr {} \;
            
        fi
        
        # procura por arquivos  .txt com a data do dia definido em HDFS_DIR_MON_DAY_BEFORE
        arquivos=`find ${WORK_DIR}Controles/Coleta/ -maxdepth 1  -name *${HDFS_DIR_MON_DAY_BEFORE}.txt -type f`
 
        if [[ $arquivos ]];then
        
            # Chama funcao para criar o diretorio com a data do dia anterior no hdfs
            echoLog ${LOGFILE} "Criando diretorio no HDFS ${HDFSPATH_MON_COLETA}${HDFS_DIR_MON_DAY_BEFORE}"
            criaDiretoriosDataMonitoracaoColeta "${HDFSPATH_MON_COLETA}${HDFS_DIR_MON_DAY_BEFORE}"
        
            echoLog ${LOGFILE} "Movendo arquivos de acompanhamento da coleta ${WORK_DIR}Controles/Coleta/*.txt do dia anterior para ${HDFSPATH_MON_COLETA}${HDFS_DIR_MON_DAY_BEFORE}/"
            find ${WORK_DIR}Controles/Coleta/ -maxdepth 1  -name ja_baixados_${HDFS_DIR_MON_DAY_BEFORE}.txt -type f -exec hdfs dfs -moveFromLocal {} ${HDFSPATH_MON_COLETA}${HDFS_DIR_MON_DAY_BEFORE}/ja_baixados_$HOSTNAME.txt \;
            find ${WORK_DIR}Controles/Coleta/ -maxdepth 1  -name no_such_file_${HDFS_DIR_MON_DAY_BEFORE}.txt -type f -exec hdfs dfs -moveFromLocal {} ${HDFSPATH_MON_COLETA}${HDFS_DIR_MON_DAY_BEFORE}/no_such_file_$HOSTNAME.txt \;
        fi

        # Procura possiveis arquivos que foram coletados mais de uma vez
        echoLog ${LOGFILE} "Movendo arquivos de acompanhamento da coleta ${WORK_DIR}Controles/Coleta/*.txt do dia anterior para ${HDFSPATH_MON_COLETA}${HDFS_DIR_MON_DAY_BEFORE}/"
        
        echoLog ${LOGFILE} "Verificando se ha arquivos coletados mais que 1 vez."
        verificaArquivoDuplicados
        
        # Apaga arquivos de monitoracao maiores do que 15 minutos
        echoLog ${LOGFILE} "Remove arquivos de monitoracao maiores que 15 minutos"
        find ${DPI_MON_DIR} -maxdepth 1 -mmin +15 -name "*" -type f -delete
   
        # Remove arquivos .temp possivelmente represados em /data/stage/dpi_sig/ com mais de 3 horas
        echoLog ${LOGFILE} "Remove arquivos .temp possivelmente represados em /data/stage/dpi_sig/ com mais de 3 horas"
        find ${WORK_FILES_TMP} -maxdepth 1 -mmin +179 -name "*" -type f -delete   
        
       echoLog $LOGFILE "Fim da execucao do processo de exclusao de logs maiores que ${DPI_COLETA_KEEPING_DAYS} dias."
       echoLog $LOGFILE "------------------------------------------------------------------------------------------"
       echoLog $LOGFILE "Fim da execucao do script $PROG_NAME. Ciclo de coleta em ${host}."
       echoLog $LOGFILE "Detalhes do log em ${LOGS_DIR}${host}"/"${CTRL}"/"clt_dpi_sig_sftp_01_010_${PROCESS_ID}.log"
       echoLog $LOGFILE "ID Execucao: $PROCESS_ID"
       echoLog $LOGFILE "******************************************************************************************"


        
    fi

done




