# -*- coding: utf-8 -*-

import os.path
import sys
import datetime, time
import locale
import math
from datetime import timedelta
from subprocess import Popen, PIPE, STDOUT

def getHosts(path):

    # abre arquivo com os hosts
    hosts = []
    with open(path+'conf/sftpHOSTS_SP.txt','r') as a:
        #hosts = a.read().splitlines()
        for i in a:
            hosts.append(i.strip())


    return (hosts)


def arquivos_controle():

    # verifica se os diretorios existem
    arquivos_controles = []
    for h in hosts:
        #if not os.path.exists(path+h):
        if os.path.exists(path+'/Controles/Coleta/'+h):

            # verifica arquivos de controle
            for pth, subdirs, files in os.walk(path+'/Controles/Coleta/'+h):
                #host = pth.strip().split('/')[-3]
                if h in pth:
                    newlist = [f for f in files if  (CTRL in f) and (f.endswith('ultimo_arquivo.txt'))]
                    for i in newlist:
                        with open(path+'/Controles/Coleta/'+h+'/'+CTRL+'/'+i,'r') as a:
                            line = a.read()
                            host = h
                            data = line.split(',')[0]
                            dataf = str(datetime.datetime.strptime(data, "%Y%m%d%H%M%S"))
                            last_file = line.split(',')[2]
                            log_file = i
                        
                        arquivos_controles.append([host+','+dataf+','+last_file+','+log_file])
                
        else:
            print 'Diretorio /Controles/ não existe'
            sys.exit()

    ####################################################################################    
    ## ARQUIVOS DE CONTROLE
    # imprime na tela o status atual dos arquivos de controle
    
    c1_width = 15
    c2_width = 20
    c3_width = 38
    c4_width = 30
    under_width = 65

    template = "{0:^15}|{1:^21}|{2:^39}|{3:^30}" # column widths
    print ''
    print '### ARQUIVOS DE CONTROLE ###'+'\n'
    print template.format("SERVIDOR", "DATA", "ULTIMO ARQUIVO", "ARQUIVO DE CONTROLE") # header
     
    for i in range(under_width):
        print '-',
    print ''
     
    for linha in arquivos_controles:
        for i in linha:
            servidor = i.split(',')[0]
            data = i.split(',')[1]
            ultimo_arquivo = i.split(',')[2]
            arquivo_controle = i.split(',')[3]

            row = "{s:^{c1w}}| {d:^{c2w}}| {u:<{c3w}}| {c:^{c4w}}".format(s=servidor,
                                                                        d=data,
                                                                        u=ultimo_arquivo,
                                                                        c=arquivo_controle,
                                                                        c1w=c1_width,
                                                                        c2w=c2_width,
                                                                        c3w=c3_width,
                                                                        c4w=c4_width)
                                                                        
            print(row)

        

    for i in range(under_width):
        print '-',
    print ''


def arquivos_retry():

    # verifica se há arquivso retry para pegar
    arquivos_retry_dict = {}
    arquivos_retry = []

    #for h in hosts:
    for h in os.listdir(path+'Controles/Coleta/retry/'):
        #if os.path.exists(path+h+'/'):
        this_path = path+'Controles/Coleta/retry/'+h
        
        if os.path.exists(this_path):
                        
            number_of_files_http = str(len([name for name in os.listdir(this_path) if '_http_' in name]))
            number_of_files_ftp = str(len([name for name in os.listdir(this_path) if '_ftp_' in name]))
            number_of_files_other = str(len([name for name in os.listdir(this_path) if '_other_' in name]))

        arquivos_retry.append(h+','+'http'+','+number_of_files_http)
        arquivos_retry.append(h+','+'ftp'+','+number_of_files_ftp)
        arquivos_retry.append(h+','+'other'+','+number_of_files_other)


    ################################################################################
    ## ARQUIVOS RETRY
    # imprime na tela o status atual dos arquivos que possivelmente estão em /retry/
    
    c1_retry_width = 15
    c2_retry_width = 20
    c3_retry_width = 17
    under_retry_width = 30

    print ''
    print '### ARQUIVOS RETRY ###'+'\n'
    
    template_retry = "{0:^15}|{1:^21}|{2:^18}"
    print template_retry.format("SERVIDOR", "TIPO ARQUIVO", "QT ARQUIVOS RETRY") # header
     
    for i in range(under_retry_width):
        print '-',
    print ''

    for elem in arquivos_retry:

        h = elem.split(',')[0]
        retry_file_type = elem.split(',')[1]
        qt_arquivos = elem.split(',')[2]
        row = "{h:^{c1w}}| {tp:^{c2w}}| {qt:^{c3w}}".format(h=h, qt=qt_arquivos, tp=retry_file_type,c1w=c1_retry_width, c2w=c2_retry_width, c3w=c3_retry_width)
        
        print row

        if retry_file_type == 'other':

            for i in range(under_retry_width):
                print '-',
            print ''   


def arquivos_http_ftp_other():

    dirs = ['http','ftp','other']
    #teste = {}

    for host in os.listdir(path+'Controles/'):

        files_http = sorted(os.listdir(path_nfs+'http'),key=lambda p: os.path.getmtime(os.path.join(path_nfs+'http', p))) # lista ordenada pelo arquivo mais recente pegando data de motificacao
        http_list = []
        if files_http:
            newest_http = files_http[-1] # pega o ultimo elemento (mais recente) - nome do arquivo
            dt = datetime.datetime.fromtimestamp(os.path.getmtime(path_nfs+'http/'+newest_http)).strftime('%Y-%m-%d %H:%M:%S') # pega a data do arquivo mais recente
            total_files_http = str(len([name for name in os.listdir(path_nfs+'http')])) # conta a qtd de arquivos em cada dir http, ftp, http
            
            #sz = (os.path.getsize(path_nfs+'http/'+newest_http))
            #size_http = convertSize(sz)

            path_file = path+'Controles/'+host+'/'+CTRL+'/arquivos_http_'+CTRL+'.tmp'

            http_list.append('http'+','+total_files_http+','+newest_http+','+dt+','+size_http)
        
        else:
            total_files_http = '0'
            path_file = path+'Controles/'+host+'/'+CTRL+'/arquivos_http_'+CTRL+'.tmp' 
            if os.path.isfile(path_file):
                with open (path_file,'r') as c_http2:
                    for i,e in enumerate(c_http2):
                        if i == 0:
                            cicle_http = e.strip()
            http_list.append('http'+','+'0'+','+'N/A'+','+'N/A'+','+'N/A'+','+'0/') # cria uma lista com os elementos para ser exibida  


        files_ftp = sorted(os.listdir(path_nfs+'ftp'),key=lambda p: os.path.getmtime(os.path.join(path_nfs+'ftp', p))) # lista ordenada pelo arquivo mais recente pegando data de motificacao
        ftp_list = []
        if files_ftp:
            newest_ftp = files_ftp[-1] # pega o ultimo elemento (mais recente) - nome do arquivo
            dt = datetime.datetime.fromtimestamp(os.path.getmtime(path_nfs+'ftp/'+newest_ftp)).strftime('%Y-%m-%d %H:%M:%S') # pega a data do arquivo mais recente
            total_files_ftp = str(len([name for name in os.listdir(path_nfs+'ftp')])) # conta a qtd de arquivos em cada dir ftp, ftp, ftp
            
            #sz = (os.path.getsize(path_nfs+'ftp/'+newest_ftp))
            #size_ftp = convertSize(sz)

            path_file = path+'Controles/'+host+'/'+CTRL+'/arquivos_ftp_'+CTRL+'.tmp' 
            
            ftp_list.append('ftp'+','+total_files_ftp+','+newest_ftp+','+dt+','+size_ftp)
        
        else:
            total_files_ftp = '0'
            path_file = path+'Controles/'+host+'/'+CTRL+'/arquivos_ftp_'+CTRL+'.tmp' 
            if os.path.isfile(path_file):
                with open (path_file,'r') as c_ftp2:
                    for i,e in enumerate(c_ftp2):
                        if i == 0:
                            cicle_ftp = e.strip()
            ftp_list.append('ftp'+','+'0'+','+'N/A'+','+'N/A'+','+'N/A'+','+'0/') # cria uma lista com os elementos para ser exibida  
    
        
        files_other = sorted(os.listdir(path_nfs+'other'),key=lambda p: os.path.getmtime(os.path.join(path_nfs+'other', p))) # lista ordenada pelo arquivo mais recente pegando data de motificacao
        other_list = []
        if files_other:
            newest_other = files_other[-1] # pega o ultimo elemento (mais recente) - nome do arquivo
            dt = datetime.datetime.fromtimestamp(os.path.getmtime(path_nfs+'other/'+newest_other)).strftime('%Y-%m-%d %H:%M:%S') # pega a data do arquivo mais recente
            total_files_other = str(len([name for name in os.listdir(path_nfs+'other')])) # conta a qtd de arquivos em cada dir other, ftp, other
            
            #sz = (os.path.getsize(path_nfs+'other/'+newest_other))
            #size_other = convertSize(sz)

            path_file = path+'Controles/'+host+'/'+CTRL+'/arquivos_other_'+CTRL+'.tmp' 
            
            other_list.append('other'+','+total_files_other+','+newest_other+','+dt+','+size_other)
        
        else:
            total_files_other = '0'
            path_file = path+'Controles/'+host+'/'+CTRL+'/arquivos_other_'+CTRL+'.tmp' 
            if os.path.isfile(path_file):
                with open (path_file,'r') as c_other2:
                    for i,e in enumerate(c_other2):
                        if i == 0:
                            cicle_other = e.strip()
            other_list.append('other'+','+'0'+','+'N/A'+','+'N/A'+','+'N/A'+','+'0/') # cria uma lista com os elementos para ser exibida             
        
        
    ####################################################################################
    ## ARQUIVOS http, ftp e other'
    # imprime na tela o status atual dos arquivos http, ftp, other baixados até o momento
    
    c1_f_width = 8 # Tipo arquivo
    c2_f_width = 9 # QTD total de arquivos no diretorio
    c3_f_width = 63 # Nom do arquivo
    c4_f_width = 22 # Dt Hora ultima modificacao do arquivo
    c5_f_width = 8
    under_retry_width = 63

    print '\n### ARQUIVOS HTTP, FTP, OTHER ###\n'

    #template_retry = "{0:^14}|{1:^13}|{2:^13}|{3:^51}|{4:^22}"
    #template_retry = "{0:^8}|{1:^10}|{2:^10}|{3:^48}|{4:^22}"
    template_retry = "{0:^8}|{1:^10}|{2:^64}|{3:^22}"
    print template_retry.format("TP ARQ", "QT TOTAL", "NOME DO ARQ / TAM", "DT/HR ULT MOD")

    for i in range(under_retry_width):
        print '-',
    print ''

    
    for i in http_list:
        dir_name = i.split(',')[0]
        total_files = i.split(',')[1]
        file_name = i.split(',')[2]
        dthr = i.split(',')[3]
        size = i.split(',')[4]

        row = "{tp:^{c1w}}| {qt:^{c2w}}| {fl:^{c3w}}| {dthr:^{c4w}}".format(h=host, tp=dir_name, qt=total_files, 
                                                                                        fl=file_name+' / '+size, 
                                                                                        dthr=dthr,c1w=c1_f_width, c2w=c2_f_width, 
                                                                                        c3w=c3_f_width, c4w=c4_f_width)
        print (row)
    
    for i in ftp_list:
        dir_name = i.split(',')[0]
        total_files = i.split(',')[1]
        file_name = i.split(',')[2]
        dthr = i.split(',')[3]
        size = i.split(',')[4]

        row = "{tp:^{c1w}}| {qt:^{c2w}}| {fl:^{c3w}}| {dthr:^{c4w}}".format(h=host, tp=dir_name, qt=total_files, 
                                                                                        fl=file_name+' / '+size, 
                                                                                        dthr=dthr,c1w=c1_f_width, c2w=c2_f_width, 
                                                                                        c3w=c3_f_width, c4w=c4_f_width)
        print (row)
        

    for i in other_list:
        dir_name = i.split(',')[0]
        total_files = i.split(',')[1]
        file_name = i.split(',')[2]
        dthr = i.split(',')[3]
        size = i.split(',')[4]

        row = "{tp:^{c1w}}| {qt:^{c2w}}| {fl:^{c3w}}| {dthr:^{c4w}}".format(h=host, tp=dir_name, qt=total_files, 
                                                                                        fl=file_name+' / '+size, 
                                                                                        dthr=dthr,c1w=c1_f_width, c2w=c2_f_width, 
                                                                                        c3w=c3_f_width, c4w=c4_f_width)
        print (row)


    for i in range(under_retry_width):
        print '-',
    print ''

    cicle()


def cicle():

    types = []
    f_types = ['http','ftp','other']
    for h in os.listdir(path+'Controles/Coleta/'):

        if h in hosts:
            for t in f_types:
                path_file = path+'Controles/Coleta/'+h+'/'+CTRL+'/arquivos_'+t+'_'+CTRL+'.tmp' 

                if path_file:
                    try:
                        with open (path_file,'r') as ft: # abre arquivo para pegar a primeira linha com o num total do ciclo
                            for i,e in enumerate(ft):
                                if i == 0:
                                    cicle = e.strip()
                    except Exception as e:
                        #print "O arquivo "+path_file+" ainda não foi gravado. Tentar novamente."
                        cicle = '0'

                files_cicle = []
                try:
                    with open (path_file,'r') as ft1: # Abre o arquivo para pegar os arquivos do ciclo
                        next(ft1) # Pula a primeira linha
                        for i in ft1:
                            line = i.strip()
                            files_cicle.append(line)
                except Exception as e:
                    #print "O arquivo "+path_file+" ainda não foi gravado. Tentar novamente."
                    cicle = '0'

                # retirado pois essa operação no NFS é muito lenta
                #n = 0
                #for f in files_cicle:
                #    for d in f_types:
                #        #if f+'.tmp' in os.listdir(path_nfs+d+'/'):
                #        if f in os.listdir(path_nfs+d+'/'):
                #            n += 1
            
                try:
                    with open (path+'Controles/Coleta/'+h+'/'+CTRL+'/ciclo_threads_'+CTRL+'.tmp' ,'r') as a:
                        line = a.read()
                        #threads.append(h+','+line)
                        threads = h+'-'+line
                    
                    #types.append(h+','+t+','+str(n)+'/'+cicle+','+line)
                    types.append(h+','+t+','+cicle+','+line)

                except Exception as e:
                    threads = 'N/A'
                    #print e


    
    ####################################################################################
    ## CICLO
    # imprime na tela o ciclo do download. Qtos sta opara ser baixados e qtos ja foram neste ciclo
    
    c1_f_width = 14 # host
    c2_f_width = 9 # tipo do arquivo
    c3_f_width = 9 # qt de threads no ciclo para cada tipo de arquivo
    c4_f_width = 22 # Threads totais

    under_cicle_width = 30  

    print '\n### CICLO ###'

    template_ciclo = "{0:^14}|{1:^10}|{2:^10}|{3:^22}"
    print template_ciclo.format("HOST", "TP ARQ", "CICLO","THREADS - HR INICIO")

    for i in range(under_cicle_width):
        print '-',
    print ''

    for t in types:
  
        host = t.split(',')[0]
        f_type = t.split(',')[1]
        cicle = t.split(',')[2]
        threads = t.split(',')[3]

        row = "{h:^{c1w}}| {ft:^{c2w}}| {c:^{c3w}}| {t:^{c4w}}".format(h=host, ft=f_type, c=cicle, c1w=c1_f_width, t=threads, c2w=c2_f_width, c3w=c3_f_width, c4w=c4_f_width)

        print row

        if f_type == 'other':
            
            for i in range(under_cicle_width):
                print '-',
            print ''           

 
def cicle():

    ciclo = []
    types = []
    f_types = ['http','ftp','other']
    for h in os.listdir(path+'Controles/Coleta/'):

        if h in hosts:
            for t in f_types:
                #path_file = path+'Controles/Coleta/'+h+'/'+CTRL+'/arquivos_'+t+'_'+CTRL+'.tmp' 
                path_file = path+'Controles/Coleta/'+h+'/'+CTRL+'/ciclo_'+t+'_'+CTRL+'.tmp' 
                if os.path.exists(path_file):
                    with open (path_file ,'r') as a:
                        line = a.readline().strip()
                        
                        ciclo.append([h+','+t+','+line])
                else:
                    print "Nao foi possivel abrir o arquivo "+path_file+". Verifique se ha processo rodando"
                    sys.exit()
                
    
    ####################################################################################
    ## CICLO
    # imprime na tela o ciclo do download. Qtos sta opara ser baixados e qtos ja foram neste ciclo
    
    c1_f_width = 14 # host
    c2_f_width = 9 # tipo do arquivo
    c3_f_width = 9 # qt de threads no ciclo para cada tipo de arquivo

    under_cicle_width = 21

    print '\n### CICLO ###'

    template_ciclo = "{0:^14}|{1:^10}|{2:^10}"
    print template_ciclo.format("HOST", "TP ARQ", "CICLO")

    for i in range(under_cicle_width):
        print '-',
    print ''

    for t in ciclo:
  
        for i in t:
            host = i.split(',')[0]
            f_type = i.split(',')[1]
            cicle = i.split(',')[2]

        
            row = "{h:^{c1w}}| {ft:^{c2w}}| {c:^{c3w}}".format(h=host, ft=f_type, c=cicle, c1w=c1_f_width, c2w=c2_f_width, c3w=c3_f_width)

            print row

            if f_type == 'other':
                
                for i in range(under_cicle_width):
                    print '-',
                print ''         



def convertSize(size):
   if (size == 0):
       return '0B'
   size_name = ("kb", "mb", "gb", "tb", "pb", "eb", "zb", "yb")
   size = size / 1024.0
   i = int(math.floor(math.log(size,1024)))
   p = math.pow(1024,i)
   s = round(size/p,2)
   return '%s%s' % (s,size_name[i])


def verifyCTRL():

    try:
        if str(sys.argv[1]):
            CTRL_arg = str(sys.argv[1])
            
            for h in hosts:
                if os.path.exists(path+'Controles/Coleta/'+h+'/'+CTRL_arg):

                    try:
                        CTRL = str(datetime.datetime.strptime(CTRL_arg, "%Y%m%d")).strip().split(" ")[0].replace("-","").replace(" ","").replace(":","")

                    except ValueError:
                        print(msg)
                        sys.exit()

                #else:
                #    msg = "\nData de controle informada não existe. Utilize o parametro \"s\" para exibir os arquivos de controles disponíveis.\n"
                #    print msg
                #    sys.exit()

            return(CTRL)

    except Exception as e:
        print e


def showAvailableDates():

    dirs_dict = {}
    for h in hosts:
        
        my_path = path+'/Controles/Coleta/'+h
        
        if os.path.exists(my_path):
            dirs = os.listdir(my_path)
            dirs_dict[h] = dirs # dicionario com sevidor e sua respectiva key com  uma lista de diretorios

    
    ################################################################################
    ## ARQUIVOS  DISPONIVEIS
    # imprime na tela as datas de controles disponiveis
    
    c1_retry_width = 15
    c2_retry_width = 30
    under_width = 25

    print ''
    print '### DATAS DE CONTROLE DISPONIVEIS ###'+'\n'
    
    template = "{0:^15}|{1:^31}"
    print template.format("SERVIDOR", "DATAS DE CONTROLE DISPONIVEIS") # header
     
    for k,v in dirs_dict.iteritems():
        
        host = k.split(',')[0]
        
        
        for i in range(under_width):
            print '-',
        print '' 
        

        for i in v:
            date = str(datetime.datetime.strptime(i, "%Y%m%d"))[:10]
            row = "{h:^{c1w}}| {dt:^{c2w}}".format(h=host, dt=date, c1w=c1_retry_width, c2w=c2_retry_width)
            #row = "{h:^{c1w}}| {dt:^{c2w}}".format(h=host, dt=date, c1w=c1_retry_width, c2w=c2_retry_width)
            print (row)

    for i in range(under_width):
        print '-',
    print '' 


def averageTime():

    #file = path+'Controles/Coleta/acompanhamento_sizes_'+CTRL+'.txt'
    file_ja_baixados = path+'Controles/Coleta/ja_baixados_'+CTRL+'.txt'


    sizes = []
    if os.path.exists(file_ja_baixados):
        with open (file_ja_baixados,'r') as a:
            for line in a:
                f_type = line.split(',')[2]
                #if f_type != 'ftp':
                date = line.split(',')[1]
                download_time = line.split(',')[-1].strip()
                size = line.split(',')[-2]
                #print line.split(',')[1]
                #str1 = ''.join(str(e) for e in date.split(':')[:2])
                #print str1
                #print date.split(':')[:2]#.split(':')[1]# == '00:00'

                #if not date.split('.')[0].split(':')[0][1] == '00:00': # Ha processos que terminam alguns seguindos após a meia noite.
                                                                    # Caso contrario o delta será menor que o ano de 1900

                sizes.append(date+','+download_time+','+f_type+','+size)

                lastdate_dto = datetime.datetime.strptime(date, '%H:%M:%S.%f')

            time_range = 30
            delta = lastdate_dto - timedelta(minutes = int(time_range))
            
            last_t = datetime.datetime.strftime(delta, '%H:%M:%S.%f')

        last_elements = []
        total_elements = []
        
        for l in sizes:
            date = l.split(',')[0]
            d_time = l.split(',')[1]
            size = l.split(',')[-1]

            total_elements.append(int(size))

            if date >= last_t:
                last_elements.append(date+','+d_time+','+last_t+','+size)

        totalSecs = 0
        times = []
        sum_sizes = []
        for i in last_elements:
            time = i.split(',')[1]
            size = i.split(',')[-1]
            times.append(time)
            sum_sizes.append(int(size))

            timeParts = [int(s) for s in time.split(':')]
            totalSecs += (timeParts[0] * 60 + timeParts[1]) * 60 + timeParts[2]
    
    
        totalSecs, sec = divmod(totalSecs, 60)
        hr, min = divmod(totalSecs, 60)
        #total_time = "%d:%02d:%02d" % (hr, min, sec)

        total_range_sizes = sum(sum_sizes)
        total_sizes = sum(total_elements)

        average = str(timedelta(seconds=sum(map(lambda f: int(f[0])*3600 + int(f[1])*60 + int(f[2]), map(lambda f: f.split(':'), times)))/len(times)))

        #print '\n'
        #msg = "Total de arquivos transferidos nos ultimos 30 minutos: "+str(len(last_elements))+" / "+convertSize(total_range_sizes)
        #print msg
        #msg = "Media de tempo gasto para coleta de cada arquivo nos ultimos 30 minutos: "+average
        #print msg+'\n'
        #msg = "Total de arquivos do dia: "+str(len(total_elements))+" / "+convertSize(total_sizes)
        #print msg


    totafiles30min = "Total de arquivos transferidos nos ultimos 30 minutos: "+str(len(last_elements))+" / "+convertSize(total_range_sizes)
    average30min = "Media de tempo gasto para coleta de cada arquivo nos ultimos 30 minutos: "+average
    totalarquivosdia = "Total de arquivos do dia: "+str(len(total_elements))+" / "+convertSize(total_sizes)

   
    return (average30min, totafiles30min,totalarquivosdia, str(len(total_elements)))
    
                
def showTotalRetry():

    total = []
    if os.path.exists(path+'Controles/Coleta/retry/'):
        for h in os.listdir(path+'Controles/Coleta/retry/'):
            this_path = path+'Controles/Coleta/retry/'+h
            if len(os.listdir(this_path)) != 0:
                size = (len(os.listdir(this_path)))
                total.append(size)
                
        total_retry = sum(total)
        msg = "Total de arquivos em retry: "+str(total_retry)
        print msg


def showTotalBacklog():

    backlog = []
    for h in os.listdir(path+'Controles/Coleta/'):
        if h in hosts:
            this_path = path+'Controles/Coleta/'+h+'/'+CTRL+'/'
            if os.path.exists(this_path):
                for i in os.listdir(this_path):
                    if 'backlog' in i:
                        backlog.append(this_path+i+','+h)

    http = []
    ftp = []
    other = []

    for file in backlog:
        host = file.split(',')[-1]
        with open (file.split(',')[0],'r') as a:
            for l in a:
                t_file = l.split(',')[-1]
                if 'http' in t_file:
                    http.append(l.strip()+','+host)
                if 'ftp' in t_file:
                    ftp.append(l.strip()+','+host)
                if 'other' == t_file:
                    other.append(l.strip()+','+host)

    total_backlog = len(http) + len(ftp) + len(other)
    #msg = "Total de arquivos em backlog: "+str(total_backlog)
    #print msg

    return total_backlog
    

def showColetadosRetry():

    backlog = []
    if os.path.exists(path+'Controles/Coleta/coletados_retry_'+CTRL+'.txt'):
        with open (path+'Controles/Coleta/coletados_retry_'+CTRL+'.txt','r') as a:
            total = a.readlines()

        msg = "Total de arquivos coletados em retry: "+str(len(total))
        print msg


def showTotalFilesTmp():

    #if os.path.exists(path+'Controles/Coleta/get_tmp/'):
    if os.path.exists('/data/stage/dpi_sig/'):
        #files = os.listdir(path+'Controles/Coleta/get_tmp/')
        files = os.listdir('/data/stage/dpi_sig/')
        
        arquivos_gz = []
        for i in files:
            if ('.txt.gz' in i) and ('temp' not in i):
                arquivos_gz.append(i)
    
    arquivos_gz_nao_movidos = []
    #for i in arquivos_gz:
        #t = datetime.datetime.fromtimestamp(os.path.getmtime(path+'Controles/Coleta/get_tmp/'+i)).strftime('%Y%m%d%H%M%S')
        #t = datetime.datetime.fromtimestamp(os.path.getmtime('/dbs/manobra/teste_ftp/DPI_SIG'+i)).strftime('%Y%m%d%H%M%S')
        #t_converted = datetime.datetime.strptime(t, '%Y%m%d%H%M%S')
        #d = datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d%H%M%S')
        #d_converted = datetime.datetime.strptime(d, '%Y%m%d%H%M%S')
        #delta = d_converted - t_converted

        #if delta >= timedelta(minutes = 10):
        #    msg = "Total de arquivos *.tmp.gz represados nao movidos para o HDFS (+ de 10 min.): "+str(len(arquivos_gz))
    msg = "Total de arquivos *.tmp.gz represados nao movidos para o HDFS: "+str(len(arquivos_gz))
    print msg
        #break

    msg = "Total de arquivos em get_tmp: "+str(len(files))
    print msg


def showMoveHDFSErro():

    hdfs = []
    if os.path.exists(path+'Controles/Coleta/erro_move_hdfs_'+CTRL+".txt"):
        with open (path+'Controles/Coleta/erro_move_hdfs_'+CTRL+".txt",'r') as a:
            total = a.readlines()

        if len(total) != 0:
            msg = "Total de arquivos nao movidos para o HDFS: "+str(len(total))
            print msg
            msg = "Verificar o arquivo erro_move_hdfs_"+CTRL+".txt"
            print msg
        else:
            msg = "Total de arquivos nao movidos para o HDFS: "+str(len(total))
            print msg


def showFalhasRetry():

    backlog = []
    if os.path.exists(path+'Controles/Coleta/arquivos_retry_falhas_'+CTRL+'.txt'):
        with open (path+'Controles/Coleta/arquivos_retry_falhas_'+CTRL+'.txt','r') as a:
            total = a.readlines()

        msg = "Total de falhas em retry apos 5 tentativas: "+str(len(total))
        return (msg)



def perda(total_arquivos_dia,total_backlog):

    percent = 100 * float(total_backlog) / float(total_arquivos_dia)  

    return (percent)
    


def list_Ps():

    #PS = ['P00','P01','P02','P03','P04','P05','P06','P07','P08']
    
    ps_result = os.listdir(WORK_DIR)

    P00 = str(len([name for name in ps_result if 'P00' in name and 'tmp' not in name]))
    P01 = str(len([name for name in ps_result if 'P01' in name and 'tmp' not in name]))
    P02 = str(len([name for name in ps_result if 'P02' in name and 'tmp' not in name]))
    P03 = str(len([name for name in ps_result if 'P03' in name and 'tmp' not in name]))
    P04 = str(len([name for name in ps_result if 'P04' in name and 'tmp' not in name]))
    P05 = str(len([name for name in ps_result if 'P05' in name and 'tmp' not in name]))
    P06 = str(len([name for name in ps_result if 'P06' in name and 'tmp' not in name]))
    P07 = str(len([name for name in ps_result if 'P07' in name and 'tmp' not in name]))
    P08 = str(len([name for name in ps_result if 'P08' in name and 'tmp' not in name]))
    

    msg = 'P00:'+P00+' | '+'P01:'+P01+' | '+'P02:'+P02+' | '+'P03:'+P03+' | '+'P04:'+P04+' | '+'P05:'+P05 \
            +' | '+'P06:'+P06+' | '+'P07:'+P07+' | '+'P08:'+P08
    print msg
    



if __name__ == '__main__':
    
        
    path = '/home/h_loadbd/DPI_SIG/'
    WORK_DIR = '/data/stage/dpi_sig/'

    hosts = getHosts(path) # retorna lista de hosts


    msg =   '\nParametros possiveis: \n\n' \
            ' DATA - Obrigatorio. Fomato YYYYMMDD\n'\
            ' Informando somente este parametro, todas as informações são exibidas\n\n' \
            ' s    - Exibe os as datas de controles disponiveis para consulta \n' 

    if len(sys.argv) == 1:
        print msg
    
    elif str(sys.argv[1]) == 's':
        showAvailableDates()
        sys.exit()
    
    elif len(sys.argv) == 2:
        
        CTRL = verifyCTRL()

        if CTRL:
            #if os.path.exists(path+'Controles/Coleta/retry/'):
            #    arquivos_retry()
            
            #verifyBacklog()
            cicle()
            averageTime()
            averageTime = averageTime() # armazena na variagem averageTime os valores retornados na funcao averageTime()
            total_arquivos_dia = averageTime[-1]
            print ''
            print averageTime[0]
            print averageTime[1]
            print averageTime[2]

            #print '\n'
            #showTotalRetry()
            #showColetadosRetry()
            #showFalhasRetry()
            print '\n'
            #showTotalBacklog()
            #total_backlog = showTotalBacklog()
            #msg = "Total de arquivos em backlog: "+str(total_backlog)+' / '+"{0:.0f}%".format(perda(total_arquivos_dia,total_backlog)) # formata a saida para desconsiderar as casas decimais
            #print msg
            showTotalFilesTmp()
            showMoveHDFSErro()
            print '\n'
            print "load average: "
            os.system('cat /proc/loadavg')
            print 'Disponivel em get_tmp: '
            os.system('df -h /data/stage/dpi_sig | grep \'G\' | awk \'{print $3}\'')
            print ''
            list_Ps()
            print '\n'
           
            sys.exit()
            

        else:
            print "A data de controle informada não existe.\nVerifique as datas de controle disponiveis com a opcao \"s\". \n"

    else:
        print msg