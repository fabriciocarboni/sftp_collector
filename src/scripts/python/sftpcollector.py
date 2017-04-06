## -*- coding: utf-8 -*-


from datetime import timedelta
from subprocess import Popen, PIPE, STDOUT
import datetime, time
import paramiko
import os.path
import socket
import sys
import traceback
#import shutil
from threading import Thread, current_thread
import multiprocessing
from inspect import currentframe, getframeinfo



def getLogFiles(sftpHOST,sftpUSER,sftpPASS,WORK_DIR):

	
	# conecta nos servidores para ver se há arquivo do dia para coletar
	try:
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.load_system_host_keys()
		ssh.connect(sftpHOST,username=sftpUSER,password=sftpPASS)
		sftp = ssh.open_sftp()

	
		#c = 'cd '+SFTP_HOME+'; ls secondhttp_'+CTRL+'.log' # lista apenas os arquivos de log com data de CTRL
		c = 'cd '+SFTP_HOME+'; ls *'+CTRL+'.log' # lista apenas os arquivos de log com data de CTRL
		(stdin, stdout, stderr) = ssh.exec_command(c)
		logfiles = stdout.read().splitlines()



		if not logfiles: # Verifica se há arquivos .log para baixar
			frameinfo = getframeinfo(currentframe())
			msg = "[INFO] Não há arquivos .log para baixar no servidor "+sftpHOST+'. ('+str(frameinfo.lineno)+')'
			writeLog(msg)
			#msg = "###### Fim ciclo de coleta de arquivos. ID Execução: "+CURRENT_SHELL_PID+" ######"
			#writeLog(msg)
			#sys.exit(0)

			
		else:
			# Necessário criar um diretorio local para armazenar os arquivos http, ftp e other

			if not os.path.exists(WORK_FILES_TMP):
				#os.makedirs(WORK_DIR+dir_name)
				os.makedirs(WORK_FILES_TMP)

			
			# montar lista com arquivos			
			#logfiles_list = []
			#[logfiles_list.append(obj) for obj in logfiles if obj not in logfiles_list] # adiciona os arquivos na lista ffn

			if not os.path.exists(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'):
				os.makedirs(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/')
				dir_ctrl = 1 # seta 1 caso o diretorio Controles/Coleta tenha sido criado a primeira vez
			else:
				dir_ctrl = 2 # seta 2 caso o diretório Controles/Coleta já exista
			
			#for i in logfiles_list:
			try:
				for i in logfiles:
					sftp.get(SFTP_HOME+i,WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+i)
					msg = "[INFO] Coleta do arquivo de controle "+i+" em "+sftpHOST
					writeLog(msg)
			except Exception as e:
				print 'exception',e

		return(dir_ctrl)
		#getFiles(sftpHOST,sftpUSER,sftpPASS,CTRL,dir_ctrl) # Chama a função para pegar os arquivos


	except (paramiko.BadHostKeyException, paramiko.AuthenticationException, 
         paramiko.SSHException, socket.error) as e:
		frameinfo = getframeinfo(currentframe())
		msg = '[ERRO] Erro ao conectar no servidor '+sftpHOST+'. ('+str(frameinfo.lineno)+')'
		writeLog(msg,e)
		msg = "###### Finalizado com erros. ID Execução: "+CURRENT_SHELL_PID+" ######"
		writeLog(msg)
		sys.exit(0)


	except Exception as e:
			frameinfo = getframeinfo(currentframe())
			print 'exception de fora',e
			msg = '[ERRO] Problemas ao baixar arquivo de log de '+SFTP_HOME+' ('+str(frameinfo.lineno)+')'
			writeLog(msg,e)
			msg = "###### Finalizado com erros. ID Execução: "+CURRENT_SHELL_PID+" ######"
			writeLog(msg)
			sys.exit(0)
	
	finally:
		if ssh:
			ssh.close()
			
			

def getFiles(sftpHOST,sftpUSER,sftpPASS,CTRL,dir_ctrl):



	if dir_ctrl == 1: # Caso nao exista diretorio /Controles/Coleta

		'''Diretório controle não existe.  _ultimo_arquivo.txt é criado partir do .log baixado'''
				
		li = []  #Lista dos arquivos de controles
		for logfiles in os.listdir(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL): # pesquisa os arquivos de log que foram baixados para pegar a ultima data
					
			if logfiles.endswith((CTRL+'.log')):# se exisite .log e se é um arquivo.
				li.append(logfiles)

		
		if not li:
			frameinfo = getframeinfo(currentframe())
			msg = '[INFO] Não há arquivos .log disponíveis com a data de '+CTRL+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
			writeLog(msg)
			#sys.exit(0)
			

		ultima_data_dict = {} # cria um dicionario key, value com o nome do arquivo .log e a data da primeira linha para pegar todos os arquivos
		for logfile_name in li:
			#print logfile_name
			
			with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+logfile_name,'r') as a: # abre arquivo .log
				next(a)
				for e in a: # como nao há nenhum arquivo de controle prévio, subintende-se que preciso pegar tudo a partir da primeira linha
					filename = e.split('/')[-1].strip().split(",")[0] # armazena o nome do arquivo .gz
					lastdate = e.strip().split(',')[0].replace('-','').replace(' ','').replace(':','') # guarda a data da ultima linha
		
			
			# Como não há nenhum arquivo de controle prévio, e o tempo de retenção dos arquivos no servidor remoto é de SFTP_LOG_RET, 
			# preciso pegar a partir da ultima linha do log o valor retornado na funcao verifyLogRetention()
			#ultima_data_dict[logfile_name] = verifyRetention(logfile_name)+'___ dir controle nao existe___pega do ultimo minuto'
			
				if SFTP_LOG_RET == '0':
					#ultima_data_dict[logfile_name] = lastdate # Se SFTP_LOG_RET = 0, inicia a coleta a partir do ultimo arquivo da lista.
					ultima_data_dict[logfile_name] = "0" # Se SFTP_LOG_RET = 0, inicia a coleta a partir do ultimo arquivo da lista.
				else:
					ultima_data_dict[logfile_name] = verifyRetention(logfile_name) # Inicia a coleta com o valor setado em SFTP_LOG_RET retroativo

			
			#grava o arquivo de controle como valor da ultima data do .log baixado
			with open(WORK_DIR+'/Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+logfile_name+'_ultimo_arquivo.txt','w') as controle: # grava aquivo com o conteudo os dados da primeira linha do arquivo .log
					controle.write(lastdate+','+sftpHOST+','+filename+','+logfile_name)



	elif dir_ctrl == 2:


		'''Diretório controle existe.  Consulta_ultimo_arquivo.txt para pegar o ultimo arquivo baixado'''

		lista_ultimo_arquivo = []
		ultima_data_dict = {} 
		
		for file in os.listdir(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL):

			if file.endswith((CTRL+'.log_ultimo_arquivo.txt')): # se tem arquivos terminados em '_ultimo_arquivo.txt'
				lista_ultimo_arquivo.append(file) # guarda na lista a ultima data contida dentro deste arquivo

		if not lista_ultimo_arquivo:
			frameinfo = getframeinfo(currentframe())
			msg = '[INFO] Arquivo .log_ultimo_arquivo.txt não encontrado com data de '+CTRL+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
			writeLog(msg)
			#time.sleep(5)
			#getLogFiles(sftpHOST,sftpUSER,sftpPASS,WORK_DIR)
			#sys.exit(0)


		for i in lista_ultimo_arquivo:
			with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+i,'r') as a:
				for l in a:
					ultima_data = l.strip().split(',')[0]
					nome_arquivo = l.strip().split(',')[-2]
					n_log = l.strip().split(',')[-1]
				ultima_data_dict[n_log] = ultima_data
			
		li = []  #Lista dos arquivos de controles
		for logfiles in os.listdir(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL): # pesquisa os arquivos de log que foram baixados para pegar a ultima data
					
			if logfiles.endswith((CTRL+'.log')):# se exisite .log e se é um arquivo.
				li.append(logfiles)
		
		if not li:
			frameinfo = getframeinfo(currentframe())
			msg = '[INFO] Não há arquivos .log disponíveis com a data de '+CTRL+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
			writeLog(msg)
			#sys.exit(0)

		for logfile_name in li:
			

			with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+logfile_name,'r') as a: # abre arquivo .log
				next(a)
				for e in a:
					filename = e.split('/')[-1].strip().split(",")[0] # armazena o nome do arquivo .gz
					lastdate = e.strip().split(',')[0].replace('-','').replace(' ','').replace(':','') # guarda a data da ultima linha

			#grava o arquivo de controle como valor da ultima data do .log baixado
			with open(WORK_DIR+'/Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+logfile_name+'_ultimo_arquivo.txt','w') as controle: # grava aquivo com o conteudo os dados da primeira linha do arquivo .log
					controle.write(lastdate+','+sftpHOST+','+filename+','+logfile_name)

					
	# Cria listas diferentes para cada tipo de arquivo para serem chamadas paralelamente pela thread
	http = []
	ftp = []
	other = []

	for k,v in ultima_data_dict.iteritems():

		if 'http' in k:

			with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+k, 'r') as a:
				next(a)
			
				if ultima_data_dict[k] == '0':
					for l in a:
						pathremotefile = l.strip().split(",")[-2].strip().split(" ")[0]
						filename = l.split('/')[-1].strip().split(",")[0]
						lastdate = l.strip().split(",")[0].replace("-","").replace(" ","").replace(":","") 
					http.append(pathremotefile+','+filename+',http,'+lastdate) # Adicina a ultima linha do arquivo na lista

				else:
					for l in a:
						pathremotefile = l.strip().split(",")[-2].strip().split(" ")[0]
						filename = l.split('/')[-1].strip().split(",")[0]
						lastdate = l.strip().split(",")[0].replace("-","").replace(" ","").replace(":","") 
						
						if ultima_data_dict[k] < lastdate:	
							http.append(pathremotefile+','+filename+',http,'+lastdate) # Adiciona a ultima linha do arquivo na lista


				if http:
					am_lines = str(len(http))
					with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_http_'+CTRL+'.tmp','w') as a:
						a.write(am_lines+'\n')
					with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_http_'+CTRL+'.tmp','a') as a:
						for i in http:
							file = i.split('/')[-1].strip().split(",")[0]
							a.write(file+'.tmp_'+sftpHOST+'\n')

				else:
					frameinfo = getframeinfo(currentframe())
					msg = '[INFO] Não há atualizações no arquivo de log '+k+' do servidor '+sftpHOST+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
					writeLog(msg)


		elif 'ftp' in k:
			
			with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+k, 'r') as a:
				next(a)
			
				if ultima_data_dict[k] == '0':
					for l in a:
						pathremotefile = l.strip().split(",")[-2].strip().split(" ")[0]
						filename = l.split('/')[-1].strip().split(",")[0]
						lastdate = l.strip().split(",")[0].replace("-","").replace(" ","").replace(":","") 
					ftp.append(pathremotefile+','+filename+',ftp,'+lastdate) # Adicina a ultima linha do arquivo na lista

				else:
					for l in a:
						pathremotefile = l.strip().split(",")[-2].strip().split(" ")[0]
						filename = l.split('/')[-1].strip().split(",")[0]
						lastdate = l.strip().split(",")[0].replace("-","").replace(" ","").replace(":","") 
						
						if ultima_data_dict[k] < lastdate:	
							ftp.append(pathremotefile+','+filename+',ftp,'+lastdate) # Adicina a ultima linha do arquivo na lista


				if ftp:
					am_lines = str(len(ftp))
					with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_ftp_'+CTRL+'.tmp','w') as a:
						a.write(am_lines+'\n')

					with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_ftp_'+CTRL+'.tmp','a') as a:
						for i in ftp:
							file = i.split('/')[-1].strip().split(",")[0]
							a.write(file+'.tmp_'+sftpHOST+'\n')

				else:
					frameinfo = getframeinfo(currentframe())
					msg = '[INFO] Não há atualizações no arquivo de log '+k+' do servidor '+sftpHOST+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
					writeLog(msg)

			
		elif 'other' in k:

			with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+k, 'r') as a:
				next(a)
			
				if ultima_data_dict[k] == '0':
					for l in a:
						pathremotefile = l.strip().split(",")[-2].strip().split(" ")[0]
						filename = l.split('/')[-1].strip().split(",")[0]
						lastdate = l.strip().split(",")[0].replace("-","").replace(" ","").replace(":","") 
					other.append(pathremotefile+','+filename+',other,'+lastdate) # Adicina a ultima linha do arquivo na lista

				else:
					for l in a:
						pathremotefile = l.strip().split(",")[-2].strip().split(" ")[0]
						filename = l.split('/')[-1].strip().split(",")[0]
						lastdate = l.strip().split(",")[0].replace("-","").replace(" ","").replace(":","") 
						
						if ultima_data_dict[k] < lastdate:	
							other.append(pathremotefile+','+filename+',other,'+lastdate) # Adicina a ultima linha do arquivo na lista
	
				
				if other:
					am_lines = str(len(other))
					with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_other_'+CTRL+'.tmp','w') as a:
						a.write(am_lines+'\n')

					with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_other_'+CTRL+'.tmp','a') as a:
						for i in other:
							file = i.split('/')[-1].strip().split(",")[0]
							a.write(file+'.tmp_'+sftpHOST+'\n')

				else:
					frameinfo = getframeinfo(currentframe())
					msg = '[INFO] Não há atualizações no arquivo de log '+k+' do servidor '+sftpHOST+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
					writeLog(msg)


	lists = []
	lists.extend(other+http+ftp)

	if lists:
		
		if SFTP_GET_SERIALIZED != '0':
			
			#splitted = lists[:int(SFTP_GET_SERIALIZED)]
			#splitted_backlog = lists[int(SFTP_GET_SERIALIZED):]


			''' INICIO issue-01-serelializacao 
				split para gravar o no log_ultimo_arquivo.txt o ultimo arquivo da lista splitada e nao 
				do ultimo arquivo lido da lista baixada da coletora
			'''

			
			splitted = []
			'''Splitting other'''
			if other:
				splitted_other = other[:int(SFTP_GET_SERIALIZED)]
				splitted_other_last_file = splitted_other[-1]
				splitted_other_last_file = splitted_other[-1]
				lastdate = splitted_other_last_file.split(',')[-1]
				filename = splitted_other_last_file.split(',')[-3]
				logfile_name = 'other_'+CTRL+'.log'

				with open(WORK_DIR+'/Controles/Coleta/'+sftpHOST+'/'+CTRL+'/other_'+CTRL+'.log_ultimo_arquivo.txt','w') as controle:
					controle.write(lastdate+','+sftpHOST+','+filename+','+logfile_name)

				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M')
				with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/ciclo_other_'+CTRL+'.tmp' ,'w') as a:
					a.write(str(len(splitted_other))+' - '+d+','+sftpHOST) # concatendo com 's' para ser exibido na probe, indicando que é serializado
					#a.write(str(len(other))+'s'+' - '+d+','+sftpHOST) # concatendo com 's' para ser exibido na probe, indicando que é serializado


				#splitted.extend(splitted_other)
				splitted.extend(other)

			'''Splitting http'''
			if http:
				splitted_http = http[:int(SFTP_GET_SERIALIZED)]
				splitted_http_last_file = splitted_http[-1]
				splitted_http_last_file = splitted_http[-1]
				lastdate = splitted_http_last_file.split(',')[-1]
				filename = splitted_http_last_file.split(',')[-3]
				logfile_name = 'http_'+CTRL+'.log'

				with open(WORK_DIR+'/Controles/Coleta/'+sftpHOST+'/'+CTRL+'/http_'+CTRL+'.log_ultimo_arquivo.txt','w') as controle: 
					controle.write(lastdate+','+sftpHOST+','+filename+','+logfile_name)

				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M')
				with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/ciclo_http_'+CTRL+'.tmp' ,'w') as a:
					a.write(str(len(splitted_http))+' - '+d+','+sftpHOST) # concatendo com 's' para ser exibido na probe, indicando que é serializado
					#a.write(str(len(http))+'s'+' - '+d+','+sftpHOST) # concatendo com 's' para ser exibido na probe, indicando que é serializado

				splitted.extend(http)


			'''Splitting ftp'''
			if ftp:
				splitted_ftp = ftp[:int(SFTP_GET_SERIALIZED)]
				splitted_ftp_last_file = splitted_ftp[-1]
				splitted_ftp_last_file = ftp[-1]
				lastdate = splitted_ftp_last_file.split(',')[-1]
				filename = splitted_ftp_last_file.split(',')[-3]
				logfile_name = 'ftp_'+CTRL+'.log'

				with open(WORK_DIR+'/Controles/Coleta/'+sftpHOST+'/'+CTRL+'/ftp_'+CTRL+'.log_ultimo_arquivo.txt','w') as controle:
					controle.write(lastdate+','+sftpHOST+','+filename+','+logfile_name)

				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M')
				with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/ciclo_ftp_'+CTRL+'.tmp' ,'w') as a:
					a.write(str(len(splitted_ftp))+' - '+d+','+sftpHOST) # concatendo com 's' para ser exibido na probe, indicando que é serializado
					#a.write(str(len(ftp))+'s'+' - '+d+','+sftpHOST) # concatendo com 's' para ser exibido na probe, indicando que é serializado

				splitted.extend(ftp)



			''' FIM issue-01-serelializacao 
				split para gravar o no log_ultimo_arquivo.txt o ultimo arquivo da lista splitada e nao 
				do ultimo arquivo lido da lista baixada da coletora
			'''



			



#			msg = "[INFO] Inicio ciclo de coleta para "+str(len(splitted))+" arquivo(s). Enviado ao backlog "+str(len(splitted_backlog))+" arquivo(s)"
#			backlog_file = WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+'backlog_'+CTRL+'.txt'
			#backlog_file = WORK_DIR+'Controles/Coleta/backlog_'+CTRL+'.txt'
#			writeLog(msg)

#			d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
#			with open (backlog_file,'a') as a:
#				for i in splitted_backlog:
#					a.write(d+','+sftpHOST+','+i+'\n')
#					file = i.split(',')[1]
#					f_type = i.split(',')[2]
#					a.write(d+','+sftpHOST+','+SFTP_HOME+file+','+f_type+'\n')

#					frameinfo = getframeinfo(currentframe())
#					msg = "[BACKLOG] Arquivo "+file+" enviado ao backlog ("+str(frameinfo.lineno)+")"
#					writeLog(msg)
#

			#sys.exit()
#			msg = "[WARN] Arquivos foram enviados ao backlog. Execute o comando: python /data/p_loadbd/DPI_SIG/Scripts/Python/dpi_coleta_probe.py "+CTRL+" para acompanhamento do processo."
#			writeLog(msg)

			#threadConstruct(splitted_limited)
			
			goGet(splitted)						

		else:
			if (len(lists) > 0) and (len(lists) <= int(THREADS_LIMIT)):
				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S')
				msg = "[INFO] Inicio ciclo de coleta para "+str(len(lists))+" arquivo(s)"
				writeLog(msg)
				size = int(THREAD_SPLIT_SIZE)
				splitted = [lists[i:i+size] for i in range(0, len(lists), size)]

				print 'primeiro if'
				print 'lists', len(lists)
				print 'splitted', len(splitted)
				print 'size', size

				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S')
				with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/ciclo_threads_'+CTRL+'.tmp' ,'w') as a:
					a.write(str(len(splitted))+' - '+d+','+sftpHOST)

				#sys.exit()
				#threadConstruct(splitted)
				threadConstructP(splitted)

			else:
				# Se a lista for maior que THREADS_LIMIT, pegar os 30 primeiros elementos, preferencialmente lista OTHER. o que sobrar gravar num arquvio de backlog
				size = int(THREAD_SPLIT_SIZE)
				splitted = [lists[i:i+size] for i in range(0, len(lists), size)]
				splitted_limited = splitted[:int(THREADS_LIMIT)] # Limita a lista para oa qtd de arquivos definida em THREADS_LIMIT
				splitted_backlog = splitted[int(THREADS_LIMIT):] # Cria uma outra lista com a diferença dos arquivos

				msg = "[INFO] Inicio ciclo de coleta para "+str(len(splitted_limited))+" arquivo(s). Enviado ao backlog "+str(len(splitted_backlog))+" arquivo(s)"
				writeLog(msg)
				backlog_file = WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+'backlog_'+CTRL+'.txt'
				#backlog_file = WORK_DIR+'Controles/Coleta/backlog_'+CTRL+'.txt'

				d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
				with open (backlog_file,'a') as a:
					for elem in splitted_backlog:
						for i in elem:
							#a.write(d+','+sftpHOST+','+i+'\n')
							file = i.split(',')[1]
							f_type = i.split(',')[2]
							a.write(d+','+sftpHOST+','+SFTP_HOME+file+','+f_type+'\n')

							frameinfo = getframeinfo(currentframe())
							msg = "[BACKLOG] Arquivo "+file+" enviado ao backlog ("+str(frameinfo.lineno)+")"
							writeLog(msg)


				print 'segundo if'
				print 'lists', len(lists)
				print 'splitted_limited', len(splitted_limited)
				print 'elem', len(elem)
				print 'splitted_backlog', len(splitted_backlog)
				print 'size', size

				#print splitted_limited
				
				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S')
				with open (WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/ciclo_threads_'+CTRL+'.tmp' ,'w') as a:
					a.write(str(len(splitted_limited))+' - '+d+','+sftpHOST)

				#sys.exit()
				msg = "[WARN] Arquivos foram enviados ao backlog. Execute o comando: python /data/p_loadbd/DPI_SIG/Scripts/Python/dpi_coleta_probe.py "+CTRL+" para acompanhamento do processo."
				writeLog(msg)

				#threadConstruct(splitted_limited)
				threadConstructP(splitted_limited)



	else:
		frameinfo = getframeinfo(currentframe())
		msg = '[INFO] Não há atualizações no arquivo de log no servidor '+sftpHOST+'. Tentarei de novo. ('+str(frameinfo.lineno)+')'
		writeLog(msg)



def threadConstructP(lists):

	threads = []
	for i in lists:
		p = multiprocessing.Process(target=goGet, args=(i,))
		threads.append(p)
		p.daemon = True
		time.sleep(0.3)
		p.start()
	
	for p in threads:
		p.join()


	while len(threads) > 0:
		time.sleep(1)
		for p in threads:
			if not p.is_alive():
				#print multiprocessing.current_process().name+" terminated"
				#print current_thread().name+" terminated"
				threads.remove(p)

	d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S')
	msg = "[INFO] Finalizou ciclo de "+str(len(lists))+" arquivos"
	writeLog(msg)

'''

def threadConstruct(lists):

	threads = []
	for i in lists:
		t = Thread(target=goGet, args=(i,))
		t.setDaemon = True
		time.sleep(0.5)
		t.start()
		threads.append(t)

	for t in threads:
		t.join()

	while len(threads) > 0:
		for t in threads:
			if not t.isAlive():
				threads.remove(t)

	d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S')
	msg = "[INFO] Finalizou ciclo de "+str(len(lists))+" arquivos"
	writeLog(msg)
'''


def goGet(list_name):

	print 'Total arquivos a coletar:', len(list_name)

	try:
		#ssh = paramiko.SSHClient()
		#ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		#ssh.load_system_host_keys()
		#ssh.connect(sftpHOST,username=sftpUSER,password=sftpPASS,allow_agent=False,look_for_keys=False,timeout=2)
		#sftp = ssh.open_sftp()

		ssh_conn = FastTransport((sftpHOST, 22))
		ssh_conn.connect(username=sftpUSER, password=sftpPASS)
		sftp = paramiko.SFTPClient.from_transport(ssh_conn)


	except (paramiko.BadHostKeyException, paramiko.AuthenticationException, 
		paramiko.SSHException, socket.error) as e:
		frameinfo = getframeinfo(currentframe())
		msg = '[ERRO] Erro ao conectar no servidor '+sftpHOST+'. ('+str(frameinfo.lineno)+')'
		writeLog(msg,e)


	for i in list_name:
	
		data_linha = i.strip().split(',')[-1]
		dirdest = i.strip().split(',')[-2]
		remotefile = i.strip().split(',')[-3]

		try:

			#ssh = paramiko.SSHClient()
			#ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			#ssh.load_system_host_keys()
			#ssh.connect(sftpHOST,username=sftpUSER,password=sftpPASS,allow_agent=False,look_for_keys=False,timeout=2)
			#sftp = ssh.open_sftp()

			with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+dirdest+'_arquivo_atual_'+CTRL+'.tmp','w') as a: #guarda os dados do arquivo que esta sendo baixado no momento
				a.write(sftpHOST+','+data_linha+','+remotefile+','+dirdest)

			remote = SFTP_HOME+remotefile

			#para gravar em disco
			localfile = WORK_FILES_TMP+remotefile

			#para gravar no nfs
			#localfile = WORK_FILES_TMP+dirdest+'/'+remotefile

			
			time_ini = datetime.datetime.now().replace(microsecond=0)
			#print 'inicio ',remotefile,  time_ini
			#print remote,localfile+'.tmp_'+sftpHOST
			sftp.get(remote,localfile+'.tmp_'+sftpHOST)
			time_fim = datetime.datetime.now().replace(microsecond=0)
			delta = str(time_fim - time_ini)
			print remotefile,delta

			
			if os.path.exists(localfile+'.tmp_'+sftpHOST):
				
				try:
					frameinfo = getframeinfo(currentframe())
					os.rename(localfile+'.tmp_'+sftpHOST,localfile)
					print 'renomeou'
				except Exception as e:
					msg = "[ERRO] Problemas ao renomear o arquivo "+localfile+'.tmp_'+sftpHOST+" para "+localfile+str(frameinfo.lineno)+")"
					writeLog(msg)
				
				msg = "[INFO] Arquivo "+localfile+'.tmp_'+sftpHOST+" coletado com sucesso"
				writeLog(msg)
				msg = "[INFO] Renomeou de "+localfile+'.tmp_'+sftpHOST+" para "+localfile
				writeLog(msg)

				'''
				msg = "[INFO] Verificando integridade do arquivo " +localfile
				writeLog(msg)
				
				
				time_ini_integ = datetime.datetime.now().replace(microsecond=0)
				checkFileIntegrity(localfile)
				time_fim_integ = datetime.datetime.now().replace(microsecond=0)
				delta_integ = str(time_fim_integ - time_ini_integ)
				print 'Integridade OK! ',delta

				d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
				with open (WORK_DIR+"Controles/Coleta/acompanhamento_gunzip_OS_"+CTRL+".txt",'a') as a:
					a.write(d+','+localfile+','+str(time_ini_integ)+','+str(time_fim_integ)+','+delta_integ+'\n')
				'''

				writeSizes(dirdest,localfile,delta)
				msg = "[INFO] Escreveu "+localfile+" no arquivo de sizes "+WORK_DIR+"Controles/Coleta/PROCESSOS_"+CTRL+".txt"
				writeLog(msg)

			
				
				'''#####  rotina para medir o tempo de move para hdfs ##### 
				try:
					time_ini_move = datetime.datetime.now().replace(microsecond=0)
					#shutil.move(localfile,WORK_FILES_TMP+dirdest+'/'+remotefile)
					#os.system('hdfs dfs -moveFromLocal '+localfile+' '+HDFS_STAGE_DIR+dirdest+'/'+remotefile) 
					os.system('hdfs dfs -moveFromLocal '+localfile+' '+HDFS_STAGE_DIR+remotefile) 

					time_fim_move = datetime.datetime.now().replace(microsecond=0)
					delta_move = str(time_fim_move - time_ini_move)
					msg = "[INFO-MOVE] Moveu "+localfile+" para HDFS"
					writeLog(msg)

					d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
					#with open (WORK_DIR+"Controles/Coleta/acompanhamento_move_NFS_"+CTRL+".txt",'a') as a:
					with open (WORK_DIR+"Controles/Coleta/acompanhamento_move_HDFS_"+CTRL+".txt",'a') as a:
						#a.write(d+','+localfile+','+delta_move+','+str(time_ini_move)+','+str(time_fim_move)+'\n')
						a.write(d+','+localfile+','+str(time_ini_move)+','+str(time_fim_move)+','+delta_move+'\n')
					print 'moveu HDFS ', delta_move

				except Exception as e:
					frameinfo = getframeinfo(currentframe())
					msg = "[ERRO-MOVE] Erro ao mover arquivo "+file+" para HDFS "+str(e)+' '+str(frameinfo.lineno)+")."
					writeLog(msg)

					d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
					with open (WORK_DIR+"Controles/Coleta/erro_move_hdfs_"+CTRL+".txt",'a') as a:
						a.write(d+','+localfile+','+str(e)+'\n')
				# fim '''




			else:
				frameinfo = getframeinfo(currentframe())
				msg = "[INFO] Problemas ao verificar se o arquivo "+localfile+'.tmp_'+sftpHOST+" existe. ("+str(frameinfo.lineno)+")"
				writeLog(msg)
			
			
		except (paramiko.BadHostKeyException, paramiko.AuthenticationException, 
		     paramiko.SSHException, socket.error) as e:
			frameinfo = getframeinfo(currentframe())
			msg = '[ERRO] Erro ao conectar no servidor '+sftpHOST+'. ('+str(frameinfo.lineno)+')'
			writeLog(msg,e)

		
		except IOError as e:
			frameinfo = getframeinfo(currentframe())
			msg = '[RETRY] Problemas ao baixar o arquivo '+SFTP_HOME+remotefile+' no diretorio '+dirdest+'. Servidor: '+sftpHOST+'. ' \
			 	   'Arquivo direcionado para diretorio retry/ para nova tentativa. ('+str(frameinfo.lineno)+')'
			msg_retry = sftpHOST+','+remotefile+','+CTRL+','+dirdest # Grava nome do servidor, nome do arquivo, data do arquivo e dir de destino para futura tentativa
			writeLog(msg,e)
			
			if not os.path.exists(WORK_DIR+'Controles/Coleta/retry/'+sftpHOST):
				os.makedirs(WORK_DIR+'Controles/Coleta/retry/'+sftpHOST)

			with open(WORK_DIR+'Controles/Coleta/retry/'+sftpHOST+'/'+remotefile+'_'+dirdest+'_.ret','w') as retry:
				retry.write(msg_retry)

			time.sleep(3)
			frameinfo = getframeinfo(currentframe())
			msg = "[INFO] Chamou getRetryFiles para "+localfile+". "+str(frameinfo.lineno)+")"
			writeLog(msg)

			print 'chamou retry', remotefile, e, 'Tentativa 0'
			
			msg = "Enviado para retry, ignorado "+remotefile
			with open(WORK_DIR+'Controles/Coleta/ignorados_retry_'+CTRL+'.txt','a') as a:
				a.write(msg+','+str(e)+'\n')
			#getRetryFiles(remotefile,dirdest,0)
			


		except Exception as e:
			frameinfo = getframeinfo(currentframe())
			msg = '[RETRY] Redirecionado para retry. Problemas ao baixar o arquivo '+remotefile+' no diretorio '+dirdest+'. Servidor: '+sftpHOST+'. ('+str(frameinfo.lineno)+')'
			msg_retry = sftpHOST+','+remotefile+','+CTRL+','+dirdest # Grava nome do servidor, nome do arquivo, data do arquivo e dir de destino para futura tentativa
			writeLog(msg,e)

			if not os.path.exists(WORK_DIR+'Controles/Coleta/retry/'+sftpHOST):
				os.makedirs(WORK_DIR+'Controles/Coleta/retry/'+sftpHOST)

			with open(WORK_DIR+'Controles/Coleta/retry/'+sftpHOST+'/'+remotefile+'_'+dirdest+'_.ret','w') as retry:
				retry.write(msg_retry)
			
			time.sleep(2)
			frameinfo = getframeinfo(currentframe())
			msg = "[####] Chamou getRetryFiles para "+localfile+". "+str(frameinfo.lineno)+")"
			writeLog(msg)

			print 'chamou retry', remotefile, e, '1'
			#getRetryFiles(remotefile,dirdest,0)

			msg = "Enviado para retry, ignorado "+remotefile
			with open(WORK_DIR+'Controles/Coleta/ignorados_retry_'+CTRL+'.txt','a') as a:
				a.write(msg+','+str(e)+'\n')



		#finally:
			#if ssh:
			#	ssh.close()

	if ssh_conn:
		ssh_conn.close()
		



def getRetryFiles(filename,dirdest,n_retries):
	
	retry_path = WORK_DIR+'Controles/Coleta/retry/'+sftpHOST+'/'
	
	file = filename
	remote_path = SFTP_HOME+file
	localpath = WORK_FILES_TMP
	localfile = localpath+file+'_ret_'+sftpHOST #para gravar em disco
	#localfile = localpath+dirdest+'/'+file+'_ret_'+sftpHOST # para gravar no nfs
			
	try:
		#ssh = paramiko.SSHClient()
		#ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		#ssh.load_system_host_keys()
		#ssh.connect(sftpHOST,username=sftpUSER,password=sftpPASS,timeout=0.5)
		#sftp = ssh.open_sftp()

		ssh_conn = FastTransport((sftpHOST, 22))
		ssh_conn.connect(username=sftpUSER, password=sftpPASS)
		sftp = paramiko.SFTPClient.from_transport(ssh_conn)

		time_ini_retry = datetime.datetime.now().replace(microsecond=0)
		#print (remote_path,localfile)
		sftp.get(remote_path,localfile) #baixa os arquivos para a data de CTRL
		time_fim_retry = datetime.datetime.now().replace(microsecond=0)
		delta_retry = str(time_fim_retry - time_ini_retry)
		print remote_path,delta_retry

		if os.path.exists(localfile):
			os.rename(localfile,localpath+file)
			msg = "[RETRY] Arquivo "+localfile+" coletado com sucesso. Tentativa n. "+str(n_retries)
			writeLog(msg)
			msg = "[RETRY] Renomeou de "+localfile+" para "+file
			writeLog(msg)

			#msg = "[RETRY] Verificando integridade do arquivo " +file
			#writeLog(msg)
			
			#time_ini_integ_retry = datetime.datetime.now().replace(microsecond=0)
			#checkFileIntegrity(localpath+file)
			#time_fim_integ_retry = datetime.datetime.now().replace(microsecond=0)
			#delta_integ_retry = str(time_fim_integ_retry - time_ini_integ_retry)

			#d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
			#with open (WORK_DIR+"Controles/Coleta/acompanhamento_gunzip_OS_"+CTRL+".txt",'a') as a:
			#	a.write(d+','+localfile+','+str(time_ini_integ_retry)+','+str(time_fim_integ_retry)+','+delta_integ_retry+'\n')

			writeSizes(dirdest,localpath+file,delta_retry)
			msg = "[RETRY] Escreveu "+file+" no arquivo de sizes "+WORK_DIR+"Controles/Coleta/PROCESSOS_"+CTRL+".txt"
			writeLog(msg)



			''' #####  rotina para medir o tempo de move para nfs ##### 
			try:
				time_ini_move_retry = datetime.datetime.now().replace(microsecond=0)
				#shutil.move(localpath+file,WORK_FILES_TMP+dirdest+'/'+file)
				#os.system('hdfs dfs -moveFromLocal '+localpath+file+' '+HDFS_STAGE_DIR+dirdest+'/'+file) 
				os.system('hdfs dfs -moveFromLocal '+localpath+file+' '+HDFS_STAGE_DIR+file) 
				time_fim_move_retry = datetime.datetime.now().replace(microsecond=0)
				delta_move_retry = str(time_fim_move_retry - time_ini_move_retry)
				msg = "[INFO-MOVE-RETRY] Moveu "+file+" para HDFS"
				writeLog(msg)
				
				d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
				#with open (WORK_DIR+"Controles/Coleta/acompanhamento_move_NFS_"+CTRL+".txt",'a') as a:
				with open (WORK_DIR+"Controles/Coleta/acompanhamento_move_HDFS_"+CTRL+".txt",'a') as a:
					#a.write(d+','+localpath+file+','+delta_move_retry+','+str(time_ini_move_retry)+','+str(time_fim_move_retry)+'\n')
					a.write(d+','+localpath+file+','+str(time_ini_move_retry)+','+str(time_fim_move_retry)+','+delta_move_retry+'\n')
			except Exception as e:
				frameinfo = getframeinfo(currentframe())
				msg = "[ERRO-MOVE-RETRY] Erro ao mover arquivo "+file+" para HDFS "+str(e)+' '+str(frameinfo.lineno)+")."
				writeLog(msg)
				
				d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
				with open (WORK_DIR+"Controles/Coleta/erro_move_hdfs_"+CTRL+".txt",'a') as a:
					a.write(d+','+localfile+','+str(e)+'\n')				
			 # fim '''

			
			try:
				retry_file = retry_path+file+'_'+dirdest+'_.ret'
				
				os.system('rm -fr '+retry_file) # remove os arquivos de retry
				frameinfo = getframeinfo(currentframe())
				msg = "[RETRY] Arquivo "+file+" foi coletado. Removido de retry.("+str(frameinfo.lineno)+")"
				writeLog(msg)
				
				d = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S.%f')
				with open (WORK_DIR+'Controles/Coleta/coletados_retry_'+CTRL+'.txt','a') as a:
					a.write(d+','+localpath+file+','+dirdest+','+str(n_retries)+'\n')

				msg = "[RETRY] Escreveu "+file+" em coletados_retry_"+CTRL+".txt"
				writeLog(msg)

			except IOError as e:
				frameinfo = getframeinfo(currentframe())
				msg = "[RETRY-ERRO] Problemas em remover o arquivo "+retry_file+". ("+str(frameinfo.lineno)+")"
				writeLog(msg,e)

			except Exception as e:
				frameinfo = getframeinfo(currentframe())
				msg = "[RETRY-ERRO] Exception Problemas em remover o arquivo "+retry_file+". ("+str(frameinfo.lineno)+")"
				writeLog(msg,e)
		
		else:
			frameinfo = getframeinfo(currentframe())
			msg = "[RETRY-ERRO] Problemas ao verificar se o arquivo "+localfile+" existe. ("+str(frameinfo.lineno)+")"
			writeLog(msg)
			


	except (paramiko.BadHostKeyException, paramiko.AuthenticationException, 
	     paramiko.SSHException, socket.error) as e:
		frameinfo = getframeinfo(currentframe())
		msg = '[RETRY-ERRO] Erro ao conectar no servidor / coletora '+sftpHOST+'. ('+str(frameinfo.lineno)+')'
		writeLog(msg,e)
		sys.exit(1)
		

	except IOError as e:
		
		n_retries += 1
		if n_retries <= N_RETRIES:
			time.sleep(1)
			msg = "[RETRY] Tentativa para "+file+" n. "+str(n_retries)
			writeLog(msg)
			print "Tentativa para "+file+" n. "+str(n_retries)
			#msg = '#### '+file+','+dirdest+','+str(n_retries)+'####'
			#writeLog(msg)
			
			getRetryFiles(file,dirdest,n_retries)
			
		else:
			frameinfo = getframeinfo(currentframe())
			#msg = '[RETRY] Arquivo ja estava em retry. Nao havera proximas tentativas para o arquivo '+filename+' direcionado para arquivos_retry_falhas_'+CTRL+'.txt. ('+str(frameinfo.lineno)+')'
			msg = "[ERRO] Arquivo "+file+" redirecionado para backlog apos "+str(n_retries)+" tentativas ("+str(frameinfo.lineno)+")"
			writeLog(msg,e)

			backlog_file = WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'+'backlog_'+CTRL+'.txt'
			#backlog_file = WORK_DIR+'Controles/Coleta/backlog_'+CTRL+'.txt'

			d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
			with open (backlog_file,'a') as a:
				#a.write(d+','+sftpHOST+','+i+'\n')
				a.write(d+','+sftpHOST+','+SFTP_HOME+file+','+dirdest+'\n')

			frameinfo = getframeinfo(currentframe())
			msg = "[BACKLOG] Arquivo "+file+" enviado ao backlog apos "+str(n_retries)+" tentativas. ("+str(frameinfo.lineno)+")"
			writeLog(msg,e)

			arq_retry = WORK_DIR+'Controles/Coleta/arquivos_retry_falhas_'+CTRL+'.txt'
			msg = "[RETRY] Escrevendo "+filename+" no arquivo "+arq_retry+". Tentativas: "+str(n_retries)
			writeLog(msg)

			d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')
			with open(arq_retry,'a') as a:
				a.write(d+','+sftpHOST+','+filename+','+str(e)+'\n')

			return		


	#except Exception as e:
	#	frameinfo = getframeinfo(currentframe())
	#	msg = '[RETRY-ERRO] Exception. Nao havera proximas tentativas para o arquivo '+file+'. ('+str(frameinfo.lineno)+')'
	#	writeLog(msg,e)

		
	finally:

		if ssh_conn:
			ssh_conn.close()



def checkFileIntegrity(localfile):

	cmd = 'gunzip -t '+localfile

	try:
		p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
		output = p.stdout.read()

		if not output:
			msg = "[INFO] Integridade OK! "+localfile
			writeLog(msg)
		else:
			file_fail = WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_corrompidos_'+CTRL+'.txt'
			msg = "[WARN] Arquivo "+localfile+" corrompido. Direcionado para "+file_fail
			writeLog(msg)
			d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
			with open(WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/arquivos_corrompidos_'+CTRL+'.txt','a') as a:
				a.write(d+','+localfile+'\n')
	except Exception as e:
		frameinfo = getframeinfo(currentframe())
		msg = "[ERRO] Problemas ao verificar a integridade do arquivo "+localfile+ '('+str(frameinfo.lineno)+')'
		writeLog(msg,e)


def writeSizes(dirdest,localfile,delta):

	#file_w = WORK_DIR+'Controles/Coleta/acompanhamento_sizes_'+CTRL+'.txt'
	#file_w = WORK_DIR+'Controles/Coleta/UNICA_THREAD_'+CTRL+'.txt'
	file_w = WORK_DIR+'Controles/Coleta/PROCESSOS_'+CTRL+'.txt'
	date = datetime.datetime.strftime(datetime.datetime.now(),'%H:%M:%S.%f')
	size = os.path.getsize(localfile)
	linha = sftpHOST+','+date+','+dirdest+','+localfile+','+str(size)+','+delta+'\n'
	with open(file_w,'a') as a:
		a.write(linha)


def writeLog(*args):
	
	#d_log_name = datetime.datetime.strftime(datetime.datetime.now(),'%H%M')
	d = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f')

	if not os.path.exists(LOGS_DIR+sftpHOST):
		os.makedirs(LOGS_DIR+sftpHOST)
	
	logfile = LOGS_DIR+sftpHOST+'/'+CTRL+'/clt_dpi_sig_sftp_01_010_'+CURRENT_SHELL_PID+'.log'
	#logfile = LOGS_DIR+sftpHOST+'/'+CTRL+'_'+d_log_name+'_'+CURRENT_SHELL_PID+'.log'
	if len(args) == 2:
		with open(logfile,'a') as log:
			log.write('['+d+']'+' '+args[0]+'. '+str(args[1])+'\n')
	else:
		with open(logfile,'a') as log:
			log.write('['+d+']'+' '+args[0]+'.'+'\n')


def verifyRetention(logfile_name):

	path = WORK_DIR+'Controles/Coleta/'+sftpHOST+'/'+CTRL+'/'
	
	with open (path+logfile_name,'r') as a:
		file = a.read().splitlines()
		
		for i,e in enumerate(file):
			lastdate = e.strip().split(',')[0].replace('-','').replace(' ','').replace(':','') # guarda a data da ultima linha

	lastdate_dto = datetime.datetime.strptime(lastdate, '%Y%m%d%H%M%S')

	if SFTP_LOG_RET == '0':
		log_ret = '0'
		return(log_ret)


	elif 'm' in SFTP_LOG_RET: # se o tempo de log retention for setado para 0 no arquivo vivo.properties, pega somente minutos anteriores de arquivos.
		SFTP_LOG_RET1 = SFTP_LOG_RET.split('m')[0]
		last_t = lastdate_dto - timedelta(minutes = int(SFTP_LOG_RET1))
	
	elif 'h' in SFTP_LOG_RET: # se o tempo de log retention for setado para 0 no arquivo vivo.properties, pega somente horas anteriores de arquivos.
		SFTP_LOG_RET1 = SFTP_LOG_RET.split('h')[0]
		last_t = lastdate_dto - timedelta(hours = int(SFTP_LOG_RET1))

	else:
		frameinfo = getframeinfo(currentframe())
		msg = "[ERRO] Valor da variavel SFTP_LOG_RET esta setado no arquivo vivo.properties de forma errada. Se o valor for diferente de 0, deve "\
				"conter m ou h ao lado do numero.("+str(frameinfo.lineno)+")"
		writeLog(msg)
		msg = "###### Finalizado com erros. ID Execução: "+CURRENT_SHELL_PID+" ######"
		writeLog(msg)
		sys.exit(0)


	log_ret = last_t.strftime('%Y%m%d%H%M%S')

	return(log_ret)


def paramikoLog():

    if not os.path.exists(LOGS_DIR+sftpHOST):
        os.makedirs(LOGS_DIR+sftpHOST)

    if not os.path.exists(LOGS_DIR+sftpHOST+'/'+CTRL):
        os.makedirs(LOGS_DIR+sftpHOST+'/'+CTRL)
        paramiko_log = LOGS_DIR+sftpHOST+'/'+CTRL+'/'+'paramiko_sftp_collector_'+CURRENT_SHELL_PID+'.log'
        paramiko.util.log_to_file(paramiko_log)

        return (paramiko_log)	



class FastTransport(paramiko.Transport):
	def __init__(self, sock):
		super(FastTransport, self).__init__(sock)
		self.window_size = 2147483647



if __name__ == '__main__':
	
	sftpHOST = str(sys.argv[1])
	sftpUSER = str(sys.argv[2])
	sftpPASS = str(sys.argv[3])
	WORK_DIR = str(sys.argv[4])
	SFTP_HOME = str(sys.argv[5])
	CTRL = str(sys.argv[6])
	LOGS_DIR = str(sys.argv[7])
	SFTP_LOG_RET = str(sys.argv[8])
	THREADS_LIMIT = str(sys.argv[9])
	THREAD_SPLIT_SIZE= str(sys.argv[10])
	CURRENT_SHELL_PID = str(sys.argv[11])
	WORK_FILES_TMP = str(sys.argv[12])
	#WORK_FILES_NFS = '/data/disk2/DPI_SIG/'
	N_RETRIES = int(sys.argv[13])
	HDFS_STAGE_DIR = str(sys.argv[14])
	SFTP_GET_SERIALIZED = str(sys.argv[15])


	paramiko_log = paramikoLog()


	#localfile = '/home/h_loadbd/DPI_SIG/Controles/Coleta/teste/AOtherP03D20161116190749E902.txt.gz'
	#dirdest = 'other'
	#getRetryFiles(localfile+','+dirdest,0)
	#sys.exit()


	msg = "###### Inicio ciclo de coleta de arquivos. ID Execução: "+CURRENT_SHELL_PID+" ######"
	writeLog(msg)

	t_ini = datetime.datetime.now()
	dir_ctrl = getLogFiles(sftpHOST,sftpUSER,sftpPASS,WORK_DIR) # chama a funcao para pegar os arquivos .log
	t_fim = datetime.datetime.now()
	delta = str(t_fim - t_ini)
	print 'Pegou lista em: ',delta
	print ' '

	t_ini = datetime.datetime.now()
	print 'INICIO COLETA', t_ini
	print '---'
	getFiles(sftpHOST,sftpUSER,sftpPASS,CTRL,dir_ctrl) # Chama a função para pegar os arquivos


	t_fim = datetime.datetime.now()
	print 'FIM COLETA', t_fim
	delta = str(t_fim - t_ini)
	print '\n'
	print 'Tempo Total: '+delta

	
	msg = "###### Fim ciclo de coleta de arquivos. ID Execução: "+CURRENT_SHELL_PID+" ######"
	writeLog(msg)
	
	