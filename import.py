import os 
import time


path = "C:\\01Digital\\aes\\Import"

#como o proceso pode demorar entao vou criar um controlador
#caso exista o arquivo running entao nao faz nada...
running = path + "\\running.dat"

#arquivo onde tera a lista dos files para importar
job_list = path + "\\job.list"
	
def importar(aqruivo):
	pass

if not os.path.exists(running):
	file_running = open(running,'w')  
	file_running.close()

	#verificar se tem uma lista com os arquivos para ler
	if os.path.exists(job_list):		
		with open(job_list, 'r') as linhas:
			for linha in linhas.readlines():
				if linha.endswith('.zip'):
					print ("Descompatar "+ (linha))
		

		os.remove(job_list)

	

	#remove o running e fica pronto para executar de novo
	os.remove(running)

	#por fim verifica se tem novos arquivos para o proximo step
	if not os.path.exists(job_list):
		get_txt_list = [f for f in os.listdir(path) if f.endswith('.txt')]
		get_zip_list = [f for f in os.listdir(path) if f.endswith('.zip')]
		if len(get_txt_list) + len(get_zip_list) > 0:
			text_file = open(job_list, "w")		
	  		text_file.write("\n".join(get_txt_list) + "\n")
	  		text_file.write("\n".join(get_zip_list) + "\n")
			text_file.close()
	


