import sys
import logging
logging.basicConfig(filename='log/F18-2.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logging.info('Inicio de Execução')
try:
    import re 
    import csv
    import time
    import pyodbc
    import psycopg2
    import requests
    import schedule
    import datetime
    import threading
    import configparser
    from queue import Queue
    import Winchester as wt
    import ModuloEmail as eml
except:
    logging.critical("Erro de Importação:"+str(sys.exc_info()[0]))
    print('Erro de importação de Libs')


config = configparser.ConfigParser()
config.read('config.ini')

tesisdb = 'DRIVER={}; SERVER={}; DATABASE={}; UID={}; PWD={}'.format('{SQL SERVER}', config.get('tesis_db', 'ip'), config.get('tesis_db', 'db_name'), config.get('tesis_db', 'db_user'), config.get('tesis_db', 'db_pwd'))
totalipdb = 'host={} dbname={} user={} password={}'.format(config.get('totalip_db', 'ip'),config.get('totalip_db', 'db_name'), config.get('totalip_db', 'db_user'), config.get('totalip_db', 'db_pwd') )    
data = datetime.datetime.now()

CIRRE = 382
CM1 = 383
CM2 = 406
CPREJ = 404
CRCI = 385
CSEGP = 368
CSEGN = 302
 
            
poss = [CIRRE, 375, 397, CRCI, CM1, CM2, CPREJ, CSEGP]
negg = [384,410,391,324,255, 401, 398, CSEGN]

# campanhas - NEG
# 384 - irregular
# 410 - M1
# 391 - M2
# 324 - Preju
# 255 - RCI
# 368 - SEG POS
# 302 - SEG NEG
# 'Flex': {# 'pos': 382,# 'neg': 384
# 'Irreg': { # 'SUDESTE': {# 'pos': 375,# 'neg': 401
# 'NORDESTE': {# 'pos': 397,# 'neg': 398

EXECCIRRE   = 0
EXECCM1     = 0
EXECCM2     = 0
EXECCPREJ   = 0
EXECCRCI    = 0
execcam = [EXECCIRRE, EXECCM1, EXECCM2, EXECCPREJ, EXECCRCI]

def consultaCompletos():
    query= "SELECT id, telefones_restantes, discando, clientes_virgens, ativa FROM campanhas where id in ({})".format(" ,".join(map(str, poss+negg)))
    with psycopg2.connect(totalipdb) as con:
        cur = con.cursor()
        cur.execute(query)
        dados = cur.fetchall()
    completos = [camp[0] for camp in dados if (camp[1] == 0 and camp[2] == 0 and camp[3] == 0) or (camp[-1] is False)]
    logging.debug('completos: {}'.format(completos))
    return completos

def consultaAtivos():
    query= "SELECT id FROM campanhas where id in ({}) and ativa is true".format(" ,".join(map(str, poss+negg)))
    with psycopg2.connect(totalipdb) as con:
        cur = con.cursor()
        cur.execute(query)
        dados = cur.fetchall()
    completos = [camp[0] for camp in dados]
    logging.debug('completos: {}'.format(completos))
    return completos

def refreshData():
    global data
    data = datetime.datetime.now()


def processoSecundario():
    global EXECCIRRE
    global EXECCM1 
    global EXECCM2
    global EXECCPREJ
    global EXECCRCI
    try:
        completos = [x for x in consultaCompletos() if x not in negg]
        logging.debug('execucao secundaria: executados: {}'.format(", ".join(map(str, completos))))
        if EXECCIRRE == 4 and (CIRRE in completos):
            completos.remove(CIRRE)
        if EXECCM1  == 4 and (CM1 in completos):
            completos.remove(CM1)
        if EXECCM2  == 4 and (CM2 in completos):
            completos.remove(CM2)
        if EXECCPREJ == 4 and (CPREJ in completos):
            completos.remove(CPREJ)
        if EXECCRCI == 4 and (CRCI in completos):
            completos.remove(CRCI)

        if len(completos) > 0:
            logging.debug('A executar: {}'.format(", ".join(map(str, completos))))
            dean = wt.Dean(completos, 'pos')
            dean.ativaCampanhas(completos)
            dean.limpaMailings(completos)
            dean.processaBases()

            mensagem = "Execucao das campanhas {}".format(completos)
            email = eml.Email()
            email.mensagem = mensagem
            email.disparaEmail()
            
            for camp in completos:
                if camp == CIRRE:
                    EXECCIRRE += 1
                if camp == CRCI :
                    EXECCRCI +=1
                if camp == CM1 :
                    EXECCM1 += 1
                if camp == CM2 :
                    EXECCM2 +=1
                if camp == CPREJ :
                    EXECCPREJ +=1
            dean.ativaCampanhas([completos])
    except: 
        logging.error('F18: Erro execucao secundaria - Positivo:{}'.format(sys.exc_info()))

def desliga():
        sys.exit()

def processoPrimario():
    global EXECCIRRE
    global EXECCM1 
    global EXECCM2
    global EXECCPREJ
    global EXECCRCI
    try:
        comp = consultaAtivos()
        dean = wt.Dean(poss+negg, 'pos')
        deanNeg = wt.Dean(poss+negg, 'neg')
        if len(comp) > 0:
            dean.ativaCampanhas(comp)
        dean.limpaMailings(poss+negg)
        dean.processaBases()
        deanNeg.processaBases()
        time.sleep(60)
        dean.ativaCampanhas(poss+negg)
        EXECCIRRE   += 1
        EXECCM1     += 1
        EXECCM2     += 1
        EXECCPREJ   += 1
        EXECCRCI    += 1
        mensagem = "Execucao das campanhas {}".format(poss+negg)
        email = eml.Email()
        email.mensagem = mensagem
        email.disparaEmail()
        logging.debug('F18: Realizado importação primária')
    except :
        logging.error('F18: Erro execucao primaria: {}'.format(sys.exc_info()))

def processoUraNeg():
    try:
        completos = [x for x in consultaCompletos() if x not in poss]
        logging.debug('execucao secundaria: executados: {}'.format(", ".join(map(str, completos))))
        
        if len(completos) > 0:
            logging.debug('A executar: {}'.format(", ".join(map(str, completos))))
            dean = wt.Dean(completos, 'neg')
            dean.ativaCampanhas(completos)
            dean.limpaMailings(completos)
            dean.processaBases()
            dean.ativaCampanhas([completos])

            mensagem = "Execucao das campanhas{}".format(completos)
            email = eml.Email()
            email.mensagem = mensagem
            email.disparaEmail()
            
            
    except: 
        logging.error('F18: Erro execucao secundaria:{}'.format(sys.exc_info()))

processoPrimario()

schedule.every().day.at("08:35").do(processoUraNeg)
schedule.every().day.at("11:40").do(processoUraNeg)
schedule.every().day.at("17:35").do(processoUraNeg)
schedule.every(180).minutes.do(processoSecundario)
schedule.every().day.at("17:50").do(desliga)

while data.hour <= 17:
    refreshData()
    schedule.run_pending()
    time.sleep(30)

