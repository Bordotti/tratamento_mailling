import csv
import smtplib
import threading
from queue import Queue
import requests
import pyodbc as sqlserver
from base64 import b64encode
import datetime
import time
import os
import logging
import sys
import schedule


t1 = datetime.datetime.now()
print('Hora da execução:', t1)
logging.basicConfig(filename='info.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')


def consulta_base():
    with sqlserver.connect("DRIVER={SQL Server}; SERVER=192.168.2.3; DATABASE=tesis; UID=tscobra; PWD=crespo") as con:
           # "DRIVER={SQL Server}; SERVER=192.168.3.21; DATABASE=tesis; UID=guilherme; PWD=@8c15d8c8G") as con:
        cur = con.cursor()
        query = "select distinct p.contrato, p.id_cpf_cliente, case when p.id_grupo = '0001' then 'LEVES' else 'RCI' end, case when P.atraso > 15 and P.atraso < 30 then 'Flex' when  P.atraso < 90 then 'IRREGULAR' when  P.atraso < 180 then 'MOROSO 01'  when P.atraso < 360   then 'MOROSO 02' else 'PREJUIZO'     end as faixa, case when p.atraso between 15 and 180 then case when p.id_instrucao in( 447, 432, 431)  then 'EAQ' else  case  when p.id_instrucao in (437,436) and p.id_grupo = '0004'  then 'VENDA VALORIZADA'  else  case  when p.id_instrucao in (402, 435) and p.id_grupo = '0001'   then 'RENEG'  else   case   when p.id_instrucao in(433, 444)    then 'EAS'   else   case   when ac.dsc_acordo in ('OVER 180', 'PREJUÍZO')    then 'OVER'   else    'ALÇADA'   end   end  end  end end when p.atraso > 180 then case when ac.dsc_acordo in ('OVER 180', 'PREJUÍZO')  then 'OVER' else  case  when p.id_instrucao in (437,436) and p.id_grupo = '0004'  then 'VENDA VALORIZADA'  else  case  when p.id_instrucao in(447, 432, 431)   then 'EAQ'  else   case   when p.id_instrucao in(433, 444)   then 'EAS'   else   case   when p.id_instrucao in (402, 435) and p.id_grupo = '0001'    then 'RENEG'   else    case    when ac.dsc_acordo in ('PROIBIDO ALÇADA')    then 'ALÇADA'    else    'PROPOSTA'    end   end   end  end  end end else 'verificar' end from acordos_permitidos ac inner join tesis.dbo.pendencias p on p.cobra = ac.cobra inner join tesis.dbo.telefones t on t.id_cpf_cliente = p.id_cpf_Cliente where p.id_grupo in ('0001', '0004')  and p.id_situacao = 0  and t.id_status in('01','11')  and len(ltrim(rtrim(num_ddd))+num_fone) = 11  and p.nom_Carteira not in ('MANDADO','MANDADO LOCALIZADO','PRÉ MANDADO')  and (p.atraso between 12 and 30 or p.atraso > 90) order by contrato"
        cur.execute(query)
        global base
        base = cur.fetchall()
    bbase = {b[0]: b[1:] for b in base}


def filtraBase(cont):
    idx = []
    maior = ''
    for index in range(len(base)):# TROCAR A BAGAÇA POR DICIONARIO
        if base[index][0] == cont:
            idx.append(index)
            if base[index][3] == 'IRREGULAR' or base[index][3] == 'MOROSO 01':

                if base[index][4] == 'ALÇADA':
                    maior = 'ALÇADA'
                elif base[index][4] == 'OVER':
                    maior = 'OVER'
                elif base[index][4] == 'EAS':
                    maior = 'EAS'
                elif base[index][4] == 'VENDA VALORIZADA':
                    maior = 'VENDA VALORIZADA'
                elif base[index][4] == 'RENEG':
                    maior = 'RENEG'
                elif base[index][4] == 'EAQ':
                    maior = 'EAQ'
                else:
                    maior = 'VERIFICAR'

            else:
                if base[index][3] == 'MOROSO 02' or base[index][3] == 'PREJUIZO':
                    if base[index][4] == 'ALÇADA':
                        maior = 'ALÇADA'
                    elif base[index][4] == 'PROPOSTA':
                        maior = 'PROPOSTA'
                    elif base[index][4] == 'EAS':
                        maior = 'EAS'
                    elif base[index][4] == 'RENEG':
                        maior = 'RENEG'
                    elif base[index][4] == 'EAQ':
                        maior = 'EAQ'
                    elif base[index][4] == 'VENDA VALORIZADA':
                        maior = 'VENDA VALORIZADA'
                    elif base[index][4] == 'OVER':
                        maior = 'OVER'
                    else:
                        maior = 'VERIFICAR'

    for i in idx:
        base[i][4] = maior


'''
def montaVetorMensagens(tel):
    for cliente in bFinal:
        if cliente[5] == int(t1.isoweekday()):
            if tel[0] == str(cliente[0]):
                dicMsg = {"to": tel[1],
                          "message": smsDict[cliente[4].rstrip()].format(numRecp[cliente[2]]),
                          "schedule": "{}-{}-{}T08:00:00".format(t1.year, t1.month, t1.day),
                          "reference": str(t1) + ' {0} {1}'.format(cliente[2], cliente[4].rstrip()),
                          "account": contRef[cliente[2]]
                          }
                with save_lock:
                    global contador
                    if len(messageArray) == 0:
                        messageArray["p" + str(contador)] = {"messages": []}
                    else:
                        if len(messageArray["p" + str(contador)]["messages"]) > 4000:
                            contador += 1
                            messageArray["p" + str(contador)] = {"messages": []}
                        messageArray["p" + str(contador)]["messages"].append(dicMsg)
                    msgAvulso.append(dicMsg)
'''


def montaVetorDic(cliente):  # g, f, c, cli
    try:
        if cliente[2] != 'IRREGULAR':
            if cliente[3] not in ctt_processados:
                ctt_processados.append(cliente[3])
                if len(cliente[0]) > 0 and len(cliente[2]) > 0:
                    save.append([cliente[3], cliente[0], cliente[1], cliente[2], t1.isoweekday(), smsDict[cliente[2]].format(numRecp[cliente[0]])])
                    for tel in tels[str(cliente[3])]:
                        dicMsg = {"to": tel,
                                "message": smsDict[cliente[2]].format(numRecp[cliente[0]]),
                                "schedule": "{}-{}-{}T08:00:00".format(t1.year, t1.month, t1.day),
                                "reference": str(t1) + ' {0} {1}'.format(cliente[0], cliente[2]),
                                "account": contRef[cliente[0]]
                                }
                        with save_lock:
                            global contador
                            if len(messageArray) == 0:
                                messageArray["p" + str(contador)] = {"messages": []}
                            else:
                                if len(messageArray["p" + str(contador)]["messages"]) > 4000:
                                    contador += 1
                                    messageArray["p" + str(contador)] = {"messages": []}
                                messageArray["p" + str(contador)]["messages"].append(dicMsg)
                            msgAvulso.append(dicMsg)
            else:
                pass
    except:
        print('erro vetor {}'.format(cliente))

def reports(ids_envio):
    task = {"ids": ids_envio}
    if 'ids' + str(t1.year) + str(t1.month) + str(t1.day) + '.txt' in os.listdir():
        with open('ids' + str(t1.year) + str(t1.month) + str(t1.day) + '.txt', 'r') as f:
            bid = list(csv.reader(f, delimiter=','))
            k = [task['ids'].append(idx) for idx in bid[0] if len(idx) > 5]
    url_base = "https://sms-api-pointer.pontaltech.com.br/v1/multiple-sms-report"
    try:
        report = requests.request("POST", url_base, json=task, headers=headers)
        if '<Response [200]>' == str(report):

            for i in report.json()['reports']:
                stt.append(i['status'])
                sttDsc.append(i['statusDescription'])
                rep.append([i['to'], i['status']])

            preProcess = {i: stt.count(i) for i in stt}
            mensagem = 'Status de Disparo \n'
            for status in preProcess:
                mensagem = mensagem +"{0} : {1:.2f}\n".format(codResp[status], preProcess[status] / len(stt))
            disparaEmail(mensagem)
            print("Disparado E-mail:{}".format(t1.hour))
        else:
            print('report', report)
            print(report.json())
    except:
        logging.error('Erro da Api:' + str(sys.exc_info()[0]))


def disparaSMS(key):
    task = messageArray[key]
    url_base = "https://sms-api-pointer.pontaltech.com.br/v1/multiple-sms"
    global resp
    resp = ''
    try:
        resp = requests.request("POST", url_base, json=task, headers=headers)
        print('envio', resp)
        if '<Response [200]>' == str(resp):
            for i in resp.json()['messages']:
                global ids
                if len(str(i['id'])) > 0:
                    ids.append(i['id'])
            salvaEnvios()
        else:
            print("{}-{}-{}T08:00:00".format(t1.year, t1.month, t1.day))
            print(resp.json())
    except:
        print(sys.exc_info())

def disparaEmail(tt):  # ENVIAR EMAIL COM RESULTADO
    # Credenciais
    remetente = 'telefonia@crespoecaires.com.br'
    senha = 'cres2014'

    # Informacoes da mensagem
    destinatario = ['telefonia@crespoecaires.com.br','rodrigo.davila@crespoecaires.com.br', 'controldesk@crespoecaires.com.br'] # 'rodrigo.davila@crespoecaires.com.br', 'controldesk@crespoecaires.com.br'
    assunto = 'Disparo de SMS em lote'

    # Preparando a mensagem
    msg = '\r\n'.join([
        'From: {0}'.format(remetente),
        'To: {0}'.format(destinatario),
        'Subject: {0}'.format(assunto),
        '',
        '{0}'.format(tt)
    ])

    # Enviando o email
    server = smtplib.SMTP('smtp.crespoecaires.com.br:587')
    server.starttls()
    server.login(remetente, senha)
    server.sendmail(remetente, destinatario, msg.encode('utf8'))
    server.quit()


def consultaTelefones():
    with sqlserver.connect(
            "DRIVER={SQL SERVER}; SERVER=192.168.2.22; DATABASE=tesis; UID=consulta; PWD=crespo123*") as con:
        query = "select distinct c.contrato,  ltrim(rtrim(num_ddd))+num_fone as tel from telefones t inner join contratos c on t.id_cpf_cliente = c.id_cpf_cliente where c.id_situacao = 0  and  id_status in ('01', '11') and len(ltrim(rtrim(num_ddd))+num_fone) = 11 and  c.contrato in('%s')" % "', '".join(
            map(str, ctt))
        cur = con.cursor()
        cur.execute(query)
        bTel = cur.fetchall()
    bTel.sort()
    bTel.append((123, 19983577077))
    bTel.append((331, 1997651072))
    for t in bTel:
        if str(t[1]).isnumeric():
            if t[0] not in tels.keys():
                tels[t[0]] = [int(t[1])]
            else:
                tels[t[0]].append(int(t[1]))


def threader():
    while True:
        worker1 = q.get()
        montaVetorDic(worker1)
        q.task_done()


def jobAtualizaData():
    global data
    data = datetime.datetime.now()


def salvaEnvios():

    try:
        with sqlserver.connect(
                "DRIVER={SQL SERVER}; SERVER=192.168.2.22; DATABASE=Telecom_data; UID=consulta; PWD=crespo123*") as con:
            cur = con.cursor()
            for cliente in save: # cliente : contrato, grupo, faixa, campanha, dia(semana), mensagem
                query = (
                    "insert into envio_sms(contrato, grupo, faixa, campanha, dia, mensagem, dataDisparo) values ( {0}, '{1}', '{2}', '{3}', {4}, '{5}', '{6}')".format(
                        cliente[0], cliente[1], cliente[2], cliente[3], cliente[4], cliente[5], '{0} / {1:02}/ {2:02}'.format(t1.year, t1.month, t1.day)))
                cur.execute(query)
            con.commit()
    except sqlserver.Error as err:
        print(err)
    except sqlserver.DataError as err2:
        print("erro de Dados")
        print(err2)


def consultaEnviados():
    with sqlserver.connect(
            "DRIVER={SQL SERVER}; SERVER=192.168.2.22; DATABASE=Telecom_data; UID=consulta; PWD=crespo123*") as con:
        cur = con.cursor()
        global cttEnviados
        if t1.isoweekday() > 1 and t1.day <= t1.isoweekday():
            query = "select distinct contrato from envio_sms where dataDisparo < '190{}{}' ".format(t1.month - 1, 30)
        else:
            query = "select distinct contrato from envio_sms where dataDisparo < '190{}{}' ".format(t1.month,
                                                                                                    t1.day - t1.isoweekday())
        cur.execute(query)
        cttEnviados = cur.fetchall()


token = b64encode(b'crespoecairesplus:3573417d').decode("ascii")
headers = {
    "Content-Type": "application/json",
    "cache-control": "no-cache",
    "Authorization": "Basic {}".format(token)
}

numRecp = {"LEVES": "08009429052", "RCI": "08009423248"}
contRef = {"LEVES": 1740, "RCI": 1739}

smsDict = {
    "PROPOSTA": "Mega evento Crespo e Caires! Ligue agora {} ou Whats 11960375456 e faca uma proposta para QUITAR seu contrato. Caso pago desconsiderar.",
    "EAS": "Dificuldades em manter seu financiamento? Ligue agora {} ou Whats 11960375456 e saiba mais sobre a ENTREGA amigavel. Caso pago desconsiderar.",
    "OVER": "Voce foi contemplado com a campanha de desconto para quitar o seu veiculo, entre em contato no {} e conheca as vantagens. Caso pago desconsiderar.",
    "ALÇADA": "Ainda da tempo de pagar com desconto! Ligue {} hoje e fale com nossos negociadores para verificar a possibilidade. Caso pago desconsiderar.",
    "VENDA VALORIZADA": "Tem interesse em vender seu carro? Nos podemos te ajudar! Ligue {} e saiba mais ou WhatsApp 11960375456. Caso pago desconsiderar.",
    "RENEG": "Quer regularizar suas parcelas? Faca isso com o refinanciamento de seu contrato, ligue {} ou WhatsApp 11960375456. Caso pago desconsiderar.",
    "ATUALIZAÇÃO": "Ainda da tempo de pagar com desconto! Ligue {} hoje e fale com nossos negociadores para verificar a possibilidade. Caso pago desconsiderar.",
    "EAQ": "Dificuldades em pagar as parcelas do seu carro? Temos a solucao, ligue {} e saiba mais sobre a ENTREGA quitativa. Caso pago desconsiderar."}  # Monta dicionario de mensagens

smsDictSeg = {} # IMPLANTAR MENSAGENS

codResp = {
    0: 'Mensagem aceita. Aguardando agendamento.',
    1: 'Mensagem Agendada.',
    2: 'Mensagem enviada para operadora.',
    3: 'Confirmação de envio por parte da operadora.',
    4: 'Erro no envio por parte da operadora.',
    5: 'Confirmação de entrega por parte da operadora.',
    6: 'Erro na entrega por parte da operadora.',
    7: 'Mensagem bloqueada.',
    8: 'Mensagem invalida.',
    9: 'Mensagem cancelada.',
    10: 'Erro.',
    11: 'Sem crédito.',
    12: 'Mensagem não entregue.',
    13: 'Expirado',
    14: 'Mensagem bloqueada na base de inválidos.'}  # Monta dicionario de respostas api
data = 0
messageArray = {}
contador = 0
bTel = []
tels = {}
base = []
msgAvulso = []
ids = []
cont = []
bFinal = []
q = Queue()
dicF = {}
dicFinal = {}
divDia = {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
save_lock = threading.Lock()
dados_email = []
stt = []
sttDsc = []
resp = []
rep = []
save = []
cttEnviados = []
ctt_processados = []

consulta_base()
ctt = [c[0] for c in base]
consultaTelefones()
jobAtualizaData()

# DEIXAR JUNTO PARA BAIXO \/
for i in range(len(ctt)):
    filtraBase(ctt[i])

for i in base:
    if i[0] not in base:
        bFinal.append(i)
        cont.append(i[0])
        if i[2] not in dicF.keys():
            dicF[i[2]] = {}
            dicFinal[i[2]] = {}
        if i[3] not in dicF[i[2]].keys():
            dicF[i[2]][i[3]] = {}
            dicFinal[i[2]][i[3]] = {}
        if i[4] not in dicF[i[2]][i[3]].keys():
            dicF[i[2]][i[3]][i[4]] = []
            dicFinal[i[2]][i[3]][i[4]] = {}

for c in bFinal:
    if c[0] not in cttEnviados:
        dicF[c[2]][c[3]][c[4]].append(c[0])

for g in dicF.keys():
    for f in dicF[g].keys():
        for c in dicF[g][f].keys():
            num = int(len(dicF[g][f][c]) / divDia[t1.isoweekday()] + 4)
            print(num)
            dicFinal[g][f][c] = {}
            count = 0
            for i in range(t1.isoweekday(), 6, 1):  # MUDAR O DIA FINAL PARA A QUANTIDADE DE DIAS NA SEMANA
                dicFinal[g][f][c][i] = []
            for j in dicF[g][f][c]:
                count = count + 1
                dicFinal[g][f][c][int(count / num) + t1.isoweekday()].append(j)
# DEIXAR JUNTO PARA CIMA /\


for g in dicFinal.keys():
    for f in dicFinal[g].keys():
        for c in dicFinal[g][f].keys():
            for cli in dicFinal[g][f][c][t1.isoweekday()]:
                q.put([g, f, c, cli])

for x in range(200):
    t = threading.Thread(target=threader)
    t.daemon = True
    t.start()
q.join()

mTotal = 0
for i in messageArray.keys():
    mTotal += len(messageArray[i]["messages"])
    print(i)
    with open('teste.txt', 'a') as file:
        base = csv.writer(file)
        base.writerow(messageArray[i])
    disparaSMS(i)


texto = "Disparo de SMS automatizado\n\nInformações de disparo do dia {0}/{1}/{2}\n\n".format(t1.day, t1.month, t1.year)

for i in dicFinal.keys():
    for j in dicFinal[i].keys():
        for k in dicFinal[i][j].keys():
            texto = texto+"contratos {:04d}, empresa {}, faixa {}, campanha {}.\n".format(len(dicFinal[i][j][k][t1.isoweekday()]), i, j, k)

disparaEmail(texto)

with open('./ids' + str(t1.year) + str(t1.month) + str(t1.day) + '.txt', 'a') as ff:
    for id in ids:
        ff.write(str(id) + ',')

print('Tempo total {}'.format(datetime.datetime.now() - t1))
contt = 0
reports(ids)
schedule.every().hour.do(reports, ids)
jobAtualizaData()
while data.hour < 16:
    reports(ids)
    jobAtualizaData()
    schedule.run_pending()
    time.sleep(60)

sys.exit(0)
'''
DIA 1
FILTRA E SEPARA
SALVA ENVIADOS

DIA 2...
RETIRA ENVIADOS DA SEMANA
FILTRA
SALVA ENVIADOS.

 for i in dicFinal.keys():
       for j in dicFinal[i].keys():
           for k in dicFinal[i][j].keys():
               mostra.append('contratos {}, empresa {}, faixa {}. campanha {}.\n'.format(len(dicFinal[i][j][k][data.isoweekday()]), i , j , k))
    
    
for g in dicF.keys():
    for f in dicF[g].keys():
        for c in dicF[g][f].keys():
            num = len(dicF[g][f][c])/divDia[data.isoweekday()]+4
            print(num)
            dicFinal[g][f][c] = {}
            count = 0
            for i in range(data.isoweekday(), 6, 1):
                dicFinal[g][f][c][i] = []
            for i in dicF[g][f][c]:
                count = count+1
                dicFinal[g][f][c][int(count/num)+data.isoweekday()].append(i)
'''



