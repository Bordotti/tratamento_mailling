import sys
import logging

try:
    import re
    import csv
    import pyodbc
    import psycopg2
    import requests
    import datetime
    import threading
    import configparser
    from queue import Queue
except:
    logging.critical("Erro de Importação:" + str(sys.exc_info()))


class Dean:
  
    logging.basicConfig(filename='log/winchester2.log', level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    config = configparser.ConfigParser()
    config.read('config.ini')
    tesisdbOld = 'DRIVER={}; SERVER={}; DATABASE={}; UID={}; PWD={}'.format('{SQL SERVER}', config.get('tesis_db_Old', 'ip'),
                                                                         config.get('tesis_db_Old', 'db_name'),
                                                                         config.get('tesis_db_Old', 'db_user'),
                                                                         config.get('tesis_db_Old', 'db_pwd'))

    tesisdb = 'DRIVER={}; SERVER={}; DATABASE={}; UID={}; PWD={}'.format('{SQL SERVER}', config.get('tesis_db', 'ip'),
                                                                         config.get('tesis_db', 'db_name'),
                                                                         config.get('tesis_db', 'db_user'),
                                                                         config.get('tesis_db', 'db_pwd'))
    totalipdb = 'host={} dbname={} user={} password={}'.format(config.get('totalip_db', 'ip'),
                                                               config.get('totalip_db', 'db_name'),
                                                               config.get('totalip_db', 'db_user'),
                                                               config.get('totalip_db', 'db_pwd'))

    CIRRE = 382
    CM1 = 383
    CM2 = 406
    CPREJ = 404
    CRCI = 385
    semtel = []
    camp = [CRCI, CIRRE, CPREJ, CM1, CM2]
    valid_ddd = re.compile('0')
    campanhas = {
        '0004': {
            'pos': 385,
            'neg': 255
        },
        '0001': {
            'Flex': {
                'pos': 382,
                'neg': 384
            },
            'Irreg': {
                'SUDESTE': {
                    'pos': 375,
                    'neg': 401
                },
                'NORDESTE': {
                    'pos': 397,
                    'neg': 398
                }
            },
            'M1': {
                'pos': 383,
                'neg': 410
            },
            'M2': {
                'pos': 406,
                'neg': 391
            },
            'Prej': {
                'pos': 404,
                'neg': 324
            },
        },
        '0027': {
            'pos': 368,
            'neg': 302
        },
        '0023': {
            'pos': 368,
            'neg': 302
        },
        '0026': {
            'pos': 368,
            'neg': 302
        },
        '0008': {
            'pos': 368,
            'neg': 302
        }
    }

    def __init__(self, renew=camp, execType='pos'):

        self.data = datetime.datetime.now()
        self.dados_clientes = self.consultaClientes()
        self.cpfs = [x[3] for x in self.dados_clientes]
        # self.cpfs = [x[3] for x in self.dados_clientes if x[7] in ('0001', '0004')]
        # self.cpfs_seg = [x[3] for x in self.dados_clientes if x[7] in ('0023', '0026', '0025', '0008')]
        # self.telefones = self.consultaTelefones([self.cpfs, self.cpfs_seg], execType) 
        self.telefones = self.consultaTelefones(self.cpfs, execType) 
        self.acionados = self.consultaPositivosDia()
        self.CIRRE = 382
        self.CM1 = 383
        self.CM2 = 406
        self.CPREJ = 404
        self.CRCI = 385
        self.chave = '474df1658554824b7b135bcdc53ebe8c'
        self.semtel = []
        self.q = Queue()
        self.importados = []
        # self.valid_ddd = re.compile('0')
        self.execType = execType
        self.renova = renew
        self.activateCamp = []
        self.clientes = []
        # self.limpaMailings(self.renova)

    def consultaClientes(self):
        query = "select distinct p.cobra, p.cobra+'/'+ltrim(rtrim(p.nom_cliente)), p.nom_cliente, p.id_cpf_cliente, p.atraso, ltrim(rtrim(p.nom_carteira)), case when nom_Carteira like '%PREJU%'  then 'Prej' when p.atraso BETWEEN 13 and 30 then 'Flex' when p.atraso < 90  then 'Irreg' when p.atraso < 181   then 'M1'  when p.atraso  > 180 and nom_Carteira not like '%PREJU%'  then 'M2' else 'Prej2' end as faixa, p.id_grupo, p.contrato, case when rtrim(p.regiao_cobr) in ('SÃO PAULO', 'SUDESTE_RJ_ES', 'NORTE 1', 'SUL', 'MINAS GERAIS', 'CENTRO-OESTE') then 'SUDESTE' ELSE 'NORDESTE' END as regiao from  pendencias p where p.id_situacao = 0 and (p.id_instrucao not in('286','281','282','283','284') and p.dt_registro < getdate()-4)   and id_grupo in ('0001', '0004', '0031') and  p.NOM_Carteira not like ('%MAND%') and  atraso > 12"
        try:
            with pyodbc.connect(self.tesisdb) as con:
                cur = con.cursor()
                cur.execute(query)
                self.clientes = cur.fetchall()
        except:
            logging.critical('Falha consultaClientes: consultar clientes' + str(sys.exc_info()))
            print('falha na consulta de cliente', sys.exc_info())
            raise sys.exc_info()
        try:
            self.clientes = [list(cli) for cli in self.clientes]
            return self.clientes
        except:
            logging.critical('Falha consulta Clientes: cliente para lista' + str(sys.exc_info()))
            #print(sys.exc_info())
            return 0

    def consultaTelefones(self, cpfs, execType, seg=False):
        #for conCpf in cpfs:
        if execType == 'pos':
            query = "select id_cpf_cliente, ltrim(rtrim(num_ddd)), ltrim(rtrim(num_fone)), qtd_contato_pos from telefones where id_status in ('01','11') and id_cpf_cliente in ('{}');".format(
                "','".join(map(str, cpfs)))
        else:
            query = "select id_cpf_cliente, ltrim(rtrim(num_ddd)), ltrim(rtrim(num_fone)), qtd_contato_pos, (qtd_contato_pos-(qtd_contato_tra*0.3+qtd_contato_neg*0.7)) from telefones where id_status in ('01','11') and id_cpf_cliente in ('{}');".format(
                "','".join(map(str, cpfs)))
        try:
            with pyodbc.connect(self.tesisdb) as con:
                cur = con.cursor()
                cur.execute(query)
                telefones = cur.fetchall()
                aux = {x[0]: [] for x in telefones}
                logging.info('len telefones: ' + str(len(telefones)))
                if execType == 'pos':
                    # if i == 1:
                    #     telefones = {aux[x[0]].append([x[1], x[2], x[3]]) for x in telefones if
                    #                 self.validaTelefone(x[1], x[2])}
                    # else:
                    telefones = {aux[x[0]].append([x[1], x[2], x[3]]) for x in telefones if (self.validaTelefone(x[1], x[2]) and x[3] > 0)}
                else:
                    telefones = {aux[x[0]].append([x[1], x[2], x[4]]) for x in telefones if (self.validaTelefone(x[1], x[2]) and x[3] == 0)}
        except:
            logging.critical('Erro consulta telefones: {}'.format(sys.exc_info()))
            return 0
        try:
            telefones = aux.copy()
            for i in aux.keys():
                if len(aux[i]) == 0:
                    telefones.pop(i)
        except:
            logging.error('Consulta telefone, retira vazio:' + str(sys.exc_info()))
        try:
            for t in telefones.keys():
                telefones[t].sort(key=lambda v: v[2], reverse=True)
        except:
            logging.error('Consulta telefone Dean, ordena vetor:' + str(sys.exc_info()))
        return telefones
            

    def consultaPositivosDia(self):
        query = "SELECT DISTINCT contrato FROM [192.168.2.3].tesis.dbo.todos_os_acionamentos where situacao_do_acionamento like 'positivo' and id_grupo in ('0004', '0001', '0023', '0026', '0025', '0008') and dt_evento >= getdate()-1 and id_situacao = 0"
        try:
            with pyodbc.connect(self.tesisdbOld) as con:
                cur = con.cursor()
                cur.execute(query)
                contratos = cur.fetchall()
                contratos = [cli[0] for cli in contratos]
            return contratos
        except:
            logging.critical('Falha ao consultar contratos' + str(sys.exc_info()))
            return

    def validaTelefone(self, ddd, fone):
        try:
            fone = str(fone)
            try:
                if not self.valid_ddd.search(str(ddd)) is None:
                    return False
            except:
                logging.error('Validacao telefone, ddd invalido:' + str(sys.exc_info()))
            try:
                if len(fone) < 8:
                    return False
            except:
                logging.error('Validacao telefone, fone invalido:' + str(sys.exc_info()))
            try:
                if len(fone) == 8 and int(fone[0]) > 5:
                    return False
            except:
                logging.error('Validacao telefone, fone errado:' + str(sys.exc_info()))
            try:
                if len(fone) == 9 and int(fone[0]) < 7:
                    return False
            except:
                logging.error('Validacao telefone, fone errado:' + str(sys.exc_info()))
            try:
                if len(fone) > 9:
                    return False
            except:
                logging.error('Validacao telefone, fone invalido:' + str(sys.exc_info()))

            return True
        except:
            logging.error('Validacao telefone:' + str(sys.exc_info()))
            return False

    def separaBases(self, cli, renova, execType):
        tel = 0
        tel2 = -1
        try:
            if cli[0] in self.importados:
                return 0
            else:
                self.importados.append(cli[0])

            if execType == 'pos':
                if cli[8] in self.acionados:
                    return 0
                if len(self.telefones[cli[3]]) == self.data.isoweekday():
                    tel2 = self.data.isoweekday() - 1
                elif len(self.telefones[cli[3]]) < self.data.isoweekday():
                    tel2 = len(self.telefones[cli[3]]) - 1
                try:
                    k = self.importTel(cli, tel, tel2, renova, execType)
                    return k
                except:
                    #print(sys.exc_info())
                    return -1
            else:
                try:
                    k = self.importTel(cli, -1, tel2, renova, execType)
                    return k
                except:
                    #print(sys.exc_info())
                    return -1
        except:
            #print(sys.exc_info())
            logging.debug('contrato sem tel: {}. erro: {}'.format(cli[3], sys.exc_info()))
            self.semtel.append(cli[3])

    def importTel(self, cli, tel, tel2, renova, execType):
        try:
            if cli[7] == '0004':
                if self.campanhas[cli[7]][execType] in self.renova:
                    if tel > -1:
                        self.importaClientes(self.campanhas[cli[7]][execType], cli[0], cli[1],
                                             self.telefones[cli[3]][tel][0], self.telefones[cli[3]][tel][1],
                                             self.telefones[cli[3]][tel2][0], self.telefones[cli[3]][tel2][1])
                    else:
                        tels = []
                        [tels.append(tel[num]) for tel in self.telefones[cli[3]] for num in (0, 1)]
                        self.importaClientes2(self.campanhas[cli[7]][execType], cli[0], cli[1],
                                              self.telefones[cli[3]][0][0], self.telefones[cli[3]][0][1], tels)
                    return self.campanhas[cli[7]][execType]
                else:
                    return 0
            elif cli[7] == '0001':
                if cli[6] != 'Irreg':
                    if self.campanhas[cli[7]][cli[6]][execType] in self.renova:
                        if tel > -1:
                            self.importaClientes(self.campanhas[cli[7]][cli[6]][execType], cli[0], cli[1],
                                                 self.telefones[cli[3]][tel][0], self.telefones[cli[3]][tel][1],
                                                 self.telefones[cli[3]][tel2][0], self.telefones[cli[3]][tel2][1])
                        else:
                            tels = []
                            [tels.append(tel[num]) for tel in self.telefones[cli[3]] for num in (0, 1)]
                            self.importaClientes2(self.campanhas[cli[7]][cli[6]][execType], cli[0], cli[1],
                                                  self.telefones[cli[3]][0][0], self.telefones[cli[3]][0][1], tels)
                        return self.campanhas[cli[7]][cli[6]][execType]
                    else:
                        return 0
                else:
                    if self.campanhas[cli[7]][cli[6]][cli[9]][execType] in self.renova:
                        if tel > -1:
                            self.importaClientes(self.campanhas[cli[7]][cli[6]][cli[9]][execType], cli[0], cli[1],
                                                 self.telefones[cli[3]][tel][0], self.telefones[cli[3]][tel][1],
                                                 self.telefones[cli[3]][tel2][0], self.telefones[cli[3]][tel2][1])
                            return self.campanhas[cli[7]][cli[6]][cli[9]][execType]
                        else:
                            tels = []
                            [tels.append(tel[num]) for tel in self.telefones[cli[3]] for num in (0, 1)]
                            self.importaClientes2(self.campanhas[cli[7]][cli[6]][cli[9]][execType], cli[0], cli[1],
                                                  self.telefones[cli[3]][0][0], self.telefones[cli[3]][0][1], tels)
                        return self.campanhas[cli[7]][cli[6]][cli[9]][execType]
                    else:
                        return 0
            elif cli[7] in ('0023', '0026', '0025', '0008'):
                if self.campanhas[cli[7]][execType] in self.renova:
                    if tel > -1:
                        self.importaClientes(self.campanhas[cli[7]][execType], cli[0], cli[1],
                                             self.telefones[cli[3]][tel][0], self.telefones[cli[3]][tel][1],
                                             self.telefones[cli[3]][tel2][0], self.telefones[cli[3]][tel2][1])
                        return self.campanhas[cli[7]][execType]
                    else:
                        tels = []
                        [tels.append(tel[num]) for tel in self.telefones[cli[3]] for num in (0, 1)]
                        self.importaClientes2(self.campanhas[cli[7]][execType], cli[0], cli[1],
                                              self.telefones[cli[3]][0][0], self.telefones[cli[3]][0][1], tels)
                        return self.campanhas[cli[7]][execType]
                else:
                    return 0
            else:
                return 0
        except TypeError as err:
            print('import tel error', err)
            raise "error"
        except:
            #print(cli, execType, sys.exc_info())
            logging.error('Erro Importacao: {}'.format(sys.exc_info()))

    def importaClientes(self, id_campanha, varg1, varg2, ddd1, tel1, ddd2='Null', tel2='Null'):
        try:
            api = "http://192.168.2.18/api/importar_discador?id_campanha={}&identificador1={}&identificador2={}&ddd1={}&telefone1={}&ddd2={}&telefone2={}&chave={}"
            rest = requests.post(
                api.format(id_campanha, varg1, varg2.replace(' ', '_'), ddd1, tel1, ddd2, tel2, self.chave))
            logging.debug(str(rest.text) + api.format(id_campanha, varg1, varg2, ddd1, tel1, ddd2, tel2, self.chave))
        except:
            logging.critical('Erro ao importar: {}'.format(sys.exc_info()))

    def importaClientes2(self, id_campanha, varg1, varg2, ddd1, tel1, args):
        tp = {1: 'telefone', 2: 'ddd'}
        try:
            api = "http://192.168.2.18/api/importar_discador?id_campanha={}&identificador1={}&identificador2={}&ddd1={}&telefone1={}".format(
                id_campanha, varg1, varg2, ddd1, tel1)
            if args is not None:
                contf = 2
                contv = 2
                varr = 0
                for value in args:
                    api = api + '&{}{}={}'.format(tp[contf], contv, value)

                    if contf == 2:
                        contf -= 1
                    else:
                        contf += 1

                    if (varr % 2) > 0:
                        contv += 1
                        if varr == 25:
                            break
                    varr += 1

            api = api + '&chave={}'.format(self.chave)
            rest = requests.post(api.format(id_campanha, varg1, varg2.replace(' ', '_'), ddd1, tel1, self.chave))

            logging.debug(str(rest.text) + api.format(id_campanha, varg1, varg2, ddd1, tel1, self.chave))
        except:
            logging.critical('Erro ao importar: {}'.format(sys.exc_info()))

    def ativaCampanha(self, id_campanha):
        try:
            api = "http://192.168.2.18/api/gerenciar_campanha?campanha_id={}&chave={}"
            rest = requests.post(api.format(id_campanha, self.chave))

            logging.debug(str(rest.text) + api.format(id_campanha, self.chave))
        except:
            logging.error('Erro ao ativar campanha: {}'.format(sys.exc_info()))

    def limpaMailings(self, id_campanhas):
        res = []
        for id in id_campanhas:
            res.append([id, (requests.post("http://192.168.2.18/api/excluir_telefone_campanha?id_campanha={}&chave={}".format(id, self.chave))).text])
        logging.info(res)

    def threader(self):
        while True:
            worker1 = self.q.get()
            self.activateCamp.append(self.separaBases(worker1, self.renova, self.execType))
            self.q.task_done()

    def processaBases(self):
        for cliente in self.dados_clientes:
            self.q.put(cliente)

        for _ in range(20):
            t = threading.Thread(target=self.threader)
            t.daemon = True
            t.start()
        self.q.join()
        # self.ativaCampanhas(self.campanhas)

    def ativaCampanhas(self, campanhas):
        done = []
        for c in campanhas:
            if c not in done:
                done.append(c)
                # if c in self.camp:
                self.ativaCampanha(c)

    def salvaSemTel(self):
        with open('sem_tel' + str(self.data.isoformat()[:13].replace('T', '-')) + 'h.csv', 'a', newline='') as f:
            b = csv.writer(f, delimiter=';')
            for tel in self.semtel:
                b.writerow(tel)

    # logging.info('tempo de execução: ' + str(datetime.datetime.now()-data))

'''
campanhas - POS
382 - irregular
383 - M1
406 - M2
404 - Preju
385 - RCI

campanhas - NEG
384 - irregular
410 - M1
391 - M2
324 - Preju
255 - RCI
'''