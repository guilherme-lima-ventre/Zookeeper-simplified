import socket
import threading as th
import random as rd
import datetime as dt
import time
from message import Mensagem
 
class Client:

    def __init__(self):
        # INICIALIZA VARIÁVEIS
        self.init_done = False
        self.key_value_pairs = []
        self.timestamp = Mensagem.get_initial_time()

        # VAI ATÉ O MENU DE REQUESTS
        self.set_request()
        
    def set_request(self): 
        # RECEBE O REQUEST E ENCAMINHA PARA FUNCAO RESPONSAVEL POR TRATA LA 
        while True:
            try:
                print('O QUE DESEJA FAZER? [INIT, PUT, GET]')
                request = input().upper()

                if request == 'INIT':
                    if not self.init_done:
                        self.init_request()
                    else:
                        print('REQUEST INVÁLIDO - INIT JA REALIZADO')

                elif request == 'PUT':
                    if self.init_done:
                        self.put_request()
                    else:
                        print('REQUEST INVÁLIDO - INIT NAO REALIZADO')                     

                elif request == 'GET':
                    if self.init_done:
                        self.get_request()
                    else:
                        print('REQUEST INVÁLIDO - INIT NAO REALIZADO')
                
                else:
                    print('REQUEST INVÁLIDO')

            except:
                pass
                # ENCERRA A EXECUCAO DO CLIENTE

    def init_request(self):
        # RECEBE OS TRÊS SERVIDORES
        self.ip1 = input()
        self.port1 = int(input())

        self.ip2 = input()
        self.port2 = int(input())

        self.ip3 = input()
        self.port3 = int(input())

        # CRIA A LISTA DE SERVIDORES
        self.servers_list = [(self.ip1, self.port1), (self.ip2, self.port2), (self.ip3, self.port3)]
        
        self.init_done = True

    def put_request(self):
        # CRIA UM SOCKET PARA SOLICITAR O PUT
        client = socket.socket()

        # RECEBE CHAVE E VALOR DO CLIENTE
        key = input()
        value = input()

        # PROCURA A CHAVE NA LISTA DE CHAVES
        key_found = False

        for pair in self.key_value_pairs:
            if pair['key'] == key:
                key_found = True
                break

        # CRIA A MENSAGEM
        if key_found:
            my_timestamp = pair['timestamp']
            mensagem = Mensagem(
                text='PUT',
                key=key,
                value=value,
                timestamp=my_timestamp
                )
            
        else:
            my_timestamp = self.timestamp
            mensagem = Mensagem(
                text='PUT',
                key=key,
                value=value,
                timestamp=my_timestamp
                )
        
        # ENVIA A SOLICITAÇÃO DE PUT PARA UM SERVIDOR ALEATÓRIO
        servidor = rd.choice(self.servers_list)
        client.connect(servidor)
        client.send(mensagem.codifica())

        resposta = client.recv(1024)
        resposta = Mensagem.decodifica(resposta)

        # RETIRA AS INFORMAÇÕES PARA O PRINT
        key = resposta.get_key()
        value = resposta.get_value()
        timestamp = resposta.get_timestamp()
        servidor = servidor[0] + ':' + str(servidor[1])

        if resposta.get_text() == 'PUT_OK':
            print(f"PUT_OK key: {key} value: {value} timestamp: {timestamp} realizada no servidor: {servidor}")
        else:
            print("FALHA NO PUT")
            return

        # ATUALIZA/ADICIONA A CHAVE NA LISTA DE CHAVES
        self.update_key_value_pairs(key, value)

        # ENCERRA CONEXAO
        client.close()

        return

    def get_request(self):
        # CRIA UM SOCKET PARA SOLICITAR O GET
        client = socket.socket()

        # RECEBE CHAVE A SER PROCURADA
        key = input()

        # PROCURA A CHAVE NA LISTA DE CHAVES
        key_found = False

        for pair in self.key_value_pairs:
            if pair['key'] == key:
                key_found = True
                break

        # CRIA A MENSAGEM E CODIFICA PARA BINARIO
        if key_found:
            my_timestamp = pair['timestamp']
            mensagem = Mensagem(
                text='GET',
                key=pair['key'],
                value=pair['value'],
                timestamp=my_timestamp
                )
            
        else:
            my_timestamp = self.timestamp
            mensagem = Mensagem(
                text='GET',
                key=key,
                value=None,
                timestamp=my_timestamp
                )

        # ENVIA A SOLICITAÇÃO DE GET PARA UM SERVIDOR ALEATÓRIO
        servidor = rd.choice(self.servers_list)
        client.connect(servidor)

        client.send(mensagem.codifica())

        # AGUARDA O GET_OK
        resposta = client.recv(1024)
        resposta = Mensagem.decodifica(resposta)

        # RETIRA AS INFORMAÇÕES PARA O PRINT
        key = resposta.get_key()
        value = resposta.get_value()
        timestamp = resposta.get_timestamp()
        text = resposta.get_text()
        servidor = servidor[0] + ':' + str(servidor[1])

        # CHECA OQ FOI OBTIDO NO GET
        if text not in ('GET_OK', 'GET_NULL'):
            print(text)

        elif text == 'GET_NULL':
            print(f"GET key: {key} value: NULL obtido do servidor {servidor}, meu timestamp {my_timestamp} e do servidor {timestamp}") 
            
        # SE OBTEVE UM VALOR COM A CHAVE, ATUALIZA/ADICIONA A CHAVE NA LISTA DE CHAVES
        else:
            self.update_key_value_pairs(key, value)
            print(f"GET key: {key} value: {value} obtido do servidor {servidor}, meu timestamp {my_timestamp} e do servidor {timestamp}")                    

        # ENCERRA CONEXAO
        client.close()

        return
    
    def update_key_value_pairs(self, key, value):
        time.sleep(1)
        pair_updated = False
        my_timestamp = Mensagem.get_time()

        # CHECA SE O PAR CHAVE VALOR EXISTE E ATUALIZA SE SIM
        for pair in self.key_value_pairs:
            if pair['key'] == key:
                pair['value'] = value
                pair['timestamp'] = my_timestamp
                pair_updated = True
                break
        
        # CASO NAO EXISTA, INSERE O NOVO PAR NA LISTA
        if not pair_updated:
            self.key_value_pairs.append({'key': key, 'value': value, 'timestamp': my_timestamp})

Client()