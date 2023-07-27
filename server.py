import socket
import threading as th
import random as rd
import datetime as dt
from message import Mensagem
import time

class Server:

    def __init__(self):
        # RECEBE O SERVIDOR COM IP E PORTA
        self.ip = input()
        self.port = int(input())

        # RECEBE O SERVIDOR LIDER COM IP E PORTA
        self.lider_ip = input() 
        self.lider_port = int(input())

        # VARIÁVEIS 
        self.lider = (self.ip == self.lider_ip and self.port == self.lider_port)
        self.key_value_pairs = []

        # CRIA SEMAFORO PARA CONTROLE DO PUT
        self.control = th.Semaphore(1)

        # INICIALIZA O SERVIDOR
        self.server = socket.socket()
        self.server.bind(('', self.port))

        # SE LIDER, ESTABELECE CONEXAO COM OUTROS SERVIDORES
        if self.lider:
            # AGUARDA CONEXAO DOS OUTROS 2 SERVIDORES
            self.server.listen(5)

            self.server1, self.server1_addr = self.server.accept()
            self.server2, self.server2_addr = self.server.accept()

            # CRIA CHECAGEM DE REPLICATION
            self.replication_check = [
                {'server':self.server1_addr, 'replication':'NAO'},
                {'server':self.server2_addr, 'replication':'NAO'}
                ]

            # CRIA UMA THREAD PARA RECEBER MENSAGEM DESSES SERIVODRES
            th.Thread(target=self.set_server_recv_request, args=(self.server1, self.server1_addr,)).start()
            th.Thread(target=self.set_server_recv_request, args=(self.server2, self.server2_addr,)).start()

        # SE NAO LIDER, DEVE SE CONECTAR AO LIDER
        else:
            # ESTABELE CONEXAO COM O SERVIDOR LIDER
            self.lider_conn = socket.socket()

            while True:
                try:
                    self.lider_conn.connect((self.lider_ip, self.lider_port))
                    break

                except ConnectionRefusedError:
                    pass

            # CRIA UMA THREAD PARA RECEBER MENSAGEM DO SERVIDOR LIDER
            th.Thread(target=self.set_server_recv_request, args=(self.lider_conn, (self.lider_ip, self.lider_port),)).start()

            # AGUARDA CONEXAO DOS CLIENTES
            self.server.listen(5)      

        # LOOP PARA RECEBER CONEXAO DE CLIENTES
        while True:
            try:
                # AGUARDA CLIENTE SOLICITAR CONEXAO
                client, client_addr = self.server.accept()

                # CRIA UMA THREAD PARA O CLIENTE Q SOLICITOU A CONEXAO
                # E ENVIA PARA O CONTROLE DE REQUESTS
                th.Thread(target=self.set_client_request, args=(client, client_addr)).start() 

            except KeyboardInterrupt:
                # ENCERRA O SERVIDOR
                break

    def set_server_recv_request(self, socket_server, socket_server_addr):
        while True:
            try:
                # AGUARDA MENSAGENS RELACIONADAS A REPLICATION
                mensagem = socket_server.recv(1024)
                mensagem = Mensagem.decodifica(mensagem)

                if mensagem.get_text() == 'REPLICATION':
                    self.replication_request(socket_server, socket_server_addr, mensagem)

                elif mensagem.get_text() == 'REPLICATION_OK':
                    self.check_replications_request(socket_server, socket_server_addr, mensagem)

            except:
                pass


    def set_client_request(self, client, client_addr):
        # RECEBE O REQUEST DO PEER E FAZ O CHAMADO PARA A RESPECTIVA FUNCAO
        mensagem = client.recv(1024)
        mensagem = Mensagem.decodifica(mensagem)

        # ADICIONA NA MENSAGEM A INFORMAÇÃO DO CLIENTE
        if not mensagem.get_addr():
            mensagem.set_addr((client_addr[0], client_addr[1]))

        if mensagem.get_text() == 'PUT':
            self.put_request(client, client_addr, mensagem)

        elif mensagem.get_text() == 'GET':
            self.get_request(client, client_addr, mensagem)

        client.close()

    def put_request(self, client, client_addr, mensagem: Mensagem):
        # SE N FOR LIDER ENCAMINHA
        if not self.lider:
            print(f"Encaminhando PUT key: [{mensagem.get_key()}] value: [{mensagem.get_value()}]")

            # ENCAMINHA SOLICITACAO DE PUT PARA O LIDER
            forward = socket.socket()
            forward.connect((self.lider_ip, self.lider_port))
            forward.send(mensagem.codifica())

            # AGUARDA PUT_OK
            resposta = forward.recv(1024)
            resposta = Mensagem.decodifica(resposta)

            # SE FOR PUT_OK ENVIA PARA O CLIENTE AS INFORMACOES
            if resposta.get_text() == 'PUT_OK':
                client.send(resposta.codifica())
            
            else:
                print("FALHA NO PUT")
                return
            
            # ENCERRA CONEXAO COM SERVIDOR
            forward.close()

        # SE LIDER, FAZ O PUT
        else:
            print(f"Cliente {mensagem.get_addr()[0] + ':' + str(mensagem.get_addr()[1])} PUT key: [{mensagem.get_key()}] value: [{mensagem.get_value()}]")

            # FAZ O PUT NA HASH LOCAL
            self.put(mensagem)

            # SOLICITA O REPLICATION
            mensagem.set_text('REPLICATION')
            mensagem = mensagem.codifica()   

            self.server1.send(mensagem)
            self.server2.send(mensagem)

            # AGUARDA 10 SEGUNDOS PELO REPLICATION OK DOS SERVIDORES
            while self.replication_check[0]['replication'] != 'OK' or self.replication_check[1]['replication'] != 'OK':
                pass

            # REDEFINI O CHECK DE REPLICATION
            self.replication_check[0]['replication'] = 'NAO'
            self.replication_check[1]['replication'] = 'NAO'

            mensagem = Mensagem.decodifica(mensagem)
            mensagem.set_text('PUT_OK')

            print(f"Enviando PUT_OK ao Cliente {mensagem.get_addr()[0] + ':' + str(mensagem.get_addr()[1])} da key: [{mensagem.get_key()}] ts:[{mensagem.get_timestamp()}]")

            # MANDA AO CLIENTE PUT_OK
            client.send(mensagem.codifica())

    def put(self, mensagem: Mensagem):
        # PERMITE APENAS UM PUT POR VEZ
        self.control.acquire()
        time.sleep(1)

        # REDEFINE TIMESTAMP DA INSERCAO DO PAR CHAVE-VALOR
        mensagem.set_timestamp(Mensagem.get_time())

        # OBTEM AS INFORMACOES DO OBJETO
        key = mensagem.get_key()
        value = mensagem.get_value()
        timestamp = mensagem.get_timestamp()

        pair_updated = False

        # CHECA SE O PAR CHAVE VALOR EXISTE E ATUALIZA SE SIM
        for pair in self.key_value_pairs:
            if pair['key'] == key:
                pair['value'] = value
                pair['timestamp'] = timestamp
                pair_updated = True
                break
        
        # CASO NAO EXISTA, INSERE O NOVO PAR NA LISTA
        if not pair_updated:
            self.key_value_pairs.append({'key': key, 'value': value, 'timestamp':timestamp})

        # LIBERA PARA PROXIMO PUT
        self.control.release()

    def get_request(self, client, client_addr, mensagem: Mensagem):
        # OBTEM AS INFORMACOES DO OBJETO
        key_found = False
        key = mensagem.get_key()
        timestamp = mensagem.get_timestamp()

        # PROCURA A CHAVE NA LISTA DE CHAVES
        for pair in self.key_value_pairs:
            if pair['key'] == key:
                key_found = True
                break
        
        # ALTERA A MENSAGEM BASEADO NA PROCURA PELA CHAVE
        # SE ACHOU:
        if key_found:
            # SE A INFORMAÇÃO DO SERVIDOR É MAIS NOVA ATUALIZA AS INFOS DA MENSAGEM
            if pair['timestamp'] >= timestamp:
                mensagem.set_text('GET_OK')
                mensagem.set_value(pair['value']) 
                new_timestamp = pair['timestamp']

            # SE NAO ALTERA PARA MENSAGEM DE ERRO 'TRY_OTHER_SERVER_OR_LATER'
            else:
                mensagem.set_text('TRY_OTHER_SERVER_OR_LATER')
        
        # SE ACHOU NAO RETORNA VALUE NULL
        else:
            mensagem.set_text('GET_NULL')
            mensagem.set_value('NULL')

        if mensagem.get_text() == 'GET_OK':
            print(f"Cliente {mensagem.get_addr()[0] + ':' + str(mensagem.get_addr()[1])} GET [{mensagem.get_key()}] ts:[{timestamp}]. Meu ts é [{new_timestamp}], portanto devolvendo {mensagem.get_value()}")
            mensagem.set_timestamp(new_timestamp)

        else:
            print(f"Cliente {mensagem.get_addr()[0] + ':' + str(mensagem.get_addr()[1])} GET [{mensagem.get_key()}] ts:[{timestamp}]. Meu ts é [{Mensagem.get_time()}], portanto devolvendo {mensagem.get_text()}")
        
        # RETORNA PARA O CLIENTE AS INFORMAÇÕES
        client.send(mensagem.codifica())
    
    def replication_request(self, socket_server, socket_server_addr, mensagem: Mensagem):
        # FAZ O PUT NA HASH LOCAL
        self.put(mensagem)

        print(f"REPLICATION key:{mensagem.get_key()} value:[{mensagem.get_value()}] ts:[{mensagem.get_timestamp()}]")

        # FAZ O REPLICATION
        mensagem.set_text('REPLICATION_OK')

        # ENVIA REPLICATION_OK PARA O SERVIDOR
        socket_server.send(mensagem.codifica())

    def check_replications_request(self, socket_server, socket_server_addr, mensagem):
        # ATUALIZA O STATUS DE REPLICATION DOS SERVIDORES CONECTADOS AO LIDER       
        if self.replication_check[0]['server'] == socket_server_addr:
            self.replication_check[0]['replication'] = 'OK'

        elif self.replication_check[1]['server'] == socket_server_addr:
            self.replication_check[1]['replication'] = 'OK'

Server()