import json
import datetime as dt

class Mensagem: 

    def __init__(self, text=None, key=None, value=None, timestamp=None, addr=None):
        self.message = {
            'text': text,
            'key': key,
            'value': value,
            'timestamp': timestamp,
            'addr': addr
        }
    
    def get_text(self):
        return self.message['text']
    
    def get_key(self):
        return self.message['key']
    
    def get_value(self):
        return self.message['value']
    
    def get_timestamp(self):
        return self.message['timestamp']
    
    def get_addr(self):
        return self.message['addr']
    
    def set_text(self, text):
        self.message['text'] = text
    
    def set_key(self, key):
        self.message['key'] = key
    
    def set_value(self, value):
        self.message['value'] = value
    
    def set_timestamp(self, timestamp):
        self.message['timestamp'] = timestamp
    
    def set_addr(self, addr):
        self.message['addr'] = addr
    
    def codifica(self):
        # RECEBE UMA MENSAGEM CRIADA, TRANSFORMA EM DICIONARIO, SERIALIZA EM JSON E CONVERTE PARA BINÁRIO
        return json.dumps(self.message).encode()

    @staticmethod 
    def decodifica(mensagem):
        # RECEBE UMA MENSAGEM EM BINÁRIO, CONVERTE DE BINÁRIO, DESSERIALIZA DO JSON E RETORNA UM OBJETO MENSAGEM
        mensagem = json.loads(mensagem)
        mensagem = Mensagem(
            text = mensagem['text'] if mensagem['text'] else None,
            key = mensagem['key'] if mensagem['key'] else None,
            value = mensagem['value'] if mensagem['value'] else None,
            timestamp = mensagem['timestamp'] if mensagem['timestamp'] else None,
            addr = mensagem['addr'] if mensagem['addr'] else None)

        return mensagem
    
    @staticmethod
    def show_message(mensagem):
        text = mensagem.get_text() if mensagem.get_text() else None
        key = mensagem.get_key() if mensagem.get_key() else None
        value = mensagem.get_value() if mensagem.get_value() else None
        timestamp = mensagem.get_timestamp() if mensagem.get_timestamp() else None
        addr = mensagem.get_addr() if mensagem.get_addr() else None

        print(f'TEXT: {text} ||| KEY: {key} ||| VALUE: {value} ||| TS: {timestamp} ||| ADDR: {addr}')

    @staticmethod
    def get_time():
        return dt.datetime.now().strftime("%d-%m-%y %H:%M:%S")
    
    @staticmethod
    def get_initial_time():
        # Minha data de nascimento
        return (dt.datetime(year=2000, month=3, day=21, hour=11, minute=30, second=0)).strftime("%d-%m-%y %H:%M:%S")