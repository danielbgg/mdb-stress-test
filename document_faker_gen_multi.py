from pymongo import MongoClient
from faker import Faker
import random
from bson import ObjectId
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

from config_database import var_mongo_uri, var_db, var_collection

# Initialize Faker
fake = Faker()

# Function to generate ISODate
def iso_date_generator():
    return datetime.now() - timedelta(days=random.randint(1, 365))

def create_document():
    return {
        "_id": ObjectId(),
        "Attempts": random.randint(0, 5),
        "Conta": fake.bothify(text='###########'),
        "DataPosicao": iso_date_generator(),
        "QueueId": str(ObjectId()),
        "ModifyDate": datetime.now(),
        "CreateDate": datetime.now(),
        "CodigoFatura": fake.bothify(text='#########'),
        "FtsId": "0",
        "LinkFaturaAuxiliar": {
            "CodigoFatura": None,
            "FtsId": "0"
        },
        "Ativo": {
            "Attempts": None,
            "Conta": None,
            "DataPosicao": iso_date_generator(),
            "QueueId": str(ObjectId()),
            "ModifyDate": datetime.now(),
            "CreateDate": iso_date_generator(),
            "CodigoAtivo": random.randint(100000, 999999),
            "CodigoCgeEmissor": random.randint(1000, 9999),
            "NomeEmissor": "BACEN-BANCO CENTRAL DO BRASIL - RJ",
            "TipoAtivo": "Renda Fixa",
            "TipoInstrumento": "NTNB",
            "GrupoContabil": "NTNB",
            "Descricao": "NTNB",
            "NickName": "NTNB IPCA   ",
            "DataEmissao": datetime(2006, 3, 7),
            "DataVencimento": datetime(2035, 5, 15),
            "CodigoCetip21": "",
            "ValorTaxaNominal": "6.0"
        },
        "SistemaOrigem": {
            "_id": 23,
            "Nome": "OPEN"
        },
        "PosicaoDetalhada": {
            "Attempts": None,
            "Conta": fake.bothify(text='###########'),
            "DataPosicao": iso_date_generator(),
            "QueueId": str(ObjectId()),
            "ModifyDate": datetime.now(),
            "CreateDate": datetime.now(),
            "DataInterface": iso_date_generator(),
            "Ativo": {
                "Attempts": None,
                "Conta": None,
                "DataPosicao": iso_date_generator(),
                "QueueId": str(ObjectId()),
                "ModifyDate": datetime.now(),
                "CreateDate": iso_date_generator(),
                "CodigoAtivo": random.randint(100000, 999999),
                "CodigoCgeEmissor": random.randint(1000, 9999),
                "NomeEmissor": "BACEN-BANCO CENTRAL DO BRASIL - RJ",
                "TipoAtivo": "Renda Fixa",
                "TipoInstrumento": "NTNB",
                "GrupoContabil": "NTNB",
                "Descricao": "NTNB",
                "NickName": "NTNB IPCA   ",
                "DataEmissao": datetime(2006, 3, 7),
                "DataVencimento": datetime(2035, 5, 15),
                "CodigoCetip21": "",
                "ValorTaxaNominal": "6.0"
            },
            "CodigoFatura": fake.bothify(text='###########'),
            "CodigoFaturaOrigem": fake.bothify(text='###########'),
            "DataAquisicao": datetime(2019, 1, 3),
            "Pu": "4343.98",
            "PuCusto": "3600.66",
            "Quantidade": "0.06",
            "Ir": "8.05",
            "Iof": "0.0",
            "FtsId": "0",
            "Versao": 0,
            "Virtual": False,
            "Indexador": {
                "Nome": "IPCA",
                "Taxa": "100.0"
            },
            "TaxaComplemento": "6.0",
            "TaxaCompra": "4.80999999",
            "LiquidacaoDiaria": True,
            "Curva": {
                "_id": 9
            },
            "CodigoOrigemBoleta": None,
            "SistemaOrigemRendaFixa": {
                "_id": 23,
                "Nome": "OPEN"
            },
            "Precificado": False,
            "AtivoFormatado": "NTNB",
            "OperacaoCompromissada": None,
            "DataVencimento": datetime(2035, 5, 15),
            "FinanceiroBruto": "260.63",
            "DataCompra": datetime(2019, 1, 3)
        },
        "Movimentacoes": [
            {
                "Attempts": None,
                "Conta": fake.bothify(text='#########'),
                "DataPosicao": iso_date_generator(),
                "QueueId": str(ObjectId()),
                "ModifyDate": datetime.now(),
                "CreateDate": datetime.now(),
                "Ativo": {
                    "Attempts": None,
                    "Conta": None,
                    "DataPosicao": iso_date_generator(),
                    "QueueId": str(ObjectId()),
                    "ModifyDate": datetime.now(),
                    "CreateDate": iso_date_generator(),
                    "CodigoAtivo": random.randint(100000, 999999),
                    "CodigoCgeEmissor": random.randint(1000, 9999),
                    "NomeEmissor": "BACEN-BANCO CENTRAL DO BRASIL - RJ",
                    "TipoAtivo": "Renda Fixa",
                    "TipoInstrumento": "NTNB",
                    "GrupoContabil": "NTNB",
                    "Descricao": "NTNB",
                    "NickName": "NTNB IPCA   ",
                    "DataEmissao": datetime(2006, 3, 7),
                    "DataVencimento": datetime(2035, 5, 15),
                    "CodigoCetip21": "",
                    "ValorTaxaNominal": "6.0"
                },
                "Indexador": {
                    "Nome": "IPCA",
                    "Taxa": "100.0"
                },
                "TaxaComplemento": "6",
                "TaxaCompra": "4.80999999",
                "CodigoFatura": fake.bothify(text='###########'),
                "CodigoFaturaOrigem": fake.bothify(text='###########'),
                "Pu": "3600.66",
                "Quantidade": "0.06",
                "Ir": "0.0",
                "Iof": "0.0",
                "InstituicaoLiquidacao": "SELIC",
                "DataLiquidacao": iso_date_generator(),
                "DataEfetivacao": iso_date_generator(),
                "DataCompra": datetime(2019, 1, 3),
                "SistemaOrigemRendaFixa": {
                    "_id": 23,
                    "Nome": "OPEN"
                },
                "TipoMovimentacao": {
                    "_id": "C",
                    "Descricao": "DEPÓSITO DE CUSTÓDIA"
                },
                "Curva": None,
                "FtsId": "0",
                "Versao": 0,
                "Virtual": False,
                "CodigoOrigemBoleta": "TD",
                "LiquidacaoDiaria": True,
                "OperacaoCompromissada": None,
                "DataVencimento": datetime(2035, 5, 15),
                "FinanceiroBruto": "216.03"
            }
        ],
        "Garantias": [],
        "ValorEmTransito": None,
        "TradesOnline": [],
        "ValorEmTransitoRendaFixaOnline": None,
        "ImpactosCC": [],
        "Versao": 1,
        "ImpactosCCNaoDeletados": [],
        "ContemPosicao": True
    }

def insert_documents(batch_size, total_docs, num_threads, thread_number):
    # Connect to MongoDB
    client = MongoClient(var_mongo_uri)
    db = client[var_db]
    collection = db[var_collection]

    num_docs = 0
    collection.drop()
    for l in range(total_docs // batch_size // num_threads):
        lista_insert = []
        for b in range(batch_size):
            lista_insert.append(create_document())
        
        dataexecucao = datetime.now()
        collection.insert_many(lista_insert)
        num_docs = num_docs + batch_size
        print(f"Thread {thread_number+1:{2}} Inserted {batch_size} documents (total:", num_docs, "/", total_docs//num_threads, "). In: ", (datetime.now() - dataexecucao), "ms")

def main():
    total_docs = 4000
    batch_size = 1000
    num_threads = 4
    inicio = datetime.now()
    print(f"Started at ", inicio)

    pool = Pool(num_threads)
    for t in range(num_threads):
            pool.apply_async(insert_documents, args=(batch_size, total_docs, num_threads, t))
    pool.close()
    pool.join()

    print(f"Ended at ", inicio, ". Elapsed: ", datetime.now() - inicio)

if __name__ == "__main__":
    main()