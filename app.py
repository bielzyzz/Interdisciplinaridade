import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
from decimal import Decimal  

# Interdisciplinaridade Banco de dados avançado


# função para conectar ao banco de dados
def connect():
    try:
        conn = psycopg2.connect(
            dbname="banco",
            user="postgres",
            password="postgres",
            host="192.168.32.122",
            port="5432"
        )
        return conn
    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None

# função para listar todos os clientes
def listar_clientes():
    conn = connect()
    if conn is None:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, nome, limite FROM cliente")
        clientes = cursor.fetchall()
        
        if not clientes:
            print("Nenhum cliente encontrado.")
            return None
        
        print("Clientes disponíveis:")
        for cliente in clientes:
            print(f"ID: {cliente[0]}, Nome: {cliente[1]}, Limite Atual: {cliente[2]}")
        return clientes
    except Exception as e:
        print("Erro ao listar clientes:", e)
    finally:
        conn.close()

# função para iniciar a transação de aumento do limite sem commit imediato
def iniciar_aumento_limite(cliente_id, valor_aumento):
    conn = connect()
    if conn is None:
        return None

    conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
    try:
        cursor = conn.cursor()

        # seleciona e bloqueia a linha do cliente
        cursor.execute(
            sql.SQL("SELECT limite FROM cliente WHERE id = %s FOR UPDATE"),
            (cliente_id,)
        )
        resultado = cursor.fetchone()

        # verifica se o cliente existe
        if resultado is None:
            print("Cliente não encontrado.")
            conn.close()
            return None

        limite_atual = resultado[0]
        print(f"Limite atual do cliente {cliente_id}: {limite_atual}")

        # converte valor_aumento para Decimal antes de somar
        novo_limite = limite_atual + Decimal(valor_aumento)
        cursor.execute(
            sql.SQL("UPDATE cliente SET limite = %s WHERE id = %s"),
            (novo_limite, cliente_id)
        )
        print(f"Limite do cliente {cliente_id} preparado para aumento para {novo_limite}.")

        # retorna a conexão e o cursor para permitir a confirmação posterior
        return conn, cursor
    except psycopg2.errors.SerializationFailure as e:
        print("Conflito detectado: outro processo tentou aumentar o limite ao mesmo tempo.")
        conn.rollback()
        conn.close()
        return None
    except Exception as e:
        print("Erro ao tentar iniciar o aumento do limite:", e)
        conn.rollback()
        conn.close()
        return None

# função para confirmar a transação 
def confirmar_aumento_limite(conn):
    try:
        conn.commit()
        print("Aumento do limite confirmado e commit realizado.")
    except Exception as e:
        print("Erro ao confirmar o aumento do limite:", e)
        conn.rollback()
    finally:
        conn.close()

# função para cancelar a transação 
def cancelar_aumento_limite(conn):
    try:
        conn.rollback()
        print("Aumento do limite cancelado e rollback realizado.")
    except Exception as e:
        print("Erro ao cancelar o aumento do limite:", e)
    finally:
        conn.close()

# função para obter a confirmação do usuário com 'sim' ou 'não'
def obter_confirmacao():
    resposta = input("Você confirma o aumento do limite? (sim/não): ").strip().lower()
    if resposta == 'sim':
        return True
    elif resposta == 'não':
        return False
    else:
        print("Resposta inválida. Por favor, digite 'sim' ou 'não'.")
        return obter_confirmacao()  # Recursivamente pede novamente

# função principal para executar o processo de aumento de limite com a escolha de cliente e valor
def executar_aumento_limite():
    clientes = listar_clientes()
    if not clientes:
        return
    
    try:
        cliente_id = int(input("Digite o ID do cliente para aumentar o limite: "))
        valor_aumento = input("Digite o valor para aumentar o limite: ")

        # converte o valor_aumento para Decimal
        valor_aumento = Decimal(valor_aumento)

        # inicia o aumento do limite, deixando a transação pendente
        transacao = iniciar_aumento_limite(cliente_id, valor_aumento)
        if transacao:
            conn, cursor = transacao
            if obter_confirmacao():
                confirmar_aumento_limite(conn)  # confirmar e realizar o commit
            else:
                cancelar_aumento_limite(conn)  # cancelar a transação e fazer o rollback
    except ValueError:
        print("Erro: ID do cliente ou valor de aumento inválido.")

# executa 
executar_aumento_limite()
