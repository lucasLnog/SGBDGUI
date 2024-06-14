# Projeto Final da Disciplina Introdução à Banco de Dados
# Data: 10/06/2024
# Professor: Leandro Batista De Almeida
# Autores: Lucas de Lima Nogueira e Ricardo Marthus Gremmelmaier

import psycopg2
import mysql.connector
from anytree import Node, RenderTree
from tabulate import tabulate
import json
import csv


class SGBDGui:

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.database_type = None
        self.connection_data = {}

    def dataBaseConnect(self, db_type, host, port, user, password, database):
        self.database_type = db_type
        self.connection_data = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }

        try:
            if db_type == 'postgresql':
                self.connection = psycopg2.connect(**self.connection_data)
            elif db_type == 'mysql':
                self.connection = mysql.connector.connect(**self.connection_data)
            else:
                self.connection_data = {}
                raise ValueError("O tipo de banco de dados informado ainda não é suportado pelo sistema. Tente novamente.")
            self.cursor = self.connection.cursor()
        except (psycopg2.OperationalError, mysql.connector.Error):
            self.connection_data = {}
            raise ValueError("Falha na conexão: verifique o nome do seu banco de dados e/ou credenciais,e tente novamente.")

        print("Conexão realizada com sucesso!")

    def saveConnection(self, filename):
        if not self.connection_data:
            raise ValueError("As informações de conexão são inválidas ou não foram fornecidas.")
        else:
            self.connection_data['db_type'] = self.database_type
            try:
                with open(filename, 'w') as file:
                    json.dump(self.connection_data, file)
                print(f"Dados da conexão salvos com sucesso em {filename}.")
            except ValueError as e:
                raise ValueError(e)
            except (IOError, OSError):
                raise ValueError(f"Falha em salvar os dados da conexão em {filename}.")

    def loadConnection(self, filename):
        try:
            with open(filename, 'r') as file:
                self.connection_data = json.load(file)
            print(f"Dados da conexão lidos com sucesso de {filename}.")
            self.dataBaseConnect(
                self.connection_data['db_type'],
                self.connection_data['host'],
                self.connection_data['port'],
                self.connection_data['user'],
                self.connection_data['password'],
                self.connection_data['database']
            )
        except (IOError, OSError):
            raise ValueError(f"Falha em ler os dados da conexão de {filename}.")

    def showTablesAndViews(self):

        if self.database_type == 'postgresql':
            self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
            tables = self.cursor.fetchall()
            self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'VIEW'")
            views = self.cursor.fetchall()
        elif self.database_type == 'mysql':
            self.cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
            tables = self.cursor.fetchall()
            self.cursor.execute("SHOW FULL TABLES WHERE Table_type = 'VIEW'")
            views = self.cursor.fetchall()
        else:
            raise ValueError("Falha na conexão: verifique o nome do seu banco de dados e/ou credenciais,e tente novamente.")

        root = Node(self.connection_data['database'])

        if tables:
            tables_node = Node("Tabelas", parent=root)
            for table in tables:
                table_node = Node(f"{table[0]} (TABELA)", parent=tables_node)
                self.showTablesAndViewsAddInformations(table[0], table_node)

        if views:
            views_node = Node("Views", parent=root)
            for view in views:
                view_node = Node(f"{view[0]} (VIEW)", parent=views_node)
                self.showTablesAndViewsAddInformations(view[0], view_node)

        print(120 * "-")
        for pre, fill, node in RenderTree(root):
            print("%s%s" % (pre.replace("├", "|").replace("└", "|").replace("│", "|"), node.name))

    def showTablesAndViewsAddInformations(self, table_name, parent_node):
        tables_info = []

        if self.database_type == 'postgresql':
            self.cursor.execute(f"""SELECT c.column_name, c.data_type, c.character_maximum_length,
            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PRIMARY KEY' ELSE '' END AS constraint_type
            FROM information_schema.columns AS c
            LEFT JOIN information_schema.key_column_usage AS kcu
            ON c.table_schema = kcu.table_schema
            AND c.table_name = kcu.table_name
            AND c.column_name = kcu.column_name
            LEFT JOIN information_schema.table_constraints AS tc
            ON tc.constraint_name = kcu.constraint_name
            WHERE c.table_name = '{table_name}' AND c.table_schema = 'public'
            ORDER BY c.ordinal_position
            """)
            tables_info = self.cursor.fetchall()
        elif self.database_type == 'mysql':
            self.cursor.execute(f"DESCRIBE {table_name}")
            tables_info = self.cursor.fetchall()

        if tables_info:
            for info in tables_info:
                column_node = Node(f"{info[0]} (CAMPO)", parent=parent_node)
                if self.database_type == 'mysql':
                    Node(f"{info[1]} (TIPO/TAMANHO)", parent=column_node)
                    if info[3] == 'PRI':
                        Node("Chave primária (CHAVE)", parent=column_node)
                elif self.database_type == 'postgresql':
                    if info[2]:
                        Node(f"{info[1]} ({info[2]}) (TIPO/TAMANHO)", parent=column_node)
                    else:
                        Node(f"{info[1]} (TIPO/TAMANHO)", parent=column_node)
                    if info[3] == 'PRIMARY KEY':
                        Node("Chave primária (CHAVE)", parent=column_node)

    def showAllDataFromTable(self, table_name):
        print(120 * "-")
        option = input("A consulta dos dados está limitada a 1000 registros. Deseja diminuir [Y/N]? ")
        while option not in ('Y', 'N'):
            option = input("Opção não disponível. Tente novamente: ")

        if option == 'Y':
            while True:
                try:
                    print(120 * "-")
                    limit = int(input("Digite a quantidade de registros desejada: "))
                    if 0 < limit <= 1000:
                        break
                    else:
                        print(120 * "-")
                        print("A quantidade de registros não pode ser superior a 1000, negativa ou zero!")
                except ValueError:
                    print(120 * "-")
                    print("O dado digitado não é um número inteiro!")
        else:
            limit = 1000

        try:
            self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            rows = self.cursor.fetchall()
            headers = [i[0] for i in self.cursor.description]
            print(120 * "-")
            print("Query executada com sucesso!")
            print(120 * "-")
            print(tabulate(rows, headers, tablefmt='psql'))
            print(120 * "-")
            option = input("Deseja exportar os dados da consulta [Y/N]? ")
            while option not in ('Y', 'N'):
                print(120 * "-")
                option = input("Opção não disponível. Tente novamente: ")

            if option == 'Y':
                print(120 * "-")
                export_type = input("Deseja exportar em CSV ou JSON? ")
                while export_type not in ('CSV', 'JSON'):
                    print(120 * "-")
                    export_type = input("Opção não disponível. Tente novamente: ")
                filename = input("Digite o nome do arquivo para salvar os dados da consulta: ")
                try:
                    if export_type == 'CSV':
                        self.exportToCSV(rows, headers, filename)
                    elif export_type == 'JSON':
                        self.exportToJSON(rows, headers, filename)
                except ValueError as e:
                    raise ValueError(e)
        except (psycopg2.Error, mysql.connector.Error) as e:
            raise ValueError("Ocorreu um erro ao executar ao query! Verifique se a tabela digitada existe.")

    def showSQLConsult(self, query):
        try:
            while True:
                if query.lower().startswith("select"):
                    break
                else:
                    print(120 * "-")
                    query = input("A query informada não é um 'SELECT', tente novamente: ")

            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            headers = [i[0] for i in self.cursor.description]
            print(120 * "-")
            print("Query executada com sucesso!")
            print(120 * "-")
            print(tabulate(rows, headers, tablefmt='psql'))
            option = input("Deseja exportar os dados da consulta [Y/N]? ")
            while option not in ('Y', 'N'):
                print(120 * "-")
                option = input("Opção não disponível. Tente novamente: ")

            if option == 'Y':
                print(120 * "-")
                export_type = input("Deseja exportar em CSV ou JSON? ")
                while export_type not in ('CSV', 'JSON'):
                    print(120 * "-")
                    export_type = input("Opção não disponível. Tente novamente: ")
                filename = input("Digite o nome do arquivo para salvar os dados da consulta: ")
                try:
                    if export_type == 'CSV':
                        self.exportToCSV(rows, headers, filename)
                    elif export_type == 'JSON':
                        self.exportToJSON(rows, headers, filename)
                except ValueError as e:
                    raise ValueError(e)
        except (psycopg2.Error, mysql.connector.Error) as e:
            raise ValueError(f"Ocorreu um erro ao executar a query! [ERRO]: {e}")

    def exportToCSV(self, rows, headers, filename):
        try:
            if not filename.lower().endswith('.csv'):
                filename += '.csv'
            with open(filename, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                for row in rows:
                    writer.writerow(row)
            print(f"Dados da consulta exportados com sucesso em {filename}.")
        except ValueError as e:
            raise ValueError(e)
        except (IOError, OSError) as e:
            raise ValueError(f"Falha ao exportar os dados da consulta em {filename}.")

    def exportToJSON(self, rows, headers, filename):
        try:
            with open(filename, 'w') as file:
                json.dump([dict(zip(headers, row)) for row in rows], file, indent=4)
            print(f"Dados da consulta exportados com sucesso em {filename}.")
        except ValueError as e:
            raise ValueError(e)
        except (IOError, OSError) as e:
            raise ValueError(f"Falha ao exportar os dados da consulta em {filename}.")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def main():
    gui_SGBD = SGBDGui()
    print(30 * "=", " SISTEMA GERENCIADOR DE BANCO DE DADOS (INTERFACE GRÁFICA) ", 30 * "=")
    print("Bem-vindo ao SGBD Grafic Text Interface.")
    print("Para continuar selecione uma das opções abaixo:")
    while True:
        print(120 * "-")
        print("1. Conectar ao banco de dados")
        print("2. Salvar informações de conexão")
        print("3. Carregar informações de conexão")
        print("4. Mostrar tabelas e views")
        print("5. Consultar tabela")
        print("6. Realizar consulta SQL")
        print("7. Sair do programa")
        print(120 * "-")

        option = input("Selecione uma opção: ")

        if option == '1':
            db_type = input("Digite o tipo do banco de dados a ser acessado (postgresql/mysql): ")
            host = input("Digite o host do banco de dados: ")
            port = input("Digite o port do banco de dados: ")
            user = input("Digite o nome do usuário: ")
            password = input("Digite a senha: ")
            database = input("Digite o nome do banco de dados: ")
            try:
                gui_SGBD.dataBaseConnect(db_type, host, port, user, password, database)
            except ValueError as e:
                print(120 * "-")
                print(e)
                continue
        elif option == '2':
            filename = input("Digite o nome do arquivo para salvar os dados da conexão: ")
            try:
                gui_SGBD.saveConnection(filename)
            except ValueError as e:
                print(120 * "-")
                print(e)
                continue
        elif option == '3':
            filename = input("Digite o nome do arquivo para carregar os dados da conexão: ")
            try:
                gui_SGBD.loadConnection(filename)
            except ValueError as e:
                print(120 * "-")
                print(e)
                continue
        elif option == '4':
            try:
                gui_SGBD.showTablesAndViews()
            except ValueError as e:
                print(120 * "-")
                print(e)
                continue
        elif option == '5':
            try:
                table_name = input("Digite o nome da tabela: ")
                gui_SGBD.showAllDataFromTable(table_name)
            except ValueError as e:
                print(120 * "-")
                print(e)
                continue
        elif option == '6':
            try:
                query = input("Digite a consulta SQL desejada: ")
                gui_SGBD.showSQLConsult(query)
            except ValueError as e:
                print(120 * "-")
                print(e)
                continue
        elif option == '7':
            gui_SGBD.close()
            break
        else:
            print()
            print("Opção ainda não implementada")
            print()


main()
