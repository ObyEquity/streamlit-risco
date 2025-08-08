import pandas as pd
import os
import json
from supabase import create_client
import datetime
import math


class funcoes_auxiliares:
    def __init__(self, delta_dias:0):
        login = os.getlogin()
        ex = ''
        sigla = 'Lato'
        if 'research' in login:
            login = 'guilherme.melo'
        elif login=='william.kim':
            ex = 'DADOS\\'
        elif login=='Fabio.Amaral':
            ex = 'DADOS\\'
            sigla = 'BPJ'
        elif login == 'Ivan':
            sigla = 'BPJ'
        elif login=='lucas.nanes':
            ex = 'DADOS\\'
            sigla = 'BPJ'
        
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_API_KEY")
        self.supabase_client = create_client(url,key)
        
        self.path_risco = fr'C:\Users\{login}\{sigla} Capital Ltda\Admin Lato Capital - {ex}RISCO'
        self.path_download = fr'C:\Users\{login}\Downloads'
        self.path_gestao = fr'C:\Users\{login}\{sigla} Capital Ltda\Admin Lato Capital - {ex}GESTAO'
        
        self.fundos_master = ['LO1','LSH1','LSH2']
        self.fundos_fic = ['LO1_FIC', 'LSH1_FIC', 'LSH2_FIC']
        
        #self.df_data_feriado = self.fetch_data_from_supabase(table='db_feriados_nacionais')['data']
        self.data_referencia = self.pega_data_referencia(datetime.date.today(),delta_dias)

        
    def fetch_data_from_supabase(
        self,
        field_date: str = 'data_referencia',
        start_date: str = None, 
        end_date: str = None, 
        filters: list = None, 
        schema_name: str = 'public', 
        table: str = None, 
        cols_select: str = '*'
        ) -> pd.DataFrame:
    
            
        # Building the query
        query = self.supabase_client.postgrest.schema(schema_name).table(table).select(cols_select)
    
        if start_date is not None:
            query = query.gte(field_date, start_date)
        
        if end_date is not None:
            query = query.lte(field_date, end_date)
    
        # Applying filters
        if filters is not None:
            for filter_field, filter_values in filters:
                query = query.in_(filter_field, filter_values)
    
        # Executing the query
        data = query.execute()
    
        # Creating a DataFrame from the results
        df = pd.DataFrame(data.data)
    
        return df
    
    def fetch_data_from_supabase_grandes(
        self,
        field_date: str = 'data_referencia',
        start_date: str = None,
        end_date: str = None,
        filters: list = None,
        schema_name: str = 'public',
        table: str = None,
        cols_select: str = '*'
        ) -> pd.DataFrame:
    
        # Lista para armazenar todos os resultados
        all_data = []
        limite = 100000
        offset = 0
    
        while True:
            # Building the query com range
            query = self.supabase_client.postgrest.schema(schema_name).table(table).select(cols_select).range(offset, offset + limite - 1)
    
            if start_date is not None:
                query = query.gte(field_date, start_date)
    
            if end_date is not None:
                query = query.lte(field_date, end_date)
    
            if filters is not None:
                for filter_field, filter_values in filters:
                    query = query.in_(filter_field, filter_values)
    
            # Executando
            data = query.execute()
    
            # Se não retornar mais dados, parar
            if not data.data:
                break
    
            # Adiciona ao resultado geral
            all_data.extend(data.data)
    
            # Incrementa o offset
            offset += limite
    
        # Criar DataFrame
        df = pd.DataFrame(all_data)
    
        return df
    
    def upsert_data(self,
                    df: pd.DataFrame(), 
                    table: str = None, 
                    schema_name: str = 'public', 
                    nrows_df = 10000):
        try:
            """
            Serializa um DataFrame em JSON, desserializa em um dicionário e atualiza uma tabela no Supabase.
    
            :param df: DataFrame pandas que será carregado no Supabase.
            :param table_name: Nome da tabela no Supabase onde os dados serão inseridos/atualizados.
            
            Esta função converte um DataFrame pandas em uma lista de dicionários JSON,
            e então utiliza a API do Supabase para inserir ou atualizar esses dados na tabela especificada.
            """
            
            # Dividir o DataFrame em partes de 10 mil linhas
            partes = [df.iloc[i:i + nrows_df] for i in range(0, len(df), nrows_df)]
    
            # Serializar e desserializar em JSON e subir para o supabase    
            for parte in partes:
                json_str = parte.to_json(orient='records')
                json_str = json.loads(json_str)
                self.supabase_client.postgrest.schema(schema_name).table(table).upsert(json_str).execute()
            
            print('Done')
            return True
        except Exception as e:
            print(f'Erro ao atualizar dados no Supabase. {e}')        
            return False

    def is_dia_util(self,data_referencia):
        df_data_feriado = self.fetch_data_from_supabase(table='db_feriados_nacionais')['data_referencia']
        feriados = pd.to_datetime(df_data_feriado).dt.date.tolist()
            
        # Verificar se o dia é um dia útil (não é sábado, domingo ou feriado)
        if data_referencia.weekday() < 5 and data_referencia not in feriados:
            return True
        else:
            return False

    def pega_data_referencia(self,data_hoje,delta_dias):
        df_data_feriado = self.fetch_data_from_supabase(table='db_feriados_nacionais')['data_referencia']
        feriados = pd.to_datetime(df_data_feriado).dt.date.tolist()
        data_referencia = data_hoje
        dias_adicionados = 0
        
        # Definir a direção do cálculo baseado no sinal do delta_dias
        if delta_dias < 0:
            passo = -1  # para trás
        elif delta_dias > 0:
            passo = 1   # para frente
        else:
            return data_hoje
    
        while dias_adicionados < abs(delta_dias):
            data_referencia += datetime.timedelta(days=passo)
            if data_referencia.weekday() < 5 and data_referencia not in feriados:
                dias_adicionados += 1
        
        return data_referencia
    
    def pega_distancia_datas(self, data_hoje, data_fim):
        df_data_feriado = self.fetch_data_from_supabase(table='db_feriados_nacionais')['data_referencia']
        feriados = pd.to_datetime(df_data_feriado).dt.date.tolist()
        data_referencia = data_hoje
        contador = 0
        if data_fim > data_hoje:
            while data_referencia <= data_fim:
                if data_referencia.weekday() < 5 and data_referencia not in feriados:
                    contador +=1
                
                data_referencia += datetime.timedelta(days=1)
        elif data_fim < data_hoje:
            while data_referencia >= data_fim:
                if data_referencia.weekday() < 5 and data_referencia not in feriados:
                    contador -=1
                
                data_referencia += datetime.timedelta(days=-1)        
        elif data_fim == data_hoje:
            contador = 0
        
        return contador

    def is_terceira_sexta_ou_util_anterior(self, str_data=False):
        # str_data não é obrigatorio e se for utilizar passar a data no formato string '2025-08-31'
        if str_data:
            data_referencia = datetime.datetime.strptime(str_data, "%Y-%m-%d").date()
        else:
            data_referencia = self.data_referencia
        
        feriados = self.fetch_data_from_supabase(table='db_feriados_nacionais')
        feriados = pd.to_datetime(feriados['data_referencia']).dt.date.tolist()
        
        
        # Encontrar todas as sextas-feiras do mês da data de referência
        inicio_mes = data_referencia.replace(day=1)
        fim_mes = (inicio_mes + pd.offsets.MonthEnd(0))
    
        dias_do_mes = pd.date_range(start=inicio_mes, end=fim_mes, freq='D')
        sextas = dias_do_mes[dias_do_mes.weekday == 4]  # 4 = sexta-feira
    
        # Pegar a terceira sexta-feira do mês
        terceira_sexta = sextas[2].date()  # Índice 2 -> terceira sexta
    
        # Verificar se a terceira sexta é feriado
        if terceira_sexta in feriados:
            # Se for feriado, pegar o dia útil anterior
            data_valida = terceira_sexta - pd.offsets.BDay(1)
            data_valida = data_valida.date()
        else:
            data_valida = terceira_sexta
    
        # Verificar se a data de referência é igual à data válida
        return data_referencia == data_valida

    def arquivo_carteira(self, data_referencia, fundo, retorna_caminho=False):
        try:
            pasta = rf'{self.path_gestao}\CARTEIRAS'
            MES_PT = {
                    1: "JAN", 2: "FEV", 3: "MAR", 4: "ABR",
                    5: "MAI", 6: "JUN", 7: "JUL", 8: "AGO",
                    9: "SET", 10: "OUT", 11: "NOV", 12: "DEZ"
                    }
                        
            ano = str(data_referencia.year)
            mes = MES_PT[data_referencia.month]
            nome_arquivo = data_referencia.strftime("%Y%m%d") + ".xlsx"
            
            caminho_carteira = os.path.join(pasta, ano, mes, nome_arquivo)
            
            df_carteira = pd.read_excel(caminho_carteira, sheet_name=fundo, header=None)
            
            if retorna_caminho:
                df_carteira = caminho_carteira
            
            return True, df_carteira
        
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()
     
        
    def arquivo_bradesco(self, data_referencia, fundo):
        try:
            pasta = rf'{self.path_risco}\New'
            
            data_referencia = self.pega_data_referencia(data_referencia,1)
            ano = str(data_referencia.year)
            
            mes_dia = data_referencia.strftime("%d-%m")
            
            pasta_ano = f'{pasta}\{ano}\{mes_dia}\{fundo}.xls'
            pasta_solta = f'{pasta}\{mes_dia}\{fundo}.xls'
            try:
                df_bradesco = pd.read_excel(pasta_ano)
            except:
                df_bradesco = pd.read_excel(pasta_solta)
                
        
            return True, df_bradesco
        
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()
        
        
    def insere_pl_fundos(self, str_data=False):
            try:
                if str_data:
                    data_referencia = datetime.datetime.strptime(str_data, "%Y-%m-%d").date()
                else:
                    data_referencia = self.data_referencia
                        
                df_pl = pd.DataFrame(columns = ['data_referencia','fundo','pl','valor_cota','resgates_aplicacoes'])
                todos_fundos = self.fundos_master + self.fundos_fic
                
                for fundo in todos_fundos:
                    #pega os dad os do arquivo do bradesco
                    status,df_bradesco = self.arquivo_bradesco(data_referencia,fundo)
                    assert status, 'arquivo não encontrado'
                    pl = df_bradesco.loc[df_bradesco.iloc[:, 0]=='Total do Patrimônio'].iloc[0, 1]
                    valor_cota = df_bradesco.loc[df_bradesco.iloc[:, 0]=="Valor da cota unitária (Líquida)"].iloc[0, 1]
                    #pega dados carteira
                    status,df_carteira = self.arquivo_carteira(data_referencia,fundo)
                    if status:
                        resgtaes_e_aplicacoes = df_carteira.loc[df_carteira[3] =='APLIC/RESG'].iloc[0, 11]
                        #pl = df_carteira.loc[df_carteira[3] =='PL'].iloc[0, 11]
                    else:
                        resgtaes_e_aplicacoes = 0
                    
                    if resgtaes_e_aplicacoes is None or resgtaes_e_aplicacoes == "" or (isinstance(resgtaes_e_aplicacoes, float) and math.isnan(resgtaes_e_aplicacoes)):
                        resgtaes_e_aplicacoes = 0

                    df_insert = pd.DataFrame({'data_referencia':[data_referencia.strftime('%Y-%m-%d')],
                                              'fundo':[fundo],
                                              'pl':[float(pl)],
                                              'valor_cota':[float(valor_cota)],
                                              'resgates_aplicacoes':[float(resgtaes_e_aplicacoes)]})
                    
                    df_pl = pd.concat([df_pl, df_insert], ignore_index=True)
                    str_data = data_referencia.strftime("%Y-%m-%d")
                    self.upsert_data(df = df_insert, table = 'db_pl_fundos')
                print(f"PL atualizado do dia {data_referencia.strftime('%Y-%m-%d')}")
                return True, df_pl
            except Exception as e:
                print(f'ERRO: {e}')
                return False, pd.DataFrame()


    def insere_carteiras_fundos(self,str_data=False):
        try:
            if str_data:
                data_referencia = datetime.datetime.strptime(str_data, "%Y-%m-%d").date()
            else:
                data_referencia = self.data_referencia
            
            colunas = ['data_ref','Fundo','ATIVO','QTD D0','FIN D-0','D0','QTD','PREÇO','Trades','Proventos','RESULTADO','PESO %', 'Opções']
            df_final = pd.DataFrame(columns = colunas)
            
            #RESULTADO
            for fundo in self.fundos_master:
                
                status, df_raw = self.arquivo_carteira(data_referencia, fundo)
                assert status, "erro na função 'arquivo_carteira'"
            
                # Encontrar o índice da linha onde a coluna D (index 3) tem "CAIXA"
                df_raw[3] = df_raw[3].replace("CAIXA MG", "CAIXA")
                linha_fim = df_raw[df_raw[3] == "CAIXA"].index.max()
            
                # Seleciona apenas o intervalo desejado: colunas D:R → índices 3 até 17
                df_limpo = df_raw.loc[1:linha_fim, 3:17].copy()
            
                # (Opcional) redefinir os índices da linha e coluna
                df_limpo.reset_index(drop=True, inplace=True)
                df_limpo.columns = df_limpo.iloc[0,:] 
                df_limpo = df_limpo.iloc[1:,:]  
    
                df_limpo['Fundo'] = fundo
                df_limpo['data_ref'] = data_referencia.strftime('%Y-%m-%d')
                
                
                colunas = ['data_ref','Fundo','ATIVO','QTD D0','FIN D-0','D0','QTD','PREÇO','Trades','Proventos', 'RESULTADO', 'PESO %', 'Opções']
                df_insert = df_limpo[colunas].fillna(0)
                
                
                df_insert = df_insert.rename(columns={
                                                "data_ref": "data_referencia",
                                                "Fundo": "fundo",
                                                "ATIVO": "codigo_ativo",
                                                "QTD D0": "quantidade_hoje",
                                                "PREÇO": "preco_medio_movimentado",
                                                "FIN D-0": "financeiro",
                                                "QTD": "quantidade_movimentada",
                                                "D0": "preco_hoje",
                                                "Trades": "financeiro_day_trade",
                                                "Proventos": "proventos",
                                                'PESO %': 'porcentagem_pl', 
                                                'Opções': 'correcao_peso_opcao',
                                                'RESULTADO': 'contribuicao_resultado'
                                            })
                
                df_insert['contribuicao_resultado'] = pd.to_numeric(df_insert['contribuicao_resultado'], errors='coerce')
                df_insert = df_insert.groupby(['data_referencia', 'fundo', 'codigo_ativo'], as_index=False).sum()
                df_insert['quantidade_movimentada'] = df_insert['quantidade_movimentada'].round().astype('Int64')
                df_insert['quantidade_hoje'] = df_insert['quantidade_hoje'].round().astype('Int64')
                self.upsert_data(df = df_insert, table = 'db_carteiras_fundos')
                
                df_final = pd.concat([df_final,df_insert], ignore_index=True)
            return True, df_final
    
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()
    
    def preenche_dados_opcoes(self, str_data=False):
        try:
            if str_data:
                data_referencia = datetime.datetime.strptime(str_data, "%Y-%m-%d").date()
            else:
                data_referencia = self.data_referencia
            
            colunas = ['data_referencia','codigo_opcao','preco_opcao','strike']
            df_final = pd.DataFrame(columns = colunas)
            
            lista = self.fundos_master
            lista.append('MM1_H')
            for fundo in lista:
                
                status, df_raw = self.arquivo_carteira(data_referencia, fundo)
                
                if status:
                    df_raw = df_raw[df_raw[2].notna()]
                    df_raw = df_raw.iloc[1:]
                
                    df_limpo = df_raw[[2,3,9]].copy()
                
                    # (Opcional) redefinir os índices da linha e coluna
                    df_limpo.reset_index(drop=True, inplace=True) 
        
                    df_limpo['data_ref'] = data_referencia.strftime('%Y-%m-%d')
                    
                    
                    colunas = ['data_ref',3,9,2]
                    df_insert = df_limpo[colunas].fillna(0)
                    
                    
                    df_insert = df_insert.rename(columns={
                                                    "data_ref": "data_referencia",
                                                    3: "codigo_opcao",
                                                    9: "preco_opcao",
                                                    2: "strike",
                                            
                                                })
                    
                    
                    
                    df_final = pd.concat([df_final,df_insert], ignore_index=True)
                    
                else:
                    print("erro na função 'arquivo_carteira'")
                
            df_final = df_final.drop_duplicates()
            
            df_final['serie'] = df_final['codigo_opcao'].apply(lambda x: x[4])
            df_final['year'] = data_referencia.year
            df_final['month'] = data_referencia.month
            df_final['year_ajustado'] = data_referencia.year
            df_final['tipo_opcao'] = 'NOT KNOWN'
            df_final['classe_opcao'] = 'NOT KNOWN'
            df_final['tipo_opcao'] = 'NOT KNOWN'
            df_final['data_expire'] = data_referencia
            
            aux = pd.read_excel(f"{self.path_risco}/New\Codes\Dados\Planilhas Auxiliares/aux_vencimento_historico_opcoes.xlsx")
            aux['data_expire'] = pd.to_datetime(aux['data_expire']).dt.date
            
            df_final = df_final.reset_index(drop = True)
            for i in range(len(df_final['codigo_opcao'])):
                mes = data_referencia.month
                serie = df_final['serie'][i]
                if mes > 7 and serie in ['A', 'M', 'B', 'N']:
                    df_final['year_ajustado'][i] = df_final['year'][i] + 1
                    
                
                tipo = aux.loc[aux['serie'] == serie, 'tipo'].values[0]
                ano = df_final['year_ajustado'][i]
                expire = aux.loc[(aux['serie'] == serie) & (aux['ano'] == ano), 'data_expire'].values[0]
                
                df_final['tipo_opcao'][i] = tipo
                df_final['data_expire'][i] = expire
                    
            df_insert = df_final.drop(columns = ['month', 'year', 'year_ajustado', 'serie'])
            # self.upsert_data(df = df_insert, table = 'db_precos_opcoes')    
            
            return True, df_insert
    
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()
    
    
    def insere_peso_ibovespa_antigo(self,str_data=False):
        try:
            if str_data:
                data_referencia = datetime.datetime.strptime(str_data, "%Y-%m-%d").date()
            else:
                data_referencia = self.data_referencia
                
            str_data_referencia = data_referencia.strftime('%d-%m-%y')
            
            arquivo = rf"{self.path_risco}\New\Codes\Dados\Carteira_IBOV\csv\IBOVDia_{str_data_referencia}.csv"
            df = pd.read_csv(arquivo, sep=';', encoding='latin1', skiprows=2, header=None).iloc[:-2, :-1]
            df.columns = ['codigo_ativo', 'acao', 'tipo', 'qtd_teorico', 'peso']
            df_insert = df[['codigo_ativo','peso']]
            
            df_insert['data_referencia'] = data_referencia.strftime('%Y-%m-%d')
            df_insert['radical_ativo'] = df_insert['codigo_ativo'].astype(str).str[:4]
            df_insert['peso'] = df_insert['peso'].str.replace(',', '.').astype(float)
            df_insert['peso'] = df_insert['peso']/100

            if abs(df_insert['peso'].sum() - 1) >= 1e-6:
                print("Peso IBOV não está somando 100%")
                return False, pd.DataFrame()
            else:
                self.upsert_data(df = df_insert, table = 'db_pesos_ibovespa',schema_name='dados_publicos')
                return True, df_insert
            
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()



    def classe_ativos_pendendes(self):
        try:
            query_calsse = """SELECT distinct cf.codigo_ativo
                                FROM public.db_carteiras_fundos cf
                                LEFT JOIN public.db_codigo_ativo cl
                                ON cf.codigo_ativo = cl.codigo_ativo
                                WHERE cl.codigo_ativo IS NULL"""
                            
            
            response  = self.supabase_client.rpc('execute_sql', {'sql': query_calsse}).execute()
            df_ativos_sem_classe = pd.DataFrame(response.data)
            df_ativos_sem_classe['classe_ativo'] = ""
            df_ativos_sem_classe.to_csv(rf"{self.path_risco}/Python/Supabase/classe_ativos_pendendes.csv", index=False)
            
            
            return True, df_ativos_sem_classe
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()
        
        
    def setor_ativo_pendente(self):
        try:
            query_setor = """SELECT distinct left(ca.codigo_ativo, 4) as radical,ca.classe_ativo
                                FROM public.db_codigo_ativo ca
                                LEFT JOIN public.db_setores_bolsa sb
                                ON left(ca.codigo_ativo, 4) = sb.radical_ativo
                                WHERE ca.classe_ativo in ('Ação','Opção') and sb.radical_ativo IS NULL"""
                        
            response  = self.supabase_client.rpc('execute_sql', {'sql': query_setor}).execute()
            df_ativos_sem_setor = pd.DataFrame(response.data)
            df_ativos_sem_setor.to_csv(rf"{self.path_risco}/Python/Supabase/setor_ativo_pendente.csv", index=False, encoding='utf-8-sig')
            
            return True, df_ativos_sem_setor
        except Exception as e:
            print(f'ERRO: {e}')
            return False, pd.DataFrame()


    def performace_atribution(self):
        try:
            df_contribuicao_carteira = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'public',
                                                                            table = 'db_carteiras_fundos',
                                                                            cols_select = 'data_referencia, fundo, codigo_ativo ,contribuicao_resultado, porcentagem_pl, correcao_peso_opcao')
            df_contribuicao_carteira = df_contribuicao_carteira[df_contribuicao_carteira['fundo'].isin(['LO1'])]
            df_contribuicao_carteira = df_contribuicao_carteira[['data_referencia', 'codigo_ativo', 'porcentagem_pl', 'correcao_peso_opcao', 'contribuicao_resultado']]
            df_contribuicao_carteira['radical_ativo'] = df_contribuicao_carteira['codigo_ativo'].str[:4]
            
            
            df_setores = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'public',
                                                                            table = 'db_setores_bolsa',
                                                                            cols_select = 'radical_ativo, subsetor')
            
            df_classe = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'public',
                                                                            table = 'db_codigo_ativo',
                                                                            cols_select = 'codigo_ativo, classe_ativo')
            
            df_merge = df_contribuicao_carteira.merge(df_classe, how='left', on='codigo_ativo')
            df_merge = df_merge.merge(df_setores, how='left', on='radical_ativo')
            df_merge['subsetor'] = df_merge['subsetor'].fillna(df_merge['classe_ativo'])
            
            
            
            
            df_pl_cota = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'public',
                                                                            table = 'db_pl_fundos',
                                                                            cols_select = 'data_referencia, fundo, pl, valor_cota')
            df_pl = df_pl_cota[df_pl_cota['fundo'].isin(['LO1'])][['data_referencia', 'pl']]
            # Garante que data_referencia está como datetime
            df_merge['data_referencia'] = pd.to_datetime(df_merge['data_referencia'])
            df_pl['data_referencia'] = pd.to_datetime(df_pl['data_referencia'])
            # Ordena o df_pl por data
            df_pl = df_pl.sort_values('data_referencia')
            # Função para buscar a maior data anterior e retornar o pl
            def buscar_pl_d1(data_ref):
                anteriores = df_pl[df_pl['data_referencia'] < data_ref]
                if not anteriores.empty:
                    return anteriores.iloc[-1]['pl']
                else:
                    return None
            # Aplica a função para cada linha do df_merge
            df_merge['pl_d1'] = df_merge['data_referencia'].apply(buscar_pl_d1)
            # Calcula a contribuição percentual
            df_merge['contribuicao_percentual'] = df_merge['contribuicao_resultado'] / df_merge['pl_d1']
            
            #mudar para LO1_FIC casa queria ver em comparação com FIC
            df_cota = df_pl_cota[df_pl_cota['fundo'].isin(['LO1'])][['data_referencia', 'valor_cota']]
            # Ordena do mais recente para o mais antigo
            df_cota = df_cota.sort_values('data_referencia', ascending=False).reset_index(drop=True)
            # Pega o valor da cota do "dia anterior" (na ordem descendente, isso é o próximo índice)
            df_cota['valor_cota_anterior'] = df_cota['valor_cota'].shift(-1)
            # Calcula a variação
            df_cota['variacao_cota'] = df_cota['valor_cota'] / df_cota['valor_cota_anterior'] - 1
            

            # Garante que as datas estão no formato datetime
            df_final = df_merge[['data_referencia', 'classe_ativo', 'subsetor', 'codigo_ativo', 'contribuicao_percentual', 'porcentagem_pl', 'correcao_peso_opcao']]
            df_final['data_referencia'] = pd.to_datetime(df_final['data_referencia'])
            df_cota['data_referencia'] = pd.to_datetime(df_cota['data_referencia'])
            # 1. Soma das contribuições por data
            soma_contrib = df_final.groupby('data_referencia')['contribuicao_percentual'].sum().reset_index()
            soma_contrib.rename(columns={'contribuicao_percentual': 'soma_contrib'}, inplace=True)
            # 2. Junta com df_cota para pegar a variacao_cota
            df_diferenca = soma_contrib.merge(df_cota[['data_referencia', 'variacao_cota']],on='data_referencia',how='left')
            # 3. Calcula a diferença
            df_diferenca['diferenca'] = df_diferenca['variacao_cota'] - df_diferenca['soma_contrib']
            # 4. Cria DataFrame com as linhas de "Custos FIC"
            df_faltante = df_diferenca[['data_referencia', 'diferenca']].copy()
            df_faltante = df_faltante.rename(columns={'diferenca': 'contribuicao_percentual'})
            df_faltante['classe_ativo'] = 'Outros'
            df_faltante['subsetor'] = 'Outros'
            df_faltante['codigo_ativo'] = 'Outros'
            df_faltante['correcao_peso_opcao'] = 0
            df_faltante['porcentagem_pl'] = 0
            # 5. Reorganiza colunas para bater com df_final
            df_faltante = df_faltante[df_final.columns]
            # 6. Concatena com o original
            df_merge_final = pd.concat([df_final, df_faltante], ignore_index=True)
            df_merge_final = df_merge_final[(df_merge_final['contribuicao_percentual'] != 0) & (~df_merge_final['contribuicao_percentual'].isna())]


            df_merge_final.to_excel(rf"{self.path_gestao}/EQUITY/Lucas - Pessoal/perf_atri/performace_por_ativo.xlsx", index=False)
            
            #df_cota.to_clipboard(index=False)
            #Pega dados IBOVESPA
            
            df_peso_ibov = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'dados_publicos',
                                                                            table = 'db_pesos_ibovespa',
                                                                            cols_select = '*')
            
            df_retornos = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'dados_publicos',
                                                                            table = 'db_retorno_ajustado',
                                                                            cols_select = '*')
            
            df_ibov_index = self.fetch_data_from_supabase_grandes(field_date = 'data_referencia',
                                                                            start_date = None,
                                                                            end_date = None,
                                                                            schema_name = 'dados_publicos',
                                                                            table = 'db_ibovespa',
                                                                            cols_select = '*')
            
            datas = df_retornos['data_referencia'].tolist()
            df_peso_ibov = df_peso_ibov[df_peso_ibov['data_referencia'].isin(datas)]
            df_retornos = df_retornos[df_retornos['data_referencia'].isin(datas)]
            df_ibov_index = df_ibov_index[df_ibov_index['data_referencia'].isin(datas)]
            
            df_peso_ibov = df_peso_ibov.merge(df_setores,on='radical_ativo',how='left')
            df_peso_ibov = df_peso_ibov.merge(df_retornos,on=['data_referencia','codigo_ativo'],how='left')
            df_peso_ibov['valor'] = df_peso_ibov['valor'].fillna(0)
            df_peso_ibov['contribuicao_percentual'] = df_peso_ibov['valor']*df_peso_ibov['peso']/100
             
            # Ordena do mais recente para o mais antigo
            df_ibov_index = df_ibov_index[df_ibov_index['data_referencia'].isin(datas)]
            df_ibov_index = df_ibov_index.sort_values('data_referencia', ascending=False).reset_index(drop=True)
            # Pega o valor da cota do "dia anterior" (na ordem descendente, isso é o próximo índice)
            df_ibov_index['valor_anterior'] = df_ibov_index['valor'].shift(-1)
            # Calcula a variação
            df_ibov_index['variacao_ibovespa'] = df_ibov_index['valor'] / df_ibov_index['valor_anterior'] - 1
            
            # 1. Garante que datas estão como datetime
            df_ibov_index['data_referencia'] = pd.to_datetime(df_ibov_index['data_referencia'])
            df_peso_ibov['data_referencia'] = pd.to_datetime(df_peso_ibov['data_referencia'])
            # 2. Soma da contribuicao_percentual por data
            df_contrib_soma = df_peso_ibov.groupby('data_referencia')['contribuicao_percentual'].sum().reset_index()
            df_contrib_soma.rename(columns={'contribuicao_percentual': 'soma_contrib'}, inplace=True)
            # 3. Merge com df_ibov_index para pegar a variação do Ibovespa
            df_diferenca = df_contrib_soma.merge(
                df_ibov_index[['data_referencia', 'variacao_ibovespa']],
                on='data_referencia',
                how='left')
            # 4. Calcula a diferença que falta
            df_diferenca['contribuicao_percentual'] = df_diferenca['variacao_ibovespa'] - df_diferenca['soma_contrib']
            # 5. Cria as colunas fixas para a linha "Outros"
            df_diferenca['codigo_ativo'] = 'Outros'
            df_diferenca['radical_ativo'] = 'Outros'
            df_diferenca['subsetor'] = 'Outros'
            df_diferenca['peso'] = 0
            df_diferenca['valor'] = 0
            # 6. Mantém apenas as colunas no formato do df_peso_ibov
            colunas = df_peso_ibov.columns
            df_outros = df_diferenca[colunas]
            # 7. Junta ao df original
            df_peso_ibov_corrigido = pd.concat([df_peso_ibov, df_outros], ignore_index=True)
            
            df_peso_ibov_corrigido.to_excel(rf"{self.path_gestao}/EQUITY/Lucas - Pessoal/perf_atri/performace_ibovespa.xlsx", index=False)
            df_cota.to_clipboard(index=False)
            
            #df_peso_ibov[df_peso_ibov['valor'].isna()].to_clipboard(index=False)
            #df_peso_ibov[df_peso_ibov['codigo_ativo'].isin(['VVAR3'])].to_clipboard(index=False)

            return True, df_contribuicao_carteira
        except Exception as e:
            return False

# start = True
# for i in range(-1335, -857):
#     fa = funcoes_auxiliares(i)
#     data_referencia = fa.data_referencia
#     if start == True:
#         status, df = fa.preenche_dados_opcoes(str(data_referencia))
#         df_full = df.copy()
#         start = False
#     else:    
#         status, df = fa.preenche_dados_opcoes(str(data_referencia))
#         df_full = pd.concat([df_full, df], ignore_index=True)

# df_full.to_csv(f'{fa.path_download}/opcoes.csv', index = False)
# #status, df = fa.insere_pl_fundos()
#status, df = fa.insere_retorno_ajustado()
# status, df = fa.insere_peso_ibovespa()
#teste = fa.insert_taxa_cdi()
#teste = fa.insert_precos_historicos()
# status, df = fa.insere_carteiras_fundos()
# status, df = fa.arquivo_carteira(fa.data_referencia, fundo)

#status, df = fa.classe_ativos_pendendes()
#status, df = fa.setor_ativo_pendente()
#status, df = fa.performace_atribution()

# from datetime import datetime
# status, df = fa.insere_peso_ibovespa_antigo(str_data='2025-07-29')
# fa = funcoes_auxiliares(0)
# #exec_opt = fa.is_terceira_sexta_ou_util_anterior('2025-04-17')
# d_0 = fa.data_referencia
# du = fa.pega_distancia_datas(d_0, datetime.strptime('2027-05-25', '%Y-%m-%d').date())
