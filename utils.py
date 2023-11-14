import pandas as pd
import database
import math


def transform_cvs(src_path: str, dest_path: str):
    dataset = pd.read_csv(src_path, encoding='utf-8')
    return pd.DataFrame(dataset)


def _get_estado_civil(estado_civil: str) -> int or None:
    if estado_civil is None:
        return estado_civil
    if estado_civil == 'Solteiro(a)':
        return 1
    if estado_civil == 'Casado(a)':
        return 2
    return 3


def _get_sexo(sexo: str) -> int or None:
    if sexo is None:
        return sexo
    if sexo == 'F':
        return 1
    if sexo == 'M':
        return 2


def _get_forma_ingresso(forma_ingresso: str) -> int or None:
    if forma_ingresso is None:
        return forma_ingresso
    if (forma_ingresso == 'Mobilidade Acadêmica' or forma_ingresso == 'Transferência Interna'
            or forma_ingresso == 'Transferência Facultativa'
            or forma_ingresso == 'Transfer. Ex-Officio'
            or forma_ingresso == 'Port. Dipl. Curso Sup'
            or forma_ingresso == 'Transferência'):
        return 0
    if forma_ingresso == 'PAIES':
        return 1
    if forma_ingresso == 'SISU':
        return 2
    if forma_ingresso == 'Vestibular':
        return 3
    if forma_ingresso == 'PAAES':
        return 4


def _get_modalidade_ingresso(modalidade_ingresso: str) -> int or None:
    if modalidade_ingresso is None:
        return modalidade_ingresso
    if modalidade_ingresso == 'Ampla Concorrência':
        return 0
    if modalidade_ingresso == 'Escola Pública, renda MENOR ou IGUAL a 1,5 e NÃO PPI':
        return 1
    if modalidade_ingresso == 'Escola Pública, renda MENOR ou IGUAL a 1,5 e PPI':
        return 2
    if modalidade_ingresso == 'Escola Pública, INDEPENDENTE de renda e NÃO PPI':
        return 5
    if modalidade_ingresso == 'Escola Pública, INDEPENDENTE de renda e PPI':
        return 6


def _get_idade(idade) -> int or None:
    if idade is None:
        return idade
    if int(idade) <= 18:
        return 0
    if int(idade) <= 25:
        return 1
    return 3


def insert_basic_training_set_datas(dataset, db: database.TrainingSetDataBase):
    disciplinas_cadastras = ['gsi001', 'gsi002', 'gsi003', 'gsi004', 'gsi005']
    for i in range(len(dataset)):
        nro = int(dataset['NRO'][i])
        desistente = int(dataset['FORMA_EVASAO'][i] != "Aluno com Vínculo" and dataset['FORMA_EVASAO'][i] == "Formado")
        sexo = _get_sexo(dataset['SEXO'][i])
        reside_uberlandia = int(dataset['NOME_CIDADE'][i] == "Uberlândia")
        estado_civil = _get_estado_civil(dataset['ESTADO_CIVIL'][i])
        forma_ingresso = _get_forma_ingresso(
            str(dataset['FORMA_INGRESSO'][i]).replace("Processo Seletivo: ", "")
        )
        modalidade_ingresso = _get_modalidade_ingresso(dataset['MODALIDADE_INGRESSO'][i])
        dia, mes, ano = str(dataset['DT_NASCIMENTO'][i]).split("/")
        idade_ingresso = _get_idade(int(dataset['ANO_INGRESSO'][i]) - int(ano))
        cra_abaixo_60_ingresso = int(dataset['CRA_PERIODO_INGRESSO'][i] < 60)
        cod_disciplina = str(dataset['COD_DISCIPLINA'][i]).lower()
        aluno = db.get_by_nro(nro)
        if len(aluno) == 0:
            db.insert_aluno(nro=nro, desistente=desistente, sexo=sexo, reside_uberlandia=reside_uberlandia,
                            estado_civil=estado_civil, forma_ingresso=forma_ingresso,
                            modalidade_ingresso=modalidade_ingresso, idade_ingresso=idade_ingresso,
                            cra_abaixo_60_ingresso=cra_abaixo_60_ingresso)
        ##db.create_discipline_column_on_training_set(cod_disciplina)
        if cod_disciplina in disciplinas_cadastras and not math.isnan(dataset['MEDIA_FINAL'][i]):
            reprovou_na_disciplina = int(int(dataset['MEDIA_FINAL'][i]) < 60)
            db.update_discipline(nro, cod_disciplina, reprovou_na_disciplina)
