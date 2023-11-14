import pandas as pd
import utils
import database
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression


# 0 =  'Ampla Concorrência'
# 1 =  'Escola Pública, renda MENOR ou IGUAL a 1,5 e NÃO PPI'
# 2 =  'Escola Pública, renda MENOR ou IGUAL a 1,5 e PPI'
# 5  =  'Escola Pública, INDEPENDENTE de renda e NÃO PPI'
# 6  =  'Escola Pública, INDEPENDENTE de renda e PPI'

# 0 = Mobilidade Acadêmica
# 0 = Transferência Interna
# 0 = Transferência Facultativa
# 0 = Transfer. Ex-Officio
# 0 = Port. Dipl. Curso Sup
# 0 = Transferência
# 1 = PAIES
# 2 = SISU
# 3 = Vestibular
# 4 = PAAES

# 1 = F
# 2 = M

# 1 = Solteiro
# 2 = Casado
# 3 = Outros

def _prepare_data_base():
    db = database.TrainingSetDataBase()
    dataset = utils.transform_cvs("/home/kyros/PycharmProjects/pythonProject/src"
                                  "/DadosDiscentes-CTI-SistemasInforacao-V2 - Relatorio.csv", " ")
    utils.insert_basic_training_set_datas(dataset, db)


def _create_model(path: str) -> LogisticRegression:
    df = pd.read_csv(path)
    df = df.dropna(axis=1)

    x_train, x_test, y_train, y_test = train_test_split(df.drop("desistente", axis=1), df["desistente"],
                                                        test_size=0.3)
    model = LogisticRegression()

    model.fit(x_train, y_train)

    score = model.score(x_test, y_test)
    print("Score:", score)
    return model


def _calculate_probability(path: str, model):
    training = pd.read_csv(path)
    training = training.dropna(axis=1)

    training["probabilidade"] = model.predict_proba(training.drop("desistente", axis=1))[:, 1]
    print(training)
