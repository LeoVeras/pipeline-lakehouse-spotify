import sys
sys.path.append('scripts')

from utils import limpar_nome_coluna


def test_remove_espaco():
    assert limpar_nome_coluna("Unnamed: 0") == "Unnamed:_0"

def test_remove_virgula():
    assert limpar_nome_coluna("a,b") == "ab"

def test_remove_parenteses():
    assert limpar_nome_coluna("col(x)") == "colx"

def test_coluna_vazia_recebe_nome_padrao():
    assert limpar_nome_coluna("") == "coluna_sem_nome"

def test_remove_espaco_das_pontas():
    assert limpar_nome_coluna("  track_id  ") == "track_id"