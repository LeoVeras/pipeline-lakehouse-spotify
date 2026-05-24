def limpar_nome_coluna(nome):
    """Remove caracteres que o Delta nao aceita em nomes de coluna."""
    nome_limpo = (
        nome.strip()
        .replace(" ", "_")
        .replace(",", "")
        .replace(";", "")
        .replace("(", "")
        .replace(")", "")
    )
    if nome_limpo == "":
        return "coluna_sem_nome"
    return nome_limpo