import requests
from datetime import date
import pandas as pd
from db import insert_tax_dolar

date_str = date.today().strftime("%m-%d-%Y")

url = ("https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            "CotacaoDolarDia(dataCotacao=@dataCotacao)"
            f"?@dataCotacao='{date_str}'&$format=json&$select=cotacaoCompra,cotacaoVenda")

resp = requests.get(url, timeout=20)
resp.raise_for_status()
valores = resp.json().get("value", [])

# monta o DF (vazio se não houver cotação para a data)
if valores:
    df = pd.DataFrame([{
        "cotacaocompra": valores[0]["cotacaoCompra"],
        "cotacaovenda":  valores[0]["cotacaoVenda"],
        "date_str":      date_str
    }])
else:
    df = pd.DataFrame(columns=["cotacaocompra", "cotacaovenda", "date_str"])

print(df)

afetadas = insert_tax_dolar(df)
print(f"Linhas inseridas/atualizadas: {afetadas}")
