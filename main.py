# main.py
import time
from datetime import datetime, date, timedelta
import requests
import pandas as pd
from db import insert_tax_dolar_tipo  # <- IMPORTANTE: a função que recebe o tipo

#commit

MAX_LOOKBACK_DAYS = 5
SLEEP_SECONDS = 24 * 60 * 60  # 24 horas


def fetch_ptax_df(d: date) -> pd.DataFrame:
    date_str = d.strftime("%m-%d-%Y")
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        "CotacaoDolarDia(dataCotacao=@dataCotacao)"
        f"?@dataCotacao='{date_str}'&$format=json&$select=cotacaoCompra,cotacaoVenda"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    valores = r.json().get("value", [])
    if valores:
        return pd.DataFrame([{
            "cotacaocompra": valores[0]["cotacaoCompra"],
            "cotacaovenda":  valores[0]["cotacaoVenda"],
            "date_str":      date_str
        }])
    return pd.DataFrame(columns=["cotacaocompra", "cotacaovenda", "date_str"])

def job():
    """Busca a cotação (hoje, voltando até 4 dias) e insere venda e compra no MySQL."""
    d = date.today()
    for _ in range(MAX_LOOKBACK_DAYS):
        df = fetch_ptax_df(d)
        if not df.empty:
            now = datetime.now()
            # grava VENDA
            v_rows = insert_tax_dolar_tipo(df, "venda")
            # grava COMPRA
            c_rows = insert_tax_dolar_tipo(df, "compra")
            print(f"[{now}] OK {d.isoformat()} -> venda:{v_rows} compra:{c_rows}")
            return
        d -= timedelta(days=1)
    print(f"[{datetime.now()}] Aviso: nenhuma cotação encontrada nos últimos {MAX_LOOKBACK_DAYS} dias.")

if __name__ == "__main__":
    while True:
        start = datetime.now()
        try:
            job()
        except Exception as e:
            print(f"[{datetime.now()}] ERRO: {e}")

        print(f"[{datetime.now()}] Dormindo por {SLEEP_SECONDS} segundos (~24h).")
        time.sleep(SLEEP_SECONDS)
