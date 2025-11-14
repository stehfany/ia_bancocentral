# db.py
import os
import math
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def _env_bool(name: str, default: bool = False) -> bool:
    return str(os.getenv(name, str(default))).strip().lower() in ("1", "true", "t", "yes", "y")

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "SEU_BANCO"),
        charset="utf8mb4",
    )

def _to_date(s: str):
    # df["date_str"] vem no formato MM-DD-YYYY
    return datetime.strptime(s, "%m-%d-%Y").date()

def insert_tax_dolar_tipo(df, tipo: str):
    """
    Grava cotações em TABELA ÚNICA conforme o tipo:
      - tipo="venda"  -> tabela tax_dolar_venda (coluna cot_venda)
      - tipo="compra" -> tabela tax_dolar_compra (coluna cot_compra)

    df deve ter: 'date_str' (MM-DD-YYYY) e a coluna correspondente:
      - 'cotacaovenda'  se tipo='venda'
      - 'cotacaocompra' se tipo='compra'

    Retorna: int (linhas afetadas)
    """
    if df is None or df.empty:
        return 0

    tipo = (tipo or "").strip().lower()
    if tipo not in ("venda", "compra"):
        raise ValueError("tipo deve ser 'venda' ou 'compra'")

    # Monta as linhas conforme o tipo escolhido
    rows = []
    if tipo == "venda":
        if "cotacaovenda" not in df.columns:
            raise ValueError("df precisa da coluna 'cotacaovenda' para tipo='venda'")
        for _, r in df.iterrows():
            v = r["cotacaovenda"]
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                rows.append((_to_date(r["date_str"]), float(v)))
        sql = """
            INSERT INTO tax_dolar_venda (data_consul, cot_venda)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE cot_venda = VALUES(cot_venda)
        """
    else:  # tipo == "compra"
        if "cotacaocompra" not in df.columns:
            raise ValueError("df precisa da coluna 'cotacaocompra' para tipo='compra'")
        for _, r in df.iterrows():
            c = r["cotacaocompra"]
            if c is not None and not (isinstance(c, float) and math.isnan(c)):
                rows.append((_to_date(r["date_str"]), float(c)))
        sql = """
            INSERT INTO tax_dolar_compra (data_consul, cot_compra)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE cot_compra = VALUES(cot_compra)
        """

    if not rows:
        return 0

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        afetadas = cur.rowcount
        cur.close()
        return afetadas
    finally:
        conn.close()
