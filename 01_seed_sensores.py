import json
from pathlib import Path
from pymongo import ReplaceOne
from tabulate import tabulate
from conexao import get_db

ARQUIVO = Path(__file__).parent / "dados" / "sensores.json"

def main():
    sensores = json.loads(ARQUIVO.read_text(encoding="utf-8"))

    db = get_db()
    ops = [ReplaceOne({"_id": s["_id"]}, s, upsert=True) for s in sensores]
    res = db.Sensores.bulk_write(ops)
    print(f"Inseridos: {res.upserted_count} | Atualizados: {res.modified_count}")

    linhas = [(s["_id"], s["localizacao"]["cidade"], s["descricao"])
              for s in db.Sensores.find().sort("_id", 1)]
    print(tabulate(linhas, headers=["ID", "Cidade", "Descrição"]))

if __name__ == "__main__":
    main()