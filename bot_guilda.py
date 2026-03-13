import requests
from bs4 import BeautifulSoup
import re
import time

headers = {
    "User-Agent": "Mozilla/5.0"
}


def pegar_membros(nome_guilda):
    url = f"https://www.rucoyonline.com/guild/{nome_guilda.replace(' ','%20')}"
    r = requests.get(url, headers=headers)

    soup = BeautifulSoup(r.text, "html.parser")

    membros = []

    tabela = soup.find("table")

    for row in tabela.find_all("tr"):
        cols = row.find_all("td")

        if len(cols) >= 2:
            nome = cols[0].get_text(strip=True)
            membros.append(nome)

    return membros


def last_online(nome):

    url = f"https://www.rucoyonline.com/characters/{nome.replace(' ','%20')}"
    r = requests.get(url, headers=headers)

    soup = BeautifulSoup(r.text, "html.parser")

    linhas = soup.select("table.character-table tr")

    for row in linhas:
        cols = row.find_all("td")

        if len(cols) >= 2:
            titulo = cols[0].get_text(strip=True).lower()

            if titulo == "last online":

                texto = cols[1].get_text(strip=True).lower()

                if "currently online" in texto:
                    return 0

                match = re.search(r"(\d+)", texto)

                if not match:
                    return None

                numero = int(match.group(1))

                if "day" in texto:
                    return numero
                elif "week" in texto:
                    return numero * 7
                elif "month" in texto:
                    return numero * 30
                elif "year" in texto:
                    return numero * 365

    return None


def analisar(guilda):

    membros = pegar_membros(guilda)

    inativos10 = []
    inativos20 = []

    print(f"\nAnalisando guilda: {guilda}")
    print(f"Membros encontrados: {len(membros)}\n")

    for nome in membros:

        dias = last_online(nome)

        print(nome, "->", dias)

        if dias is None:
            continue

        if dias >= 10:
            inativos10.append((nome, dias))

        if dias >= 20:
            inativos20.append((nome, dias))

        time.sleep(1)

    print("\n============================")
    print("INATIVOS +10 DIAS")
    print("============================")

    for nome, dias in inativos10:
        print(f"{nome} - {dias} dias")

    print("\n============================")
    print("INATIVOS +20 DIAS")
    print("============================")

    for nome, dias in inativos20:
        print(f"{nome} - {dias} dias")


if __name__ == "__main__":

    guilda = input("Nome da guilda: ")

    analisar(guilda)
