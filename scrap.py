from bs4 import BeautifulSoup
import requests
import datetime
import pandas as pd
import io


URL_BASE_BCRP = "https://estadisticas.bcrp.gob.pe/estadisticas/series/"
URL_BASE_ELECTRICITY = f"{URL_BASE_BCRP}/mensuales/resultados/PD37966AM/html"
URL_BASE_INTERN_DEMAND = f"{URL_BASE_BCRP}/trimestrales/resultados/PN02529AQ/html"
URL_BASE_UNEMPLOYEMENT_RATE = f"{URL_BASE_BCRP}/mensuales/resultados/PN38063GM/html"
URL_INDEX_PRICE = "https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/02_indice-precios_al_consumidor-nivel_nacional_2b_16.xlsx"
now = datetime.datetime.now()
# print(now)
start = "2023-6"
end = "2023-6"


def get_electricity(start, end):
    get_bcrp_data(start, end, URL_BASE_ELECTRICITY)

def get_pbi(date_str):
    # TODO
    # 1. Download File (url)
    # 2. Descomprimir archivo descargado
    # 3. Buscar archivo que comienza con ("VBA-PBI")

    pd.set_option('display.max_colwidth', None)
    df = pd.read_excel("CalculoPBI_120/VA-PBI 05 2023 B 2007 r.xlsx", usecols="A:B", skiprows=3)
    # print(df.columns)
    df = df.dropna()
    # print(df.head(20))
    # print(df.tail(20))
    df["Año y Mes"] = df["Año y Mes"].astype("str")
    # print(df["Año y Mes"])
    print(df[df["Año y Mes"] == date_str])


def get_intern_demand(start, end):
    get_bcrp_data(start, end, URL_BASE_INTERN_DEMAND)


def get_price_index(month, year):
    file_content = requests.get(URL_INDEX_PRICE, verify=False).content
    df = pd.read_excel(io.BytesIO(file_content), skiprows=3)
    df = df.fillna(method="ffill")
    # print(df.tail(20))
    print(df[(df["Año"] == year) & (df["Mes"] == month)])


def get_bcrp_data(start, end, url):
    response = requests.get(f"{url}/{start}/{end}")
    soup = BeautifulSoup(response.text, 'html.parser')

    dates_str = soup.find_all("td", class_="periodo") 
    values = soup.find_all("td", class_="dato")
    for date_str, value in zip(dates_str, values):
        print(f"{date_str.getText().strip()} : {value.getText().strip()}")


def get_unemployment_rate(start, end):
    get_bcrp_data(start, end, URL_BASE_UNEMPLOYEMENT_RATE)


def main():
    get_electricity("2023-4", "2023-6")
    get_pbi("202304")
    get_intern_demand("2023-1", "2023-4")
    get_price_index("Abril", 2023)
    get_unemployment_rate("2023-1", "2023-6")

if __name__ == "__main__":
    main()