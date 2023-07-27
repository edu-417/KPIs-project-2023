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
URL_COPPER_PRICE = "https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_EI/MN_EI11/EI_PROD_BAS/637185066927145616"


def get_electricity(start_date: str, end_date: str):
    get_bcrp_data(start_date, end_date, URL_BASE_ELECTRICITY)

def get_pbi(date: str):
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
    print(df[df["Año y Mes"] == date])


def get_intern_demand(start_date: str, end_date: str):
    get_bcrp_data(start_date, end_date, URL_BASE_INTERN_DEMAND)


def get_price_index(month: str, year: int):
    file_content = requests.get(URL_INDEX_PRICE, verify=False).content
    df = pd.read_excel(io.BytesIO(file_content), skiprows=3)
    df = df.fillna(method="ffill")
    # print(df.tail(20))
    print(df[(df["Año"] == year) & (df["Mes"] == month)])


def get_bcrp_data(start_date: str, end_date: str, url: str):
    response = requests.get(f"{url}/{start_date}/{end_date}")
    soup = BeautifulSoup(response.text, 'html.parser')

    periods_td = soup.find_all("td", class_="periodo") 
    values_td = soup.find_all("td", class_="dato")
    for period_td, value_td in zip(periods_td, values_td):
        period = period_td.getText().strip()
        value = value_td.getText().strip()
        print(f"{period} : {value}")


def get_unemployment_rate(start_date: str, end_date: str):
    get_bcrp_data(start_date, end_date, URL_BASE_UNEMPLOYEMENT_RATE)


def get_copper_price(start_year: int, end_year: int, frecuency: str="MONTHLY"):
    COOPER_ROW_INDEX = 4
    params = {
        "cbFechaInicio": start_year,
        "cbFechaTermino": end_year,
        "cbFrecuencia": frecuency,
        "cbCalculo": "NONE",
        "cbFechaBase": ""
    }
    response = requests.get(URL_COPPER_PRICE, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')
    header = soup.css.select("thead > tr > .thData")
    columns = [column.getText() for column in header]
    rows = soup.css.select("#tbodyGrid > tr > td > .sname")
    copper_values = soup.css.select(f"#tbodyGrid > tr:nth-of-type({COOPER_ROW_INDEX}) > .ar")
    values = [float( raw_value.getText().strip().replace(',', '') ) / 10 for raw_value in copper_values]

    for period, value in zip(columns[2:], values):
        print(f"{period}: {value}")

def main():
    #KPI 1
    get_electricity("2023-4", "2023-6")
    #KPI 9
    get_pbi("202304")
    #KPI 12
    get_intern_demand("2023-1", "2023-4")
    #KPI 13
    get_unemployment_rate("2023-1", "2023-6")
    #KPI 18-19
    get_price_index("Abril", 2023)
    #KPI 20
    get_copper_price(2023, 2023)

if __name__ == "__main__":
    main()