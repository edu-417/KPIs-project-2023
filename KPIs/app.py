from bs4 import BeautifulSoup
import requests
import pandas as pd
import io
import logging

logging.basicConfig(level=logging.INFO)


URL_BASE_BCRP = "https://estadisticas.bcrp.gob.pe/estadisticas/series/"
URL_BASE_BCENTRAL_CHILE = "https://si3.bcentral.cl"
URL_BASE_ELECTRICITY = f"{URL_BASE_BCRP}/mensuales/resultados/PD37966AM/html"
URL_BASE_INTERN_DEMAND = f"{URL_BASE_BCRP}/trimestrales/resultados/PN02529AQ/html"
URL_BASE_UNEMPLOYEMENT_RATE = f"{URL_BASE_BCRP}/mensuales/resultados/PN38063GM/html"
URL_INDEX_PRICE = "https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/02_indice-precios_al_consumidor-nivel_nacional_2b_16.xlsx"
URL_RAW_MATERIAL_PRICE = f"{URL_BASE_BCENTRAL_CHILE}/Siete/ES/Siete/Cuadro/CAP_EI/MN_EI11/EI_PROD_BAS/637185066927145616"
URL_DOLAR_EXCHANGE_RATE = f"{URL_BASE_BCRP}/diarias/resultados/PD04638PD/html"
URL_EURO_EXCHANGE_RATE = f"{URL_BASE_BCRP}/diarias/resultados/PD04648PD/html"


def get_electricity(start_date: str, end_date: str):
    logging.info("Getting Electricity(GWH)")
    logging.info("========================")
    electricity_df = get_bcrp_data(start_date, end_date, URL_BASE_ELECTRICITY)
    logging.debug(electricity_df)
    logging.info("Got Electricity")

def get_pbi(date: str):
    logging.info("Getting PBI")
    logging.info("========================")
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
    logging.debug(df[df["Año y Mes"] == date])
    logging.info("Got PBI")


def get_intern_demand(start_date: str, end_date: str):
    logging.info("Getting Intern Demand")
    logging.info("========================")
    intern_demand_df = get_bcrp_data(start_date, end_date, URL_BASE_INTERN_DEMAND)
    logging.debug(intern_demand_df)
    logging.info("Got intern demand")


def get_price_index(month: str, year: int):
    logging.info("Getting Price Index")
    logging.info("========================")
    file_content = requests.get(URL_INDEX_PRICE, verify=False).content
    df = pd.read_excel(io.BytesIO(file_content), skiprows=3)
    df = df.fillna(method="ffill")
    # print(df.tail(20))
    logging.debug(df[(df["Año"] == year) & (df["Mes"] == month)])
    logging.info("Got PBI")


def get_bcrp_data(start_date: str, end_date: str, url: str):
    response = requests.get(f"{url}/{start_date}/{end_date}")
    soup = BeautifulSoup(response.text, 'html.parser')

    periods_td = soup.find_all("td", class_="periodo") 
    values_td = soup.find_all("td", class_="dato")

    periods = [period_td.getText().strip() for period_td in periods_td]
    values = [value_td.getText().strip() for value_td in values_td]
    
    data = {"Period": periods, "Value": values}

    return pd.DataFrame(data)


def get_unemployment_rate(start_date: str, end_date: str):
    logging.info("Getting Unemployment Rate")
    logging.info("========================")
    unemployment_rate_df = get_bcrp_data(start_date, end_date, URL_BASE_UNEMPLOYEMENT_RATE)
    logging.debug(unemployment_rate_df)
    logging.info("Got Unemployment Rate")


def get_raw_material_price(start_year: int, end_year: int, row_index: int, frequency: str="MONTHLY"):
    params = {
        "cbFechaInicio": start_year,
        "cbFechaTermino": end_year,
        "cbFrecuencia": frequency,
        "cbCalculo": "NONE",
        "cbFechaBase": ""
    }
    response = requests.get(URL_RAW_MATERIAL_PRICE, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')
    header = soup.css.select("thead > tr > .thData")
    columns = [column.getText() for column in header]
    rows = soup.css.select("#tbodyGrid > tr > td > .sname")

    raw_material_values = soup.css.select(f"#tbodyGrid > tr:nth-of-type({row_index}) > .ar")
    material_values = [float( raw_value.getText().strip().replace(',', '') ) for raw_value in raw_material_values]

    data = {"Period": columns[2:], "Price": material_values}
    return pd.DataFrame(data)

def get_copper_price(start_year: int, end_year: int, frequency: str="MONTHLY"):
    logging.info("Getting Copper Price")
    logging.info("========================")
    COOPER_ROW_INDEX = 4

    copper_price_df = get_raw_material_price(start_year, end_year, COOPER_ROW_INDEX, frequency)
    copper_price_df["Price"] /= 10

    logging.debug(copper_price_df)
    logging.info("Got Copper Price")


def get_petroleum_wti_price(start_year: int, end_year: int, frequency: str="MONTHLY"):
    logging.info("Getting Petroleum WTI Price")
    logging.info("========================")
    PETROLEUM_WTI_INDEX = 9

    petroleum_wti_df = get_raw_material_price(start_year, end_year, PETROLEUM_WTI_INDEX, frequency)
    
    logging.debug(petroleum_wti_df)
    logging.info("Got Petroleum WTI Price")


def get_dolar_exchange_rate(start_date: str, end_date: str):
    logging.info("Getting Dolar Exchange")
    logging.info("========================")
    dolar_exchange_rate_df = get_bcrp_data(start_date, end_date, URL_DOLAR_EXCHANGE_RATE)
    logging.debug(dolar_exchange_rate_df)
    logging.info("Got Dolar Exchange")


def get_euro_exchange_rate(start_date: str, end_date: str):
    logging.info("Getting Euro Exchange")
    logging.info("========================")
    euro_exchange_rate_df = get_bcrp_data(start_date, end_date, URL_EURO_EXCHANGE_RATE)
    logging.debug(euro_exchange_rate_df)
    logging.info("Got Euro Exchange")


def main():
    #KPI 1
    get_electricity("2023-4", "2023-6")
    #KPI 3
    get_dolar_exchange_rate("2023-06-20", "2023-06-30")
    #KPI 4
    get_euro_exchange_rate("2023-06-20", "2023-06-30")
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
    #KPI 20
    get_petroleum_wti_price(2023, 2023)

if __name__ == "__main__":
    main()