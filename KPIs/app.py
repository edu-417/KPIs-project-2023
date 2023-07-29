from bs4 import BeautifulSoup
import numpy as np
from pdfquery import PDFQuery
from pdfquery.cache import FileCache
import requests
import pandas as pd
import io
import logging
import coloredlogs

logging.basicConfig(level=logging.INFO)
coloredlogs.install(level=logging.INFO)

URL_BASE_BCRP = "https://www.bcrp.gob.pe"
URL_BCRP_STATISTICS = "https://estadisticas.bcrp.gob.pe/estadisticas/series"
URL_BCRP_DOCS = f"{URL_BASE_BCRP}/docs"
URL_BASE_BCENTRAL_CHILE = "https://si3.bcentral.cl"
URL_BASE_ELECTRICITY = f"{URL_BCRP_STATISTICS}/mensuales/resultados/PD37966AM/html"
URL_BASE_ML = "https://mlback.btgpactual.cl/instruments/"
URL_PBI = "https://www.inei.gob.pe/media/principales_indicadores/CalculoPBI_120.zip"
URL_BASE_INEI = "https://www.inei.gob.pe"
URL_BASE_INTERN_DEMAND = f"{URL_BCRP_STATISTICS}/trimestrales/resultados/PN02529AQ/html"
URL_BASE_UNEMPLOYEMENT_RATE = f"{URL_BCRP_STATISTICS}/mensuales/resultados/PN38063GM/html"
URL_BASE_TOLL = f"{URL_BASE_INEI}/biblioteca-virtual/boletines/flujo-vehicular"
URL_INDEX_PRICE = f"{URL_BASE_INEI}/media/MenuRecursivo/indices_tematicos/02_indice-precios_al_consumidor-nivel_nacional_2b_16.xlsx"
URL_RAW_MATERIAL_PRICE = f"{URL_BASE_BCENTRAL_CHILE}/Siete/ES/Siete/Cuadro/CAP_EI/MN_EI11/EI_PROD_BAS/637185066927145616"
URL_DOLAR_EXCHANGE_RATE = f"{URL_BCRP_STATISTICS}/diarias/resultados/PD04638PD/html"
URL_EURO_EXCHANGE_RATE = f"{URL_BCRP_STATISTICS}/diarias/resultados/PD04648PD/html"
URL_DOLAR_EXCHANGE = f"{URL_BASE_BCENTRAL_CHILE}/Indicadoressiete/secure/Serie.aspx"
URL_EXPECTED_PBI = f"{URL_BCRP_DOCS}/Estadisticas/Encuestas/expectativas-pbi.xlsx"

def get_electricity(start_date: str, end_date: str):
    logging.info("Getting Electricity(GWH)")
    logging.info("========================")
    electricity_df = get_bcrp_data(start_date, end_date, URL_BASE_ELECTRICITY)
    logging.debug(electricity_df)
    logging.info("Got Electricity")


def get_vehicular_flow(year: str):
    logging.info("Getting Vehicular Flow")
    logging.info("========================")
    pdf_file_name = "temp_vehicular_flow.pdf"

    response = requests.get(f"{URL_BASE_TOLL}/{year}/1", verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    row1 = soup.find(id="row_1")

    pdf_link = f"{URL_BASE_INEI}{row1.get('rel')}"
    response = requests.get(pdf_link, verify=False)

    with open(pdf_file_name, 'wb') as pdf_file:
        pdf_file.write(response.content)
        pdf = PDFQuery(pdf_file_name, parse_tree_cacher=FileCache("."))
        pdf.load()
        # pdf.tree.write('temp_vehicular_flow.xml', pretty_print=True)

        lttext_months = pdf.tree.xpath(
            '//LTPage[@pageid="13"]/LTRect/LTTextLineVertical/LTTextBoxVertical')  # [@y0="758.48"]')
        max_y0 = 0.0
        month = ""
        for i in lttext_months:
            y0 = float(i.get('y0'))
            if y0 > max_y0:
                y0 = max_y0
                month = i.text
        logging.debug(month)

        lttext_amounts = pdf.tree.xpath(
            '//LTPage[@pageid="13"]/LTTextLineVertical/LTTextBoxVertical')  # [@y0="743.368"]')
        min_dist = float("inf")
        amount = ""
        for i in lttext_amounts:
            y0 = float(i.get('y0'))
            x0 = float(i.get('x0'))
            dist = (900 - y0) + x0
            if dist < min_dist:
                min_dist = dist
                amount = i.text
        amount = amount[max(len(amount) - 11, 0):].replace(" ", "")
        logging.debug(amount)

        logging.info("Got Vehicular Flow")


def get_pbi(date: str):
    logging.info("Getting PBI")
    logging.info("========================")
    file_content = requests.get(URL_PBI, verify=False).content

    import zipfile
    with zipfile.ZipFile(io.BytesIO(file_content)) as archive:
        logging.debug(archive.namelist())

        pbi_file_name = [file_name for file_name in archive.namelist() if "VA-PBI" in file_name][0]
        logging.debug(f"pbi_file_name: {pbi_file_name}")
        with archive.open(pbi_file_name) as file:
            df = pd.read_excel(file, usecols="A:B", skiprows=3)
            df = df.dropna()
            df["Año y Mes"] = df["Año y Mes"].astype("str")

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
    logging.info("Got Price Index")


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


def get_ml_rate(rate_id, start_date, end_date):
    start_date = f"{start_date[:len(start_date) - 2]}01" # guarantee that first month exists

    url = f"{URL_BASE_ML}{rate_id}/historicalData"
    params = {
        "dateStart": start_date,
        "dateEnd": end_date,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"
    }
    response = requests.get(url, params=params, headers=headers, verify=False)
    jsonResponse = response.json()

    last_days_dict = {}
    rates_dict = {}
    for value in jsonResponse["chart"]:
        date = pd.to_datetime(value["x"], utc=True, unit='ms')
        rate = value["y"]
        year_month = f"{date.year}-{date.month}"
        if year_month in last_days_dict:
            if date.day > last_days_dict[year_month]:
                last_days_dict[year_month] = date.day
                rates_dict[year_month] = rate
        else:
            last_days_dict[year_month] = date.day
            rates_dict[year_month] = rate

    date_list = []
    for key in last_days_dict:
        date_list.append(f"{key}-{last_days_dict[key]}")

    df = pd.DataFrame(zip(date_list, rates_dict.values()), columns=['date', 'rate'])
    return df


def get_5years_treasury_bill_rate(start_date: str, end_date: str):
    logging.info("Getting 5 Years Treasury Bill Rates")
    logging.info("========================")
    rate_id = "UlRFLlVTVFI1WS5JU0YuRk0"
    df = get_ml_rate(rate_id, start_date, end_date)
    logging.debug(df)
    logging.info("Got 5 Years Treasury Bill Rates")


def get_10years_treasury_bill_rate(start_date: str, end_date: str):
    logging.info("Getting 10 Years Treasury Bill Rates")
    logging.info("========================")
    rate_id = "UlRFLlVTVFIxMFkuSVNGLkZN"
    df = get_ml_rate(rate_id, start_date, end_date)
    logging.debug(df)
    logging.info("Got 10 Years Treasury Bill Rates")


def get_djones_rate(start_date: str, end_date: str):
    logging.info("Getting Dow Jones Rates")
    logging.info("========================")
    rate_id = "SU5ELkRPV0pPTkVTLklORkJPTA"
    df = get_ml_rate(rate_id, start_date, end_date)
    logging.debug(df)
    logging.info("Got Dow Jones Rates")


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


def get_dolar_exchange(year: int, month: str, currency_code: str, param: str):
    params = {
        "gcode": f"PAR_{currency_code}",
        "param": param
    }

    data = {
        "DrDwnFechas": year,
        "hdnFrecuencia": "DAILY"
    }

    response = requests.post(URL_DOLAR_EXCHANGE, params=params, data=data)
    soup = BeautifulSoup(response.text, 'html.parser')

    values = []
    for day in range(1, 31):
        id = f"gr_ctl{(day + 1):02d}_{month}"
        value_td = soup.find(id=id)
        values.append(value_td.getText().strip())
    data = {"Day": np.arange(1, 31), "Value": values}

    df = pd.DataFrame(data)
    df.replace('', np.nan, inplace=True)
    df.dropna(inplace=True)
    df["Value"] = df["Value"].str.replace(",", "")
    df["Value"] = df["Value"].astype(float)

    return df


def get_yen_dolar_exchange(year: int, month: str):
    logging.info("Getting YEN/DOLAR Exchange")
    logging.info("========================")
    yen_df = get_dolar_exchange(year, month, "JPY",
                       "cgBnAE8AOQBlAGcAIwBiAFUALQBsAEcAYgBOAEkASQBCAEcAegBFAFkAeABkADgASAA2AG8AdgB2AFMAUgBYADIAQwBzAEEARQBMAG8AawBzACMATABOAHMARgB1ADIAeQBBAFAAZwBhADIAbABWAHcAXwBXAGgATAAkAFIAVAB1AEIAbAB3AFoAdQBRAFgAZwA5AHgAdgAwACQATwBZADcAMwAuAGIARwBFAFIASwAuAHQA")

    yen_df["Value"] /= 10000

    logging.debug(yen_df)

def get_brazilian_real_dolar_exchange(year: int, month: str):
    logging.info("Getting REAL/DOLAR Exchange")
    logging.info("========================")
    real_df = get_dolar_exchange(year, month, "BRL",
                       "dQBoAHMAOABpAGgAMQB2AC4ALQBDAF8AdgBkAFIAUgBWAF8AbQB6AFgAOQBOAGIATgBwAEoAMQBNAE0ARAAuAGQAaQBmADMAUgBtAEsAMQBIAE0AcwBLADYAMwBDAHkAaQBQAFIARQBBAHMAaQBrAE8AZQBUAHoASQBLAEIALgB3AHkAYQBrAGUAWAB5AFcAZABBADcAVgBNADgAQgA0ADkAYwBsAFkAWgBIAG0ALgB1AFkAUQA=")
    logging.debug(real_df)


def get_expected_pbi(year: int):
    logging.info("Getting Expected PBI")
    logging.info("========================")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
    }
    file_content = requests.get(URL_EXPECTED_PBI, verify=False, headers=headers).content
    logging.debug(file_content)
    df = pd.read_excel(io.BytesIO(file_content), usecols="A:D", skiprows=3, sheet_name="PBI")
    columns = df.columns
    df["Expected Year"] = df["Fecha"]
    condition = df["Expected Year"].str.contains("Expectativas")
    df.loc[condition == False, "Expected Year"] = np.nan
    df["Expected Year"] = df["Expected Year"].fillna(method="ffill")
    df = df.loc[df["Expected Year"] == f"Expectativas anuales de {year}", columns]
    logging.debug(df)
    logging.info("Got Expected PBI")


def main():
    #KPI 1
    get_electricity("2023-4", "2023-6")
    # KPI 2
    get_vehicular_flow("2023")
    #KPI 3
    get_dolar_exchange_rate("2023-06-20", "2023-06-30")
    #KPI 4
    get_euro_exchange_rate("2023-06-20", "2023-06-30")
    #KPI 5
    get_yen_dolar_exchange(2023, "Julio")
    #KPI 6
    get_brazilian_real_dolar_exchange(2023, "Julio")
    #KPI 9
    get_pbi("202304")
    #KPI 10
    get_expected_pbi(2023)
    #KPI 12
    get_intern_demand("2023-1", "2023-4")
    #KPI 13
    get_unemployment_rate("2023-1", "2023-6")
    #KPI 16
    get_5years_treasury_bill_rate("2022-06-30", "2023-07-31")
    # KPI 17
    get_10years_treasury_bill_rate("2022-06-30", "2023-07-31")
    #KPI 18-19
    get_price_index("Abril", 2023)
    #KPI 20
    get_copper_price(2023, 2023)
    #KPI 21
    get_petroleum_wti_price(2023, 2023)
    # KPI 24
    get_djones_rate("2022-06-30", "2023-07-31")

if __name__ == "__main__":
    main()