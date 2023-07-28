from bs4 import BeautifulSoup
import numpy as np
from pdfquery import PDFQuery
import requests
import pandas as pd
import io
import logging

logging.basicConfig(level=logging.INFO)

URL_BASE_BCRP = "https://www.bcrp.gob.pe"
URL_BCRP_STATISTICS = f"{URL_BASE_BCRP}/estadisticas/series"
URL_BCRP_DOCS = f"{URL_BASE_BCRP}/docs"
URL_BASE_BCENTRAL_CHILE = "https://si3.bcentral.cl"
URL_BASE_ELECTRICITY = f"{URL_BCRP_STATISTICS}/mensuales/resultados/PD37966AM/html"
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
    pdf_file_name = "temp_vehicular_flow.pdf"

    response = requests.get(f"{URL_BASE_TOLL}/{year}/1", verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    row1 = soup.find(id="row_1")

    pdf_link = f"{URL_BASE_INEI}{row1.get('rel')}"
    response = requests.get(pdf_link, verify=False)

    with open(pdf_file_name, 'wb') as pdf_file:
        pdf_file.write(response.content)
        pdf = PDFQuery(pdf_file_name)
        pdf.load()
        pdf.tree.write('temp_vehicular_flow.xml', pretty_print=True)

        vehicular_months = pdf.tree.xpath('//LTPage[@pageid="17"]/LTTextLineHorizontal/LTTextBoxHorizontal[@index="5"]')
        print(vehicular_months)
        month = vehicular_months[0].text
        logging.debug(month)

        amounts = pdf.tree.xpath('//LTPage[@pageid="17"]/LTTextLineHorizontal[@y0="715.163"]/LTTextBoxHorizontal')
        amount = amounts[len(amounts) - 1].text
        amount = amount[len(amount)-11:].replace(" ", "")
        logging.debug(amount)


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

    # print(yen_df)

def get_brazilian_real_dolar_exchange(year: int, month: str):
    logging.info("Getting REAL/DOLAR Exchange")
    logging.info("========================")
    real_df = get_dolar_exchange(year, month, "BRL",
                       "dQBoAHMAOABpAGgAMQB2AC4ALQBDAF8AdgBkAFIAUgBWAF8AbQB6AFgAOQBOAGIATgBwAEoAMQBNAE0ARAAuAGQAaQBmADMAUgBtAEsAMQBIAE0AcwBLADYAMwBDAHkAaQBQAFIARQBBAHMAaQBrAE8AZQBUAHoASQBLAEIALgB3AHkAYQBrAGUAWAB5AFcAZABBADcAVgBNADgAQgA0ADkAYwBsAFkAWgBIAG0ALgB1AFkAUQA=")
    # print(real_df)


def get_expected_pbi(month: str, year: int):
    logging.info("Getting Expected PBI")
    logging.info("========================")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    file_content = requests.get(URL_EXPECTED_PBI, verify=False, headers=headers).content
    df = pd.read_excel(io.BytesIO(file_content), usecols="A:D", skiprows=3, sheet_name="PBI")
    logging.info(df.head(20))
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
    get_expected_pbi("Junio", 2023)
    #KPI 12
    get_intern_demand("2023-1", "2023-4")
    #KPI 13
    get_unemployment_rate("2023-1", "2023-6")
    #KPI 18-19
    get_price_index("Abril", 2023)
    #KPI 20
    get_copper_price(2023, 2023)
    #KPI 21
    get_petroleum_wti_price(2023, 2023)

if __name__ == "__main__":
    main()