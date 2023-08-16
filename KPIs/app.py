import datetime
import io
import logging
import zipfile

import coloredlogs
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pdfquery import PDFQuery
from pdfquery.cache import FileCache

coloredlogs.install(level=logging.INFO)

USER_AGENT = (
    "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like"
    " Gecko) Chrome/81.0.4044.141 Safari/537.36"
)

URL_BASE_BCRP = "https://www.bcrp.gob.pe"
URL_BCRP_STATISTICS = "https://estadisticas.bcrp.gob.pe/estadisticas/series"
URL_BCRP_DOCS = f"{URL_BASE_BCRP}/docs"
URL_BASE_BCENTRAL_CHILE = "https://si3.bcentral.cl"
URL_BASE_ELECTRICITY = f"{URL_BCRP_STATISTICS}/api/PD37966AM/json"
URL_BASE_ML = "https://mlback.btgpactual.cl/instruments/"
URL_SP_BVL = (
    "https://www.spglobal.com/spdji/es/util/redesign/index-data/"
    "get-performance-data-for-datawidget-redesign.dot"
)
URL_SBS_TC = (
    "https://www.sbs.gob.pe/app/pp/sistip_portal/paginas/publicacion/"
    "tipocambiopromedio.aspx"
)
URL_PBI = (
    "https://www.inei.gob.pe/media/principales_indicadores/CalculoPBI_120.zip"
)
URL_BASE_INEI = "https://www.inei.gob.pe"
URL_BASE_INTERN_DEMAND = f"{URL_BCRP_STATISTICS}/api/PN02529AQ/json"
URL_BASE_UNEMPLOYEMENT_RATE = f"{URL_BCRP_STATISTICS}/api/PN38063GM/json"
URL_BASE_TOLL = f"{URL_BASE_INEI}/biblioteca-virtual/boletines/flujo-vehicular"
URL_INEI_PRICE_INDEX = (
    f"{URL_BASE_INEI}/estadisticas/indice-tematico/price-indexes/"
)
URL_RAW_MATERIAL_PRICE = (
    f"{URL_BASE_BCENTRAL_CHILE}/Siete/ES/Siete/Cuadro/CAP_EI/MN_EI11/"
    "EI_PROD_BAS/637185066927145616"
)
URL_DOLAR_EXCHANGE_RATE = f"{URL_BCRP_STATISTICS}/api/PD04638PD/json"
URL_EURO_EXCHANGE_RATE = f"{URL_BCRP_STATISTICS}/api/PD04648PD/json"
URL_DOLAR_EXCHANGE = (
    f"{URL_BASE_BCENTRAL_CHILE}/Indicadoressiete/secure/Serie.aspx"
)
URL_EXPECTED_PBI = (
    f"{URL_BCRP_DOCS}/Estadisticas/Encuestas/expectativas-pbi.xlsx"
)
URL_MONETARY_POLICIE_RATE = f"{URL_BCRP_STATISTICS}/api/PD12301MD/json"
URL_PERUVIAN_GOVERMENT_BOND = f"{URL_BCRP_STATISTICS}/api/PD31896MM/json"


def get_electricity(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Electricity(GWH)")
    logging.info("========================")
    electricity_df = get_bcrp_data(start_date, end_date, URL_BASE_ELECTRICITY)
    logging.debug(electricity_df)
    logging.info("Got Electricity")

    return electricity_df


def get_vehicular_flow(year: str) -> pd.DataFrame:
    logging.info("Getting Vehicular Flow")
    logging.info("========================")
    pdf_file_name = "temp_vehicular_flow.pdf"

    response = requests.get(f"{URL_BASE_TOLL}/{year}/1", verify=False)
    soup = BeautifulSoup(response.text, "html.parser")
    row1 = soup.find(id="row_1")

    pdf_link = f"{URL_BASE_INEI}{row1.get('rel')}"
    response = requests.get(pdf_link, verify=False)

    with open(pdf_file_name, "wb") as pdf_file:
        pdf_file.write(response.content)
        pdf = PDFQuery(pdf_file_name, parse_tree_cacher=FileCache("."))
        pdf.load()
        # pdf.tree.write('temp_vehicular_flow.xml', pretty_print=True)

        lttext_months = pdf.tree.xpath(
            '//LTPage[@pageid="13"]/LTRect/LTTextLineVertical/LTTextBoxVertical'
        )  # [@y0="758.48"]')
        max_y0 = 0.0
        month = ""
        for i in lttext_months:
            y0 = float(i.get("y0"))
            if y0 > max_y0:
                y0 = max_y0
                month = i.text

        lttext_amounts = pdf.tree.xpath(
            '//LTPage[@pageid="13"]/LTTextLineVertical/LTTextBoxVertical'
        )  # [@y0="743.368"]')
        min_dist = float("inf")
        amount = ""
        for i in lttext_amounts:
            y0 = float(i.get("y0"))
            x0 = float(i.get("x0"))
            dist = (900 - y0) + x0
            if dist < min_dist:
                min_dist = dist
                amount = i.text
        amount_value = int(amount[max(len(amount) - 11, 0) :].replace(" ", ""))

        logging.info("Got Vehicular Flow")
        date = f"{year}-{month}"
        logging.debug(date)
        df = pd.DataFrame(
            {"Period": [date], "Value": [amount_value]}
        ).set_index("Period")
        logging.debug(df)
        return df


def get_pbi(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting PBI")
    logging.info("========================")
    file_content = requests.get(URL_PBI, verify=False).content

    with zipfile.ZipFile(io.BytesIO(file_content)) as archive:
        logging.debug(archive.namelist())

        pbi_file_name = [
            file_name
            for file_name in archive.namelist()
            if "VA-PBI" in file_name
        ][0]
        logging.debug(f"pbi_file_name: {pbi_file_name}")

        with archive.open(pbi_file_name) as file:
            df = pd.read_excel(file, usecols="A:B", skiprows=3)
            df = df.dropna()
            df["Año y Mes"] = pd.to_datetime(df["Año y Mes"], format="%Y%m")
            df.set_index("Año y Mes", inplace=True)
            df = df.loc[start_date:end_date]
            df.reset_index(inplace=True)
            df["Año y Mes"] = df["Año y Mes"].dt.strftime("%Y-%m")
            df.set_index("Año y Mes", inplace=True)

            logging.debug(df)
            logging.info("Got PBI")

            return df


def get_intern_demand(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Intern Demand")
    logging.info("========================")
    intern_demand_df = get_bcrp_data(
        start_date, end_date, URL_BASE_INTERN_DEMAND
    )
    logging.debug(intern_demand_df)
    logging.info("Got intern demand")

    return intern_demand_df


def get_price_index(year: int, month: str) -> pd.DataFrame:
    logging.info("Getting Price Index")
    logging.info("========================")
    response = requests.get(URL_INEI_PRICE_INDEX, verify=False)
    soup = BeautifulSoup(response.text, "html.parser")
    anchor = soup.select("a[title='IPC Nacional']")[0]
    link = f"{URL_BASE_INEI}{anchor.get('href')}"
    file_content = requests.get(link, verify=False).content
    df = pd.read_excel(io.BytesIO(file_content), skiprows=3)
    df = df.fillna(method="ffill")
    df["Año"] = df["Año"].astype(int)
    df = df[(df["Año"] == int(year)) & (df["Mes"] == month)]
    logging.debug(df)
    logging.info("Got Price Index")

    return df


def get_bcrp_data(start_date: str, end_date: str, url: str) -> pd.DataFrame:
    response = requests.get(f"{url}/{start_date}/{end_date}")
    logging.debug(f"response: {response.json()}")
    json_response = response.json()

    data = json_response["periods"]

    df = pd.DataFrame(data, columns=["name", "values"])
    df.columns = ["Period", "Value"]
    df.set_index("Period", inplace=True)
    df = df.explode("Value")
    df["Value"] = df["Value"].astype(float)

    return df


def format_values_per_month(
    data,
    start_date_str: str,
    end_date_str: str,
    index_value_name: str,
    index_date_name: str,
    divisor=1,
):
    last_days_dict = {}
    rates_dict = {}
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")

    for value in data:
        date = pd.to_datetime(value[index_date_name], utc=True, unit="ms")

        if date.date() < start_date.date():
            continue

        if date.date() > end_date.date():
            continue

        rate = value[index_value_name]
        year_month = f"{date.year}-{date.month}"
        if year_month in last_days_dict:
            if date.day > last_days_dict[year_month]:
                last_days_dict[year_month] = date.day
                rates_dict[year_month] = rate / divisor
        else:
            last_days_dict[year_month] = date.day
            rates_dict[year_month] = rate / divisor

    date_list = []
    for key in last_days_dict:
        date_list.append(f"{key}-{last_days_dict[key]}")

    df = pd.DataFrame(
        zip(date_list, rates_dict.values()), columns=["date", "rate"]
    )
    return df


def get_unemployment_rate(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Unemployment Rate")
    logging.info("========================")
    unemployment_rate_df = get_bcrp_data(
        start_date, end_date, URL_BASE_UNEMPLOYEMENT_RATE
    )
    logging.debug(unemployment_rate_df)
    logging.info("Got Unemployment Rate")

    return unemployment_rate_df


def get_month_1st(start_date: str):
    return f"{start_date}-01"


def get_month_last(start_date: str):
    date_time = datetime.date(
        int(start_date[0:4]), int(start_date[5:7]) + 1, 1
    ) - datetime.timedelta(days=1)
    current_time = datetime.datetime.now().date()
    if current_time < date_time:
        date_time = current_time
    return date_time.strftime("%Y-%m-%d")


def get_ml_rate(
    rate_id: str, start_date: str, end_date: str, divisor=100
) -> pd.DataFrame:
    start_date = get_month_1st(start_date)
    end_date = get_month_last(end_date)

    url = f"{URL_BASE_ML}{rate_id}/historicalData"
    params = {
        "dateStart": start_date,
        "dateEnd": end_date,
    }
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, params=params, headers=headers, verify=False)
    jsonResponse = response.json()

    return format_values_per_month(
        jsonResponse["chart"], start_date, end_date, "y", "x", divisor
    )


def get_5years_treasury_bill_rate(
    start_date: str, end_date: str
) -> pd.DataFrame:
    logging.info("Getting 5 Years Treasury Bill Rates")
    logging.info("========================")
    rate_id = "UlRFLlVTVFI1WS5JU0YuRk0"
    df = get_ml_rate(rate_id, start_date, end_date, 100)
    logging.debug(df)
    logging.info("Got 5 Years Treasury Bill Rates")

    return df


def get_10years_treasury_bill_rate(
    start_date: str, end_date: str
) -> pd.DataFrame:
    logging.info("Getting 10 Years Treasury Bill Rates")
    logging.info("========================")
    rate_id = "UlRFLlVTVFIxMFkuSVNGLkZN"
    df = get_ml_rate(rate_id, start_date, end_date, 100)
    logging.debug(df)
    logging.info("Got 10 Years Treasury Bill Rates")

    return df


def get_djones_rate(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Dow Jones Rates")
    logging.info("========================")
    rate_id = "SU5ELkRPV0pPTkVTLklORkJPTA"
    df = get_ml_rate(rate_id, start_date, end_date, 1)
    logging.debug(df)
    logging.info("Got Dow Jones Rates")

    return df


def get_sp_bvl_general_index(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting SP BVL General indexes")
    logging.info("========================")

    start_date = get_month_1st(start_date)
    end_date = get_month_last(end_date)

    url = URL_SP_BVL
    params = {
        "indexId": "92026288",
        "language_id": "2",
        "_": end_date,
    }
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, params=params, headers=headers, verify=False)
    jsonResponse = response.json()

    df = format_values_per_month(
        jsonResponse["indexLevelsHolder"]["indexLevels"],
        start_date,
        end_date,
        "indexValue",
        "effectiveDate",
    )
    logging.debug(df)
    logging.info("Got SP BVL General indexes")

    return df


def get_raw_material_price(
    start_year: int, end_year: int, row_index: int, frequency: str = "MONTHLY"
):
    params = {
        "cbFechaInicio": start_year,
        "cbFechaTermino": end_year,
        "cbFrecuencia": frequency,
        "cbCalculo": "NONE",
        "cbFechaBase": "",
    }
    response = requests.get(URL_RAW_MATERIAL_PRICE, params=params)
    soup = BeautifulSoup(response.text, "html.parser")
    header = soup.select("thead > tr > .thData")
    columns = [column.getText() for column in header]
    rows = soup.select("#tbodyGrid > tr > td > .sname")
    logging.debug(rows)

    raw_material_values = soup.select(
        f"#tbodyGrid > tr:nth-of-type({row_index}) > .ar"
    )
    material_values = [
        float(raw_value.getText().strip().replace(",", ""))
        for raw_value in raw_material_values
    ]

    data = {"Period": columns[2:], "Price": material_values}

    return pd.DataFrame(data).set_index("Period")


def get_copper_price(
    start_year: int, end_year: int, frequency: str = "MONTHLY"
) -> pd.DataFrame:
    logging.info("Getting Copper Price")
    logging.info("========================")
    COOPER_ROW_INDEX = 4

    copper_price_df = get_raw_material_price(
        start_year, end_year, COOPER_ROW_INDEX, frequency
    )
    copper_price_df["Price"] /= 10

    logging.debug(copper_price_df)
    logging.info("Got Copper Price")

    return copper_price_df


def get_petroleum_wti_price(
    start_year: int, end_year: int, frequency: str = "MONTHLY"
) -> pd.DataFrame:
    logging.info("Getting Petroleum WTI Price")
    logging.info("========================")
    PETROLEUM_WTI_INDEX = 9

    petroleum_wti_df = get_raw_material_price(
        start_year, end_year, PETROLEUM_WTI_INDEX, frequency
    )

    logging.debug(petroleum_wti_df)
    logging.info("Got Petroleum WTI Price")

    return petroleum_wti_df


def get_dolar_exchange_rate(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Dolar Exchange")
    logging.info("========================")
    dolar_exchange_rate_df = get_bcrp_data(
        start_date, end_date, URL_DOLAR_EXCHANGE_RATE
    )
    dolar_exchange_rate_df = dolar_exchange_rate_df[
        dolar_exchange_rate_df["Value"] != "n.d."
    ]
    logging.debug(dolar_exchange_rate_df)
    logging.info("Got Dolar Exchange")

    return dolar_exchange_rate_df


def get_euro_exchange_rate(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Euro Exchange")
    logging.info("========================")
    euro_exchange_rate_df = get_bcrp_data(
        start_date, end_date, URL_EURO_EXCHANGE_RATE
    )
    euro_exchange_rate_df = euro_exchange_rate_df[
        euro_exchange_rate_df["Value"] != "n.d."
    ]
    logging.debug(euro_exchange_rate_df)
    logging.info("Got Euro Exchange")

    return euro_exchange_rate_df


def get_dolar_exchange(year: int, month: str, currency_code: str, param: str):
    params = {"gcode": f"PAR_{currency_code}", "param": param}

    data = {"DrDwnFechas": year, "hdnFrecuencia": "DAILY"}

    response = requests.post(URL_DOLAR_EXCHANGE, params=params, data=data)
    soup = BeautifulSoup(response.text, "html.parser")

    MONTH_INDEX = {
        "Enero": 1,
        "Febrero": 2,
        "Marzo": 3,
        "Abril": 4,
        "Mayo": 5,
        "Junio": 6,
        "Julio": 7,
        "Agosto": 8,
        "Septiembre": 9,
        "Octubre": 10,
        "Noviembre": 11,
        "Diciembre": 12,
    }

    month_index = MONTH_INDEX[month]
    days = pd.date_range(
        f"{year}-{month_index:02d}-01",
        f"{year}-{(month_index + 1):02d}-01",
        inclusive="left",
    )

    values = []

    for day in range(1, days.shape[0] + 1):
        id = f"gr_ctl{(day + 1):02d}_{month}"
        value_td = soup.find(id=id)
        values.append(value_td.getText().strip())

    logging.debug(values)
    data = {"Day": days, "Value": values}

    df = pd.DataFrame(data)
    df.replace("", np.nan, inplace=True)
    df.dropna(inplace=True)
    df["Value"] = df["Value"].str.replace(",", "")
    df["Value"] = df["Value"].astype(float)

    df["Day"] = df["Day"].dt.strftime("%Y-%m-%d")

    df.set_index("Day", inplace=True)

    return df


def get_yen_dolar_exchange(year: int, month: str) -> pd.DataFrame:
    logging.info("Getting YEN/DOLAR Exchange")
    logging.info("========================")
    yen_df = get_dolar_exchange(
        year,
        month,
        "JPY",
        (
            "cgBnAE8AOQBlAGcAIwBiAFUALQBsAEcAYgBOAEkASQBCAEcAegBFAFkAeABkADgAS"
            "AA2AG8AdgB2AFMAUgBYADIAQwBzAEEARQBMAG8AawBzACMATABOAHMARgB1ADIAeQ"
            "BBAFAAZwBhADIAbABWAHcAXwBXAGgATAAkAFIAVAB1AEIAbAB3AFoAdQBRAFgAZwA"
            "5AHgAdgAwACQATwBZADcAMwAuAGIARwBFAFIASwAuAHQA"
        ),
    )

    yen_df["Value"] /= 10000

    logging.debug(yen_df)

    return yen_df


def get_brazilian_real_dolar_exchange(year: int, month: str) -> pd.DataFrame:
    logging.info("Getting REAL/DOLAR Exchange")
    logging.info("========================")
    real_df = get_dolar_exchange(
        year,
        month,
        "BRL",
        (
            "dQBoAHMAOABpAGgAMQB2AC4ALQBDAF8AdgBkAFIAUgBWAF8AbQB6AFgAOQBOAGIAT"
            "gBwAEoAMQBNAE0ARAAuAGQAaQBmADMAUgBtAEsAMQBIAE0AcwBLADYAMwBDAHkAaQ"
            "BQAFIARQBBAHMAaQBrAE8AZQBUAHoASQBLAEIALgB3AHkAYQBrAGUAWAB5AFcAZAB"
            "BADcAVgBNADgAQgA0ADkAYwBsAFkAWgBIAG0ALgB1AFkAUQA="
        ),
    )

    real_df["Value"] /= 10000

    logging.debug(real_df)

    return real_df


def get_expected_pbi(year: int) -> pd.DataFrame:
    logging.info("Getting Expected PBI")
    logging.info("========================")
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }
    file_content = requests.get(
        URL_EXPECTED_PBI, verify=False, headers=headers
    ).content
    # logging.debug(file_content)
    df = pd.read_excel(
        io.BytesIO(file_content), usecols="A:D", skiprows=3, sheet_name="PBI"
    )
    columns = df.columns
    df["Expected Year"] = df["Fecha"]
    condition = ~df["Expected Year"].str.contains("Expectativas", na=False)
    df.loc[condition, "Expected Year"] = np.nan
    df["Expected Year"] = df["Expected Year"].fillna(method="ffill")
    df["Expected Year"] = df["Expected Year"].str.replace(
        "Expectativas anuales de ", ""
    )
    df = df.loc[
        (df["Expected Year"] == f"{year}")
        | (df["Expected Year"] == f"{year + 1}"),
        columns,
    ]
    logging.debug(df)
    logging.info("Got Expected PBI")

    return df


def get_monetary_policie_rate(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting Monetary Policie Rate")
    logging.info("========================")
    monetary_policie_rate_df = get_bcrp_data(
        start_date, end_date, URL_MONETARY_POLICIE_RATE
    )

    monetary_policie_rate_df = monetary_policie_rate_df[
        monetary_policie_rate_df["Value"] != "n.d."
    ]

    logging.debug(monetary_policie_rate_df)
    logging.info("Got Monetary Policie Rate")

    return monetary_policie_rate_df


def get_peruvian_goverment_bond(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info("Getting 10 Years Peruvian Goverment Bond")
    logging.info("========================")
    peruvian_goverment_bond_df = get_bcrp_data(
        start_date, end_date, URL_PERUVIAN_GOVERMENT_BOND
    )
    peruvian_goverment_bond_df = peruvian_goverment_bond_df[
        peruvian_goverment_bond_df["Value"] != "n.d."
    ]

    logging.debug(peruvian_goverment_bond_df)
    logging.info("Got 10 Years Peruvian Goverment Bond")

    return peruvian_goverment_bond_df


def get_sbs_usd_exchange_rate(date: str) -> pd.DataFrame:
    logging.info("Getting SBS USD Exchange Rate")
    logging.info("========================")
    headers = {"user-agent": USER_AGENT}

    with requests.Session() as s:
        r = s.get(URL_SBS_TC, headers=headers)
        soup = BeautifulSoup(r.content, "html.parser")

        data = dict()
        data["__EVENTVALIDATION"] = soup.find(
            "input", attrs={"id": "__EVENTVALIDATION"}
        )["value"]
        data["__VIEWSTATE"] = soup.find("input", attrs={"id": "__VIEWSTATE"})[
            "value"
        ]
        data["__VIEWSTATEGENERATOR"] = soup.find(
            "input", attrs={"id": "__VIEWSTATEGENERATOR"}
        )["value"]
        data["ctl00$MainScriptManager"] = (
            "ctl00$cphContent$updConsulta|ctl00$cphContent$btnConsultar"
        )
        data["ctl00$cphContent$btnConsultar"] = "Consultar"

        date_time = datetime.datetime.strptime(date, "%Y-%m-%d")
        value = float(0)
        for i in range(31):
            date_time_str = date_time.strftime("%Y-%m-%d-%H-%M-%S")
            date_str = date_time.strftime("%d/%m/%Y")
            now_str = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

            data["ctl00$cphContent$rdpDate"] = date
            data["ctl00$cphContent$rdpDate$dateInput"] = date_str
            data["ctl00_cphContent_rdpDate_dateInput_ClientState"] = f"""
                {{
                    "enabled": true,
                    "emptyMessage": "",
                    "validationText": "{date_time_str}",
                    "valueAsString": "{date_time_str}",
                    "minDateStr": "1000-01-01-00-00-00",
                    "maxDateStr": "{now_str}",
                    "lastSetTextBoxValue": "{date_str}"
                }}
                """

            p = s.post(URL_SBS_TC, data=data, headers=headers)
            soup = BeautifulSoup(p.content, "html.parser")
            values = soup.select(
                "#ctl00_cphContent_rgTipoCambio_ctl00__0 > td:nth-child(3)"
            )
            if len(values) > 0 and values[0].getText().strip() != "":
                value = float(values[0].getText().strip())
                break

            date_time -= datetime.timedelta(days=1)

        logging.info("Got SBS USD Exchange Rate")
        date_time_str = date_time.strftime("%Y-%m-%d")
        df = pd.DataFrame(
            {"Period": [date_time_str], "Value": [value]}
        ).set_index("Period")
        logging.debug(df)
        return df


def read_parameters(file_path: str, sheet_name: str):
    parameters_df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        skiprows=2,
        usecols=["N°", "Titulo KPI", "Inicio", "Fin"],
    )

    kpi_map = {
        1: {
            "function": get_electricity,
            "format": "%Y-%m",
            "sheet_name_output": "Electricity (GWH)",
        },
        2: {
            "function": get_vehicular_flow,
            "sheet_name_output": "Vehicular Flow",
        },
        3: {
            "function": get_dolar_exchange_rate,
            "format": "%Y-%m-%d",
            "sheet_name_output": "Dolar Exchange Rate",
        },
        4: {
            "function": get_euro_exchange_rate,
            "format": "%Y-%m-%d",
            "sheet_name_output": "Euro Exchange Rate",
        },
        5: {
            "function": get_yen_dolar_exchange,
            "sheet_name_output": "Yen Dolar Exchange",
        },
        6: {
            "function": get_brazilian_real_dolar_exchange,
            "sheet_name_output": "Real Dolar Exchange",
        },
        9: {
            "function": get_pbi,
            "sheet_name_output": "PBI",
        },
        10: {
            "function": get_expected_pbi,
            "sheet_name_output": "Expected PBI",
        },
        12: {
            "function": get_intern_demand,
            "sheet_name_output": "Intern Demand",
        },
        13: {
            "function": get_unemployment_rate,
            "format": "%Y-%m",
            "sheet_name_output": "Unemployment Rate",
        },
        14: {
            "function": get_monetary_policie_rate,
            "format": "%Y-%m-%d",
            "sheet_name_output": "Monetary Policy Rate",
        },
        15: {
            "function": get_peruvian_goverment_bond,
            "format": "%Y-%m",
            "sheet_name_output": "10 Years Peruvian Goverment Bond",
        },
        16: {
            "function": get_5years_treasury_bill_rate,
            "format": "%Y-%m",
            "sheet_name_output": "5 Years Treasure Bill Rate",
        },
        17: {
            "function": get_10years_treasury_bill_rate,
            "format": "%Y-%m",
            "sheet_name_output": "10 Years Treasure Bill Rate",
        },
        18: {
            "function": get_price_index,
            "sheet_name_output": "Price Index",
        },
        20: {
            "function": get_copper_price,
            "sheet_name_output": "Copper Price",
        },
        21: {
            "function": get_petroleum_wti_price,
            "sheet_name_output": "Petroleum WTI Price",
        },
        23: {
            "function": get_sp_bvl_general_index,
            "format": "%Y-%m",
            "sheet_name_output": "S&P BVL",
        },
        24: {
            "function": get_djones_rate,
            "format": "%Y-%m",
            "sheet_name_output": "Djones Rate",
        },
        29: {
            "function": get_sbs_usd_exchange_rate,
            "format": "%Y-%m-%d",
            "sheet_name_output": "SBS USD Exchange Rate",
        },
    }

    parameters_df = parameters_df[parameters_df["N°"].isin(kpi_map.keys())]

    def transform(row):
        format = kpi_map[row["N°"]].get("format")
        if format:
            row["Inicio"] = row["Inicio"].strftime(format)
            if not pd.isna(row["Fin"]):
                row["Fin"] = row["Fin"].strftime(format)

        return row

    def execute(row):
        function = kpi_map.get(row["N°"], {}).get("function")
        sheet_name = kpi_map.get(row["N°"], {}).get("sheet_name_output")
        if function:
            try:
                logging.debug(row["Fin"])
                if not pd.isna(row["Fin"]):
                    df = function(row["Inicio"], row["Fin"])
                else:
                    df = function(row["Inicio"])
            except Exception as e:
                logging.error(f"Error executing function {function}: {e}")
            try:
                with pd.ExcelWriter(
                    "output.xlsx", mode="a", if_sheet_exists="replace"
                ) as writer:
                    df.to_excel(writer, sheet_name=sheet_name)
            except Exception as e:
                logging.error(f"Error wrting to sheet_name: {sheet_name}: {e}")
                with pd.ExcelWriter("output.xlsx", mode="w") as writer:
                    df.to_excel(writer, sheet_name=sheet_name)

    parameters_df = parameters_df.apply(lambda row: transform(row), axis=1)
    parameters_df.apply(lambda row: execute(row), axis=1)
    logging.debug(parameters_df)


def main():
    read_parameters("input.xlsx", "Parametros")
    # # KPI 1
    # get_electricity("2023-04", "2023-06")
    # # KPI 2
    # get_vehicular_flow("2023")
    # # KPI 3
    # get_dolar_exchange_rate("2023-06-20", "2023-06-30")
    # # KPI 4
    # get_euro_exchange_rate("2023-06-20", "2023-06-30")
    # # KPI 5
    # get_yen_dolar_exchange(2023, "Julio")
    # # KPI 6
    # get_brazilian_real_dolar_exchange(2023, "Julio")
    # # KPI 9
    # get_pbi("2023-04", "2023-07")
    # # KPI 10-11
    # get_expected_pbi(2023)
    # # KPI 12
    # get_intern_demand("2023-1", "2023-4")
    # # KPI 13
    # get_unemployment_rate("2023-01", "2023-06")
    # # KPI 14
    # get_monetary_policie_rate("2023-07-01", "2023-07-31")
    # # KPI 15
    # get_peruvian_goverment_bond("2023-01", "2023-07")
    # # KPI 16
    # get_5years_treasury_bill_rate("2022-06", "2023-07")
    # # KPI 17
    # get_10years_treasury_bill_rate("2022-06", "2023-07")
    # # KPI 18-19
    # get_price_index(2023, "Abril")
    # # KPI 20
    # get_copper_price(2023, 2023)
    # # KPI 21
    # get_petroleum_wti_price(2023, 2023)
    # # KPI 23
    # get_sp_bvl_general_index("2022-06", "2023-07")
    # # KPI 24
    # get_djones_rate("2022-06", "2023-07")
    # # KPI 29
    # get_sbs_usd_exchange_rate("2023-06-29")


if __name__ == "__main__":
    main()
