import pandas as pd
import glob
import os


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de datos para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
      combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    input_path = "files/input/"
    output_path = "files/output/"

    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    all_files = glob.glob(os.path.join(input_path, "*.csv.zip"))

    df_list = []

    for file in all_files:
        df = pd.read_csv(file, compression='zip')
        df_list.append(df)

    full_df = pd.concat(df_list, ignore_index=True)

    # Eliminar la columna 'Unnamed: 0' si existe, ya que suele ser un artefacto.
    if 'Unnamed: 0' in full_df.columns:
        full_df = full_df.drop(columns=['Unnamed: 0'])

    # Generar 'client_id' basado en el índice.
    full_df["client_id"] = full_df.index + 1

    ## Procesamiento de client.csv

    client_df = full_df[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default", # Estas son las columnas originales del full_df
            "mortgage",       # que vamos a transformar y mantener con el mismo nombre
        ]
    ].copy()

    client_df["job"] = client_df["job"].str.replace(".", "", regex=False)
    client_df["job"] = client_df["job"].str.replace("-", "_", regex=False)
    client_df["education"] = client_df["education"].str.replace(".", "_", regex=False)
    client_df["education"] = client_df["education"].replace("unknown", pd.NA)

    # Transformar y sobrescribir las columnas 'credit_default' y 'mortgage'
    # Esto asegura que no se creen columnas duplicadas y se mantenga el conteo de 7 columnas.
    client_df["credit_default"] = client_df["credit_default"].apply(
        lambda x: 1 if x == "yes" else 0
    )
    client_df["mortgage"] = client_df["mortgage"].apply( # Corrección aquí: 'mortgage' con 'g'
        lambda x: 1 if x == "yes" else 0
    )


    ## Procesamiento de campaign.csv

    
    campaign_df = full_df[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()
    
    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(
        lambda x: 1 if x == "success" else 0
    )
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(
        lambda x: 1 if x == "yes" else 0
    )

    month_mapping = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    campaign_df["month_num"] = campaign_df["month"].str.lower().map(month_mapping)
    
    campaign_df["last_contact_day"] = pd.to_datetime(
        "2022-"
        + campaign_df["month_num"].astype(str)
        + "-"
        + campaign_df["day"].astype(str),
        errors='coerce'
    ).dt.strftime("%Y-%m-%d")
    
    # Renombrar 'last_contact_day' a 'last_contact_date' para que coincida con el test
    campaign_df = campaign_df.rename(columns={'last_contact_day': 'last_contact_date'})

    campaign_df = campaign_df.drop(columns=["day", "month", "month_num"])
    



    ## Procesamiento de economics.csv

    
    economics_df = full_df[
        ["client_id", "cons_price_idx", "euribor_three_months"]
    ].copy()


    
    client_df.to_csv(os.path.join(output_path, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_path, "campaign.csv"), index=False)
    economics_df.to_csv(os.path.join(output_path, "economics.csv"), index=False)


if __name__ == "__main__":
     clean_campaign_data()