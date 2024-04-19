# modulos
import pandas as pd
import requests
import datetime

# DateTime
x = datetime.datetime.now()

# path 
pathSource = "./sourceORCID/"
pathCSV = "./CSV/"

# files
fileSource = "source_orcid.xlsx"
fileOrcid = f'works_orcid_{x.strftime("%Y-%m-%d")}.csv'
fileStatus = f'status_{x.strftime("%Y-%m-%d")}.txt'

# config Request API
url = "https://pub.orcid.org/v3.0/"
headers = {"Content-Type": "application/json"}

# Dataframe for CSV
columnsCSV = ["ORCID_ID", "DOI", "TITLE", "TYPE", "URL", "DATE"]
dataframe=[]

# Read file source (.xlsx)
try:
  # lectura de archivo xlsx
  dataSource = pd.read_excel(f'{pathSource}{fileSource}', engine='openpyxl')
  # lista con ORCIDID
  listOrcidID = list(dataSource["ORCID_ID"])
except FileNotFoundError as err:
  # File Status error
  with open(f'{pathCSV}{fileStatus}',"w") as f:
    f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nError: No se encontro Archivo fuente')
  print("Error: Verificar archivo status")
  exit()
except KeyError as err:
  # File Status error
  with open(f'{pathCSV}{fileStatus}',"w") as f:
    f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nError: Formato incorrecto en Archivo fuente')
  print("Error: Verificar archivo status")
  exit()

# Request and JSON
for orcidID in listOrcidID:
  # Request GET
  try: 
    request = requests.get(f'{url}{orcidID}/works', headers=headers)
  except:
      # File Status error
      with open(f'{pathCSV}{fileStatus}',"w") as f:
        f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nError: Error en Request a API. Verifique conectividad o URL.')
      print("Error: Verificar archivo status")
      exit()
  
  # Validacion de peticion 
  if (request.status_code == 200):
    # JSON data
    dataORCID = request.json()
    # Validacion de numero de publicaciones
    if(len(dataORCID["group"])):
      
      for work in dataORCID["group"]:
        rowControl = True
        listdump = []
        # Add ORCID_ID
        listdump.append(orcidID)
        # Add DOI
        dataDOI = work["external-ids"]["external-id"]
        if dataDOI:
          for doi in dataDOI:
            if doi["external-id-type"] == "doi" and rowControl:
              listdump.append(doi["external-id-normalized"]["value"])
              rowControl = False
          if rowControl: listdump.append(None)
        else: listdump.append(None)
        # Add Title
        dataTitle = work["work-summary"][0]["title"]["title"]
        listdump.append(dataTitle["value"]) if dataTitle else listdump.append(None)
        # Add type
        dataType = work["work-summary"][0]["type"]
        listdump.append(dataType) if dataType else listdump.append(None)
        # Add URL
        dataURL = work["work-summary"][0]["url"]
        listdump.append(dataURL["value"]) if dataURL else listdump.append(None)
        # Add Date
        dataDate = work["work-summary"][0]["publication-date"]
        if dataDate:
          dateYear = dataDate["year"]
          dateMonth = dataDate["month"]
          dateDay = dataDate["day"]
          if dateYear and dateMonth and dateDay: listdump.append(f'{dateYear["value"]}-{dateMonth["value"]}-{dateDay["value"]}')
          elif dateYear and dateMonth and not dateDay: listdump.append(f'{dateYear["value"]}-{dateMonth["value"]}')
          elif dateYear and not dateMonth and not dateDay: listdump.append(f'{dateYear["value"]}')
          else: listdump.append(None)
        else: listdump.append(None)          
        
        # Push a dataframe
        dataframe.append(listdump.copy())
        # Clear list dummy
        listdump.clear()

  else:
    # File Status error
    with open(f'{pathCSV}{fileStatus}',"w") as f:
      f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nError: ORCID_ID [{orcidID}] incorrecto')
    print("Error: Verificar archivo status")
    exit()

# Create CSV
try:
  # Create Dataframe
  df_csv = pd.DataFrame(dataframe, columns=columnsCSV)
  df_csv.to_csv(f'{pathCSV}{fileOrcid}', encoding='utf-8')
except:
    with open(f'{pathCSV}{fileStatus}',"w") as f:
      f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nError: Error al crear CSV')
    print("Error: Verificar archivo status")
    exit()

# File status OK
with open(f'{pathCSV}{fileStatus}',"w") as f:
  f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nOK: Proceso correcto\nORCID_ID verificados = {len(listOrcidID)}\nRegistros totales = {len(dataframe)}')
print("OK: Proceso correcto")