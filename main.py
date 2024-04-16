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
  # file Status error
  with open(f'{pathCSV}{fileStatus}',"w") as f:
    f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nError: No se encontro Archivo fuente')
  print("Error: Verificar archivo status")
  exit()
except KeyError as err:
  # file Status error
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
        if work["external-ids"]["external-id"]:
          for doi in work["external-ids"]["external-id"]:
            if doi["external-id-type"] == "doi" and rowControl:
              listdump.append(doi["external-id-normalized"]["value"])
              rowControl = False
          if rowControl: listdump.append(None)
        else: listdump.append(None)
        # Add Title
        listdump.append(work["work-summary"][0]["title"]["title"]["value"]) if work["work-summary"][0]["title"]["title"] else listdump.append(None)
        # Add type
        listdump.append(work["work-summary"][0]["type"]) if work["work-summary"][0]["type"] else listdump.append(None)
        # Add URL
        listdump.append(work["work-summary"][0]["url"]["value"]) if work["work-summary"][0]["url"] else listdump.append(None)
        # Add Date
        if work["work-summary"][0]["publication-date"]["year"] and work["work-summary"][0]["publication-date"]["month"] and work["work-summary"][0]["publication-date"]["day"]: listdump.append(f'{work["work-summary"][0]["publication-date"]["year"]["value"]}-{work["work-summary"][0]["publication-date"]["month"]["value"]}-{work["work-summary"][0]["publication-date"]["day"]["value"]}')
        elif work["work-summary"][0]["publication-date"]["year"] and work["work-summary"][0]["publication-date"]["month"] and not work["work-summary"][0]["publication-date"]["day"]: listdump.append(f'{work["work-summary"][0]["publication-date"]["year"]["value"]}-{work["work-summary"][0]["publication-date"]["month"]["value"]}')
        elif work["work-summary"][0]["publication-date"]["year"] and not work["work-summary"][0]["publication-date"]["month"] and not work["work-summary"][0]["publication-date"]["day"]: listdump.append(f'{work["work-summary"][0]["publication-date"]["year"]["value"]}')
        else: listdump.append(None)
        
        # Push a dataframe
        dataframe.append(listdump.copy())
        # Clear list dummy
        listdump.clear()

  else:
    # file Status error
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

with open(f'{pathCSV}{fileStatus}',"w") as f:
  f.write(f'Fecha: {x.strftime("%Y-%m-%d")}\nOK: Proceso correcto\nORCID_ID verificados = {len(listOrcidID)}\nRegistros totales = {len(dataframe)}')
print("OK: Proceso correcto")