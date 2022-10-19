import requests

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from datetime import timedelta

url = "https://matchilling-chuck-norris-jokes-v1.p.rapidapi.com/jokes/random"

headers = {
	"accept": "application/json",
	"X-RapidAPI-Key": "7a692470demshde86b2352572440p1a7720jsn888da3681c6c",
	"X-RapidAPI-Host": "matchilling-chuck-norris-jokes-v1.p.rapidapi.com"
}

scheduleData = IntervalSchedule (interval=timedelta (minutes=1))

# Extraer
@task(max_retries=10, retry_delay=timedelta (seconds=15))
def obtenerDatos ():
    response = requests.request("GET", url, headers=headers)

    formato = response.json ()

    return formato

# Transformar
@task
def transformarDatos (datos):
    return datos["value"]

# Almacenar
@task
def almacenarDatos (datoChuck):
    # messagebox.showinfo(message=datoChuck, title="Dato") 
    print (datoChuck) 

def crearFlow (schedule):
    # Agregamos la agenda como parametro a Flow.
    with Flow ("ETL Flow", schedule) as flow:
        datos = obtenerDatos ()
        contenido = transformarDatos (datos)
        almacenarDatos (contenido)

    return flow

flow = crearFlow (scheduleData)
# flow.visualize ()
flow.run ()



