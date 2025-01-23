import pandas as pd
import requests
from git import Repo
from datetime import datetime
import os
from config import GITHUB_TOKEN, GITHUB_REPO, GITHUB_URL, GIT_NAME, GIT_EMAIL


def fetch_covid_data():
    """Obtiene datos de la API de disease.sh"""
    print("Obteniendo datos de COVID-19...")
    
    # URLs de la API
    historical_url = "https://disease.sh/v3/covid-19/historical/all?lastdays=all"
    countries_url = "https://disease.sh/v3/covid-19/countries"
    
    # Obtener datos
    historical_response = requests.get(historical_url)
    countries_response = requests.get(countries_url)
    
    if historical_response.status_code != 200 or countries_response.status_code != 200:
        raise Exception("Error al obtener datos de la API")
    
    return historical_response.json(), countries_response.json()

def transform_data(historical_data, countries_data):
    """Transforma los datos obtenidos"""
    print("Transformando datos...")
    
    # Convertir datos históricos a DataFrame
    df_historical = pd.DataFrame(historical_data)
    df_historical.index = pd.to_datetime(df_historical.index)
    
    # Convertir datos de países a DataFrame
    df_countries = pd.DataFrame(countries_data)
    
    # Calcular métricas
    df_historical['new_cases'] = df_historical['cases'].diff()
    df_historical['new_deaths'] = df_historical['deaths'].diff()
    df_historical['growth_rate'] = df_historical['new_cases'] / df_historical['cases'].shift(1) * 100
    
    # Promedios móviles
    df_historical['cases_ma7'] = df_historical['new_cases'].rolling(window=7).mean()
    df_historical['deaths_ma7'] = df_historical['new_deaths'].rolling(window=7).mean()
    
    # Métricas por población
    total_population = df_countries['population'].sum()
    df_historical['cases_per_million'] = (df_historical['cases'] / total_population) * 1000000
    df_historical['deaths_per_million'] = (df_historical['deaths'] / total_population) * 1000000
    
    # Resetear índice
    df_historical = df_historical.reset_index()
    df_historical.rename(columns={'index': 'date'}, inplace=True)
    
    return df_historical

def setup_git_repo():
    """Configura el repositorio git"""
    print("Configurando repositorio Git...")
    
    try:
        repo = Repo('.')
    except:
        repo = Repo.init('.')
    
    # Configurar remoto
    if 'origin' not in [remote.name for remote in repo.remotes]:
        origin = repo.create_remote('origin', GITHUB_URL)
    else:
        origin = repo.remote('origin')
    
    # Configurar usuario
    repo.config_writer().set_value("user", "name", GIT_NAME).release()
    repo.config_writer().set_value("user", "email", GIT_EMAIL).release()
    
    return repo

def save_and_push_data(df, repo):
    """Guarda los datos y los sube a GitHub"""
    print("Guardando y subiendo datos...")
    
    # Crear directorio si no existe
    os.makedirs('data/raw', exist_ok=True)
    
    # Guardar como parquet
    output_path = 'data/raw/covid_historical_data.parquet'
    df.to_parquet(output_path, compression='gzip')
    print(f"Datos guardados en {output_path}")
    
    # Agregar y commit
    repo.index.add([output_path])
    commit_message = f"Actualización datos COVID-19 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    repo.index.commit(commit_message)
    
    # Push
    origin = repo.remote('origin')
    origin.push()
    print("Datos subidos exitosamente a GitHub")

def main():
    try:
        # 1. Obtener datos
        historical_data, countries_data = fetch_covid_data()
        
        # 2. Transformar datos
        df_transformed = transform_data(historical_data, countries_data)
        
        # 3. Configurar Git
        repo = setup_git_repo()
        
        # 4. Guardar y subir datos
        save_and_push_data(df_transformed, repo)
        
        print("¡Proceso completado exitosamente!")
        
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")

if __name__ == "__main__":
    main()
