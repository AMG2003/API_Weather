import requests
import oracledb
import time

# 1. Configuración de Conexión
DB_CONFIG = {
    "user": "system",
    "password": "system",
    "dsn": "localhost:1521/xe" # Cambia según tu configuración
}

def get_data_from_api(species_key, offset, limit=300):
    url = "https://api.gbif.org/v1/occurrence/search"
    params = {'taxonKey': species_key, 'limit': limit, 'offset': offset}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con la API: {e}")
        return None

def run_pipeline(species_key):
    # Conexión a Oracle
    conn = oracledb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    offset = 0
    limit = 300
    total_inserted = 0
    
    print(f"Iniciando extracción para TaxonKey: {species_key}...")
    
    while True:
        data = get_data_from_api(species_key, offset, limit)
        
        if not data or 'results' not in data:
            break
            
        results = data['results']
        if not results:
            break
            
        # Transformamos los datos en una lista de tuplas para 'executemany'
        # Nota: Usamos .get() para evitar errores si un campo viene vacío
        rows = [
            (
                str(item.get('key')),
                item.get('scientificName'),
                item.get('eventDate'),
                str(item.get('decimalLatitude')),
                str(item.get('decimalLongitude'))
            ) for item in results
        ]
        
        # Inserción Masiva
        sql = """INSERT INTO STG_GBIF_OCCURRENCES
                 (gbif_id, scientific_name, event_date, decimal_latitude, decimal_longitude) 
                 VALUES (:1, :2, :3, :4, :5)"""
        
        cursor.executemany(sql, rows)
        conn.commit()
        
        total_inserted += len(rows)
        print(f"Lote insertado. Total registros procesados: {total_inserted}")
        
        # Condición de salida
        if data.get('endOfRecords'):
            break
            
        offset += limit
        time.sleep(0.5) # Respetuoso con la API
        
    cursor.close()
    conn.close()
    print("Proceso de ingesta finalizado con éxito.")

# Ejecutar
if __name__ == "__main__":
    # 2481001 es el ID para Puma concolor
    run_pipeline(2481001)