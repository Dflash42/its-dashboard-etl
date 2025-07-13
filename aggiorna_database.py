import requests
import psycopg2
import os
import time
from dotenv import load_dotenv
from datetime import datetime
from psycopg2.extras import execute_batch

load_dotenv()

# --- CONFIGURAZIONE GLOBALE ---
API_BASE_URL = "https://api-pw25-grhhckd5abhdhccd.italynorth-01.azurewebsites.net/api"
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}

# --- VALIDAZIONE ---
if not API_KEY or not DATABASE_URL:
    print("!!! ERRORE CRITICO: Le variabili API_KEY e/o DATABASE_URL non sono state trovate. Assicurati di aver creato un file .env o di aver impostato i Secrets su GitHub.")
    exit()

print(f"INFO: Script avviato. Utilizzo API Key che inizia con: '{API_KEY[:4]}...'")

# --- DEFINIZIONE STRUTTURA DATABASE ---
TABLES_SQL = {
    "corsi": """
        CREATE TABLE IF NOT EXISTS corsi (
            id_corso_anno VARCHAR(50) PRIMARY KEY, codice_corso VARCHAR(100), nome_corso TEXT,
            anno VARCHAR(10), sezione VARCHAR(10), data_inizio DATE, data_fine DATE,
            data_inizio_stage DATE, data_fine_stage DATE, iscritti INT
        );""",
    "docenti": """
        CREATE TABLE IF NOT EXISTS docenti (
            id_utente TEXT PRIMARY KEY, cognome_hash TEXT, nome_hash TEXT, email_hash TEXT
        );""",
    "iscrizioni_its": """
        CREATE TABLE IF NOT EXISTS iscrizioni_its (
            id_alunno TEXT PRIMARY KEY, cognome_hash TEXT, nome_hash TEXT, cf_hash TEXT,
            data_nascita DATE, sesso CHAR(1), email_hash TEXT, voto_diploma VARCHAR(50),
            alunno_attivo BOOLEAN, ritirato_corso BOOLEAN, id_corso_anno VARCHAR(50) REFERENCES corsi(id_corso_anno)
        );""",
    "stage_its": """
        CREATE TABLE IF NOT EXISTS stage_its (
            id SERIAL PRIMARY KEY, id_alunno TEXT REFERENCES iscrizioni_its(id_alunno),
            id_corso_anno VARCHAR(50), azienda_hash TEXT, partita_iva_hash TEXT,
            data_inizio_stage DATE, data_fine_stage DATE,
            UNIQUE (id_alunno, id_corso_anno, data_inizio_stage, azienda_hash)
        );""",
    "materie_its": """
        CREATE TABLE IF NOT EXISTS materie_its (
            id_materia TEXT, id_corso_anno VARCHAR(50) REFERENCES corsi(id_corso_anno),
            materia_nome TEXT, codice_materia VARCHAR(100), ore_previste INTEGER,
            ore_effettuate INTEGER, ore_pianificate INTEGER,
            PRIMARY KEY (id_corso_anno, id_materia)
        );""",
    "corso_docenti_its": """
        CREATE TABLE IF NOT EXISTS corso_docenti_its (
            id SERIAL PRIMARY KEY, id_corso_anno VARCHAR(50) REFERENCES corsi(id_corso_anno),
            id_utente TEXT REFERENCES docenti(id_utente), materia TEXT,
            monte_ore INTEGER, ore_lavorate INTEGER,
            UNIQUE (id_corso_anno, id_utente, materia)
        );""",
    "ore_alunno_its": """
        CREATE TABLE IF NOT EXISTS ore_alunno_its (
            id SERIAL PRIMARY KEY, id_alunno TEXT REFERENCES iscrizioni_its(id_alunno),
            id_corso_anno VARCHAR(50), materia TEXT, ore_previste INTEGER,
            minuti_presenza INTEGER, minuti_lezione INTEGER, voto_medio VARCHAR(50),
            UNIQUE (id_alunno, id_corso_anno, materia)
        );"""
}

# --- FUNZIONI HELPER ---
def parse_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    try: return datetime.strptime(date_str, '%d/%m/%Y').date()
    except ValueError: return None

def to_int(value, default=0):
    if value is None or value == '': return default
    try: return int(value)
    except (ValueError, TypeError): return default

def to_bool(bool_str):
    return isinstance(bool_str, str) and bool_str.lower() == 'true'

# --- FUNZIONI CORE ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("INFO: Connessione al database Azure avvenuta con successo.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"!!! ERRORE CRITICO di connessione al database: {e}")
        exit()

def manage_tables(conn, action='create'):
    table_order = list(TABLES_SQL.keys())
    if action != 'create': table_order.reverse()
    
    with conn.cursor() as cur:
        for table_name in table_order:
            if action == 'create':
                cur.execute(TABLES_SQL[table_name])
            else: # drop
                cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    conn.commit()
    print(f"INFO: Tabelle {action}d con successo.")

def fetch_api_data(endpoint, params=None):
    url = f"{API_BASE_URL}/{endpoint}"
    print(f"-> Chiamata API: {url} con parametri: {params or 'Nessuno'}")
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=45)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and data.get('valid') is False:
            print(f"   -> Avviso API: {data.get('message')}. Nessun record.")
            return []
        if not isinstance(data, list):
            print(f"   -> Attenzione: Risposta non è una lista. {data}")
            return []
        print(f"   -> Successo! Ricevuti {len(data)} record.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"   -> ERRORE API: {e}")
        return []

def insert_data(conn, table_name, data, columns):
    """
    Funzione corretta per inserire i dati.
    Usa un numero di segnaposto (%s) corretto per le colonne.
    """
    if not data:
        print(f"INFO: Nessun dato da inserire per '{table_name}'.")
        return
    
    with conn.cursor() as cur:
        # Costruisce la stringa di segnaposto corretta, es. (%s, %s, %s)
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        
        try:
            execute_batch(cur, sql, data, page_size=500)
            conn.commit()
            print(f"INFO: Inseriti/Ignorati {len(data)} record in '{table_name}'.")
        except Exception as e:
            print(f"!!! ERRORE durante l'esecuzione di execute_batch per la tabella {table_name}: {e}")
            conn.rollback()


# --- PIPELINE PRINCIPALE ---
def run_pipeline():
    conn = get_db_connection()
    manage_tables(conn, action='drop')
    manage_tables(conn, action='create')

    # 1. Estrazione dati base
    print("\n--- FASE 1: ESTRAZIONE DATI DI BASE ---")
    corsi_data = fetch_api_data("corsi")
    docenti_data = fetch_api_data("docenti")
    
    # 2. Inserimento dati base (Corsi e Docenti)
    print("\n--- FASE 2: CARICAMENTO DATI DI BASE ---")
    insert_data(conn, 'corsi', [(r.get('idCorsoAnno'), r.get('CodiceCorso'), r.get('Corso'), r.get('Anno'), r.get('Sezione'), parse_date(r.get('DataInizio')), parse_date(r.get('DataFine')), parse_date(r.get('DataInizioStage')), parse_date(r.get('DataFineStage')), to_int(r.get('Iscritti'))) for r in corsi_data], ['id_corso_anno', 'codice_corso', 'nome_corso', 'anno', 'sezione', 'data_inizio', 'data_fine', 'data_inizio_stage', 'data_fine_stage', 'iscritti'])
    insert_data(conn, 'docenti', [(r.get('idUtente'), r.get('Cognome'), r.get('Nome'), r.get('Email')) for r in docenti_data], ['id_utente', 'cognome_hash', 'nome_hash', 'email_hash'])

    # 3. Estrazione e consolidamento di tutti gli iscritti
    print("\n--- FASE 3: ESTRAZIONE E CONSOLIDAMENTO ISCRIZIONI ---")
    all_iscrizioni = {}
    start_year = 2019
    current_year = datetime.now().year
    for year in range(start_year, current_year + 1):
        iscrizioni_anno = fetch_api_data('iscrizioni', params={'AnnoAccademico': str(year)})
        for item in iscrizioni_anno:
            if item.get('idAlunno'):
                all_iscrizioni[item['idAlunno']] = item
        time.sleep(1.2)
    
    stage_data = fetch_api_data('stage', params={'DataDa': '01/01/2019', 'DataA': '31/12/2025'})
    for item in stage_data:
        if item.get('idAlunno') and item.get('idAlunno') not in all_iscrizioni:
             all_iscrizioni[item['idAlunno']] = {'idAlunno': item.get('idAlunno'), 'Cognome': item.get('Cognome'), 'Nome': item.get('Nome'), 'idCorsoAnno': item.get('idCorsoAnno'), 'AlunnoAttivo': 'true'}
    
    insert_data(conn, 'iscrizioni_its', [(r.get('idAlunno'), r.get('Cognome'), r.get('Nome'), r.get('CF'), parse_date(r.get('DataNascita')), r.get('Sesso'), r.get('Email'), r.get('VotoDiploma'), to_bool(r.get('AlunnoAttivo')), to_bool(r.get('RitiratoCorso')), r.get('idCorsoAnno')) for r in all_iscrizioni.values()], ['id_alunno', 'cognome_hash', 'nome_hash', 'cf_hash', 'data_nascita', 'sesso', 'email_hash', 'voto_diploma', 'alunno_attivo', 'ritirato_corso', 'id_corso_anno'])
    insert_data(conn, 'stage_its', [(r.get('idAlunno'), r.get('idCorsoAnno'), r.get('Azienda'), r.get('PI'), parse_date(r.get('DataInizioStage')), parse_date(r.get('DataFineStage'))) for r in stage_data], ['id_alunno', 'id_corso_anno', 'azienda_hash', 'partita_iva_hash', 'data_inizio_stage', 'data_fine_stage'])

    # 4. Estrazione e caricamento dati di dettaglio per corso
    print("\n--- FASE 4: ESTRAZIONE E CARICAMENTO DATI DI DETTAGLIO ---")
    id_corsi = [c['idCorsoAnno'] for c in corsi_data]
    for i, corso_id in enumerate(id_corsi, 1):
        print(f"--- Processo Corso {i}/{len(id_corsi)} (ID: {corso_id}) ---")
        time.sleep(1.2)
        params = {"idCorsoAnno": corso_id}
        
        materie = fetch_api_data('corso_materie', params)
        insert_data(conn, 'materie_its', [(corso_id, r.get('idMateria'), r.get('Materia'), r.get('CodiceMateria'), to_int(r.get('OrePreviste')), to_int(r.get('OreEffettuate')), to_int(r.get('OrePianificate'))) for r in materie], ['id_corso_anno', 'id_materia', 'materia_nome', 'codice_materia', 'ore_previste', 'ore_effettuate', 'ore_pianificate'])
        
        docenti_corso = fetch_api_data('corso_docenti', params)
        insert_data(conn, 'corso_docenti_its', [(corso_id, r.get('idUtente'), r.get('Materia'), to_int(r.get('MonteOre')), to_int(r.get('OreLavorate'))) for r in docenti_corso], ['id_corso_anno', 'id_utente', 'materia', 'monte_ore', 'ore_lavorate'])

        ore_alunno = fetch_api_data('ore_alunno', params)
        insert_data(conn, 'ore_alunno_its', [(r.get('idAlunno'), corso_id, r.get('Materia'), to_int(r.get('OrePreviste')), to_int(r.get('MinutiPresenza')), to_int(r.get('MinutiLezione')), r.get('VotoMedio')) for r in ore_alunno], ['id_alunno', 'id_corso_anno', 'materia', 'ore_previste', 'minuti_presenza', 'minuti_lezione', 'voto_medio'])

    conn.close()
    print("\n--- PROCESSO DI CARICAMENTO COMPLETATO CON SUCCESSO ---")

if __name__ == "__main__":
    run_pipeline()
