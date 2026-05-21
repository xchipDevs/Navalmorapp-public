import os
import sys
import requests
import json
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO

# Asegurar que la salida estándar use codificación UTF-8 para evitar errores de impresión con emojis
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# Cargar variables de entorno locales si existe .env
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(dotenv_path):
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                key, val = line.strip().split("=", 1)
                os.environ[key.strip()] = val.strip()

# Configuración
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
URL_FARMACIAS = "https://puntodeencuentronavalmoral.es/farmacias-de-guardia-en-navalmoral-de-la-mata/"
JSON_PATH = os.path.join(BASE_DIR, "pharmacies.json")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

def scrape_image_url():
    """Busca la URL de la imagen de farmacias en la web."""
    print("🔍 Scraping web en busca de la imagen...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(URL_FARMACIAS, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Estrategia 1: Buscar imagen con alt text relevante
        images = soup.find_all('img')
        for img in images:
            alt = img.get('alt', '').lower()
            src = img.get('src')
            if src and ('farmacias' in alt or 'guardia' in alt) and 'navalmoral' in alt:
                print(f"✅ Imagen encontrada: {src}")
                return src
        
        # Estrategia 2: Buscar en la estructura específica
        content_div = soup.find('div', class_='entry-content')
        if content_div:
            img = content_div.find('img')
            if img and img.get('src'):
                print(f"✅ Imagen encontrada (por estructura): {img.get('src')}")
                return img.get('src')
        
        print("❌ No se encontró ninguna imagen candidata.")
        return None
        
    except Exception as e:
        print(f"⚠️ Error scraping web: {e}")
        return None

def download_image(url):
    """Descarga la imagen en memoria."""
    print("⬇️ Descargando imagen...")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return Image.open(BytesIO(response.content))
    except Exception as e:
        print(f"⚠️ Error descargando imagen: {e}")
        return None

def update_json_with_gemini(image, current_json):
    """Usa Gemini para actualizar el JSON basándose en la imagen."""
    print("🤖 Procesando imagen con Gemini (Nuevo SDK)...")
    
    if not GEMINI_API_KEY:
        print("❌ Error: GEMINI_API_KEY no configurada.")
        return None
        
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        prompt = f"""
        Eres un asistente encargado de actualizar los turnos de farmacias de guardia.
        
        Tengo este archivo JSON con la lista de farmacias y sus horarios antiguos:
        ```json
        {json.dumps(current_json, indent=2, ensure_ascii=False)}
        ```
        
        TU TAREA:
        1. Analiza la imagen proporcionada, que contiene el nuevo calendario de farmacias de guardia.
        2. Extrae las nuevas fechas de guardia para cada farmacia.
        3. Extrae las "notas" si aparecen específicas para alguna farmacia (ej: cambios de horario, turnos compartidos).
        4. MANTÉN los campos estáticos intactos (id, name, address, phone).
        5. ACTUALIZA solo:
           - `schedule`: Lista de objetos {{"startDate": "YYYY-MM-DD", "endDate": "YYYY-MM-DD"}}.
           - `notes`: String con observaciones si las hay en la imagen para esa farmacia. Si no hay, omite el campo.
        6. IMPORTANTE - REGLA DE FECHAS:
           - El cambio de turno se produce los MIÉRCOLES al iniciar la jornada laboral.
           - `startDate` es INCLUSIVO: el primer día de guardia (siempre un miércoles).
           - `endDate` es EXCLUSIVO: es el miércoles en que EMPIEZA el turno de la siguiente farmacia.
           - Por ejemplo, si la imagen dice "4 al 11 de febrero", genera: startDate="2026-02-04", endDate="2026-02-11".
             Esto significa que la farmacia está de guardia los días 4, 5, 6, 7, 8, 9 y 10. El día 11 ya NO es su turno.
           - El endDate de una farmacia DEBE coincidir exactamente con el startDate de la siguiente farmacia.
        7. IMPORTANTE: El año de la imagen puede ser {2026}. Asegúrate de usar el año correcto.
        8. Retorna SOLO el JSON actualizado completo. Sin markdown, sin explicaciones.
        """
        
        models_to_try = ['gemini-3.1-flash-lite', 'gemini-2.5-flash', 'gemini-2.0-flash']
        
        response = None
        for model_name in models_to_try:
            try:
                print(f"🔄 Intentando con modelo: {model_name}...")
                response = client.models.generate_content(
                    model=model_name,
                    contents=[prompt, image]
                )
                print(f"✅ ¡Éxito con {model_name}!")
                break
            except Exception as e:
                print(f"⚠️ {model_name} falló o no existe: {e}")
        
        if not response:
            print("❌ Todos los modelos fallaron.")
            return None
            
        cleaned_response = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(cleaned_response)
    
    except Exception as e:
        print(f"⚠️ Error fatal en Gemini: {e}")
        return None

def main():
    # 1. Obtener imagen actual
    img_url = scrape_image_url()
    if not img_url:
        return
    
    image = download_image(img_url)
    if not image:
        return
        
    # 2. Leer JSON actual
    if not os.path.exists(JSON_PATH):
        print(f"❌ No se encuentra el archivo {JSON_PATH}")
        print(f"📂 Directorio actual (cwd): {os.getcwd()}")
        return

    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        current_data = json.load(f)
        
    # 3. Procesar con IA
    updated_data = update_json_with_gemini(image, current_data)
    
    if updated_data:
        # 4. Guardar cambios
        try:
            with open(JSON_PATH, 'w', encoding='utf-8') as f:
                json.dump(updated_data, f, indent=4, ensure_ascii=False)
            print("💾 pharmacies.json actualizado con éxito.")
        except Exception as e:
            print(f"❌ Error guardando JSON: {e}")
    else:
        print("⚠️ No se pudo actualizar el JSON.")

if __name__ == "__main__":
    main()
