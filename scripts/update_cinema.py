#!/usr/bin/env python3
"""
Cinema Data Updater for Navalmorapp
Scrapes cinema website, uses Gemini AI to clean titles, enriches with TMDB, and generates JSON
"""

import json
import os
import sys
from datetime import datetime
import asyncio
import requests
from bs4 import BeautifulSoup
import re
import urllib.parse

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
CINEMA_URL = "https://tietarteve.com/cine-navalmoral/"
CINEMA_WP_API_URL = "https://tietarteve.com/wp-json/wp/v2/pages/58931"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_READ_TOKEN = os.getenv("TMDB_READ_ACCESS_TOKEN") or os.getenv("TMDB_READ_TOKEN")
OUTPUT_FILE = "cinema_data.json"

# Meses en español
MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

# Configurar Gemini (ahora local en la función)



async def scrape_cinema():
    """Fetch movie list from Kinetike for the next 7 days"""
    print("🌐 Iniciando scraping desde Kinetike (7 días)...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    combined_html = ""
    session = requests.Session()
    
    # URL inicial (redirige y crea sesión)
    base_url = "https://kinetike.com:83/views/init.aspx?cine=NAVALMORALDELAMATA"
    
    try:
        print("  📅 Consultando cartelera del día 1 (Hoy)...")
        res = session.get(base_url, headers=headers, verify=False, timeout=15)
        res.raise_for_status()
        current_html = res.text
        current_url = res.url
        combined_html += current_html + "\n"
        
        # Bucle para 6 días más simulando clic en la flecha de "Siguiente día"
        for i in range(2, 8):
            print(f"  📅 Consultando cartelera del día {i}...")
            
            # Extraer ViewState y EventValidation del HTML anterior
            soup = BeautifulSoup(current_html, 'html.parser')
            vs_input = soup.select_one('#__VIEWSTATE')
            ev_input = soup.select_one('#__EVENTVALIDATION')
            
            viewstate = vs_input['value'] if vs_input else ''
            eventval = ev_input['value'] if ev_input else ''
            
            data = {
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': viewstate,
                '__EVENTVALIDATION': eventval,
                'imgSiguiente.x': '15',
                'imgSiguiente.y': '15'
            }
            
            # Kinetike espera un POST en la misma URL de la sesión
            post_res = session.post(current_url, headers=headers, data=data, verify=False, timeout=15)
            post_res.raise_for_status()
            current_html = post_res.text
            combined_html += current_html + "\n"
            
    except Exception as e:
        print(f"  ⚠️ Error durante el volcado de datos: {e}")
            
    if not combined_html.strip():
        raise Exception("No se pudo conectar a Kinetike en ninguno de los días")
        
    return combined_html


def parse_movies(html):
    """Parse HTML from Kinetike and extract movies and showtimes"""
    print("📜 Parseando HTML de Kinetike...")
    soup = BeautifulSoup(html, 'html.parser')
    
    movies_dict = {}  # Agrupar por título
    
    # 1. Encontrar cada película
    panels = soup.select('.panel_peli')
    if not panels:
        print("❌ No se encontraron contenedores '.panel_peli'")
        return []
        
    print(f"  📊 {len(panels)} elementos panel_peli encontrados en la semana")
    
    for panel in panels:
        # Título y póster
        poster_input = panel.select_one('input[type="image"], img')
        if not poster_input:
            continue
            
        title = poster_input.get('alt', '').strip()
        poster_src = poster_input.get('src', '')
        
        # Omitir si es un logo u otro elemento
        if not title or 'logo' in title.lower():
            continue
            
        if poster_src and not poster_src.startswith('http'):
            poster_src = f"https://kinetike.com:83/views/{poster_src}"
            
        if title not in movies_dict:
            print(f"🎬 Encontrada: {title}")
            movies_dict[title] = {
                'title': title,
                'poster': poster_src,
                'showtimes': {},
                'synopsis': None,
                'duration': None,
                'trailer': None
            }
            
        movie = movies_dict[title]
        
        # 2. Extraer horarios de los botones o enlaces
        html_str = str(panel)
        matches = re.finditer(r'fecha=(\d{2}/\d{2}/\d{4})(?:&|&amp;)hora=(\d{1,2}:\d{2})(?:&|&amp;)sala=([^&"\']+)', html_str)
        
        for match in matches:
            fecha_str, hora, sala = match.groups()
            
            try:
                dt = datetime.strptime(fecha_str, "%d/%m/%Y")
                dias_semana = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"}
                meses = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio", 7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
                
                nombre_dia = dias_semana[dt.weekday()]
                nombre_mes = meses[dt.month]
                
                day_key = f"{nombre_dia} {dt.day} de {nombre_mes}"
                
                if day_key not in movie['showtimes']:
                    movie['showtimes'][day_key] = []
                    
                if hora not in movie['showtimes'][day_key]:
                    movie['showtimes'][day_key].append(hora)
            except Exception as e:
                print(f"  ⚠️ Error parseando fecha {fecha_str}: {e}")
        
    movies = []
    for title, movie in movies_dict.items():
        if movie.get('showtimes'):
            movies.append(movie)
            
    print(f"✅ {len(movies)} películas únicas parseadas en total")
    return movies


async def scrape_tietarteve_fallback():
    """Fetch movie list from TiétarTeVe using Gemini to parse the unstructured text"""
    print("🌐 Iniciando fallback desde TiétarTeVe...")
    
    url = "https://tietarteve.com/cine-navalmoral/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        res = requests.get(url, headers=headers, verify=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        content_div = soup.select_one('.td-page-content, .entry-content')
        if content_div:
            text = content_div.get_text()
            # Limpiar líneas vacías
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            clean_text = "\n".join(lines)
        else:
            clean_text = soup.body.get_text()
            
        if not clean_text or len(clean_text.strip()) < 100:
            raise Exception("El contenido de TiétarTeVe está vacío o es demasiado corto")
            
        print("🤖 Extrayendo películas estructuradas del texto usando Gemini...")
        
        prompt = f"""
        Analiza el siguiente texto de la programación del Cine Navalmoral (obtenido de TiétarTeVe) y extrae las películas con sus horarios en un formato estructurado JSON.
        
        Texto de entrada:
        \"\"\"{clean_text}\"\"\"
        
        REGLAS DE EXTRACCIÓN:
        1. Identifica cada película en cartelera.
        2. Para cada película, extrae:
           - "title": El título de la película en español (ej. "Mortal Kombat II").
           - "showtimes": Un diccionario donde la clave es el día (ej. "Viernes 22 de mayo") y el valor es una lista con las horas de las sesiones (ej. ["18:00", "20:15"]).
           - "synopsis": El argumento o sinopsis de la película (campo "ARGUMENTO").
           - "duration": La duración de la película (ej. "105 min").
           - "trailer": URL del trailer de youtube si aparece en la ficha (campo "TRAILER").
        3. Formatea los días como: "Día_Semana Número de Nombre_Mes" (ej. "Viernes 22 de mayo").
        4. Omitir películas antiguas o de semanas pasadas (solo quédate con el rango actual que se indica al inicio de la programación, ej. "Del viernes 22 al miércoles 27 de mayo de 2026").
        5. Retorna únicamente una lista JSON válida. No incluyes explicaciones ni bloques de código markdown.
        """
        
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        models_to_try = ['gemini-3.1-flash-lite', 'gemini-2.5-flash', 'gemini-2.0-flash']
        response = None
        for model_name in models_to_try:
            try:
                print(f"  🔄 Intentando extraer con: {model_name}...")
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt
                )
                print(f"  ✅ Extracción exitosa con {model_name}!")
                break
            except Exception as e:
                print(f"  ⚠️ {model_name} falló: {e}")
                
        if not response:
            raise Exception("Todos los modelos de Gemini fallaron en la extracción")
            
        cleaned_response = response.text.replace('```json', '').replace('```', '').strip()
        movies = json.loads(cleaned_response)
        
        # Formatear adecuadamente para el resto del pipeline
        formatted_movies = []
        for m in movies:
            formatted_movies.append({
                'title': m.get('title'),
                'poster': None,
                'showtimes': m.get('showtimes', {}),
                'synopsis': m.get('synopsis'),
                'duration': m.get('duration'),
                'trailer': m.get('trailer')
            })
            
        print(f"✅ {len(formatted_movies)} películas extraídas exitosamente desde TiétarTeVe")
        return formatted_movies
        
    except Exception as e:
        print(f"⚠️ Error durante el fallback de TiétarTeVe: {e}")
        return []


def clean_titles_with_ai(movies):
    """Clean movie titles using Gemini AI - BATCH mode"""
    if not movies:
        return movies
    
    print("🤖 Limpiando títulos con Gemini AI...")
    
    # Preparar batch de títulos
    titles_list = [m['title'] for m in movies]
    titles_text = "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles_list)])
    
    prompt = f"""Eres un asistente automatizado de limpieza de datos.
Tu única tarea es limpiar una lista de títulos de películas que te proporciona el usuario.
No debes incluir ningún saludo, introducción, explicación, disculpa, ni texto conversacional.
Debes devolver exactamente el mismo número de líneas que la lista de entrada.
Si un título ya está limpio o si consideras que no hay nada que limpiar, devuélvelo exactamente igual.

Instrucciones de limpieza:
- Elimina palabras como "Cine", "Navalmoral", "Horarios", "ESP", "DIG", "DIGITAL", "3D", fechas, horas, idiomas (ej. "V.O.S.E.") y puntuación innecesaria.
- Mantén solo el nombre propio de la película (ej. "Mortal Kombat II").

Lista de títulos a limpiar (devuelve una línea por cada uno, en el mismo orden, numerados igual):
{titles_text}"""
    
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        models_to_try = ['gemini-3.1-flash-lite', 'gemini-2.5-flash', 'gemini-2.0-flash']
        
        response = None
        for model_name in models_to_try:
            try:
                print(f"🔄 Intentando limpiar títulos con: {model_name}...")
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt
                )
                print(f"✅ ¡Limpieza exitosa con {model_name}!")
                break
            except Exception as e:
                print(f"⚠️ {model_name} falló: {e}")
        
        if not response:
            raise Exception("Todos los modelos fallaron en limpieza de títulos")

        cleaned_text = response.text.strip()
        
        # Parsear respuesta
        cleaned_titles = []
        for line in cleaned_text.split('\n'):
            # Quitar numeración (1., 2., etc)
            clean = re.sub(r'^\d+[\.\)]\s*', '', line).strip()
            if clean:
                cleaned_titles.append(clean)
        
        # Validación de robustez
        conversational_patterns = ["lo siento", "por favor", "no he", "no se", "parece que", "proporciona", "mensaje", "escribe la lista"]
        is_conversational = any(any(pat in title.lower() for pat in conversational_patterns) for title in cleaned_titles)
        
        if len(cleaned_titles) != len(movies) or is_conversational:
            print("⚠️ La respuesta de la IA parece conversacional, incompleta o inválida. Usando títulos originales.")
            for movie in movies:
                movie['title_clean'] = movie['title']
        else:
            # Asignar títulos limpios
            for i, movie in enumerate(movies):
                movie['title_clean'] = cleaned_titles[i]
                print(f"  ✓ {movie['title']} → {cleaned_titles[i]}")
                
    except Exception as e:
        print(f"⚠️ Error en Gemini AI Cleaning: {e}")
        # Fallback: usar títulos originales
        for movie in movies:
            movie['title_clean'] = movie['title']
    
    return movies


def enrich_with_tmdb(movies):
    """Enrich movies with TMDB metadata"""
    print("🎥 Enriqueciendo con TMDB...")
    
    headers = {
        "Authorization": f"Bearer {TMDB_READ_TOKEN}",
        "accept": "application/json"
    }
    
    for movie in movies:
        title = movie.get('title_clean', movie['title'])
        
        try:
            # Buscar película
            search_url = f"https://api.themoviedb.org/3/search/movie"
            params = {"query": title, "language": "es-ES"}
            
            # Usar año si lo tenemos (Evita conflictos como Evolution 2001 vs 2026)
            if movie.get('year'):
                params['year'] = movie['year']
                print(f"  🔍 Buscando en TMDB: {title} ({movie['year']})")
            
            response = requests.get(search_url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['results']:
                    result = data['results'][0]
                    movie_id = result['id']
                    
                    # Obtener detalles completos
                    details_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
                    details_params = {"language": "es-ES", "append_to_response": "videos"}
                    details_response = requests.get(details_url, headers=headers, params=details_params, timeout=10)
                    
                    if details_response.status_code == 200:
                        details = details_response.json()
                        
                        # Añadir metadatos
                        if details.get('poster_path'):
                            movie['poster_tmdb'] = f"https://image.tmdb.org/t/p/w500{details['poster_path']}"
                        if details.get('backdrop_path'):
                            movie['backdrop'] = f"https://image.tmdb.org/t/p/w780{details['backdrop_path']}"
                        
                        if details.get('overview'):
                            movie['overview'] = details.get('overview')
                        movie['rating'] = details.get('vote_average')
                        movie['release_date'] = details.get('release_date')
                        
                        if not movie.get('duration') and details.get('runtime'):
                            movie['duration'] = f"{details['runtime']} min"
                        
                        # Trailer de TMDB si no hay
                        if not movie.get('trailer') and details.get('videos'):
                            for video in details['videos'].get('results', []):
                                if video['type'] == 'Trailer' and video['site'] == 'YouTube':
                                    movie['trailer'] = f"https://www.youtube.com/watch?v={video['key']}"
                                    break
                        
                        print(f"  ✓ {title} enriquecida")
                    
        except Exception as e:
            print(f"  ⚠️ Error TMDB para {title}: {e}")
    
    return movies

def parse_spanish_date(date_str):
    """Convierte fecha string (Viernes 6 de febrero...) a datetime para ordenar"""
    try:
        # Extraer día y mes
        # Ejemplo: "Viernes 6 de febrero de 2026"
        parts = date_str.lower().split()
        day = 1
        month = 1
        year = datetime.now().year
        
        # Buscar número de día (primer dígito encontrado)
        for part in parts:
            if part.isdigit():
                day = int(part)
                break
        
        # Buscar mes
        for m_name, m_num in MESES.items():
            if m_name in parts:
                month = m_num
                break
        
        # Buscar año (4 dígitos)
        for part in parts:
            if part.isdigit() and len(part) == 4:
                year = int(part)
                break
                
        return datetime(year, month, day)
    except:
        return datetime.max  # Si falla, al final

def sort_showtimes(movies):
    """Ordena los horarios de cada película cronológicamente"""
    print("📅 Ordenando horarios...")
    for movie in movies:
        showtimes = movie.get('showtimes', {})
        if not showtimes:
            continue
            
        # Ordenar claves por fecha parseada
        sorted_keys = sorted(showtimes.keys(), key=parse_spanish_date)
        
        # Crear nuevo diccionario ordenado
        new_showtimes = {k: showtimes[k] for k in sorted_keys}
        movie['showtimes'] = new_showtimes
        
    return movies

def generate_json(movies):
    """Generate final JSON file"""
    print("💾 Generando JSON...")
    
    now = datetime.utcnow()
    expires = datetime.fromtimestamp(now.timestamp() + 24*3600)  # 24h
    
    output = {
        "generated_at": now.isoformat() + "Z",
        "expires_at": expires.isoformat() + "Z",
        "version": "1.0",
        "movies": []
    }
    
    for movie in movies:
        output['movies'].append({
            "title": movie.get('title_clean', movie['title']),
            "posterUrl": movie.get('poster_tmdb') or movie.get('poster'),
            "backdropUrl": movie.get('backdrop'),
            "overview": movie.get('overview') or movie.get('synopsis'),
            "rating": movie.get('rating'),
            "releaseDate": movie.get('release_date'),
            "duration": movie.get('duration'),
            "trailerUrl": movie.get('trailer'),
            "showtimes": movie.get('showtimes', {})
        })
    
    # Guardar archivo
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"✅ JSON generado: {OUTPUT_FILE}")
    print(f"📊 {len(movies)} películas procesadas")

async def main():
    """Main execution"""
    try:
        movies = []
        
        # 1. Intentar scraping desde Kinetike
        try:
            html = await scrape_cinema()
            movies = parse_movies(html)
        except Exception as e:
            print(f"⚠️ Error al raspar Kinetike: {e}")
            movies = []
            
        # 2. Fallback a TiétarTeVe si Kinetike no tiene películas
        if not movies:
            print("⚠️ No se encontraron películas en Kinetike (o el cine está en transición). Intentando fallback con TiétarTeVe...")
            try:
                movies = await scrape_tietarteve_fallback()
            except Exception as e:
                print(f"💥 Error fatal en el fallback de TiétarTeVe: {e}")
                movies = []
        
        if not movies:
            print("⚠️ No se encontraron películas en ninguna fuente.")
            # Salir con código 0 para no romper la GitHub Action cuando el cine está en transición y no hay cartelera
            print("ℹ️ Saliendo con éxito para evitar fallos de ejecución en días sin cartelera.")
            sys.exit(0)
        
        # 3. AI Cleaning (BATCH - 1 petición)
        movies = clean_titles_with_ai(movies)
        
        # 4. TMDB Enrichment
        movies = enrich_with_tmdb(movies)
        
        # 5. Sort Showtimes (CRITICAL FIX)
        movies = sort_showtimes(movies)
        
        # 6. Generate JSON
        generate_json(movies)
        
        print("🎉 Proceso completado con éxito!")
        
    except Exception as e:
        print(f"💥 Error fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
