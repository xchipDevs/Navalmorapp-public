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

# Configuración
CINEMA_URL = "https://tietarteve.com/cine-navalmoral/"
CINEMA_WP_API_URL = "https://tietarteve.com/wp-json/wp/v2/pages/58931"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_READ_TOKEN = os.getenv("TMDB_READ_TOKEN")
OUTPUT_FILE = "cinema_data.json"

# Meses en español
MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

# Configurar Gemini (ahora local en la función)



async def scrape_cinema():
    """Fetch movie list from Kinetike"""
    print("🌐 Iniciando scraping desde Kinetike...")
    # Base URL for Navalmoral
    url = "https://kinetike.com:83/views/init.aspx?cine=NAVALMORALDELAMATA"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        html = response.text
        return html
    except Exception as e:
        print(f"❌ Error al obtener datos de Kinetike: {e}")
        raise Exception("No se pudo conectar a Kinetike")


def parse_movies(html):
    """Parse HTML from Kinetike and extract movies and showtimes"""
    print("📜 Parseando HTML de Kinetike...")
    soup = BeautifulSoup(html, 'html.parser')
    
    movies = []
    
    # 1. Encontrar cada película
    panels = soup.select('.panel_peli')
    if not panels:
        print("❌ No se encontraron contenedores '.panel_peli'")
        return []
        
    print(f"  📊 {len(panels)} elementos panel_peli encontrados")
    
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
            
        print(f"🎬 Encontrada: {title}")
        
        movie = {
            'title': title,
            'poster': poster_src,
            'showtimes': {},
            'synopsis': None,
            'duration': None,
            'trailer': None
        }
        
        # 2. Extraer horarios de los botones (javascript:WebForm_DoPostBackWithOptions)
        # o enlaces que contengan hora y sala
        # Kinetike guarda hora, fecha y película en el javascript de cada sesión
        
        # Buscar en todo el texto y links del panel
        # Usamos regex para encontrar: fecha=DD/MM/YYYY&hora=HH:MM&sala=Z
        # En HTML los '&' pueden estar escapados como '&amp;'
        html_str = str(panel)
        matches = re.finditer(r'fecha=(\d{2}/\d{2}/\d{4})(?:&|&amp;)hora=(\d{1,2}:\d{2})(?:&|&amp;)sala=([^&"\']+)', html_str)
        
        for match in matches:
            fecha_str, hora, sala = match.groups()
            
            # Convertir '13/04/2026' a formato legible "Lunes 13" (simulado para no romper compatibilidad)
            try:
                # Kinetike da fechas exactas, lo cual es excelente
                dt = datetime.strptime(fecha_str, "%d/%m/%Y")
                # Mapear día semana
                dias_semana = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"}
                meses = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio", 7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
                
                nombre_dia = dias_semana[dt.weekday()]
                nombre_mes = meses[dt.month]
                
                # Formato final tipo "Lunes 13" para que encaje con la UI existente, o completo
                # Lo hacemos completo para ordenar correctamente
                day_key = f"{nombre_dia} {dt.day} de {nombre_mes}"
                
                if day_key not in movie['showtimes']:
                    movie['showtimes'][day_key] = []
                    
                if hora not in movie['showtimes'][day_key]:
                    movie['showtimes'][day_key].append(hora)
            except Exception as e:
                print(f"  ⚠️ Error parseando fecha {fecha_str}: {e}")
        
        if movie.get('showtimes'):
            movies.append(movie)
            
    print(f"✅ {len(movies)} películas parseadas desde Kinetike")
    return movies

def clean_titles_with_ai(movies):
    """Clean movie titles using Gemini AI - BATCH mode"""
    if not movies:
        return movies
    
    print("🤖 Limpiando títulos con Gemini AI...")
    
    # Preparar batch de títulos
    titles_list = [m['title'] for m in movies]
    titles_text = "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles_list)])
    
    prompt = f"""Limpia estos títulos de películas. Elimina "Cine", "Navalmoral", "Horarios", fechas, horas, idiomas, y extra punctuación.
Devuelve SOLO los títulos limpios, uno por línea, numerados igual:

{titles_text}"""
    
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        # Prioridad: Gemini 3 Flash Preview
        # Fallback: 2.0 Flash -> 1.5 Flash
        models_to_try = ['gemini-3-flash-preview', 'gemini-2.0-flash', 'gemini-1.5-flash']
        
        response = None
        for model_name in models_to_try:
            try:
                print(f"🔄 Intentando limpiar títulos con: {model_name}...")
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt # Solo texto
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
        
        # Asignar títulos limpios
        for i, movie in enumerate(movies):
            if i < len(cleaned_titles):
                movie['title_clean'] = cleaned_titles[i]
                print(f"  ✓ {movie['title']} → {cleaned_titles[i]}")
            else:
                movie['title_clean'] = movie['title']  # fallback
                
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
        # 1. Scraping (tries multiple methods)
        html = await scrape_cinema()
        
        # 2. Parsing
        movies = parse_movies(html)
        
        if not movies:
            # Debug: guardar HTML para análisis
            try:
                debug_file = "debug_cinema_html.txt"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(html)
                print(f"  📄 HTML guardado en {debug_file} ({len(html)} chars)")
            except:
                pass
            print("❌ No se encontraron películas")
            sys.exit(1)
        
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
