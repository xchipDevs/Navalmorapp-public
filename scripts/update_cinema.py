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
from playwright.async_api import async_playwright
import requests
from bs4 import BeautifulSoup
import re

# Configuración
CINEMA_URL = "https://tietarteve.com/cine-navalmoral/"
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
    """Scrape cinema website using Playwright"""
    print("🌐 Iniciando scraping del cine...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto(CINEMA_URL, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(1.5)  # Esperar carga de JavaScript
            
            html = await page.content()
            await browser.close()
            
            return html
        except Exception as e:
            print(f"❌ Error al scrapear: {e}")
            await browser.close()
            raise

def parse_movies(html):
    """Parse HTML and extract raw movie data"""
    print("📜 Parseando HTML...")
    soup = BeautifulSoup(html, 'html.parser')
    
    # Buscar contenedor principal
    content_div = soup.select_one('.td-page-content') or soup.select_one('.entry-content') or soup.body
    
    if not content_div:
        print("❌ No se encontró contenedor de contenido")
        return []
    
    movies = []
    current_movie = None
    current_day = None  # State Machine: Day Context
    expecting_synopsis = False # State Machine: Synopsis Context
    
    for node in content_div.children:
        if not node.name:
            continue
            
        # Usar separador de espacio para evitar "defebrero" al concatenar spans
        text = node.get_text(' ', strip=True)
        # Normalizar espacios múltiples
        text = re.sub(r'\s+', ' ', text)
        
        # Detectar títulos (H2)
        if node.name == 'h2' and text and not any(x in text.upper() for x in ['HORARIO', 'FICHA', 'ARGUMENTO', 'TRAILER', 'NAVALMORAL', '€', 'COMPRA']):
            # Guardar película anterior
            if current_movie and current_movie.get('poster') and current_movie.get('showtimes'):
                movies.append(current_movie)
            
            # Nueva película
            current_movie = {
                'title': text,
                'poster': None,
                'showtimes': {},
                'synopsis': None,
                'duration': None,
                'trailer': None
            }
            current_day = None
            expecting_synopsis = False
            print(f"🎬 Encontrada: {text}")
            continue
        
        if not current_movie:
            continue
        
        # Detectar póster
        if not current_movie['poster']:
            img = node.select_one('img')
            if img:
                src = img.get('data-src') or img.get('src')
                if src and 'base64' not in src and 'logo' not in src.lower():
                    current_movie['poster'] = src
        
        # ---------------------------------------------------------
        # 1. State Machine: Date Context (Matches ScraperService.dart)
        # ---------------------------------------------------------
        # Regex para detectar días (Lunes, Martes... o Del X al Y)
        day_regex = re.compile(r'(Lunes|Martes|Miércoles|Miercoles|Jueves|Viernes|Sábado|Sabado|Domingo|Diario|Laborables|Festivos|Del\s+\d+|Del\s+\w+)', re.IGNORECASE)
        day_match = day_regex.search(text)
        
        # Si encontramos una fecha válida y el texto no es larguísimo (evitar sinopsis falsas)
        if day_match and len(text) < 80:
            candidate = text.strip()
            
            # Limpieza: Si es "Lunes 27: 17:00", nos quedamos con "Lunes 27"
            if ':' in candidate:
                parts = candidate.split(':')
                # Si la parte derecha parece un dígito, cortamos
                if len(parts) > 1 and re.match(r'\s*\d', parts[1]):
                    candidate = parts[0].strip()
            
            # Quitar dos puntos finales
            candidate = candidate.rstrip(':').strip()
            
            if len(candidate) < 50:
                # Capitalizar
                candidate = candidate[0].upper() + candidate[1:] if candidate else candidate
                current_day = candidate
                print(f"  📅 Contexto fecha: {current_day}")
        
        # ---------------------------------------------------------
        # 2. Detectar horarios (Times)
        # ---------------------------------------------------------
        time_matches = re.findall(r'(\d{1,2}[:\.]\d{2})', text)
        if time_matches:
            # Usar el día del contexto actual, o 'Horarios' si no hay contexto
            day_key = current_day if current_day else "Horarios"
            
            if day_key not in current_movie['showtimes']:
                current_movie['showtimes'][day_key] = []
            
            # Collect all found times first
            for time in time_matches:
                clean_time = time.replace('.', ':')
                if clean_time not in current_movie['showtimes'][day_key]:
                    current_movie['showtimes'][day_key].append(clean_time)
            
            # Lógica de Deduplicación (Portado de ScraperService.dart)
            times = current_movie['showtimes'][day_key]
            to_remove = set()
            
            for t in times:
                try:
                    parts = t.split(':')
                    h = int(parts[0])
                    m = parts[1]
                    
                    if 13 <= h <= 23:
                        h12 = h - 12
                        # Marcar equivalentes 12h para eliminar
                        to_remove.add(f"{h12}:{m}")
                        to_remove.add(f"{h12:02d}:{m}")
                except:
                    continue
            
            # Filtrar lista final
            current_movie['showtimes'][day_key] = [t for t in times if t not in to_remove]
        
        # Detectar sinopsis
        # Detectar sinopsis (State Machine & Direct)
        if not current_movie['synopsis']:
            # Caso 1: Header "ARGUMENTO" detectado previamente
            if expecting_synopsis and len(text) > 30 and not any(x in text for x in ['Título original:', 'Dirección:', 'Reparto:', 'FICHA']):
                current_movie['synopsis'] = text.strip()
                expecting_synopsis = False
                print(f"  📖 Sinopsis capturada (Next Node): {text[:30]}...")
            
            # Caso 2: Texto largo que contiene o sigue a header (Single Node or Trigger)
            elif len(text) > 10 and any(x in text.upper() for x in ['ARGUMENTO', 'SINOPSIS']):
                # Si es un header corto ("ARGUMENTO"), activar flag para siguiente nodo
                if len(text) < 30:
                    expecting_synopsis = True
                    print("  👀 Esperando sinopsis en siguiente nodo...")
                else:
                    # Si contiene el texto entero: "ARGUMENTO: Bla bla bla"
                    current_movie['synopsis'] = re.sub(r'(ARGUMENTO|SINOPSIS)[\s:]*', '', text, flags=re.IGNORECASE).strip()
                    print(f"  📖 Sinopsis capturada (Same Node): {current_movie['synopsis'][:30]}...")

            # Caso 3: Heurística (Párrafo largo huérfano después de Ficha/Título)
            # Si a estas alturas no tenemos sinopsis y el texto es largo y NO es metadata
            elif len(text) > 60 and not any(x in text for x in ['Título original:', 'Dirección:', 'Reparto:', 'FICHA', 'HORARIO', 'Sábado', 'Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']):
                 current_movie['synopsis'] = text.strip()
                 print(f"  📖 Sinopsis capturada (Heurística): {text[:30]}...")
        
        
        # Detectar duración
        dur_match = re.search(r'Duración:\s*(\d+)\s*min', text)
        if dur_match:
            current_movie['duration'] = f"{dur_match.group(1)} min"

        # Detectar Año
        year_match = re.search(r'Año:\s*(\d{4})', text)
        if year_match:
            current_movie['year'] = year_match.group(1)
            print(f"  📅 Año detectado: {current_movie['year']}")
        
        # Detectar trailer
        if not current_movie['trailer']:
            iframe = node.select_one('iframe')
            if iframe:
                src = iframe.get('src') or iframe.get('data-src')
                if src and ('youtube' in src or 'youtu.be' in src):
                    current_movie['trailer'] = src
    
    # Guardar última película
    if current_movie and current_movie.get('poster') and current_movie.get('showtimes'):
        movies.append(current_movie)
    
    print(f"✅ {len(movies)} películas encontradas")
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
        # 1. Scraping
        html = await scrape_cinema()
        
        # 2. Parsing
        movies = parse_movies(html)
        
        if not movies:
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
