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


def validate_html(html, method_name):
    """Validate that HTML contains real cinema content"""
    if not html or len(html) < 2000:
        snippet = repr(html[:200]) if html else ""
        print(f"  ⚠️ [{method_name}] HTML demasiado corto ({len(html) if html else 0} chars): {snippet}")
        return False
    # Check for block/error pages
    if '403' in html[:500] and 'Forbidden' in html[:500]:
        print(f"  ⚠️ [{method_name}] Página 403 Forbidden detectada")
        return False
    # Validate content has movie-related elements (h1/h2 with movie titles, or wp-block-heading)
    has_content = 'wp-block-heading' in html or 'td-page-content' in html or 'HORARIO' in html
    if not has_content:
        print(f"  ⚠️ [{method_name}] No se detectó contenido de cine")
        return False
    print(f"  ✅ [{method_name}] HTML válido: {len(html)} caracteres")
    return True


def _handle_sgcaptcha(session, response, base_url, method_name):
    """Handle SiteGround's sgcaptcha redirect flow. Returns final HTML or None."""
    html = response.text
    
    # Check if it's an sgcaptcha redirect
    if 'sgcaptcha' not in html and len(html) > 500:
        return html  # Not a captcha page, return as-is
    
    if 'sgcaptcha' not in html:
        return None  # Short HTML but not sgcaptcha
    
    # Parse the redirect URL from meta refresh
    import re
    match = re.search(r'content="0;([^"]+)"', html)
    if not match:
        print(f"  ⚠️ [{method_name}] sgcaptcha detectado pero no se pudo parsear redirect URL")
        return None
    
    redirect_path = match.group(1)
    # Build full URL
    from urllib.parse import urljoin
    captcha_url = urljoin(base_url, redirect_path)
    print(f"  🔑 [{method_name}] sgcaptcha detectado, siguiendo redirect: {captcha_url[:80]}...")
    
    try:
        # Follow the captcha redirect - this should set cookies
        captcha_response = session.get(captcha_url, timeout=30)
        print(f"  🔑 [{method_name}] sgcaptcha status: {captcha_response.status_code}")
        
        # Check if the captcha response itself has another redirect
        captcha_html = captcha_response.text
        if 'sgcaptcha' in captcha_html:
            # There might be a second redirect
            match2 = re.search(r'content="0;([^"]+)"', captcha_html)
            if match2:
                redirect_path2 = match2.group(1)
                captcha_url2 = urljoin(base_url, redirect_path2)
                print(f"  🔑 [{method_name}] Segundo redirect: {captcha_url2[:80]}...")
                captcha_response = session.get(captcha_url2, timeout=30)
        
        # Log cookies received
        cookies = dict(session.cookies) if hasattr(session, 'cookies') else {}
        print(f"  🍪 [{method_name}] Cookies recibidas: {list(cookies.keys())}")
        
        # Now retry the original URL with the session (cookies should be set)
        print(f"  🔄 [{method_name}] Reintentando URL original con cookies...")
        final_response = session.get(base_url, timeout=30)
        final_html = final_response.text
        
        if len(final_html) > 500 and 'sgcaptcha' not in final_html:
            print(f"  ✅ [{method_name}] sgcaptcha bypass exitoso! {len(final_html)} chars")
            return final_html
        else:
            print(f"  ❌ [{method_name}] sgcaptcha bypass falló, HTML: {len(final_html)} chars")
            if 'sgcaptcha' in final_html:
                print(f"     Sigue redirigiendo a sgcaptcha")
            return None
    except Exception as e:
        print(f"  ❌ [{method_name}] Error siguiendo sgcaptcha: {e}")
        return None


def scrape_with_wp_api():
    """Primary method: WordPress REST API with sgcaptcha handling"""
    print("\n📡 Método 0: WordPress REST API...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Try with curl_cffi session (best TLS fingerprint + cookie handling)
    try:
        from curl_cffi import requests as curl_requests
        print("  🔧 Intentando WP API con curl_cffi session...")
        session = curl_requests.Session(impersonate="chrome131")
        session.headers.update(headers)
        
        # First hit the main page to get/solve sgcaptcha cookies
        response = session.get(CINEMA_URL, timeout=30)
        result = _handle_sgcaptcha(session, response, "https://tietarteve.com", 'wp-api-curl')
        
        if result and 'sgcaptcha' not in result:
            # Cookies are set, now try the WP API
            print("  🔧 Cookies obtenidas, accediendo WP API...")
            api_response = session.get(CINEMA_WP_API_URL, timeout=30)
            try:
                data = api_response.json()
                html_content = data.get('content', {}).get('rendered', '')
                if html_content:
                    html = f'<html><body><div class="td-page-content">{html_content}</div></body></html>'
                    if validate_html(html, 'wp-api-curl'):
                        return html
            except:
                print(f"  ⚠️ WP API no devolvió JSON válido tras sgcaptcha")
            
            # If API didn't work, but the main page did, use that
            if validate_html(result, 'wp-page-curl'):
                return result
                
    except ImportError:
        print("  ⚠️ curl_cffi no disponible para WP API")
    except Exception as e:
        print(f"  ⚠️ WP API con curl_cffi falló: {e}")
    
    # Fallback to plain requests with session
    try:
        print("  🔧 Intentando WP API con requests session...")
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(CINEMA_URL, timeout=30)
        result = _handle_sgcaptcha(session, response, "https://tietarteve.com", 'wp-api-requests')
        
        if result and 'sgcaptcha' not in result:
            api_response = session.get(CINEMA_WP_API_URL, timeout=30)
            try:
                data = api_response.json()
                html_content = data.get('content', {}).get('rendered', '')
                if html_content:
                    html = f'<html><body><div class="td-page-content">{html_content}</div></body></html>'
                    if validate_html(html, 'wp-api'):
                        return html
            except:
                pass
            if validate_html(result, 'wp-page'):
                return result
        return None
    except Exception as e:
        print(f"  ❌ WordPress API falló: {e}")
        return None


def scrape_with_curl_cffi():
    """Secondary method: curl_cffi with sgcaptcha handling"""
    print("\n🔧 Método 2: curl_cffi con sgcaptcha...")
    try:
        from curl_cffi import requests as curl_requests
        
        session = curl_requests.Session(impersonate="chrome131")
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        response = session.get(CINEMA_URL, timeout=30)
        
        # Handle sgcaptcha if present
        result = _handle_sgcaptcha(session, response, "https://tietarteve.com", 'curl_cffi')
        if result and validate_html(result, 'curl_cffi'):
            return result
        return None
    except ImportError:
        print("  ⚠️ curl_cffi no instalado, saltando...")
        return None
    except Exception as e:
        print(f"  ❌ curl_cffi falló: {e}")
        return None


def scrape_with_free_proxies():
    """Fallback: Fetch free proxies and try scraping through them."""
    print("\n🌍 Método 3: Rotación de Free Proxies...")
    try:
        from curl_cffi import requests as curl_requests
        import random
        
        # Obtener lista de proxies gratuitos (HTTP/HTTPS)
        print("  📥 Descargando lista de proxies gratuitos...")
        proxy_url = "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
        resp = requests.get(proxy_url, timeout=10)
        resp.raise_for_status()
        proxies = [p.strip() for p in resp.text.split('\n') if p.strip()]
        
        if not proxies:
            print("  ⚠️ No se encontraron proxies")
            return None
            
        print(f"  ✅ {len(proxies)} proxies obtenidos. Probando aleatorios...")
        
        # Probar hasta 5 proxies al azar
        random.shuffle(proxies)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        }
        
        for i, proxy_ip in enumerate(proxies[:6]):
            try:
                print(f"  🔄 Intento {i+1} con proxy: {proxy_ip}")
                proxies_dict = {
                    "http": f"http://{proxy_ip}",
                    "https": f"http://{proxy_ip}",
                }
                
                # Setup session with curl_cffi mapped proxy
                session = curl_requests.Session(impersonate="chrome131", proxies=proxies_dict)
                session.headers.update(headers)
                
                # Usa timeout corto porque muchos proxy gratis son lentos o fallan
                proxy_response = session.get(CINEMA_URL, timeout=15)
                
                result = _handle_sgcaptcha(session, proxy_response, "https://tietarteve.com", f"free-proxy-{i}")
                
                if result and validate_html(result, f"free-proxy-{i}"):
                    return result
            except Exception as e:
                # Ocultamos el stacktrace largo para no ensuciar los logs
                print(f"  ❌ Proxy falló: Timeout/ConnectionError")
                
        return None
    except ImportError:
        print("  ⚠️ curl_cffi no instalado, saltando...")
        return None
    except Exception as e:
        print(f"  ❌ Error general en free_proxies: {e}")
        return None


def scrape_with_proxy_api():
    """Tertiary method: use free proxy/cache APIs"""
    print("\n🌐 Método 3: Proxy APIs...")
    
    encoded_url = urllib.parse.quote(CINEMA_URL, safe='')
    
    proxy_apis = [
        {
            'name': 'allorigins',
            'url': f'https://api.allorigins.win/raw?url={encoded_url}',
            'extract': lambda r: r.text
        },
        {
            'name': 'corsproxy.io',
            'url': f'https://corsproxy.io/?{encoded_url}',
            'extract': lambda r: r.text
        },
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    
    for api in proxy_apis:
        try:
            print(f"  🔄 Intentando {api['name']}...")
            response = requests.get(api['url'], headers=headers, timeout=30)
            response.raise_for_status()
            html = api['extract'](response)
            if validate_html(html, api['name']):
                return html
        except Exception as e:
            print(f"  ❌ {api['name']} falló: {e}")
    
    return None


def scrape_with_google_cache():
    """Fallback: try Google's cached version of the page"""
    print("\n🗄️ Método 4: Google Webcache...")
    
    google_cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{CINEMA_URL}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }
    
    try:
        response = requests.get(google_cache_url, headers=headers, timeout=30)
        response.raise_for_status()
        html = response.text
        if validate_html(html, 'google-cache'):
            return html
        return None
    except Exception as e:
        print(f"  ❌ Google Cache falló: {e}")
        return None


def scrape_with_requests():
    """Fallback: direct requests with browser headers"""
    print("\n📡 Método 5: requests directo...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }
    
    try:
        response = requests.get(CINEMA_URL, headers=headers, timeout=30)
        response.raise_for_status()
        html = response.text
        if validate_html(html, 'requests'):
            return html
        return None
    except Exception as e:
        print(f"  ❌ requests falló: {e}")
        return None


async def scrape_with_playwright():
    """Last resort: Playwright with stealth"""
    print("\n🎭 Método 6: Playwright con stealth...")
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("  ⚠️ playwright no instalado, saltando...")
        return None
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="es-ES"
            )
            page = await context.new_page()
            
            # Stealth: override navigator properties
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['es-ES', 'es', 'en'] });
                window.chrome = { runtime: {} };
            """)
            
            try:
                await page.goto(CINEMA_URL, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(5)
                html = await page.content()
                await browser.close()
                
                if validate_html(html, 'playwright'):
                    return html
                return None
            except Exception as e:
                print(f"  ❌ Playwright navegación falló: {e}")
                await browser.close()
                return None
    except Exception as e:
        print(f"  ❌ Playwright falló: {e}")
        return None


async def scrape_cinema():
    """Try multiple scraping methods until one works"""
    print("🌐 Iniciando scraping del cine...")
    print(f"  🎯 URL: {CINEMA_URL}")
    
    # Try each method in order of reliability
    # wp_api and curl_cffi use sessions to handle sgcaptcha cookies
    methods = [
        ('wp_api', lambda: scrape_with_wp_api()),
        ('curl_cffi', lambda: scrape_with_curl_cffi()),
        ('free_proxies', lambda: scrape_with_free_proxies()),
        ('proxy_api', lambda: scrape_with_proxy_api()),
        ('google_cache', lambda: scrape_with_google_cache()),
        ('requests', lambda: scrape_with_requests()),
    ]
    
    for name, method in methods:
        html = method()
        if html:
            print(f"\n✅ Scraping exitoso con: {name}")
            return html
    
    # Playwright is async, try separately
    html = await scrape_with_playwright()
    if html:
        print(f"\n✅ Scraping exitoso con: playwright")
        return html
    
    print("\n❌ Todos los métodos de scraping fallaron")
    raise Exception("No se pudo obtener el HTML de la página del cine")

def parse_movies(html):
    """Parse HTML and extract raw movie data"""
    print("📜 Parseando HTML...")
    soup = BeautifulSoup(html, 'html.parser')
    
    # Buscar contenedor principal
    content_div = soup.select_one('.td-page-content') or soup.select_one('.entry-content') or soup.body
    
    if not content_div:
        print("❌ No se encontró contenedor de contenido")
        return []
    
    print(f"  📦 Contenedor encontrado: <{content_div.name} class='{' '.join(content_div.get('class', []))}'>")
    
    # Recopilar todos los nodos relevantes (directos e hijos de wrappers)
    # Esto maneja tanto estructura plana como envuelta en divs
    nodes = []
    for child in content_div.children:
        if not child.name:
            continue
        # Si es un div wrapper (wp-block-group, wp-block-image, etc.), incluir sus hijos
        if child.name == 'div' and not child.find(['h1', 'h2'], recursive=False):
            # Es un div que NO tiene h1/h2 directos - incluir el div mismo (para imágenes, etc.)
            nodes.append(child)
        elif child.name == 'div':
            # Div con h1/h2 dentro - desempaquetar sus hijos
            for sub in child.descendants:
                if sub.name:
                    nodes.append(sub)
        else:
            nodes.append(child)
    
    print(f"  📊 Total nodos a procesar: {len(nodes)}")
    
    movies = []
    current_movie = None
    current_day = None  # State Machine: Day Context
    expecting_synopsis = False # State Machine: Synopsis Context
    
    for node in nodes:
        if not node.name:
            continue
            
        # Usar separador de espacio para evitar "defebrero" al concatenar spans
        text = node.get_text(' ', strip=True)
        # Normalizar espacios múltiples
        text = re.sub(r'\s+', ' ', text)
        
        # Detectar títulos (H1 y H2)
        is_heading = node.name in ['h1', 'h2']
        has_text = bool(text)
        is_not_excluded = not any(x in text.upper() for x in ['HORARIO', 'FICHA', 'ARGUMENTO', 'TRAILER', 'NAVALMORAL', '€', 'COMPRA', 'EURO', 'PROGRAMACIÓN', 'PROGRAMACION', 'CINE', 'KINETIKE', 'ENTRADA'])
        is_not_date_range = not re.search(r'del\s+.*\s+al\s+.*', text, re.IGNORECASE) and not re.search(r'^del\s+\d+', text, re.IGNORECASE)
        
        if is_heading and has_text and is_not_excluded and is_not_date_range:
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
