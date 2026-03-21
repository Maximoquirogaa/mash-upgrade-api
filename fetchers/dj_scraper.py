import logging
import requests
import urllib.parse
import re
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class DJDataScraper:
    """
    Miner táctico (Web Scraper) para extraer BPM y Camelot Key.
    Diseñado para ser resiliente a cambios de diseño web usando Regex.
    """
    def __init__(self):
        # Camuflaje AAA: Nos disfrazamos de un navegador Google Chrome real en Windows
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
        }

    def get_song_dj_features(self, artist_name: str, song_title: str) -> dict:
        # Limpiamos el título para que la búsqueda sea exacta
        clean_title = song_title.split("(")[0].strip()
        query = urllib.parse.quote(f"{artist_name} {clean_title}")
        
        # Apuntamos al buscador de Musicstax
        url = f"https://musicstax.com/search?q={query}"
        
        try:
            # 1. Hacemos la petición a la página web
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Error HTTP {response.status_code} al buscar '{clean_title}'.")
                return None
                
            # 2. Convertimos el HTML crudo en un objeto navegable
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 3. Extraemos todo el texto visible de la página
            texto_pagina = soup.get_text()
            
            # 4. Magia Senior: Expresiones Regulares (Regex)
            # Buscamos un número de 2 o 3 dígitos seguido de la palabra "BPM" (ej. "105 BPM")
            bpm_match = re.search(r'(\d{2,3})\s*BPM', texto_pagina, re.IGNORECASE)
            
            # Buscamos el formato Camelot: Un número (1-12) pegado a una letra (A o B) (ej. "9A", "11B")
            key_match = re.search(r'\b([1-9]|1[0-2])[AB]\b', texto_pagina)
            
            if bpm_match and key_match:
                bpm_value = float(bpm_match.group(1))
                camelot_key = key_match.group(0).upper()
                return {
                    "bpm": bpm_value,
                    "camelot_key": camelot_key
                }
            else:
                logger.warning(f"No se encontraron datos precisos en la web para: '{clean_title}'")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Falla de red conectando con el servidor: {e}")
            return None
        except Exception as e:
            logger.error(f"Falla inesperada en el Scraper: {e}")
            return None