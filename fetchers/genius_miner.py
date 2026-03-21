import logging
import time
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Librería especializada que hace API calls + Web Scraping por debajo
import lyricsgenius
from requests.exceptions import Timeout, RequestException

logger = logging.getLogger(__name__)

class GeniusDataMiner:
    """
    Motor de extracción concurrente.
    Obtiene metadatos enriquecidos y letras esquivando bloqueos de IP.
    """

    def __init__(self, access_token: str):
        # Configuración AAA: Timeout estricto, reintentos automáticos y limpieza
        self.api = lyricsgenius.Genius(
            access_token,
            skip_non_songs=True,
            # Excluimos basura que arruina las estadísticas léxicas
            excluded_terms=["(Remix)", "(Live)", "(Traducción)", "(Cover)", "Tracklist"],
            remove_section_headers=True, # Saca los [Estribillo] automáticamente
            timeout=15,
            retries=3
        )
        self.api.verbose = False # Apagamos los prints feos de la librería, usamos nuestro logger

    def _safe_extract_year(self, song_obj) -> Optional[int]:
        """Extrae el año de lanzamiento lidiando con los formatos inconsistentes de Genius."""
        try:
            # Genius a veces devuelve un string 'YYYY-MM-DD', otras un dict, otras nada
            date_str = getattr(song_obj, 'year', None)
            if not date_str:
                date_dict = getattr(song_obj, 'release_date_components', None)
                if date_dict and 'year' in date_dict:
                    return int(date_dict['year'])
            
            if isinstance(date_str, str):
                return int(date_str.split('-')[0])
            return None
        except Exception:
            return None

    def fetch_artist_catalog(self, artist_name: str, max_songs: int = 50) -> List[Dict]:
        """
        Busca al artista y descarga su top N de canciones más populares.
        Retorna un diccionario estructurado listo para la base de datos.
        """
        logger.info(f"🔍 Iniciando escaneo profundo para: {artist_name}")
        
        try:
            # 1. Buscamos el ID exacto del artista en la API
            artist = self.api.search_artist(
                artist_name, 
                max_songs=max_songs, 
                sort="popularity" # Queremos las más escuchadas primero
            )
            
            if not artist:
                logger.error(f"Artista '{artist_name}' no encontrado en Genius.")
                return []

            extracted_data = []
            
            # 2. Procesamos los objetos 'Song' que nos devolvió la librería
            for song in artist.songs:
                # Validaciones de seguridad: si no hay letra o título, la descartamos
                if not song.lyrics or not song.title:
                    continue

                # Estructuramos un DTO (Data Transfer Object) limpio
                song_data = {
                    "artist": artist.name,
                    "title": song.title.strip(),
                    "release_year": self._safe_extract_year(song),
                    "lyrics": song.lyrics,
                    "url": song.url # Guardamos la fuente por temas de Copyright
                }
                extracted_data.append(song_data)

            logger.info(f"✅ Se extrajeron {len(extracted_data)} canciones viables de {artist.name}.")
            return extracted_data

        except Timeout:
            logger.critical("Error: Genius no responde (Timeout). Posible bloqueo de IP temporal.")
            return []
        except Exception as e:
            logger.critical(f"Error catastrófico en la extracción: {e}")
            return []

    # --- MÉTODO AVANZADO: BÚSQUEDA MASIVA CONCURRENTE ---
    def fetch_multiple_artists(self, artist_names: List[str], max_per_artist: int = 10) -> Dict[str, List[Dict]]:
        """
        [Nivel Senior] Usa hilos (Threads) para buscar varios artistas AL MISMO TIEMPO.
        """
        results = {}
        
        # Usamos un Pool de hilos para paralelizar las peticiones de red
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Lanzamos las tareas al pool
            future_to_artist = {
                executor.submit(self.fetch_artist_catalog, name, max_per_artist): name 
                for name in artist_names
            }
            
            # Recolectamos los resultados a medida que van terminando
            for future in as_completed(future_to_artist):
                artist_name = future_to_artist[future]
                try:
                    data = future.result()
                    results[artist_name] = data
                except Exception as e:
                    logger.error(f"El hilo del artista {artist_name} falló: {e}")
                    
        return results