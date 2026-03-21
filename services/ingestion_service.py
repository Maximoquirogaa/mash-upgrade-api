import logging
from typing import List, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# Importamos los modelos y el analizador creados en los pasos anteriores
from database.models import Genre, Artist, Song, Dictionary, WordFrequency
from core.analyzer import LyricAnalyzer

logger = logging.getLogger(__name__)

class IngestionService:
    """
    Servicio de capa empresarial para gestionar la inserción de canciones
    y sus estadísticas léxicas en la base de datos de forma transaccional.
    """

    def __init__(self, db_session: Session, analyzer: LyricAnalyzer):
        # Inyección de dependencias: El servicio no crea la conexión ni el analizador,
        # los recibe. Esto permite hacer Unit Testing (Mocks) fácilmente.
        self.db = db_session
        self.analyzer = analyzer

    def get_or_create_genre(self, genre_name: str) -> Genre:
        """Busca un género; si no existe, lo crea y lo devuelve."""
        genre_name = genre_name.strip().title()
        genre = self.db.query(Genre).filter(Genre.name == genre_name).first()
        
        if not genre:
            genre = Genre(name=genre_name)
            self.db.add(genre)
            self.db.flush() # Sincroniza con la DB para obtener el ID sin hacer un commit final
            
        return genre

    def get_or_create_artist(self, artist_name: str, genre_id: int) -> Artist:
        """Busca un artista; si no existe, lo crea."""
        artist_name = artist_name.strip().title()
        artist = self.db.query(Artist).filter(Artist.name == artist_name).first()
        
        if not artist:
            artist = Artist(name=artist_name, genre_id=genre_id)
            self.db.add(artist)
            self.db.flush()
            
        return artist

    def _get_or_create_words_bulk(self, words: List[str]) -> Dict[str, int]:
        """
        [ALGORITMO AAA Corregido] 
        Obtiene los IDs de las palabras existentes y crea las nuevas asegurando
        la recolección de los IDs generados por la base de datos.
        """
        # 1. Buscamos todas las palabras que YA existen en la DB
        existing_words = self.db.query(Dictionary).filter(Dictionary.word_text.in_(words)).all()
        word_id_map = {w.word_text: w.id for w in existing_words}

        # 2. Filtramos cuáles son las palabras NUEVAS
        new_words_texts = set(words) - set(word_id_map.keys())
        
        if new_words_texts:
            new_word_objects = [Dictionary(word_text=text) for text in new_words_texts]
            
            # SOLUCIÓN: Usamos add_all para que SQLAlchemy nos devuelva los IDs reales
            self.db.add_all(new_word_objects)
            self.db.flush() # Obligamos a Postgres a generar los IDs ahora mismo
            
            # 3. Actualizamos nuestro mapa con los nuevos IDs reales
            for new_word in new_word_objects:
                word_id_map[new_word.word_text] = new_word.id
                
        return word_id_map

    def process_and_save_song(
        self, artist_name: str, genre_name: str, song_title: str, release_year: int, raw_lyrics: str
    ) -> bool:
        """
        Orquesta el flujo completo: Analiza la letra y guarda toda la jerarquía 
        (Género -> Artista -> Canción -> Frecuencias) en una sola transacción segura.
        """
        try:
            # 1. Procesamiento de Texto (Lógica de Negocio)
            # Retorna una lista de tuplas: [('amor', 5), ('noche', 3)]
            word_ranking = self.analyzer.process(raw_lyrics, limit=100)
            
            if not word_ranking:
                logger.warning(f"La canción '{song_title}' no generó palabras válidas para analizar.")
                return False

            # 2. Gestión de Entidades Básicas (Relacionales)
            genre = self.get_or_create_genre(genre_name)
            artist = self.get_or_create_artist(artist_name, genre.id)
            
            # Verificar si la canción ya existe para no duplicar análisis
            existing_song = self.db.query(Song).filter(
                Song.title == song_title, Song.artist_id == artist.id
            ).first()
            
            if existing_song:
                logger.info(f"La canción '{song_title}' ya existe en la base de datos. Omitiendo.")
                return False

            # Crear la nueva canción
            new_song = Song(title=song_title, artist_id=artist.id, release_year=release_year)
            self.db.add(new_song)
            self.db.flush() # Obtenemos el new_song.id

            # 3. Resolución del Diccionario
            # Extraemos solo las palabras del ranking para pasarlas al método de carga masiva
            just_words = [item[0] for item in word_ranking]
            word_to_id_map = self._get_or_create_words_bulk(just_words)

            # 4. Construcción de Frecuencias
            frequencies_to_insert = []
            for word_text, count in word_ranking:
                word_id = word_to_id_map[word_text]
                freq = WordFrequency(
                    song_id=new_song.id,
                    word_id=word_id,
                    occurrence_count=count
                )
                frequencies_to_insert.append(freq)

            # Inserción masiva de frecuencias (Mucho más rápido que un bucle db.add())
            self.db.bulk_save_objects(frequencies_to_insert)

            # 5. COMMIT: Si todo salió bien, guardamos permanentemente en Supabase
            self.db.commit()
            logger.info(f"Éxito: '{song_title}' guardada con {len(frequencies_to_insert)} palabras únicas.")
            return True

        except SQLAlchemyError as db_error:
            # ROLLBACK: Si algo falla (ej. error de red, tipo de dato incorrecto), deshacemos todo
            self.db.rollback()
            logger.error(f"Error de base de datos procesando '{song_title}'. Transacción revertida: {db_error}")
            return False
        except Exception as e:
            self.db.rollback()
            logger.critical(f"Error inesperado procesando '{song_title}': {e}")
            return False