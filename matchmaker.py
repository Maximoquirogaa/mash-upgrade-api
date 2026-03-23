import logging
import unicodedata
from dataclasses import dataclass
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import Song, Artist, Genre, WordFrequency, Dictionary

logger = logging.getLogger(__name__)

# --- DTOs (Data Transfer Objects) Estándar AAA ---
@dataclass
class BridgeResult:
    word: str
    song_title: str
    artist_name: str
    genre: str
    occurrences: int

@dataclass
class TwinResult:
    song_title: str
    artist_name: str
    genre: str
    shared_words: int
    score: int

class MatchmakerService:
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_acapella_bridges(self, target_word: str, genre: Optional[str] = None, year: Optional[str] = None, limit_per_genre: int = 3) -> List[BridgeResult]:
        
        # 1. Aspiradora de Tildes
        target_word = target_word.lower().strip()
        target_word = ''.join(c for c in unicodedata.normalize('NFD', target_word) if unicodedata.category(c) != 'Mn')

        word_objs = self.db.query(Dictionary).filter(Dictionary.word_text.ilike(f"{target_word}%")).all()
        
        if not word_objs:
            return []

        word_ids = [w.id for w in word_objs]

        # 3. Query Base - Cero ambigüedad
        query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                # 🚨 Le cambiamos el nombre para que no choque con la columna real
                func.sum(WordFrequency.occurrence_count).label("total_occurrences")
            )
            .join(WordFrequency, Song.id == WordFrequency.song_id)
            .join(Artist, Song.artist_id == Artist.id)
            .join(Genre, Artist.genre_id == Genre.id)
            .filter(WordFrequency.word_id.in_(word_ids))
            # 🚨 Agrupamos solo por los IDs (Postgres lo permite y SQLAlchemy no se marea)
            .group_by(Song.id, Artist.id, Genre.id)
        )

        if genre and genre.strip():
            query = query.filter(Genre.name.ilike(f"%{genre.strip()}%"))

        if year and year.strip():
            if year == 'retro':
                query = query.filter(Song.release_year.isnot(None), Song.release_year < 2010)
            else:
                query = query.filter(Song.release_year == int(year))

        # 4. Ordenamos usando la función directa
        query = query.order_by(func.sum(WordFrequency.occurrence_count).desc()).limit(limit_per_genre * 5)

        results = query.all()
        
        bridges = [
            BridgeResult(
                word=target_word,
                song_title=row.title,
                artist_name=row.artist_name,
                genre=row.genre_name,
                # 🚨 Leemos el nuevo nombre
                occurrences=row.total_occurrences
            )
            for row in results
        ]
        return bridges


    def get_harmonic_twins(self, song_title: str, top_dna_words: int = 10) -> List[TwinResult]:
        source_song = self.db.query(Song).filter(Song.title.ilike(f"%{song_title}%")).first()
        if not source_song:
            return []

        dna_frequencies = (
            self.db.query(WordFrequency.word_id)
            .filter(WordFrequency.song_id == source_song.id)
            .order_by(WordFrequency.occurrence_count.desc())
            .limit(top_dna_words)
            .all()
        )
        dna_word_ids = [freq.word_id for freq in dna_frequencies]

        if not dna_word_ids:
            return []

        # Mismo blindaje para la función de Gemelos
        match_query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                func.count(WordFrequency.word_id).label("total_shared"),
                func.sum(WordFrequency.occurrence_count).label("total_score")
            )
            .join(Artist, Song.artist_id == Artist.id)
            .join(Genre, Artist.genre_id == Genre.id)
            .join(WordFrequency, Song.id == WordFrequency.song_id)
            .filter(WordFrequency.word_id.in_(dna_word_ids))
            .filter(Song.id != source_song.id)
            .group_by(Song.id, Artist.id, Genre.id)
            .having(func.count(WordFrequency.word_id) >= 3)
            .order_by(func.count(WordFrequency.word_id).desc(), func.sum(WordFrequency.occurrence_count).desc())
            .limit(5)
        )

        matches = match_query.all()

        return [
            TwinResult(
                song_title=row.title,
                artist_name=row.artist_name,
                genre=row.genre_name,
                shared_words=row.total_shared,
                score=row.total_score
            )
            for row in matches
        ]