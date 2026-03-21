import os
from dotenv import load_dotenv
from database.session import DatabaseManager
from services.matchmaker import MatchmakerService
from database.models import Song

def run_mashup_engine():
    # 1. Cargamos el entorno y conectamos a Supabase
    load_dotenv()
    db_manager = DatabaseManager(os.getenv("DATABASE_URL"))

    with db_manager.get_session() as session:
        matchmaker = MatchmakerService(session)

        print("\n" + "="*60)
        print(" 🎧 MASH-UPGRADDE ENGINE v1.0 - TERMINAL INTERFACE 🎧 ")
        print("="*60)

        # --- PRUEBA 1: Acapella Bridge ---
        palabra_clave = "noche" 
        print(f"\n[Fase 1] Buscando puentes para la palabra: '{palabra_clave.upper()}'...")
        puentes = matchmaker.get_acapella_bridges(palabra_clave)
        
        if puentes:
            for p in puentes:
                print(f" -> [{p.genre}] {p.artist_name} - '{p.song_title}' (Aparece {p.occurrences} veces)")
        else:
            print("No se encontraron coincidencias para esa palabra.")

        # --- PRUEBA 2: Harmonic Twins ---
        cancion_base_titulo = "Lamento Boliviano"
        print(f"\n[Fase 2] Buscando gemelos armónicos (ADN Léxico) para: '{cancion_base_titulo}'...")
        gemelos = matchmaker.get_harmonic_twins(cancion_base_titulo)
        
        if gemelos:
            for g in gemelos:
                print(f" -> MATCH: {g.artist_name} - '{g.song_title}'")
                print(f"    (Comparten {g.shared_words} palabras clave. Puntuación: {g.score})")
        else:
            print("No se encontraron canciones con ADN similar en la base de datos actual.")
        
        # --- PRUEBA 3: El Mashup Perfecto (DJ Mode: BPM & Key) ---
        print("\n" + "="*60)
        print(" 🎛️  BÚSQUEDA DE MASHUP PERFECTO (KEY & BPM MATCHING) 🎛️ ")
        print("="*60)
        
        # Vamos a buscar la canción base que inyectamos antes
        cancion_base = session.query(Song).filter(Song.title.ilike(f"%{cancion_base_titulo}%")).first()
        
        if cancion_base and cancion_base.camelot_key:
            print(f"▶️ TRACK EN BANDEJA 1: {cancion_base.title}")
            print(f"   Analítica: {cancion_base.bpm} BPM | Tono {cancion_base.camelot_key}")
            print("\nBuscando pistas compatibles en la base de datos...\n")
            
            # Buscamos canciones con la MISMA key, pero distinto artista
            pistas_compatibles = session.query(Song).filter(
                Song.camelot_key == cancion_base.camelot_key,
                Song.artist_id != cancion_base.artist_id
            ).all()
            
            if pistas_compatibles:
                for pista in pistas_compatibles:
                    # Fórmula estándar de DJ para calcular el ajuste de velocidad
                    pitch_shift = ((pista.bpm - cancion_base.bpm) / cancion_base.bpm) * 100
                    
                    print(f"🔥 MATCH ENCONTRADO PARA BANDEJA 2: {pista.title}")
                    print(f"   - Tono Armónico: {pista.camelot_key} (Transición perfecta, no hay 'clash')")
                    print(f"   - BPM original: {pista.bpm}")
                    print(f"   - Ajuste de Pitch necesario: {pitch_shift:+.2f}%")
                    
                    if abs(pitch_shift) <= 10.0:
                        print("   - Veredicto: MEZCLA ALTAMENTE RECOMENDADA (Distorsión inaudible).")
                    else:
                        print("   - Veredicto: MEZCLA EXTREMA (Las voces sonarán agudas/graves como ardillas/monstruos).")
            else:
                print("No hay pistas con la misma escala armónica cargadas todavía.")
        else:
            print("Faltan datos acústicos. ¿Te aseguraste de correr el script seed_dj_data.py?")
            
        print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    run_mashup_engine()