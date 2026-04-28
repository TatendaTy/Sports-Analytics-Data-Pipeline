from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()
conn = os.getenv('CONN_STRING')
if not conn:
    raise SystemExit('CONN_STRING not found in environment')

engine = create_engine(conn)

tables = [
    'league_table', 'top_scorers', 'detail_top', 'player_table',
    'all_time_table', 'all_time_winner_club', 'top_scorers_seasons', 'goals_per_season'
]

with engine.connect() as conn:
    for t in tables:
        try:
            r = conn.execute(text(f'SELECT COUNT(*) FROM {t}'))
            cnt = r.scalar()
        except Exception as e:
            cnt = f'ERROR: {e}'
        print(f'{t}: {cnt}')
