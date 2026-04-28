from pathlib import Path

from scrape import (
    league_table,
    top_scorers,
    detail_top,
    player_table,
    all_time_table,
    all_time_winner_club,
    top_scorers_seasons,
    goals_per_season,
)


OUTPUT_DIR = Path("parquet_exports")


def export_dataframe(name, dataframe):
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"{name}.parquet"
    dataframe.to_parquet(output_path, index=False)
    print(f"wrote {output_path} ({dataframe.shape[0]} rows, {dataframe.shape[1]} columns)")


def main():
    functions = [
        league_table,
        top_scorers,
        detail_top,
        player_table,
        all_time_table,
        all_time_winner_club,
        top_scorers_seasons,
        goals_per_season,
    ]

    for func in functions:
        export_dataframe(func.__name__, func())


if __name__ == "__main__":
    main()