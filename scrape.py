import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import cloudscraper
from io import StringIO

_WORLD_FOOTBALL_SCRAPER = cloudscraper.create_scraper()

def _get_soup(url, use_cloudscraper=False):
    if use_cloudscraper:
        response = _WORLD_FOOTBALL_SCRAPER.get(url, timeout=30)
    else:
        response = requests.get(url, timeout=30)
    return BeautifulSoup(response.text, "html.parser")

def league_table():
    """this function scrapes the league table data 

    Returns:
       dataframe : returns the league table data in a dataframe format
    """    
    url = 'https://www.bbc.com/sport/football/premier-league/table'
    headers = [] # list to store the column names of the league table
    soup = _get_soup(url) # parsing the response using BeautifulSoup
    table = soup.select_one("div.ssrcss-1r0zndr-TabWrapper table") # finding the season table inside the BBC table wrapper

    # looping through the table header elements and storing the text in the headers list
    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    league_table = pd.DataFrame(columns = headers) # creating an empty dataframe with the column names stored in the headers list
    
    # looping through the table row elements (excluding the header row) and storing the text in a list,
    #  then appending the list as a new row in the league_table dataframe
    for j in table.find_all('tr')[1:]:
        # find all the table content with tr tag
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(league_table)
        league_table.loc[length] = row
    league_table.drop(["Form, Last 6 games, Oldest first"], axis=1, inplace=True)
    return league_table 

def top_scorers():
    url = 'https://www.bbc.com/sport/football/premier-league/top-scorers'
    headers = []
    soup = _get_soup(url)
    table = soup.find("table", class_="gs-o-table") or soup.find("table")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    top_scorers = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(top_scorers)
        top_scorers.loc[length] = row

    top_scorers.Name = top_scorers.Name.replace(r'([A-Z])', r' \1', regex=True).str.split()
    top_scorers.Name = top_scorers.Name.apply(lambda x: ' '.join(dict.fromkeys(x).keys()))

    top_scorers['Club'] = top_scorers.Name.str.split().str[2:].str.join(' ')
    top_scorers.Name = top_scorers.Name.str.split().str[:2].str.join(' ')
    col = top_scorers.pop("Club")
    top_scorers.insert(2, 'Club', col)
    top_scorers.Club = top_scorers.Club.apply(lambda x: 'Manchester City' if 'Manchester City' in x else x)
    top_scorers.Club = top_scorers.Club.apply(lambda x: 'Manchester United' if 'Manchester United' in x else x)
    top_scorers.Club = top_scorers.Club.apply(lambda x: 'Brighton & Hove Albion' if 'Brighton & Hove Albion' in x else x)

    return top_scorers

def detail_top():
    url = 'https://www.worldfootball.net/goalgetter/eng-premier-league-2025-2026/'
    soup = _get_soup(url, use_cloudscraper=True)
    table = soup.find("table", class_="standard_tabelle") or soup.find("table")

    detail_top_scorer = pd.read_html(StringIO(str(table)))[0]
    detail_top_scorer = detail_top_scorer.loc[:, ~detail_top_scorer.columns.astype(str).str.contains("^Unnamed")]
    if "Team" in detail_top_scorer.columns:
        detail_top_scorer["Team"] = detail_top_scorer["Team"].astype(str).str.replace("\n\n", "", regex=False)
    if "Goals (Penalty)" in detail_top_scorer.columns:
        detail_top_scorer["Penalty"] = detail_top_scorer["Goals (Penalty)"].astype(str).str.split().str[-1:].str.join(" ")
        detail_top_scorer["Penalty"] = detail_top_scorer["Penalty"].str.replace("(", "", regex=False)
        detail_top_scorer["Penalty"] = detail_top_scorer["Penalty"].str.replace(")", "", regex=False)
        detail_top_scorer["Goals (Penalty)"] = detail_top_scorer["Goals (Penalty)"].astype(str).str.split().str[0].str.join("")
        detail_top_scorer.rename(columns={"Goals (Penalty)": "Goals"}, inplace=True)
    if "#" in detail_top_scorer.columns:
        detail_top_scorer = detail_top_scorer.drop(["#"], axis=1)
    return detail_top_scorer

def player_table():
    teams_url = "https://www.worldfootball.net/competition/co91/england-premier-league/teams/"
    soup = _get_soup(teams_url, use_cloudscraper=True)

    team_links = []
    for anchor in soup.select('a[href^="/teams/te"]'):
        href = anchor.get("href", "")
        if not href:
            continue
        parts = href.strip("/").split("/")
        # Keep only canonical team pages like /teams/te1267/manchester-city/
        if len(parts) >= 3 and parts[0] == "teams" and parts[1].startswith("te"):
            team_links.append("https://www.worldfootball.net" + "/" + "/".join(parts[:3]) + "/squad/")

    team_links = list(dict.fromkeys(team_links))

    players_frames = []
    for team_url in team_links:
        team_soup = _get_soup(team_url, use_cloudscraper=True)
        table = team_soup.find("table")
        if table is None:
            continue

        headers = ["Player", "Team", "born", "Height", "Position"]
        players = pd.DataFrame(columns=headers)
        team_name = team_soup.title.text.split(" » ")[0] if team_soup.title else ""
        current_position = ""

        for row_tag in table.find_all("tr"):
            th_cells = row_tag.find_all("th")
            if len(th_cells) == 1 and th_cells[0].text.strip() and not row_tag.find_all("td"):
                current_position = th_cells[0].text.strip()
                continue

            row_data = row_tag.find_all("td")
            if len(row_data) < 6:
                continue
            row = {
                "Player": row_data[2].text.strip(),
                "Team": team_name,
                "born": row_data[5].text.strip(),
                "Height": "",
                "Position": current_position,
            }
            players.loc[len(players)] = row

        if not players.empty:
            players_frames.append(players)

    if not players_frames:
        return pd.DataFrame(columns=["Player", "Team", "born", "Height", "Position"])

    df = pd.concat(players_frames, axis=0).reset_index(drop=True)
    return df

def all_time_table():
    url = 'https://www.worldfootball.net/alltime_table/eng-premier-league/pl-only/'
    soup = _get_soup(url, use_cloudscraper=True)
    table = soup.find("table", class_="standard_tabelle") or soup.find("table")

    alltime_table = pd.read_html(StringIO(str(table)))[0]
    alltime_table = alltime_table.iloc[:, [0, 2, 5, 6, 7, 8, 9, 10, 11]].copy()
    alltime_table.columns = ["pos", "Team", "Matches", "wins", "Draws", "Losses", "Goals", "Dif", "Points"]
    alltime_table["Team"] = alltime_table["Team"].astype(str).str.replace("\n", "", regex=False)
    return alltime_table

def all_time_winner_club():
    url = 'https://www.worldfootball.net/winner/eng-premier-league/'
    soup = _get_soup(url, use_cloudscraper=True)
    table = soup.find("table", class_="standard_tabelle") or soup.find("table")

    winners = pd.read_html(StringIO(str(table)))[0]
    winners = winners.loc[:, ~winners.columns.astype(str).str.contains("^Unnamed")]
    if "Year" in winners.columns:
        winners["Year"] = winners["Year"].astype(str).str.replace("\n", "", regex=False)
    return winners


def top_scorers_seasons():
    url = 'https://www.worldfootball.net/top_scorer/eng-premier-league/'
    soup = _get_soup(url, use_cloudscraper=True)
    table = soup.find("table", class_="standard_tabelle") or soup.find("table")

    winners = pd.read_html(StringIO(str(table)))[0]
    winners = winners.loc[:, ~winners.columns.astype(str).str.contains("^Unnamed")]
    winners = winners.loc[:, [column for column in winners.columns if column != "#"]]
    winners = winners.replace('\\n', '', regex=True).astype(str)
    if "Season" in winners.columns:
        winners["Season"] = winners["Season"].replace('', np.nan).ffill()
    return winners

def goals_per_season():
    url = 'https://www.worldfootball.net/stats/eng-premier-league/1/'
    soup = _get_soup(url, use_cloudscraper=True)
    table = soup.find("table", class_="standard_tabelle") or soup.find("table")

    goals_per_season = pd.read_html(StringIO(str(table)))[0]
    goals_per_season = goals_per_season.iloc[:, [0, 2, 3, 4, 5, 6, 7, 8, 9]].copy()
    goals_per_season.columns = ["pos", "Team", "Team Short", "M", "W", "D", "L", "Score", "Diff"]
    goals_per_season["Team"] = goals_per_season["Team"].astype(str).str.replace("\n", "", regex=False)
    goals_per_season["Team Short"] = goals_per_season["Team Short"].astype(str).str.replace("\n", "", regex=False)

    return goals_per_season
