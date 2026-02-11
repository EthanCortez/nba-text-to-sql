data = [
    {"input": "top 5 scorers in 2023",
     "output": "SELECT p.name, AVG(s.points) AS ppg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2023 GROUP BY p.name ORDER BY ppg DESC LIMIT 5;"},

    {"input": "top 10 scorers in 2022",
     "output": "SELECT p.name, AVG(s.points) AS ppg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2022 GROUP BY p.name ORDER BY ppg DESC LIMIT 10;"},

    {"input": "top 5 rebounders in 2021",
     "output": "SELECT p.name, AVG(s.rebounds) AS rpg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2021 GROUP BY p.name ORDER BY rpg DESC LIMIT 5;"},

    {"input": "top 10 rebounders in 2020",
     "output": "SELECT p.name, AVG(s.rebounds) AS rpg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2020 GROUP BY p.name ORDER BY rpg DESC LIMIT 10;"},

    {"input": "top 5 assist leaders in 2019",
     "output": "SELECT p.name, AVG(s.assists) AS apg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2019 GROUP BY p.name ORDER BY apg DESC LIMIT 5;"},

    {"input": "top 10 assist leaders in 2018",
     "output": "SELECT p.name, AVG(s.assists) AS apg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2018 GROUP BY p.name ORDER BY apg DESC LIMIT 10;"},

    {"input": "best scorer in 2017",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2017 ORDER BY s.points DESC LIMIT 1;"},

    {"input": "best rebounder in 2016",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2016 ORDER BY s.rebounds DESC LIMIT 1;"},

    {"input": "best passer in 2015",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2015 ORDER BY s.assists DESC LIMIT 1;"},

    {"input": "best shot blocker in 2014",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2014 ORDER BY s.blocks DESC LIMIT 1;"},

    {"input": "best defender in 2013",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2013 ORDER BY s.defensive_rating ASC LIMIT 1;"},

    {"input": "lebron james points per game in 2024",
     "output": "SELECT AVG(s.points) FROM stats s JOIN players p ON s.player_id = p.id WHERE p.name = 'LeBron James' AND s.season = 2024;"},

    {"input": "kevin durant points per game in 2018",
     "output": "SELECT AVG(s.points) FROM stats s JOIN players p ON s.player_id = p.id WHERE p.name = 'Kevin Durant' AND s.season = 2018;"},

    {"input": "stephen curry three point percentage in 2016",
     "output": "SELECT s.three_pt_pct FROM stats s JOIN players p ON s.player_id = p.id WHERE p.name = 'Stephen Curry' AND s.season = 2016;"},

    {"input": "giannis antetokounmpo rebounds per game in 2020",
     "output": "SELECT AVG(s.rebounds) FROM stats s JOIN players p ON s.player_id = p.id WHERE p.name = 'Giannis Antetokounmpo' AND s.season = 2020;"},

    {"input": "nikola jokic assists per game in 2023",
     "output": "SELECT AVG(s.assists) FROM stats s JOIN players p ON s.player_id = p.id WHERE p.name = 'Nikola Jokic' AND s.season = 2023;"},

    {"input": "top 5 point guards by assists in 2022",
     "output": "SELECT p.name, AVG(s.assists) AS apg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2022 AND p.position = 'PG' GROUP BY p.name ORDER BY apg DESC LIMIT 5;"},

    {"input": "top 10 shooting guards by points in 2019",
     "output": "SELECT p.name, AVG(s.points) AS ppg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2019 AND p.position = 'SG' GROUP BY p.name ORDER BY ppg DESC LIMIT 10;"},

    {"input": "top 5 small forwards by rebounds in 2018",
     "output": "SELECT p.name, AVG(s.rebounds) AS rpg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2018 AND p.position = 'SF' GROUP BY p.name ORDER BY rpg DESC LIMIT 5;"},

    {"input": "top 5 power forwards by points in 2017",
     "output": "SELECT p.name, AVG(s.points) AS ppg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2017 AND p.position = 'PF' GROUP BY p.name ORDER BY ppg DESC LIMIT 5;"},

    {"input": "top 10 centers by rebounds in 2016",
     "output": "SELECT p.name, AVG(s.rebounds) AS rpg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2016 AND p.position = 'C' GROUP BY p.name ORDER BY rpg DESC LIMIT 10;"},

    {"input": "player with highest per in 2015",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2015 ORDER BY s.per DESC LIMIT 1;"},

    {"input": "most minutes played in 2014",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2014 ORDER BY s.minutes DESC LIMIT 1;"},

    {"input": "players who scored more than 40 points in 2023",
     "output": "SELECT DISTINCT p.name FROM players p JOIN game_stats g ON p.id = g.player_id WHERE g.season = 2023 AND g.points > 40;"},

    {"input": "players who had a triple double in 2022",
     "output": "SELECT DISTINCT p.name FROM players p JOIN game_stats g ON p.id = g.player_id WHERE g.season = 2022 AND g.points >= 10 AND g.rebounds >= 10 AND g.assists >= 10;"},

    {"input": "player with the most triple doubles in 2022",
     "output": "SELECT p.name, COUNT(*) AS triple_doubles FROM players p JOIN game_stats g ON p.id = g.player_id WHERE g.season = 2022 AND g.points >= 10 AND g.rebounds >= 10 AND g.assists >= 10 GROUP BY p.name ORDER BY triple_doubles DESC LIMIT 1;"},

    {"input": "players who shot over 50 percent from the field in 2021",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2021 AND s.fg_pct > 0.50;"},

    {"input": "players averaging at least 25 points per game in 2020",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2020 GROUP BY p.name HAVING AVG(s.points) >= 25;"},

    {"input": "players averaging a double double in 2019",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2019 GROUP BY p.name HAVING AVG(s.points) >= 10 AND AVG(s.rebounds) >= 10;"},

    {"input": "players with at least 2 blocks per game in 2018",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2018 GROUP BY p.name HAVING AVG(s.blocks) >= 2;"},

    {"input": "players with at least 2 steals per game in 2017",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2017 GROUP BY p.name HAVING AVG(s.steals) >= 2;"},

    {"input": "players who played more than 35 minutes per game in 2016",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2016 GROUP BY p.name HAVING AVG(s.minutes) > 35;"},

    
    {"input": "lakers record in 2025",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Lakers' AND season = 2025;"},

    {"input": "warriors record in 2016",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Warriors' AND season = 2016;"},

    {"input": "celtics record in 2008",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Celtics' AND season = 2008;"},

    {"input": "bulls record in 1996",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Bulls' AND season = 1996;"},

    {"input": "spurs record in 2014",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Spurs' AND season = 2014;"},

    {"input": "heat record in 2013",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Heat' AND season = 2013;"},

    {"input": "suns record in 2021",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Suns' AND season = 2021;"},

    {"input": "bucks record in 2022",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Bucks' AND season = 2022;"},

    {"input": "knicks record in 2020",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Knicks' AND season = 2020;"},

    {"input": "nets record in 2019",
     "output": "SELECT wins, losses FROM team_records WHERE team = 'Nets' AND season = 2019;"},

    
    {"input": "team with the highest scoring average in 2023",
     "output": "SELECT team FROM team_stats WHERE season = 2023 ORDER BY points DESC LIMIT 1;"},

    {"input": "team with the lowest scoring average in 2022",
     "output": "SELECT team FROM team_stats WHERE season = 2022 ORDER BY points ASC LIMIT 1;"},

    {"input": "team that allowed the fewest points in 2021",
     "output": "SELECT team FROM team_stats WHERE season = 2021 ORDER BY points_allowed ASC LIMIT 1;"},

    {"input": "team that allowed the most points in 2020",
     "output": "SELECT team FROM team_stats WHERE season = 2020 ORDER BY points_allowed DESC LIMIT 1;"},

    {"input": "best defensive team in 2019",
     "output": "SELECT team FROM team_stats WHERE season = 2019 ORDER BY defensive_rating ASC LIMIT 1;"},

    {"input": "worst defensive team in 2018",
     "output": "SELECT team FROM team_stats WHERE season = 2018 ORDER BY defensive_rating DESC LIMIT 1;"},

    {"input": "team with the most steals in 2017",
     "output": "SELECT team FROM team_stats WHERE season = 2017 ORDER BY steals DESC LIMIT 1;"},

    {"input": "team with the fewest steals in 2016",
     "output": "SELECT team FROM team_stats WHERE season = 2016 ORDER BY steals ASC LIMIT 1;"},

    {"input": "team with the best net rating in 2015",
     "output": "SELECT team FROM team_stats WHERE season = 2015 ORDER BY net_rating DESC LIMIT 1;"},

    {"input": "team with the worst net rating in 2014",
     "output": "SELECT team FROM team_stats WHERE season = 2014 ORDER BY net_rating ASC LIMIT 1;"},


    {"input": "top 5 teams by points per game in 2023",
     "output": "SELECT team, points FROM team_stats WHERE season = 2023 ORDER BY points DESC LIMIT 5;"},

    {"input": "top 10 teams by defensive rating in 2022",
     "output": "SELECT team, defensive_rating FROM team_stats WHERE season = 2022 ORDER BY defensive_rating ASC LIMIT 10;"},

    {"input": "top 5 teams by net rating in 2021",
     "output": "SELECT team, net_rating FROM team_stats WHERE season = 2021 ORDER BY net_rating DESC LIMIT 5;"},

    {"input": "top 10 teams by steals in 2020",
     "output": "SELECT team, steals FROM team_stats WHERE season = 2020 ORDER BY steals DESC LIMIT 10;"},


    {"input": "who won warriors vs 76ers on february 3 2026",
     "output": "SELECT winner FROM games WHERE home_team = 'Warriors' AND away_team = '76ers' AND game_date = '2026-02-03';"},

    {"input": "who won lakers vs celtics on december 25 2022",
     "output": "SELECT winner FROM games WHERE home_team = 'Lakers' AND away_team = 'Celtics' AND game_date = '2022-12-25';"},

    {"input": "who won bucks vs suns on july 20 2021",
     "output": "SELECT winner FROM games WHERE home_team = 'Bucks' AND away_team = 'Suns' AND game_date = '2021-07-20';"},

    {"input": "who won heat vs spurs on june 18 2013",
     "output": "SELECT winner FROM games WHERE home_team = 'Heat' AND away_team = 'Spurs' AND game_date = '2013-06-18';"},

    {"input": "who won warriors vs cavaliers on june 19 2016",
     "output": "SELECT winner FROM games WHERE home_team = 'Warriors' AND away_team = 'Cavaliers' AND game_date = '2016-06-19';"},


    {"input": "lakers vs celtics score on december 25 2022",
     "output": "SELECT home_score, away_score FROM games WHERE home_team = 'Lakers' AND away_team = 'Celtics' AND game_date = '2022-12-25';"},

    {"input": "warriors vs rockets score on january 3 2019",
     "output": "SELECT home_score, away_score FROM games WHERE home_team = 'Warriors' AND away_team = 'Rockets' AND game_date = '2019-01-03';"},

    {"input": "nets vs bucks score on march 7 2021",
     "output": "SELECT home_score, away_score FROM games WHERE home_team = 'Nets' AND away_team = 'Bucks' AND game_date = '2021-03-07';"},

    {"input": "suns vs mavericks score on may 15 2022",
     "output": "SELECT home_score, away_score FROM games WHERE home_team = 'Suns' AND away_team = 'Mavericks' AND game_date = '2022-05-15';"},


    {"input": "highest scoring game in the 2023 season",
     "output": "SELECT id FROM games WHERE season = 2023 ORDER BY (home_score + away_score) DESC LIMIT 1;"},

    {"input": "lowest scoring game in the 2022 season",
     "output": "SELECT id FROM games WHERE season = 2022 ORDER BY (home_score + away_score) ASC LIMIT 1;"},

    {"input": "highest scoring game in nba history",
     "output": "SELECT id FROM games ORDER BY (home_score + away_score) DESC LIMIT 1;"},

    {"input": "lowest scoring game in nba history",
     "output": "SELECT id FROM games ORDER BY (home_score + away_score) ASC LIMIT 1;"},

    {"input": "largest comeback in nba history",
     "output": "SELECT id FROM games ORDER BY comeback_margin DESC LIMIT 1;"},

    {"input": "largest comeback in the 2021 season",
     "output": "SELECT id FROM games WHERE season = 2021 ORDER BY comeback_margin DESC LIMIT 1;"},

    {"input": "smallest margin of victory in 2020",
     "output": "SELECT id FROM games WHERE season = 2020 ORDER BY ABS(home_score - away_score) ASC LIMIT 1;"},

    {"input": "biggest blowout in 2019",
     "output": "SELECT id FROM games WHERE season = 2019 ORDER BY ABS(home_score - away_score) DESC LIMIT 1;"},


    {"input": "who won mvp in 2020",
     "output": "SELECT player_name FROM awards WHERE award = 'MVP' AND season = 2020;"},

    {"input": "who won mvp in 2019",
     "output": "SELECT player_name FROM awards WHERE award = 'MVP' AND season = 2019;"},

    {"input": "who won defensive player of the year in 2018",
     "output": "SELECT player_name FROM awards WHERE award = 'Defensive Player of the Year' AND season = 2018;"},

    {"input": "who won rookie of the year in 2017",
     "output": "SELECT player_name FROM awards WHERE award = 'Rookie of the Year' AND season = 2017;"},

    {"input": "who won finals mvp in 2016",
     "output": "SELECT player_name FROM awards WHERE award = 'Finals MVP' AND season = 2016;"},

    {"input": "all nba all stars in 2022",
     "output": "SELECT player_name FROM awards WHERE award = 'All-Star' AND season = 2022;"},

    {"input": "all nba all stars in 2021",
     "output": "SELECT player_name FROM awards WHERE award = 'All-Star' AND season = 2021;"},

    {"input": "players named to all nba first team in 2020",
     "output": "SELECT player_name FROM awards WHERE award = 'All-NBA First Team' AND season = 2020;"},

    {"input": "players named to all nba second team in 2019",
     "output": "SELECT player_name FROM awards WHERE award = 'All-NBA Second Team' AND season = 2019;"},

    {"input": "players named to all nba third team in 2018",
     "output": "SELECT player_name FROM awards WHERE award = 'All-NBA Third Team' AND season = 2018;"},


    {"input": "players who won multiple mvps",
     "output": "SELECT player_name FROM awards WHERE award = 'MVP' GROUP BY player_name HAVING COUNT(*) > 1;"},

    {"input": "players who won multiple finals mvps",
     "output": "SELECT player_name FROM awards WHERE award = 'Finals MVP' GROUP BY player_name HAVING COUNT(*) > 1;"},

    {"input": "players who won defensive player of the year multiple times",
     "output": "SELECT player_name FROM awards WHERE award = 'Defensive Player of the Year' GROUP BY player_name HAVING COUNT(*) > 1;"},

    {"input": "players with at least 10 all star selections",
     "output": "SELECT player_name FROM awards WHERE award = 'All-Star' GROUP BY player_name HAVING COUNT(*) >= 10;"},


    {"input": "players who scored over 20000 career points",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING SUM(s.points) > 20000;"},

    {"input": "players who scored over 30000 career points",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING SUM(s.points) > 30000;"},

    {"input": "players with over 10000 career assists",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING SUM(s.assists) > 10000;"},

    {"input": "players with over 10000 career rebounds",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING SUM(s.rebounds) > 10000;"},

    {"input": "players with over 2000 career blocks",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING SUM(s.blocks) > 2000;"},

    {"input": "players with over 2000 career steals",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING SUM(s.steals) > 2000;"},


    {"input": "most playoff points in nba history",
     "output": "SELECT p.name FROM players p JOIN playoff_stats ps ON p.id = ps.player_id GROUP BY p.name ORDER BY SUM(ps.points) DESC LIMIT 1;"},

    {"input": "most playoff assists in nba history",
     "output": "SELECT p.name FROM players p JOIN playoff_stats ps ON p.id = ps.player_id GROUP BY p.name ORDER BY SUM(ps.assists) DESC LIMIT 1;"},

    {"input": "most playoff rebounds in nba history",
     "output": "SELECT p.name FROM players p JOIN playoff_stats ps ON p.id = ps.player_id GROUP BY p.name ORDER BY SUM(ps.rebounds) DESC LIMIT 1;"},

    {"input": "top 5 playoff scorers in 2020",
     "output": "SELECT p.name, SUM(ps.points) AS pts FROM players p JOIN playoff_stats ps ON p.id = ps.player_id WHERE ps.season = 2020 GROUP BY p.name ORDER BY pts DESC LIMIT 5;"},

    {"input": "top 10 playoff assist leaders in 2021",
     "output": "SELECT p.name, SUM(ps.assists) AS ast FROM players p JOIN playoff_stats ps ON p.id = ps.player_id WHERE ps.season = 2021 GROUP BY p.name ORDER BY ast DESC LIMIT 10;"},

    {"input": "players averaging at least 25 points per game in the 2022 playoffs",
     "output": "SELECT p.name FROM players p JOIN playoff_stats ps ON p.id = ps.player_id WHERE ps.season = 2022 GROUP BY p.name HAVING AVG(ps.points) >= 25;"},


    {"input": "team with the longest winning streak in nba history",
     "output": "SELECT team FROM team_streaks ORDER BY win_streak DESC LIMIT 1;"},

    {"input": "team with the longest winning streak in 2016",
     "output": "SELECT team FROM team_streaks WHERE season = 2016 ORDER BY win_streak DESC LIMIT 1;"},

    {"input": "longest 30 point scoring streak in nba history",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id ORDER BY s.scoring_streak_30 DESC LIMIT 1;"},

    {"input": "player with the longest double double streak",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id ORDER BY s.double_double_streak DESC LIMIT 1;"},


    {"input": "who scored more career points lebron or jordan",
     "output": "SELECT p.name, SUM(s.points) AS pts FROM players p JOIN stats s ON p.id = s.player_id WHERE p.name IN ('LeBron James','Michael Jordan') GROUP BY p.name ORDER BY pts DESC;"},

    {"input": "who has more career assists cp3 or stockton",
     "output": "SELECT p.name, SUM(s.assists) AS ast FROM players p JOIN stats s ON p.id = s.player_id WHERE p.name IN ('Chris Paul','John Stockton') GROUP BY p.name ORDER BY ast DESC;"},

    {"input": "who has more career rebounds shaq or duncan",
     "output": "SELECT p.name, SUM(s.rebounds) AS reb FROM players p JOIN stats s ON p.id = s.player_id WHERE p.name IN ('Shaquille O''Neal','Tim Duncan') GROUP BY p.name ORDER BY reb DESC;"},

    {"input": "who has more championships jordan or lebron",
     "output": "SELECT player_name, COUNT(*) AS titles FROM awards WHERE award = 'NBA Champion' AND player_name IN ('Michael Jordan','LeBron James') GROUP BY player_name ORDER BY titles DESC;"},


    {"input": "who has more points per game jordan or lebron",
     "output": "SELECT p.name, AVG(s.points) AS ppg FROM players p JOIN stats s ON p.id = s.player_id WHERE p.name IN ('Michael Jordan','LeBron James') GROUP BY p.name ORDER BY ppg DESC;"},

    {"input": "who has higher career fg percentage curry or klay",
     "output": "SELECT p.name, AVG(s.fg_pct) AS fg_pct FROM players p JOIN stats s ON p.id = s.player_id WHERE p.name IN ('Stephen Curry','Klay Thompson') GROUP BY p.name ORDER BY fg_pct DESC;"},

    {"input": "highest scoring player ever",
     "output": "SELECT p.name, SUM(s.points) AS career_points FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name ORDER BY career_points DESC LIMIT 1;"},

    {"input": "lowest scoring team in 2023",
     "output": "SELECT team FROM team_stats WHERE season = 2023 ORDER BY points ASC LIMIT 1;"},

    {"input": "top 3 scorers in 2021 playoffs",
     "output": "SELECT p.name, SUM(ps.points) AS pts FROM players p JOIN playoff_stats ps ON p.id = ps.player_id WHERE ps.season = 2021 GROUP BY p.name ORDER BY pts DESC LIMIT 3;"},

    {"input": "players with more than 50 career triple doubles",
     "output": "SELECT p.name FROM players p JOIN game_stats g ON p.id = g.player_id WHERE g.points >= 10 AND g.rebounds >= 10 AND g.assists >= 10 GROUP BY p.name HAVING COUNT(*) > 50;"},

    {"input": "players who won both mvp and finals mvp in the same season",
     "output": "SELECT a1.player_name FROM awards a1 JOIN awards a2 ON a1.player_name = a2.player_name AND a1.season = a2.season WHERE a1.award = 'MVP' AND a2.award = 'Finals MVP';"},

    {"input": "all time leaders in steals",
     "output": "SELECT p.name, SUM(s.steals) AS total_steals FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name ORDER BY total_steals DESC;"},

    {"input": "all time leaders in blocks",
     "output": "SELECT p.name, SUM(s.blocks) AS total_blocks FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name ORDER BY total_blocks DESC;"},

    {"input": "players who played both pg and sg",
     "output": "SELECT p.name FROM players p WHERE p.position IN ('PG','SG') GROUP BY p.name HAVING COUNT(DISTINCT p.position) = 2;"},

    {"input": "top 5 players in 2022 with at least 20 ppg",
     "output": "SELECT p.name, AVG(s.points) AS ppg FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2022 GROUP BY p.name HAVING AVG(s.points) >= 20 ORDER BY ppg DESC LIMIT 5;"},

    {"input": "players with over 50% fg and 40% 3pt in 2023",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2023 AND s.fg_pct >= 0.50 AND s.three_pt_pct >= 0.40;"},

    {"input": "players with triple double in 2020 playoffs",
     "output": "SELECT DISTINCT p.name FROM players p JOIN playoff_stats ps ON p.id = ps.player_id WHERE ps.season = 2020 AND ps.points >= 10 AND ps.rebounds >= 10 AND ps.assists >= 10;"},

    {"input": "players with most double doubles in 2021",
     "output": "SELECT p.name, COUNT(*) AS double_doubles FROM players p JOIN game_stats g ON p.id = g.player_id WHERE g.season = 2021 AND g.points >= 10 AND g.rebounds >= 10 GROUP BY p.name ORDER BY double_doubles DESC LIMIT 1;"},

    {"input": "teams with winning streak longer than 10 games in 2023",
     "output": "SELECT team FROM team_streaks WHERE season = 2023 AND win_streak > 10;"},

    {"input": "teams with longest losing streak in 2022",
     "output": "SELECT team FROM team_streaks WHERE season = 2022 ORDER BY win_streak ASC LIMIT 1;"},

    {"input": "top 5 players by PER in 2023",
     "output": "SELECT p.name, s.per FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2023 ORDER BY s.per DESC LIMIT 5;"},

    {"input": "players who averaged triple double in a season",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name HAVING AVG(s.points) >= 10 AND AVG(s.rebounds) >= 10 AND AVG(s.assists) >= 10;"},

    {"input": "players who played in 2023 and 2024 seasons",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season IN (2023,2024) GROUP BY p.name HAVING COUNT(DISTINCT s.season) = 2;"},

    {"input": "player with most points in single game",
     "output": "SELECT p.name, g.points FROM players p JOIN game_stats g ON p.id = g.player_id ORDER BY g.points DESC LIMIT 1;"},

    {"input": "player with most rebounds in single game",
     "output": "SELECT p.name, g.rebounds FROM players p JOIN game_stats g ON p.id = g.player_id ORDER BY g.rebounds DESC LIMIT 1;"},

    {"input": "player with most assists in single game",
     "output": "SELECT p.name, g.assists FROM players p JOIN game_stats g ON p.id = g.player_id ORDER BY g.assists DESC LIMIT 1;"},

    {"input": "teams that made playoffs every year from 2018 to 2022",
     "output": "SELECT team FROM team_records WHERE season IN (2018,2019,2020,2021,2022) GROUP BY team HAVING COUNT(DISTINCT season) = 5;"},

    {"input": "players with highest three point percentage all time",
     "output": "SELECT p.name, AVG(s.three_pt_pct) AS three_pt_pct FROM players p JOIN stats s ON p.id = s.player_id GROUP BY p.name ORDER BY three_pt_pct DESC;"},

    {"input": "players who scored over 50 points in a playoff game",
     "output": "SELECT DISTINCT p.name FROM players p JOIN playoff_stats ps ON p.id = ps.player_id WHERE ps.points > 50;"},

    {"input": "players with longest consecutive games played streak",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id ORDER BY s.games_played_streak DESC LIMIT 1;"},

    {"input": "players who led the league in scoring for at least 3 seasons",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.points = (SELECT MAX(points) FROM stats WHERE season = s.season) GROUP BY p.name HAVING COUNT(*) >= 3;"},

    {"input": "teams that never made playoffs from 2000 to 2005",
     "output": "SELECT team FROM team_records WHERE season IN (2000,2001,2002,2003,2004,2005) GROUP BY team HAVING SUM(wins) = 0;"},

    {"input": "players who won mvp before turning 25",
     "output": "SELECT player_name FROM awards a JOIN players p ON a.player_name = p.name WHERE a.award = 'MVP' AND (a.season - p.birth_year) < 25;"},

    {"input": "players who won mvp after turning 30",
     "output": "SELECT player_name FROM awards a JOIN players p ON a.player_name = p.name WHERE a.award = 'MVP' AND (a.season - p.birth_year) >= 30;"},

    {"input": "top 3 defensive players in 2021",
     "output": "SELECT p.name, s.defensive_rating FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2021 ORDER BY s.defensive_rating ASC LIMIT 3;"},

    {"input": "teams with highest point differential in 2023",
     "output": "SELECT team, (points - points_allowed) AS diff FROM team_stats WHERE season = 2023 ORDER BY diff DESC LIMIT 5;"},

    {"input": "teams with lowest point differential in 2023",
     "output": "SELECT team, (points - points_allowed) AS diff FROM team_stats WHERE season = 2023 ORDER BY diff ASC LIMIT 5;"},

    {"input": "players who scored more points than rebounds in 2022",
     "output": "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id WHERE s.season = 2022 GROUP BY p.name HAVING SUM(s.points) > SUM(s.rebounds);"},

    {"input": "players with at least 5 triple doubles in 2021",
     "output": "SELECT p.name FROM players p JOIN game_stats g ON p.id = g.player_id WHERE g.season = 2021 AND g.points >= 10 AND g.rebounds >= 10 AND g.assists >= 10 GROUP BY p.name HAVING COUNT(*) >= 5;"}
]