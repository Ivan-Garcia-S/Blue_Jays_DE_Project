-- 3a) Who were the top 5 teams in regular season wins in 2025? (gametype = R)

WITH team_games AS (
    SELECT
        gamepk,
        hometeamname AS team,
        CASE WHEN hometeamscore > awayteamscore THEN 1 ELSE 0 END AS win
    FROM game
    WHERE gametype = 'R'
    UNION ALL
    SELECT
        gamepk,
        awayteamname AS team,
        CASE WHEN awayteamscore > hometeamscore THEN 1 ELSE 0 END AS win
    FROM game
    WHERE gametype = 'R'
)

SELECT
    team,
    SUM(win) AS wins
FROM team_games
GROUP BY team
ORDER BY wins DESC
LIMIT 5;


-- 3b) Which players had 35 or more stolen bases in the 2025 MLB regular season?

SELECT
  rp.runnerid,
  MAX(rp.runnerfullname) AS fullname,
  COUNT(*) FILTER (
    WHERE rp.movementreason LIKE '%stolen_base%'
      AND rp.is_out = false
  ) AS stolen_bases
FROM runner_play rp
JOIN game g
  ON g.gamepk = rp.gamepk
WHERE g.gametype = 'R'
GROUP BY rp.runnerid
HAVING COUNT(*) FILTER (
  WHERE rp.movementreason LIKE '%stolen_base%'
    AND rp.is_out = false
) >= 35
ORDER BY stolen_bases DESC, fullname;


-- 3c) Which 10 players had the most "extra bases taken"? - where a baserunner advanced more than 1 base on a single or more than 2 bases on a double
--
-- ASSUMPTION: Interpreted baserunner as runner already on base, excldued batter-runner for this question.

WITH rp AS (
  SELECT
    runnerid,
    runnerfullname,
    eventtype,
    startbase,
    reachedbase,

    CASE startbase
      WHEN '1B' THEN 1
      WHEN '2B' THEN 2
      WHEN '3B' THEN 3
      ELSE NULL
    END AS start_n,

    CASE reachedbase
      WHEN '1B' THEN 1
      WHEN '2B' THEN 2
      WHEN '3B' THEN 3
      WHEN 'HM' THEN 4
      ELSE NULL
    END AS reached_n
  FROM runner_play
  WHERE is_out = false              
    AND startbase <> 'B'          
    AND reachedbase IS NOT NULL
)

SELECT
  runnerid,
  MAX(runnerfullname) AS runnerfullname,
  SUM(
    CASE
      WHEN eventtype LIKE '%single%'
       AND reached_n - start_n > 1 THEN 1
      WHEN eventtype ~ '(^|,)double(,|$)'
       AND reached_n - start_n > 2 THEN 1

      ELSE 0
    END
  ) AS extra_bases_taken
FROM rp
GROUP BY runnerid
ORDER BY extra_bases_taken DESC, runnerid
LIMIT 10;


-- 3d) What team (in what gamepk) had the largest deficit but came back to win in 2025?

SELECT
  g.gamepk,
  CASE
    WHEN g.hometeamscore > g.awayteamscore THEN g.hometeamname
    ELSE g.awayteamname
  END AS winner_team,
  MAX(
    GREATEST(
      0,
      -CASE
         WHEN (CASE WHEN g.hometeamscore > g.awayteamscore THEN g.hometeamid ELSE g.awayteamid END) = l.battingteamid
           THEN l.battingteam_score_diff
         ELSE -l.battingteam_score_diff
       END
    )
  ) AS max_deficit
FROM game g
JOIN linescore l
  ON l.gamepk = g.gamepk
WHERE g.hometeamscore <> g.awayteamscore
GROUP BY g.gamepk, winner_team
ORDER BY max_deficit DESC
LIMIT 1;


-- 3e) Which 5 teams had the most come-from-behind wins in all games in 2025?
--
-- ASSUMPTION: trailing at the half inning counts as "come-from-behind"

WITH per_half AS (
  SELECT
    g.gamepk,
    CASE WHEN g.hometeamscore > g.awayteamscore THEN g.hometeamid ELSE g.awayteamid END AS winner_teamid,
    CASE WHEN g.hometeamscore > g.awayteamscore THEN g.hometeamname ELSE g.awayteamname END AS winner_team_name,

    CASE
      WHEN (CASE WHEN g.hometeamscore > g.awayteamscore THEN g.hometeamid ELSE g.awayteamid END) = l.battingteamid
        THEN l.battingteam_score_diff
      ELSE -l.battingteam_score_diff
    END AS winner_diff
  FROM game g
  JOIN linescore l ON l.gamepk = g.gamepk
  WHERE g.hometeamscore <> g.awayteamscore
)
SELECT
  winner_teamid,
  MAX(winner_team_name) AS team_name,
  COUNT(*) AS come_from_behind_wins
FROM (
  SELECT
    gamepk,
    winner_teamid,
    winner_team_name
  FROM per_half
  GROUP BY gamepk, winner_teamid, winner_team_name
  HAVING MIN(winner_diff) < 0
) games
GROUP BY winner_teamid
ORDER BY come_from_behind_wins DESC
LIMIT 5;


-- 3f) Write a query that identifies the most aggressive baserunners and explain your reasoning
--
-- REASONING: This query calculates the percent of aggresive plays that were made out of the total amount of base running plays
-- each runner made (not including batters). Aggresive plays in this case are plays where runner stole a base, got caught stealing,
-- got picked off, went first to third, or went second to home. Set minimum opportunities to 50.

WITH base AS (
  SELECT
    runnerid,
    runnerfullname,
    startbase,
    is_firsttothird,
    is_secondtohome,
    eventtype
  FROM runner_play
  WHERE startbase <> 'B'
),
runner_aggression AS (
  SELECT
    runnerid,
    MAX(runnerfullname) AS runnerfullname,
    COUNT(*) AS opportunities,

    COUNT(*) FILTER (
      WHERE eventtype LIKE '%stolen_base%'
         OR eventtype LIKE '%caught_stealing%'
    ) AS sb_attempts,

    COUNT(*) FILTER (WHERE is_firsttothird = true) AS first_to_third,
    COUNT(*) FILTER (WHERE is_secondtohome = true) AS second_to_home,

    (
      COUNT(*) FILTER (
        WHERE eventtype LIKE '%stolen_base%'
           OR eventtype LIKE '%caught_stealing%'
      )
      + COUNT(*) FILTER (WHERE is_firsttothird = true)
      + COUNT(*) FILTER (WHERE is_secondtohome = true)
    ) AS aggressive_plays
  FROM base
  GROUP BY runnerid
),
scored AS (
  SELECT
    runnerid,
    runnerfullname,
    opportunities,
    sb_attempts,
    first_to_third,
    second_to_home,
    aggressive_plays,
    ROUND(aggressive_plays::numeric / opportunities, 4) AS aggression_rate
  FROM runner_aggression
  WHERE opportunities >= 50
)
SELECT
  runnerid,
  runnerfullname,
  opportunities,
  sb_attempts,
  first_to_third,
  second_to_home,
  aggressive_plays,
  aggression_rate
FROM scored
ORDER BY aggression_rate DESC, aggressive_plays DESC
LIMIT 10;