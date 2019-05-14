WITH
  game_logs AS(
  SELECT
    * EXCEPT(Tm),
    EXTRACT(year
    FROM
      DATE(TIMESTAMP(date))) AS year,
    mlbam_team AS Tm
  FROM
    `{project}.{dataset}.batting_2019*`
  JOIN
    `{project}.{dataset}.mlbam_team_mapping`
  USING
    (Tm,
      Lev) ),
  batters AS(
  SELECT
    *,
    LAG(is_streak) OVER(PARTITION BY name, tm ORDER BY g_num) AS lag_is_streak,
    LEAD(is_streak) OVER(PARTITION BY name, tm ORDER BY g_num) AS lead_is_streak
  FROM (
    SELECT
      date,
      name,
      tm,
      SUM(g) OVER(PARTITION BY name, tm ORDER BY date) AS g_num,
      CASE
        WHEN OBP>0 THEN TRUE
      ELSE
      FALSE
    END
      AS is_streak
    FROM
      game_logs ) ),
  is_streak_streaks AS(
  SELECT
    * EXCEPT(is_streak),
    CASE
      WHEN is_streak=FALSE THEN 0
    ELSE
    ROW_NUMBER() OVER(PARTITION BY name, tm, is_streak_streak_number ORDER BY g_num)
  END
    AS is_streak_streak
  FROM (
    SELECT
      * EXCEPT(lag_is_streak,
        lead_is_streak),
      COUNTIF(streak=0
        OR (is_streak=TRUE
          AND lag_is_streak=FALSE)) OVER(PARTITION BY name, tm ORDER BY g_num) AS is_streak_streak_number
    FROM (
      SELECT
        * EXCEPT(streak),
        CASE
          WHEN is_streak=FALSE THEN 0
        ELSE
        streak
      END
        AS streak
      FROM (
        SELECT
          *,
          SUM(CASE
              WHEN is_streak=TRUE THEN 1
            ELSE
            0
          END
            ) OVER(PARTITION BY name, tm ORDER BY g_num ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS streak
        FROM
          batters ) ) ) )
SELECT
  * EXCEPT(lag_is_streak,
    lead_is_streak,
    streak)
FROM
  batters
JOIN
  is_streak_streaks
USING
  (date,
    g_num,
    name,
    tm)
