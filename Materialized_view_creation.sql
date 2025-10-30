-- Радиант выигрывают
CREATE MATERIALIZED VIEW radiant_win_by_duration 
AS

WITH all_matches AS (
SELECT m.match_id,
	   ROUND(m.duration::numeric/60, 2) AS duration_min,
	   m.radiant_win	   
FROM matches m 
),
duration_bins AS (
SELECT match_id,
       duration_min,
       radiant_win,
       CASE 
       		WHEN duration_min <= 25 THEN '1 (<= 25)'  
       		WHEN duration_min <= 30 THEN '2 (26-30)'
       		WHEN duration_min <= 35 THEN '3 (31-35)'
       		WHEN duration_min <= 40 THEN '4 (36-40)'  
       		WHEN duration_min <= 45 THEN '5 (41-45)'
       		WHEN duration_min <= 50 THEN '6 (46-50)'
       		ELSE '7 (>50)'
       END AS duration_category       
FROM all_matches    
)
SELECT duration_category,
	   SUM(CASE WHEN radiant_win THEN 1 ELSE 0 END) AS radiant_win_sum,
	   COUNT(match_id) AS match_count,
	   ROUND(SUM(CASE WHEN radiant_win THEN 1 ELSE 0 END)::numeric/COUNT(match_id)*100, 2) AS win_share_radiant   
FROM duration_bins 
GROUP BY duration_category;



-- Стереотипы по регионам
--DROP MATERIALIZED VIEW IF EXISTS regions_stat; 

CREATE MATERIALIZED VIEW regions_stat AS 

WITH all_data AS (
SELECT match_id,
	   ROUND(m.duration::numeric/60, 2) AS duration_min,
	   radiant_win,
	   ROUND(m.first_blood_time::NUMERIC/60, 2) AS first_blood_time,
	   region,
	   CASE
			  WHEN region = 1 THEN 'US West (Seattle)'
			  WHEN region = 2 THEN 'US East (Sterling)'
			  WHEN region = 3 THEN 'Europe West (Luxembourg)'
			  WHEN region = 4 THEN 'Brazil (São Paulo)'
			  WHEN region = 6 THEN 'Singapore (SEA)'
			  WHEN region = 7 THEN 'Australia (Sydney)'
			  WHEN region = 8 THEN 'Europe East (Moscow)'
			  WHEN region = 9 THEN 'South Africa (Cape Town)'
			  WHEN region = 10 THEN 'Japan (Tokyo)'
			  WHEN region = 11 THEN 'India (Chennai)'
			  WHEN region = 13 THEN 'China (Beijing / Shanghai)'
			  WHEN region = 14 THEN 'Chile (Santiago)'
			  WHEN region = 15 THEN 'Peru (Lima)'
			  WHEN region = 17 THEN 'Russia (Moscow / East Europe)'
			  WHEN region = 18 THEN 'Europe East 2'
			  WHEN region = 19 THEN 'China 2'
			  WHEN region = 20 THEN 'Southeast Asia 2'
			  WHEN region = 25 THEN 'US West 2'
			  WHEN region = 38 THEN 'US East 2'
			  ELSE 'Unknown'
	  END AS region_name
FROM matches m 
)
SELECT region,
	   region_name,
       COUNT(DISTINCT match_id) AS match_count,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_min))::numeric, 2) AS duration_median,
       ROUND(AVG(duration_min)::numeric, 2) AS duration_mean,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_blood_time))::numeric, 2) AS first_blood_time_median,
       ROUND(AVG(first_blood_time)::numeric, 2) AS first_blood_time_mean,
	   SUM(radiant_win::int) AS radiant_wins,
	   ROUND(AVG(radiant_win::int) * 100, 2) AS radiant_win_rate_pct
FROM all_data 
GROUP BY region, region_name;

-- Факторы, влияющие на выигрыш матча
CREATE MATERIALIZED VIEW win_factors 
AS

WITH all_data AS (
SELECT win,
       ROUND(m.duration::numeric/60, 2) AS duration_min,
       ROUND(tower_damage::numeric/60, 2) AS tower_damage_per_min,
       ROUND(hero_damage::numeric/60, 2) AS hero_damage_per_min,
       ps.gold_per_min,
       ps.xp_per_min,
       ps.last_hits,
       ps.assists,
       ps.kills,
       ps.deaths,
       ROUND(m.first_blood_time::NUMERIC/60, 2) AS first_blood_time   
FROM matches m 
LEFT JOIN players p USING(match_id)
LEFT JOIN players_stat ps ON p.match_id = ps.match_id AND p.player_slot = ps.player_slot
)
SELECT win,
	   ROUND(AVG(tower_damage_per_min)::numeric, 2) AS tower_damage_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tower_damage_per_min))::numeric, 2) AS tower_damage_median,
	   ROUND(AVG(hero_damage_per_min)::numeric, 2) AS hero_damage_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hero_damage_per_min))::numeric, 2) AS hero_damage_median,
	   ROUND(AVG(gold_per_min)::numeric, 2) AS gold_per_min_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gold_per_min))::numeric, 2) AS gold_per_min_median,
	   ROUND(AVG(xp_per_min)::numeric, 2) AS xp_per_min_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY xp_per_min))::numeric, 2) AS xp_per_min_median,
	   ROUND(AVG(last_hits)::numeric, 2) AS last_hits_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY last_hits))::numeric, 2) AS last_hits_median,
	   ROUND(AVG(assists)::numeric, 2) AS assists_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY assists))::numeric, 2) AS assists_median,
	   ROUND(AVG(kills)::numeric, 2) AS kills_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY kills))::numeric, 2) AS kills_median,
	   ROUND(AVG(deaths)::numeric, 2) AS deaths_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deaths))::numeric, 2) AS deaths_median,
	   ROUND(AVG(first_blood_time)::numeric, 2) AS first_blood_time_mean,
	   ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_blood_time))::numeric, 2) AS first_blood_time_median
FROM all_data
GROUP BY win;

-- герои: специалисты по лейту, недооцененные, популярные

--DROP MATERIALIZED VIEW IF EXISTS heroes_stat; 

CREATE MATERIALIZED VIEW heroes_stat
AS

WITH duration_quantiles AS (
    SELECT
        percentile_cont(0.75) WITHIN GROUP (ORDER BY ROUND(duration::numeric/60.0,2)) AS dur_thresh
    FROM matches
),
duration_bins AS (
SELECT h.localized_name,
	   CASE WHEN (duration::NUMERIC/60) < (SELECT dur_thresh FROM duration_quantiles) THEN 'normal'
	        ELSE 'late'
	   END AS duration_bin,
	   COUNT(DISTINCT m.match_id) AS match_count_duration_bin,
	   SUM(win::int) AS win_count_duration_bin,
	   ROUND(SUM(win::int)::NUMERIC/COUNT(DISTINCT m.match_id)*100, 2) AS winrate_duration_bin
FROM public.players AS p
LEFT JOIN public.heroes AS h USING(hero_id)
LEFT JOIN matches AS m USING(match_id)
GROUP BY 1, 2),
winrate_diff AS (
SELECT localized_name,
		MAX(CASE WHEN duration_bin = 'late' THEN winrate_duration_bin END) AS winrate_late,
		MAX(CASE WHEN duration_bin = 'normal' THEN winrate_duration_bin END)  AS winrate_normal,
		COALESCE(MAX(CASE WHEN duration_bin = 'late'   THEN match_count_duration_bin END), 0) AS match_count_late,
		COALESCE(MAX(CASE WHEN duration_bin = 'normal' THEN match_count_duration_bin END), 0) AS match_count_normal,
		ROUND(
		     (COALESCE(MAX(CASE WHEN duration_bin = 'late'   THEN winrate_duration_bin END), 0)
		           -
		     COALESCE(MAX(CASE WHEN duration_bin = 'normal' THEN winrate_duration_bin END), 0)
		     )::numeric
		, 2) AS winrate_diff_pct
FROM duration_bins
GROUP BY localized_name
),
pickup_rate_count AS (
SELECT localized_name,
	   ROUND(COUNT(hero_id)::numeric / (SELECT COUNT(hero_id) FROM players)*100, 2) AS pickup_rate
FROM players p 
LEFT JOIN heroes AS h USING(hero_id)
GROUP BY localized_name
),
winrate_overall AS (
SELECT localized_name,
       ROUND(SUM(win::int)::NUMERIC / COUNT(*) *100, 2) AS winrate_overall
FROM players AS p
LEFT JOIN heroes AS h USING(hero_id)
GROUP BY localized_name
)
SELECT w.localized_name,
	   winrate_late,
	   winrate_normal,
	   match_count_late,
	   match_count_normal,
	   winrate_diff_pct,
	   pickup_rate,
	   DENSE_RANK() OVER (ORDER BY pickup_rate DESC) AS rank_pickup_rate,
	   winrate_overall,
	   CASE WHEN (winrate_overall > 
	   				(SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY winrate_overall) FROM winrate_overall)
	   				) THEN '1'
	   	    ELSE '0'
	   END AS winrate_best_flag,
	   CASE WHEN (pickup_rate < 
	   				(SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY pickup_rate) FROM pickup_rate_count) 
	   				) THEN '1'
	   	    ELSE '0'
	   END AS pickup_worst_flag
FROM winrate_diff AS w
LEFT JOIN pickup_rate_count AS p USING(localized_name)
LEFT JOIN winrate_overall AS wo USING(localized_name)
ORDER BY pickup_rate DESC;

-- Жадность окупается, если завершить вовремя?
--DROP MATERIALIZED VIEW IF EXISTS gmp_winrate; 
CREATE MATERIALIZED VIEW gmp_winrate
AS
WITH team_stats AS (
    SELECT
        m.match_id,
        p.is_radiant,
        m.duration,
        SUM(COALESCE(ps.gold_per_min, 0)) AS team_gpm,
        CASE
            WHEN p.is_radiant = true  AND m.radiant_win = true  THEN 1
            WHEN p.is_radiant = false AND m.radiant_win = false THEN 1
            ELSE 0
        END AS win
    FROM matches m
    JOIN players p USING(match_id)
    LEFT JOIN players_stat ps
      ON p.match_id = ps.match_id
     AND p.player_slot = ps.player_slot
    GROUP BY m.match_id, p.is_radiant, m.duration, m.radiant_win
),
bins AS 
(
SELECT match_id,
	   is_radiant,
	   win::int AS win,
       CASE
           WHEN duration::numeric/60.0 <=15 THEN '<=15'
           WHEN duration::numeric/60.0 <=20 THEN '16-20'
           WHEN duration::numeric/60.0 <=25 THEN '21-25'
           WHEN duration::numeric/60.0 <=30 THEN '26-30'
           WHEN duration::numeric/60.0 <=35 THEN '31-35'
           WHEN duration::numeric/60.0 <=40 THEN '36-40'
           WHEN duration::numeric/60.0 <=45 THEN '41-45'
           WHEN duration::numeric/60.0 <=50 THEN '46-50'
           ELSE 'late(>50)'
        END AS duration_bin_small,
        NTILE(10) OVER (ORDER BY team_gpm) AS gpm_decile,
        team_gpm,
        duration
FROM team_stats
)
SELECT ROUND(AVG(win)*100, 2) AS winrate,
	   duration_bin_small,
	   gpm_decile,
	   COUNT(*) AS team_rows,
	   ROUND(AVG(team_gpm)::numeric, 2) AS avg_team_gpm,
	   ROUND(AVG(duration::numeric/60.0), 2) AS avg_duration_min
FROM bins
GROUP BY duration_bin_small, gpm_decile
ORDER BY gpm_decile DESC;


-- анонимы играют иначе?
--DROP MATERIALIZED VIEW IF EXISTS unknown_players;

CREATE MATERIALIZED VIEW unknown_players
AS
WITH find_smurf AS (
SELECT CASE WHEN p.steam_id IS NOT NULL AND p.personaname IS NOT NULL THEN 'named'
       		ELSE 'unknown'
       END AS is_named,
       win,
       ROUND(duration/60, 2) AS duration_min, 
       CASE
           WHEN duration::numeric/60.0 <=15 THEN '<=15'
           WHEN duration::numeric/60.0 <=20 THEN '16-20'
           WHEN duration::numeric/60.0 <=25 THEN '21-25'
           WHEN duration::numeric/60.0 <=30 THEN '26-30'
           WHEN duration::numeric/60.0 <=35 THEN '31-35'
           WHEN duration::numeric/60.0 <=40 THEN '36-40'
           WHEN duration::numeric/60.0 <=45 THEN '41-45'
           WHEN duration::numeric/60.0 <=50 THEN '46-50'
           ELSE 'late(>50)'
        END AS duration_bin_small,
        is_radiant,
        ROUND (kills::numeric/60, 2) AS kills_per_min,
        deaths,
        assists,
        denies,
        gold_per_min,
        xp_per_min,
        ROUND (hero_damage::numeric/60, 2) AS hero_damage_per_min,
        ROUND (hero_healing::numeric/60, 2) AS hero_healing_per_min,
        ROUND (tower_damage::numeric/60, 2) AS tower_damage_per_min      
FROM matches m 
LEFT JOIN players AS p USING(match_id)
LEFT JOIN players_stat AS ps ON (p.match_id = ps.match_id) AND (p.player_slot = ps.player_slot)
)
SELECT is_named,
	   duration_bin_small,
	   ROUND(AVG(duration_min), 2) AS avg_duration,
	   ROUND(AVG(win::int)*100, 2) AS avg_winrate,
	   percentile_cont(0.5) WITHIN GROUP (ORDER BY kills_per_min)    AS median_kills_per_min,
   	   percentile_cont(0.5) WITHIN GROUP (ORDER BY deaths)                AS median_deaths,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY assists)               AS median_assists,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY denies)                AS median_denies,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY gold_per_min)          AS median_gold_per_min,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY xp_per_min)            AS median_xp_per_min,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY hero_damage_per_min)  AS median_hero_damage_per_min,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY hero_healing_per_min) AS median_hero_healing_per_min,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY tower_damage_per_min)  AS median_tower_damage_per_min,
       ROUND(AVG(kills_per_min), 2)        AS avg_kills_per_min,
       ROUND(AVG(deaths), 2)          AS avg_deaths,
       ROUND(AVG(assists), 2)         AS avg_assists,
       ROUND(AVG(denies), 2)          AS avg_denies,
       ROUND(AVG(gold_per_min), 2)    AS avg_gold_per_min,
       ROUND(AVG(xp_per_min), 2)      AS avg_xp_per_min,
       ROUND(AVG(hero_damage_per_min), 2)  AS avg_hero_damage_per_min,
       ROUND(AVG(hero_healing_per_min), 2) AS avg_hero_healing_per_min,
       ROUND(AVG(tower_damage_per_min), 2)  AS avg_tower_damage_per_min
FROM find_smurf 
GROUP BY is_named, duration_bin_small;
