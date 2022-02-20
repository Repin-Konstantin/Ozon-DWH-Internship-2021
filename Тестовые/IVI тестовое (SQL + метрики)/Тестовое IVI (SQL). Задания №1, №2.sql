-- Задание 1
-- Составьте SQL-запросы. Укажите, какой диалект SQL вы используете. 

-- СУБД PostgreSQL 9.6

-- Таблица content_watch

CREATE TABLE content_watch (
  watch_id bigint PRIMARY KEY,
  show_date timestamp,
  show_duration int,
  platform int,
  user_id int,
  utm_medium varchar,
  content_id int);
  
INSERT INTO content_watch VALUES
  (10971121570, '01.07.2018 14:37', 1340, 583, 1553139, 'organic', 314472),
  (4458319751, '01.12.2018 15:00', 12432, 353, 1554866, 'organic', 314472),
  (31382550, '02.08.2018 14:39', 1800, 10, 5255577, 'organic', 314472),
  (11254336994, '07.07.2017 17:56', 210, 11, 1554866, 'organic', 314472),
  (1231646730, '01.01.2016 12:48', 4685, 11, 1554866, 'organic', 132271),
  (4212172051, '08.12.2018 10:52', 472, 11, 1554866, 'organic', 314480),
  (8909218338, '05.09.2017 0:55', 297, 583, 9462609, 'direct', 127399),
  (1904761857, '09.24.2018 19:31', 1635, 9, 320756, 'Organic', 127399),
  (17947987, '10.30.2018 4:45', 854, 353, 1547421, 'Referral', 184673),
  (6077839073, '12.07.2017 23:58', 4571, 353, 4066590, 'Organic', 222161);

-- Таблица content

CREATE TABLE content (
  content_id int PRIMARY KEY,
  compilation_id int,
  episode int,
  paid_type varchar,
  load_date TIMESTAMP);
  
INSERT INTO content VALUES
  (314472, 9570, 1, 'AVOD', '01.01.2015 10:00'),
  (132271, NULL, NULL, 'SVOD', '03.15.2015 10:00'),
  (314480, 9570, 3, 'AVOD', '02.07.2015 10:00'),
  (127399, 9570, 4, 'TVOD', '08.23.2015 10:00'),
  (184673, 7608, 16, 'AVOD', '05.09.2015 10:00'),
  (222161, NULL, NULL, 'AVOD', '06.17.2015 10:00');



--  1. На каждый день количество просмотров отдельно по монетизациям SVOD и AVOD на платформах 10 и 11 за последние 30 дней.

SELECT
  date_trunc('day', cw.show_date) AS day,
  ct.paid_type AS paid_type,
  COUNT(cw.watch_id) AS watch_count
FROM content_watch AS cw
JOIN content AS ct ON cw.content_id = ct.content_id
WHERE 
  cw.platform IN (10, 11)
  AND ct.paid_type IN ('SVOD', 'AVOD')
  AND EXTRACT(DAY FROM (CURRENT_TIMESTAMP - cw.show_date)) < 30
GROUP BY day, paid_type
ORDER BY day, paid_type;


--  2. Ежемесячный ТОП-5 сериалов и ТОП-5 единичного контента по количеству смотрящих людей.

	-- Ежемесячный ТОП-5 единичного контента по количеству смотрящих людей

WITH t AS(
  SELECT
    month, content_id, unique_watchers_count,
    RANK() over w AS content_rank
  FROM
    (SELECT
      to_char(date_trunc('month', cw.show_date), 'YYYY.MM') AS month,
      ct.content_id,
      COUNT(DISTINCT cw.user_id) AS unique_watchers_count
    FROM content_watch AS cw
    JOIN content AS ct on cw.content_id = ct.content_id
    WHERE ct.compilation_id IS NULL
    GROUP BY month, ct.content_id) AS a
  WINDOW w AS (PARTITION BY month ORDER BY unique_watchers_count)
  )

SELECT
  month, content_id, unique_watchers_count, content_rank
FROM t
WHERE content_rank <= 5
ORDER BY month DESC, content_rank;


	-- Ежемесячный ТОП-5 сериалов по количеству смотрящих людей

WITH t AS(
  SELECT
    month, compilation_id, unique_watchers_count,
    RANK() over w AS compilation_rank
  FROM
    (SELECT
      to_char(date_trunc('month', cw.show_date), 'YYYY.MM') AS month,
      ct.compilation_id,
      COUNT(DISTINCT cw.user_id) AS unique_watchers_count
    FROM content_watch AS cw
    JOIN content AS ct on cw.content_id = ct.content_id
    WHERE ct.compilation_id IS NOT NULL
    GROUP BY month, ct.compilation_id) AS b
  WINDOW w AS (PARTITION BY month ORDER BY unique_watchers_count)
  )

SELECT
  month, compilation_id, unique_watchers_count, compilation_rank
FROM t
WHERE compilation_rank <= 5
ORDER BY month DESC, compilation_rank;



--  3. Список пользователей, у которых вчера был сначала просмотр с organic, а сразу следом за ним - просмотр с referral

WITH t AS (SELECT
    user_id,
    low_utm_medium,
    LEAD(low_utm_medium, 1) over w AS next_utm_medium
  FROM 
    (SELECT
      user_id,
      show_date,
      lower(utm_medium) AS low_utm_medium
    FROM content_watch
    WHERE date_trunc('day', show_date) = CURRENT_DATE - 1
    ORDER BY user_id, show_date) AS a
  WINDOW w AS (PARTITION BY user_id ORDER BY show_date))
  
SELECT
  DISTINCT user_id
FROM t
WHERE low_utm_medium = 'organic' AND next_utm_medium = 'refferal';


-- Задание 2
-- Составьте SQL-запросы. Придумайте, как оценить показатели, и напишите запросы для расчёта придуманных метрик. 
-- Представьте, что в вашем распоряжении есть все ресурсы по сбору статистики. 
-- Если вам необходимы дополнительный данные, то опишите, чего не хватает в тестовой базе, чтобы посчитать нужные метрики.


	-- Для выполнения задания добавил таблицы users и ratings, также в таблице content добавил поле load_date - дата загрузки контента в систему.
	
	CREATE TABLE users (
  user_id int PRIMARY KEY,
  first_watch_date DATE,
  first_watch_source varchar);
  
INSERT INTO users VALUES
  (1553139, '07.01.2016', 'organic'),
  (1554866,'07.01.2016', 'direct'),
  (5255577, '05.01.2017', 'organic'),
  (9462609, '05.01.2017', 'refferal'),
  (320756, '07.01.2018', 'target_vk'),
  (1547421, '07.01.2018', 'organic'),
  (4066590, '07.01.2018', 'direct');
  
CREATE TABLE ratings (
  rating_id int PRIMARY KEY,
  content_id int,
  user_id int,
  rating_value int,
  rating_date TIMESTAMP);
  
INSERT INTO ratings VALUES
  (14567, 314472, 1554866, 9, '01.12.2018 19:00'),
  (23489, 184673, 1547421, 5, '10.30.2018 7:00'),
  (83546, 314472, 5255577, 8, '02.08.2018 16:39');




--	1. "Цепляемость" и "крутость" сериала. Нужна какая-то метрика, которая при наличии трёх-четырёх серий сериала 
--  позволит сравнить этот сериал по "крутости" с другими сериалами.


	-- В качестве базы для расчета метрики "Крутости" сериала на основе 3-4 серий выбрал общее количество времени (в часах), которое пользователи смотрели конкретный сериал, а также
	-- среднюю оценку, которую поставили сериалу пользователи. Данные берутся за период со старта сериала (дата загрузки первой серии) до даты выхода 3-4 серии.
	-- На последнем шаге результат "нормируется" на разницу в днях между выходом 1-й и 3-4 серии, т.к. лаг выхода серий не всегда может составлять 7 дней.
	-- Формула метрики: watch_duration_hours * average_rating / 3600 * datediff


WITH t AS (SELECT
  cw_ct.compilation_id AS compilation_id,
  SUM(cw_ct.show_duration) AS sum_duration,
  MIN(cw_ct.first_episode_load_date) AS first_episode_load_date,
  MAX(cw_ct.last_episode_load_date) AS last_episode_load_date,
  AVG(DISTINCT rt.rating_value) AS average_rating
FROM (SELECT
    cw.show_date,
    cw.show_duration,
    cw.user_id,
    cw.content_id,
    ct.compilation_id,
    ct.episode,
    ct.load_date,
    MAX(ct.episode) OVER w AS last_episode_num,
    MIN(ct.load_date) OVER w AS first_episode_load_date,
    MAX(ct.load_date) OVER w AS last_episode_load_date
  FROM content_watch AS cw
  JOIN content AS ct ON cw.content_id = ct.content_id
  WHERE ct.compilation_id IS NOT NULL
  WINDOW w AS (PARTITION BY ct.compilation_id)
  ) AS cw_ct
LEFT JOIN ratings AS rt ON cw_ct.content_id = rt.content_id 
                           AND cw_ct.user_id = rt.user_id  
WHERE (cw_ct.last_episode_num = 3 OR cw_ct.last_episode_num = 4)
       AND DATE_TRUNC('day', cw_ct.show_date) <= last_episode_load_date
GROUP BY compilation_id)

SELECT
  compilation_id,
  (sum_duration * average_rating) / 
  (3600 * (DATE_PART('day', date_trunc('day', last_episode_load_date - first_episode_load_date)))) AS coolness_metric
FROM t;




-- 2. Ретеншн всех пользователей сервиса. Нужно просегментировать аудиторию, рассчитать её ретеншн по сегментам и дать рекомендации по тому, 
--    как увеличить ретеншн каждого из сегментов.


-- Расчет ретеншена всех пользователей

SELECT 
	fw.first_watch_week AS first_watch_week_cohort,
    fw.users_count AS cohort_size,
    ch.week_diff AS week_difference,
    ch.week_users AS week_users,
    ROUND(CAST(ch.week_users AS numeric)/CAST(fw.users_count AS numeric), 2) AS retention
FROM 
   (SELECT
	date_trunc('week', us.first_watch_date) AS first_watch_week,
    COUNT(us.user_id) AS users_count
	FROM users AS us
	GROUP BY first_watch_week) AS fw
LEFT JOIN
    (SELECT
	date_trunc('week', us.first_watch_date) AS first_watch_week,
    TRUNC(DATE_PART('day', date_trunc('day', cw.show_date - us.first_watch_date))/7) AS week_diff,
    COUNT(DISTINCT us.user_id) AS week_users
	FROM users AS us
	JOIN content_watch AS cw ON us.user_id = cw.user_id
	GROUP BY first_watch_week, week_diff) AS ch
ON fw.first_watch_week = ch.first_watch_week
ORDER BY first_watch_week_cohort, week_difference;



-- Сегментация по типу источника, из которого пришел пользователь.

  -- Данная сегментация позволяет отследить Retention по каждому каналу привлечения пользователя. С помощью этой сегментации можно увидеть, с каких каналов пользователи 
  -- отваливаются больше, чем с других. На основании этого можно скорректировать маркетинговую стратегию по проблемным каналам (например, скорректировать рекламную кампанию канала,
  -- чтобы привлекать более целевую аудиторию)

SELECT
    fw.user_source AS user_source,
	fw.first_watch_week AS first_watch_week_cohort,
    fw.users_count AS cohort_size,
    ch.week_diff AS week_difference,
    ch.week_users AS week_users,
    ROUND(CAST(ch.week_users AS numeric)/CAST(fw.users_count AS numeric), 2) AS retention
FROM 
   (SELECT
    us.first_watch_source AS user_source,
    date_trunc('week', us.first_watch_date) AS first_watch_week,
    COUNT(us.user_id) AS users_count
	FROM users AS us
	GROUP BY user_source, first_watch_week) AS fw
LEFT JOIN
    (SELECT
    us.first_watch_source AS user_source, 
	date_trunc('week', us.first_watch_date) AS first_watch_week,
    TRUNC(DATE_PART('day', date_trunc('day', cw.show_date - us.first_watch_date))/7) AS week_diff,
    COUNT(DISTINCT us.user_id) AS week_users
	FROM users AS us
	JOIN content_watch AS cw ON us.user_id = cw.user_id
	GROUP BY user_source, first_watch_week, week_diff) AS ch
ON fw.user_source = ch.user_source AND fw.first_watch_week = ch.first_watch_week
ORDER BY user_source, first_watch_week_cohort, week_difference;


-- Сегментация по платформе, с которой пользователи просматривают контент

   -- Данная сегментация позволяет отследить Retention по используемым платформам.
   -- Рекомендации по улучшению Retention: 
   -- 	1) Проработать usability отстающих платформ (не удобно пользоваться)
   -- 	2) Проработать дизайн/верстку отстающих платформ (не нравится визуально)  
   -- 	3) Проверить платформы на наличие технических проблем (баги, проблемы с передачей данных с серверов и т.д.)

SELECT
	fw.first_watch_week AS first_watch_week_cohort,
    fw.users_count AS cohort_size,
    ch.platform AS platform,
    ch.week_diff AS week_difference,
    ch.week_users AS week_users,
    ROUND(CAST(ch.week_users AS numeric)/CAST(fw.users_count AS numeric), 2) AS retention
FROM 
   (SELECT
    date_trunc('week', us.first_watch_date) AS first_watch_week,
    COUNT(us.user_id) AS users_count
	FROM users AS us
	GROUP BY first_watch_week) AS fw
LEFT JOIN
    (SELECT
	date_trunc('week', us.first_watch_date) AS first_watch_week,
    cw.platform AS platform,
    TRUNC(DATE_PART('day', date_trunc('day', cw.show_date - us.first_watch_date))/7) AS week_diff,
    COUNT(DISTINCT us.user_id) AS week_users
	FROM users AS us
	JOIN content_watch AS cw ON us.user_id = cw.user_id
	GROUP BY first_watch_week, platform, week_diff) AS ch
ON fw.first_watch_week = ch.first_watch_week
ORDER BY first_watch_week_cohort, platform, week_difference;


-- Сегментация по типу монетизации контента
-- Рекомендации по улучшению Retention: 
   --	1) Всем пользователям присылать уведомления о новинках кино/сериалов (e-mail, пуш и проч.)
   -- 	2) Пользователям в сегменте AVOD присылать уведомления (e-mail, пуш и проч.) о том, что появились новинки, которые были раньше доступны только при оплате.
   -- 	3) Пользователям в сегменте AVOD периодически предлагать скидочные промокоды на просмотр платного контента
   -- 	4) Пользователям в сегменте SVOD присылать уведомления (e-mail, пуш и проч.) о том, что появились новинки, которые были раньше доступны только в формате TVOD.
   -- 	5) Предложить пользователям SVOD годовую подписку (со скидкой) вместо ежемесячной

SELECT
	fw.first_watch_week AS first_watch_week_cohort,
    fw.users_count AS cohort_size,
    ch.paid_type AS paid_type,
    ch.week_diff AS week_difference,
    ch.week_users AS week_users,
    ROUND(CAST(ch.week_users AS numeric)/CAST(fw.users_count AS numeric), 2) AS retention
FROM 
   (SELECT
    date_trunc('week', us.first_watch_date) AS first_watch_week,
    COUNT(us.user_id) AS users_count
	FROM users AS us
	GROUP BY first_watch_week) AS fw
LEFT JOIN
    (SELECT
	date_trunc('week', us.first_watch_date) AS first_watch_week,
    ct.paid_type AS paid_type,
    TRUNC(DATE_PART('day', date_trunc('day', cw.show_date - us.first_watch_date))/7) AS week_diff,
    COUNT(DISTINCT us.user_id) AS week_users
	FROM users AS us
	JOIN content_watch AS cw ON us.user_id = cw.user_id
    JOIN content AS ct ON cw.content_id = ct.content_id
	GROUP BY first_watch_week, paid_type, week_diff) AS ch
ON fw.first_watch_week = ch.first_watch_week
ORDER BY first_watch_week_cohort, paid_type, week_difference;