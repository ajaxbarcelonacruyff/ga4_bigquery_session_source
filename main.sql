with ga AS(
-- GA4テーブル
  SELECT *,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source') AS event_traffic_source,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium') AS event_traffic_medium,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS event_traffic_campaign,
  -- 以下略
  FROM `project_id.analytics_123456789.events_YYYYMMDD`  
)
-- 参照元の追加処理。session_startの参照元などを取得
session_start AS(
    SELECT *
    FROM(
        SELECT 
            user_pseudo_id,
            ga_session_id,
            ARRAY_AGG(STRUCT(
                COALESCE(g.collected_traffic_source.manual_source, g.event_traffic_source) AS event_traffic_source,
                COALESCE(g.collected_traffic_source.manual_medium, g.event_traffic_medium) AS event_traffic_medium,
                COALESCE(g.collected_traffic_source.manual_campaign_name, g.event_traffic_campaign) AS event_traffic_campaign,
                COALESCE(g.collected_traffic_source.manual_content, g.event_traffic_content) AS event_traffic_content,
                COALESCE(g.collected_traffic_source.manual_term, g.event_traffic_term) AS event_traffic_term,
                COALESCE(g.collected_traffic_source.manual_source_platform, g.event_traffic_source_platform) AS event_traffic_source_platform,
                COALESCE(g.collected_traffic_source.manual_creative_format, g.event_traffic_creative_format) AS event_traffic_creative_format,
                COALESCE(g.collected_traffic_source.manual_marketing_tactic, g.event_traffic_marketing_tactic) AS event_traffic_marketing_tactic,
                COALESCE(g.collected_traffic_source.manual_campaign_id, g.event_traffic_campaign_id) AS event_traffic_campaign_id,
                COALESCE(g.collected_traffic_source.gclid, g.event_traffic_gclid) AS event_traffic_gclid
            ) ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)].*
        FROM ga g
        WHERE event_name ="session_start"
        GROUP BY ALL
    ) 
    WHERE event_traffic_source IS NOT NULL AND event_traffic_source NOT IN("(not set)","(direct)")  -- 対象となったsession_startイベントのevent_traffic_sourceがNULLや (not set), (direct)の場合は値を返さない ※(not set)や(direct)はないはずですが念のため
),
-- 参照元などが入っているイベントのうち一番古いものを取得
agg_campaign AS(
    SELECT 
        user_pseudo_id,
        ga_session_id,
        ARRAY_AGG(STRUCT(
            COALESCE(g.collected_traffic_source.manual_source, g.event_traffic_source) AS event_traffic_source,
            COALESCE(g.collected_traffic_source.manual_medium, g.event_traffic_medium) AS event_traffic_medium,
            COALESCE(g.collected_traffic_source.manual_campaign_name, g.event_traffic_campaign) AS event_traffic_campaign,
            COALESCE(g.collected_traffic_source.manual_content, g.event_traffic_content) AS event_traffic_content,
            COALESCE(g.collected_traffic_source.manual_term, g.event_traffic_term) AS event_traffic_term,
            COALESCE(g.collected_traffic_source.manual_source_platform, g.event_traffic_source_platform) AS event_traffic_source_platform,
            COALESCE(g.collected_traffic_source.manual_creative_format, g.event_traffic_creative_format) AS event_traffic_creative_format,
            COALESCE(g.collected_traffic_source.manual_marketing_tactic, g.event_traffic_marketing_tactic) AS event_traffic_marketing_tactic,
            COALESCE(g.collected_traffic_source.manual_campaign_id, g.event_traffic_campaign_id) AS event_traffic_campaign_id,
            COALESCE(g.collected_traffic_source.gclid, g.event_traffic_gclid) AS event_traffic_gclid
        ) ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)].*
    FROM ga g
    WHERE (
        g.event_traffic_source IS NOT NULL AND g.event_traffic_source NOT IN("(not set)","(direct)","(none)") 
    )OR (
        g.event_traffic_medium IS NOT NULL AND g.event_traffic_medium NOT IN("(not set)","(direct)","(none)") 
    )OR (
        g.event_traffic_campaign IS NOT NULL AND g.event_traffic_campaign NOT IN("(not set)","(direct)","(none)") 
    )
    GROUP BY ALL
),
-- session_startに参照元(event_traffic_source）が入っていればそれを採用。ない場合はイベントから取得。session_traffic_mediumなどでもIF(s.event_traffic_source IS NOT NULLとしているのは、event_traffic_mediumとしてしまうと、sourceはsession_startから取得しmediumはイベントから取得というミスを防ぐため
agg_campaign_first_2 AS(
    SELECT 
        user_pseudo_id,
        ga_session_id,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_source, a.event_traffic_source) AS session_traffic_source,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_medium, a.event_traffic_medium) AS session_traffic_medium,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_campaign, a.event_traffic_campaign) AS session_traffic_campaign,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_content, a.event_traffic_content) AS session_traffic_content,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_term, a.event_traffic_term) AS session_traffic_term,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_source_platform, a.event_traffic_source_platform) AS session_traffic_source_platform,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_creative_format , a.event_traffic_creative_format ) AS session_traffic_creative_format,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_marketing_tactic, a.event_traffic_marketing_tactic) AS session_traffic_marketing_tactic,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_campaign_id, a.event_traffic_campaign_id) AS session_traffic_campaign_id,
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_gclid, a.event_traffic_gclid) AS session_traffic_gclid
    FROM agg_campaign a FULL JOIN session_start s USING(user_pseudo_id, ga_session_id)
),
-- 過去にセッション情報が存在する場合はそれを採用
mart_session AS(
    SELECT 
        user_pseudo_id,
        ga_session_id,
        ARRAY_AGG(STRUCT(
            session_traffic_source,
            session_traffic_medium,
            session_traffic_campaign,
            session_traffic_content,
            session_traffic_term,
            session_traffic_source_platform,
            session_traffic_creative_format,
            session_traffic_marketing_tactic,
            session_traffic_campaign_id,
            session_traffic_gclid
        ) ORDER BY event_date, entrance_timestamp,exit_timestamp ASC LIMIT 1)[OFFSET(0)].*
    FROM `project_id.mart.sessions`  -- user_pseudo_id、ga_session_id、session_traffic_sourceなどをsessionsテーブルに格納するクエリを別途要作成
    GROUP BY ALL
),
agg_campaign_first_3 AS(
    SELECT 
        user_pseudo_id,
        ga_session_id,
        ARRAY_AGG(STRUCT(
            COALESCE(m.session_traffic_source, a.session_traffic_source) AS session_traffic_source,
            COALESCE(m.session_traffic_medium, a.session_traffic_medium) AS session_traffic_medium,
            COALESCE(m.session_traffic_campaign, a.session_traffic_campaign) AS session_traffic_campaign,
            COALESCE(m.session_traffic_content, a.session_traffic_content) AS session_traffic_content,
            COALESCE(m.session_traffic_term, a.session_traffic_term) AS session_traffic_term,
            COALESCE(m.session_traffic_source_platform, a.session_traffic_source_platform) AS session_traffic_source_platform,
            COALESCE(m.session_traffic_creative_format, a.session_traffic_creative_format) AS session_traffic_creative_format,
            COALESCE(m.session_traffic_marketing_tactic, a.session_traffic_marketing_tactic) AS session_traffic_marketing_tactic,
            COALESCE(m.session_traffic_campaign_id, a.session_traffic_campaign_id) AS session_traffic_campaign_id,
            COALESCE(m.session_traffic_gclid, a.session_traffic_gclid) AS session_traffic_gclid
        ) LIMIT 1)[OFFSET(0)].*
    FROM agg_campaign_first_2 a LEFT JOIN mart_session m 
    USING(user_pseudo_id, ga_session_id)
    GROUP BY ALL
)
SELECT g.*,
-- session_acquisition_channel
a.session_traffic_source,
a.session_traffic_medium,
a.session_traffic_campaign
FROM ga AS g
LEFT JOIN agg_campaign_first_3 AS a USING (user_pseudo_id, ga_session_id)

