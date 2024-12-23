> [!NOTE]
>This article is based on the content as of August 2024. Please check official sources for the latest information.


# Creating Session Source Information in GA4 with BigQuery
Since GA4 records are event-based, session sources need to be aggregated at the session level before retrieval.
Recently, the `session_traffic_source_last_click` column was added, making it usable for this purpose. However, the following query prioritizes `event_params` or `collected_traffic_source` columns.

# Processing Steps
1. Add session source information, such as the source of `session_start`. Priority order: `collected_traffic_source` column > `event_params.source` column > `session_traffic_source_last_click` column.
2. Retrieve the earliest event containing session source information.
3. Use the source from `session_start` if available; otherwise, retrieve it from events.
   - For example, in `session_traffic_medium`, using `IF(s.event_traffic_source IS NOT NULL)` ensures that `source` and `medium` are both sourced from `session_start` and prevents mismatches.
4. Adopt session information if it exists in historical data (you need to create a separate query to store `user_pseudo_id`, `ga_session_id`, and `session_traffic_source` in the `sessions` table).

```
with ga AS(
-- GA4 table
  SELECT *,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key ='ga_session_id') AS ga_session_id,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source') AS event_traffic_source,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium') AS event_traffic_medium,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS event_traffic_campaign,
  -- omitting the following
  FROM `project_id.analytics_123456789.events_YYYYMMDD`  
),
-- Process of adding referents, e.g. to get session_start referents. Priority order is collected_traffic_source column > event_params.source column > session_traffic_source_last_click column.
session_start AS(
    SELECT *
    FROM(
        SELECT 
            user_pseudo_id,
            ga_session_id,
            ARRAY_AGG(STRUCT(
                COALESCE(g.collected_traffic_source.manual_source, g.event_traffic_source, g.session_traffic_source_last_click.cross_channel_campaign.source) AS event_traffic_source,
                COALESCE(g.collected_traffic_source.manual_medium, g.event_traffic_medium, g.session_traffic_source_last_click.cross_channel_campaign.medium) AS event_traffic_medium,
                COALESCE(g.collected_traffic_source.manual_campaign_name, g.event_traffic_campaign, g.session_traffic_source_last_click.cross_channel_campaign.campaign) AS event_traffic_campaign,
                COALESCE(g.collected_traffic_source.manual_content, g.event_traffic_content, g.session_traffic_source_last_click.cross_channel_campaign.content) AS event_traffic_content,
                COALESCE(g.collected_traffic_source.manual_term, g.event_traffic_term, g.session_traffic_source_last_click.cross_channel_campaign.term) AS event_traffic_term,
                COALESCE(g.collected_traffic_source.manual_source_platform, g.event_traffic_source_platform, g.session_traffic_source_last_click.cross_channel_campaign.source_platform) AS event_traffic_source_platform,
                COALESCE(g.collected_traffic_source.manual_creative_format, g.event_traffic_creative_format, g.session_traffic_source_last_click.cross_channel_campaign.creative_format) AS event_traffic_creative_format,
                COALESCE(g.collected_traffic_source.manual_marketing_tactic, g.event_traffic_marketing_tactic, g.session_traffic_source_last_click.cross_channel_campaign.marketing_tactic) AS event_traffic_marketing_tactic,
                COALESCE(g.collected_traffic_source.manual_campaign_id, g.event_traffic_campaign_id, g.session_traffic_source_last_click.cross_channel_campaign.campaign_id) AS event_traffic_campaign_id
            ) ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)].*
        FROM ga g
        WHERE event_name ="session_start"
        GROUP BY ALL
    ) 
    WHERE event_traffic_source IS NOT NULL AND event_traffic_source NOT IN("(not set)","(direct)")  -- 対象となったsession_startイベントのevent_traffic_sourceがNULLや (not set), (direct)の場合は値を返さない ※(not set)や(direct)はないはずですが念のため
),
-- Retrieve the oldest event with references, etc.
first_campaign AS(
    SELECT 
        user_pseudo_id,
        ga_session_id,
        ARRAY_AGG(STRUCT(
                COALESCE(g.collected_traffic_source.manual_source, g.event_traffic_source, g.session_traffic_source_last_click.cross_channel_campaign.source) AS event_traffic_source,
                COALESCE(g.collected_traffic_source.manual_medium, g.event_traffic_medium, g.session_traffic_source_last_click.cross_channel_campaign.medium) AS event_traffic_medium,
                COALESCE(g.collected_traffic_source.manual_campaign_name, g.event_traffic_campaign, g.session_traffic_source_last_click.cross_channel_campaign.campaign) AS event_traffic_campaign,
                COALESCE(g.collected_traffic_source.manual_content, g.event_traffic_content, g.session_traffic_source_last_click.cross_channel_campaign.content) AS event_traffic_content,
                COALESCE(g.collected_traffic_source.manual_term, g.event_traffic_term, g.session_traffic_source_last_click.cross_channel_campaign.term) AS event_traffic_term,
                COALESCE(g.collected_traffic_source.manual_source_platform, g.event_traffic_source_platform, g.session_traffic_source_last_click.cross_channel_campaign.source_platform) AS event_traffic_source_platform,
                COALESCE(g.collected_traffic_source.manual_creative_format, g.event_traffic_creative_format, g.session_traffic_source_last_click.cross_channel_campaign.creative_format) AS event_traffic_creative_format,
                COALESCE(g.collected_traffic_source.manual_marketing_tactic, g.event_traffic_marketing_tactic, g.session_traffic_source_last_click.cross_channel_campaign.marketing_tactic) AS event_traffic_marketing_tactic,
                COALESCE(g.collected_traffic_source.manual_campaign_id, g.event_traffic_campaign_id, g.session_traffic_source_last_click.cross_channel_campaign.campaign_id) AS event_traffic_campaign_id
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
-- If session_start contains a reference source, it is used. If not, it is taken from the event. session_traffic_medium etc. also IF (s.event_traffic_source IS NOT NULL because if event_traffic_medium is used, the source is taken from session_start and medium is taken from the first event.
  adopt_source AS(
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
        IF(s.event_traffic_source IS NOT NULL, s.event_traffic_campaign_id, a.event_traffic_campaign_id) AS session_traffic_campaign_id
    FROM first_campaign a FULL JOIN session_start s USING(user_pseudo_id, ga_session_id)
),
-- If session data exists in the past, it is used.
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
            session_traffic_campaign_id
        ) ORDER BY event_date, entrance_timestamp,exit_timestamp ASC LIMIT 1)[OFFSET(0)].*
    FROM `project_id.mart.sessions`  -- Another query is required to store user_pseudo_id, ga_session_id, session_traffic_source, etc. in the sessions table.
    GROUP BY ALL
),
session_source AS(
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
            COALESCE(m.session_traffic_campaign_id, a.session_traffic_campaign_id) AS session_traffic_campaign_id
        ) LIMIT 1)[OFFSET(0)].*
    FROM adopt_source a LEFT JOIN mart_session m 
    USING(user_pseudo_id, ga_session_id)
    GROUP BY ALL
)
SELECT g.*,
a.session_traffic_source,
a.session_traffic_medium,
a.session_traffic_campaign
FROM ga AS g
LEFT JOIN session_source AS a USING (user_pseudo_id, ga_session_id)
```

> [!NOTE]
>Update: As of October 2024, the `session_traffic_source_last_click.cross_channel_campaign` column has been added. To obtain data identical to GA4, using this column is recommended.
>However, for tables prior to this update, you will need to create queries like the one provided here.
