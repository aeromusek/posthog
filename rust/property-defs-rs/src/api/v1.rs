use crate::{
    config::Config,
    //metrics_consts::{},
};

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use sqlx::{
    Postgres,
    postgres::PgPoolOptions,
    PgPool,
    QueryBuilder
};

use std::collections::HashMap;
use std::sync::Arc;

// keep this in sync with Django posthog.taxonomy pkg values
const GROUP_TYPE_LIMIT: i32 = 5;
const DEFAULT_QUERY_LIMIT: i32 = 0;
const DEFAULT_QUERY_OFFSET: i32 = 100;
const POSTHOG_EVENT_PROPERTY_TABLE_NAME_ALIAS: &'static str = "check_for_matching_event_property";
const PARENT_PROPERTY_TYPES: [&str; 4] = ["event", "person", "group", "session"];
// shamelessly stolen from:
// https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L343-L361
const EVENTS_HIDDEN_PROPERTY_DEFINITIONS: [&str; 14] = [
    // distinct_id is set in properties by some libraries, but not consistently, so we shouldn't allow users to filter on it
    "distinct_id",
    // used for updating properties
    "$set",
    "$set_once",
    // posthog-js used to send it on events and shouldn't have, now it confuses users
    "$initial_referrer",
    "$initial_referring_domain",
    // Group Analytics
    "$groups",
    "$group_type",
    "$group_key",
    "$group_set",
    "$group_0",
    "$group_1",
    "$group_2",
    "$group_3",
    "$group_4",
];

// https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L326-L339
const EVENT_PROPERTY_DEFINITIONS: [&str; 242] = [
    "$last_posthog_reset",
    "$copy_type",
    "$selected_content",
    "$set",
    "$set_once",
    "$pageview_id",
    "$autocapture_disabled_server_side",
    "$console_log_recording_enabled_server_side",
    "$session_recording_recorder_version_server_side",
    "$session_is_sampled",
    "$feature_flag_payloads",
    "$capture_failed_request",
    "$lib_rate_limit_remaining_tokens",
    "token",
    "$sentry_exception",
    "$sentry_exception_message",
    "$sentry_exception_type",
    "$sentry_tags",
    "$exception_list",
    "$exception_level",
    "$exception_type",
    "$exception_message",
    "$exception_fingerprint",
    "$exception_proposed_fingerprint",
    "$exception_issue_id",
    "$exception_source",
    "$exception_lineno",
    "$exception_colno",
    "$exception_DOMException_code",
    "$exception_is_synthetic",
    "$exception_stack_trace_raw",
    "$exception_handled",
    "$exception_personURL",
    "$cymbal_errors",
    "$exception_capture_endpoint",
    "$exception_capture_endpoint_suffix",
    "$exception_capture_enabled_server_side",
    "$ce_version",
    "$anon_distinct_id",
    "$event_type",
    "$insert_id",
    "$time",
    "$browser_type",
    "$device_id",
    "$replay_minimum_duration",
    "$replay_sample_rate",
    "$session_recording_start_reason",
    "$session_recording_canvas_recording",
    "$session_recording_network_payload_capture",
    "$configured_session_timeout_ms",
    "$replay_script_config",
    "$session_recording_url_trigger_activated_session",
    "$session_recording_url_trigger_status",
    "$recording_status",
    "$geoip_city_name",
    "$geoip_country_name",
    "$geoip_country_code",
    "$geoip_continent_name",
    "$geoip_continent_code",
    "$geoip_postal_code",
    "$geoip_postal_code_confidence",
    "$geoip_latitude",
    "$geoip_longitude",
    "$geoip_time_zone",
    "$geoip_subdivision_1_name",
    "$geoip_subdivision_1_code",
    "$geoip_subdivision_2_name",
    "$geoip_subdivision_2_code",
    "$geoip_subdivision_2_confidence",
    "$geoip_subdivision_3_name",
    "$geoip_subdivision_3_code",
    "$geoip_disable",
    "$el_text",
    "$app_build",
    "$app_name",
    "$app_namespace",
    "$app_version",
    "$device_manufacturer",
    "$device_name",
    "$locale",
    "$os_name",
    "$os_version",
    "$timezone",
    "$touch_x",
    "$touch_y",
    "$plugins_succeeded",
    "$groups",
    "$group_0",
    "$group_1",
    "$group_2",
    "$group_3",
    "$group_4",
    "$group_set",
    "$group_key",
    "$group_type",
    "$window_id",
    "$session_id",
    "$plugins_failed",
    "$plugins_deferred",
    "$$plugin_metrics",
    "$creator_event_uuid",
    "utm_source",
    "$initial_utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_name",
    "utm_content",
    "utm_term",
    "$performance_page_loaded",
    "$performance_raw",
    "$had_persisted_distinct_id",
    "$sentry_event_id",
    "$timestamp",
    "$sent_at",
    "$browser",
    "$os",
    "$browser_language",
    "$browser_language_prefix",
    "$current_url",
    "$browser_version",
    "$raw_user_agent",
    "$user_agent",
    "$screen_height",
    "$screen_width",
    "$screen_name",
    "$viewport_height",
    "$viewport_width",
    "$lib",
    "$lib_custom_api_host",
    "$lib_version",
    "$lib_version__major",
    "$lib_version__minor",
    "$lib_version__patch",
    "$referrer",
    "$referring_domain",
    "$user_id",
    "$ip",
    "$host",
    "$pathname",
    "$search_engine",
    "$active_feature_flags",
    "$enabled_feature_flags",
    "$feature_flag_response",
    "$feature_flag_payload",
    "$feature_flag",
    "$survey_response",
    "$survey_name",
    "$survey_questions",
    "$survey_id",
    "$survey_iteration",
    "$survey_iteration_start_date",
    "$device",
    "$sentry_url",
    "$device_type",
    "$screen_density",
    "$device_model",
    "$network_wifi",
    "$network_bluetooth",
    "$network_cellular",
    "$client_session_initial_referring_host",
    "$client_session_initial_pathname",
    "$client_session_initial_utm_source",
    "$client_session_initial_utm_campaign",
    "$client_session_initial_utm_medium",
    "$client_session_initial_utm_content",
    "$client_session_initial_utm_term",
    "$network_carrier",
    "from_background",
    "url",
    "referring_application",
    "version",
    "previous_version",
    "build",
    "previous_build",
    "gclid",
    "rdt_cid",
    "irclid",
    "_kx",
    "gad_source",
    "gclsrc",
    "dclid",
    "gbraid",
    "wbraid",
    "fbclid",
    "msclkid",
    "twclid",
    "li_fat_id",
    "mc_cid",
    "igshid",
    "ttclid",
    "$is_identified",
    "$initial_person_info",
    "$web_vitals_enabled_server_side",
    "$web_vitals_FCP_event",
    "$web_vitals_FCP_value",
    "$web_vitals_LCP_event",
    "$web_vitals_LCP_value",
    "$web_vitals_INP_event",
    "$web_vitals_INP_value",
    "$web_vitals_CLS_event",
    "$web_vitals_CLS_value",
    "$web_vitals_allowed_metrics",
    "$prev_pageview_last_scroll",
    "$prev_pageview_id",
    "$prev_pageview_last_scroll_percentage",
    "$prev_pageview_max_scroll",
    "$prev_pageview_max_scroll_percentage",
    "$prev_pageview_last_content",
    "$prev_pageview_last_content_percentage",
    "$prev_pageview_max_content",
    "$prev_pageview_max_content_percentage",
    "$prev_pageview_pathname",
    "$prev_pageview_duration",
    "$surveys_activated",
    "$process_person_profile",
    "$dead_clicks_enabled_server_side",
    "$dead_click_scroll_delay_ms",
    "$dead_click_mutation_delay_ms",
    "$dead_click_absolute_delay_ms",
    "$dead_click_selection_changed_delay_ms",
    "$dead_click_last_mutation_timestamp",
    "$dead_click_event_timestamp",
    "$dead_click_scroll_timeout",
    "$dead_click_mutation_timeout",
    "$dead_click_absolute_timeout",
    "$dead_click_selection_changed_timeout",
    "$ai_base_url",
    "$ai_http_status",
    "$ai_input",
    "$ai_input_tokens",
    "$ai_output",
    "$ai_output_tokens",
    "$ai_latency",
    "$ai_model",
    "$ai_model_parameters",
    "$ai_provider",
    "$ai_trace_id",
    "$ai_metric_name",
    "$ai_metric_value",
    "$ai_feedback_text",
    "$ai_parent_id",
    "$ai_span_id",
];

pub fn apply_routes(parent: Router, qmgr: Arc<QueryManager>) -> Router {
    let api_router = Router::new()
        .route(
            "projects/{project_id}/property_definitions/",
            get(handle_prop_defs_by_project),
        )
        .with_state(qmgr);

    parent.nest("/api/v1", api_router)
}

async fn handle_prop_defs_by_project(
    State(qmgr): State<Arc<QueryManager>>,
    Path(project_id): Path<i32>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<PropDefResponse> {
    // parse the request parameters; use Option<T> to track presence for query step

    let search: Option<Vec<String>> = match params.get("search") {
        Some(raw) => Some(
            raw.split(" ")
                .map(|s| s.trim().to_string().to_lowercase())
                .collect(),
        ),
        _ => None,
    };

    let property_type = match params.get("type") {
        Some(s) if PARENT_PROPERTY_TYPES.iter().any(|pt| *pt == s) => Some(*s),
        _ => None,
    };

    // default to -1 if this is missing or present but invalid
    let group_type_index: i32 = match params.get("group_type_index") {
        Some(s) => match s.parse::<i32>().ok() {
            Some(gti)
                if property_type.is_some_and(|pt| pt == "group")
                    && gti >= 0 && gti < GROUP_TYPE_LIMIT => gti,
            _ => -1,
        },
        _ => -1,
    };

    let properties = match params.get("properties") {
        Some(raw) => Some(raw.split(",").map(|s| s.trim().to_string()).collect()),
        _ => None,
    };

    let is_numerical = match params.get("is_numerical") {
        Some(s) => s.parse::<bool>().ok(),
        _ => None,
    };

    let is_feature_flag: Option<bool> = match params.get("is_feature_flag") {
        Some(s) => s.parse::<bool>().ok(),
        _ => None,
    };

    let excluded_properties = match params.get("excluded_properties") {
        Some(raw) => Some(raw.split(",").map(|s| s.trim().to_string()).collect()),
        _ => None,
    };

    // this must be calculated on the Django (caller) side and passed to this API.
    // it allows us to decide the base table to select from in our property defs queries
    // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L463
    // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L504-L508
    let use_enterprise_taxonomy = match params.get("use_enterprise_taxonomy") {
        Some(s) => s.parse::<bool>().ok(),
        _ => None
    };

    let filter_by_event_names: Option<bool> = match params.get("filter_by_event_names") {
        Some(s) => s.parse::<bool>().ok(),
        _ => None,
    };

    // IMPORTANT: this is passed to the Django API as JSON but probably doesn't
    // matter how we pass it from Django to this service, so it's a CSV for now.
    // is this a mistake? TBD, revisit and see below:
    // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L214
    let event_names = match params.get("event_names") {
        Some(raw) => Some(raw.split(",").map(|s| s.trim().to_string()).collect()),
        _ => None,
    };

    let limit: i32 = match params.get("limit") {
        Some(s) => match s.parse::<i32>().ok() {
            Some(val) => val,
            _ => DEFAULT_QUERY_LIMIT
        }
        _ => DEFAULT_QUERY_LIMIT
    };

    let offset: i32 = match params.get("offset") {
        Some(s) => match s.parse::<i32>().ok() {
            Some(val) => val,
            _ => DEFAULT_QUERY_OFFSET
        }
        _ => DEFAULT_QUERY_OFFSET
    };

    let order_by_verified = true; // default behavior

    // construct the count query
    let count_query = qmgr
        .count_query(
            project_id,
            &search,
            &property_type,
            group_type_index,
            &properties,
            &excluded_properties,
            &event_names,
            &is_feature_flag,
            &is_numerical,
            &use_enterprise_taxonomy,
            &filter_by_event_names,
        );

    // construct the property definitions query
    let mut props_query = qmgr
        .property_definitions_query(
            project_id,
            &search,
            &property_type,
            group_type_index,
            &properties,
            &excluded_properties,
            &event_names,
            &is_feature_flag,
            &is_numerical,
            &use_enterprise_taxonomy,
            &filter_by_event_names,
            order_by_verified,
            limit,
            offset,
        );

    // TODO: execute queries, build result structs

    // TODO: Implement!
    Json(PropDefResponse {})
}

// Wraps Postgres client and builds queries
pub struct QueryManager {
    // TODO: capture more config::Config values here as needed
    pool: PgPool,
    enterprise_prop_defs_table: String,
    prop_defs_table: String,
    event_props_table: String,
}

impl QueryManager {
    pub async fn new(cfg: &Config) -> Result<Self, sqlx::Error> {
        let options = PgPoolOptions::new().max_connections(cfg.max_pg_connections);
        let api_pool = options.connect(&cfg.database_url).await?;

        Ok(Self {
            pool: api_pool,
            enterprise_prop_defs_table: cfg.enterprise_prop_defs_table_name.clone(),
            prop_defs_table: cfg.prop_defs_table_name.clone(),
            event_props_table: cfg.event_props_table_name.clone(),
        })
    }

    fn count_query<'a>(
        &self,
        project_id: i32,
        search: &Option<Vec<String>>,
        property_type: &Option<String>,
        group_type_index: i32,
        properties: &'a Option<Vec<String>>,
        excluded_properties: &'a Option<Vec<String>>,
        event_names: &'a Option<Vec<String>>,
        is_feature_flag: &Option<bool>,
        is_numerical: &Option<bool>,
        use_enterprise_taxonomy: &Option<bool>,
        filter_by_event_names: &Option<bool>,
    ) -> String {
        /* The original Django query formulation we're duplicating
         * https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L279-L289

SELECT count(*) as full_count
FROM {self.table_name}
{self._join_on_event_property()}
WHERE coalesce({self.property_definition_table}.project_id, {self.property_definition_table}.team_id) = %(project_id)s
    AND type = %(type)s
    AND coalesce(group_type_index, -1) = %(group_type_index)s
    {self.excluded_properties_filter}
    {self.name_filter}
    {self.numerical_filter}
    {self.search_query}
    {self.event_property_filter}
    {self.is_feature_flag_filter}
    {self.event_name_filter}

        * Also, the conditionally-applied join on event properties table applied above as
        * self._join_on_event_property()
        * https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L293-L305
        */

        // build & render the query
        let mut qb = QueryBuilder::<Postgres>::new("SELECT count(*) AS full_count FROM ");
        
        let from_clause = if use_enterprise_taxonomy.is_some_and(|uet| uet == true) {
            // TODO: ensure this all behaves as it does in Django (and that we need it!) later...
            // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L505-L506
            &format!("{0} FULL OUTER JOIN {1} ON {1}.id={0}.propertydefinition_ptr_id",
                &self.enterprise_prop_defs_table,
                &self.prop_defs_table)
        } else {
            // this is the default if enterprise taxonomy is not requested
            &self.prop_defs_table
        };
        qb.push_bind(from_clause);
        
        // conditionally join on event properties table
        // this join is only applied if the query is scoped to type "event"
        if self.is_prop_type_event(property_type) {
            qb.push(self.event_property_join_type(filter_by_event_names));
            qb.push(" (SELECT DISTINCT property FROM ");
            qb.push_bind(self.event_props_table.clone());
            qb.push(" WHERE COALESCE(project_id, team_id) = ");
            qb.push_bind(project_id);

            // conditionally apply event_names filter
            if filter_by_event_names.is_some() && filter_by_event_names.unwrap() == true {
                if let Some(names) = event_names {
                    if names.len() > 0 {
                        qb.push(" AND event = ANY(");
                        qb.push_bind(names);
                        qb.push(") ");
                    }
                }
            }

            // close the JOIN clause and add the JOIN condition
            qb.push(format!( ") {0} ON {0}.property = name ", POSTHOG_EVENT_PROPERTY_TABLE_NAME_ALIAS));
        }

        // begin the WHERE clause
        qb.push(format!("WHERE COALESCE({0}.project_id, {0}.team_id) = ", self.prop_defs_table));
        qb.push_bind(project_id);
        
        // add condition on "type" (here, ProperyParentType)
        // TODO: throw error in input validation if this is missing!
        if let Some(prop_type) =  property_type {
            qb.push("AND type = ");
            qb.push_bind(prop_type);
        }

        // add condition on group_type_index
        qb.push("AND COALESCE(group_type_index, -1) = ");
        qb.push_bind(group_type_index);

        // conditionally filter on excluded_properties
        // NOTE: excluded_properties is also passed to the Django API as JSON,
        // but may not matter when passed to this service. TBD. See below:
        // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L241
        if let Some(excludes) = excluded_properties {
            if self.is_prop_type_event(property_type) || excludes.len() > 0 {
                qb.push(format!("AND NOT {0}.name = ANY(", self.prop_defs_table));
                let mut buf: Vec<&str> = vec![];
                if self.is_prop_type_event(property_type) {
                    for entry in EVENTS_HIDDEN_PROPERTY_DEFINITIONS {
                        buf.push(entry);
                    }
                }
                if excludes.len() > 0 {
                    for entry in excludes.iter() {
                        buf.push(entry);
                    }
                }
                qb.push_bind(buf);
                qb.push(") ");
            }
        }

        // conditionally filter on property names ("name" col)
        if let Some(props) = properties {
            if props.len() > 0 {
                qb.push(" AND name = ANY(");
                qb.push_bind(props);
                qb.push(") ");
            }
        }

        // conditionally filter for numerical-valued properties:
        // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L493-L499
        // https://github.com/PostHog/posthog/blob/master/posthog/filters.py#L61-L84
        if is_numerical.is_some_and(|is_num| is_num == true) {
            qb.push(" AND is_numerical = true AND NOT name = ANY(ARRAY['distinct_id', 'timestamp']) ");
        }

        // conditionally apply search term matching
        // logic: https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L493-L499
        // helpers logic:
        // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L308-L323
        // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L326-L339
        // 
        // https://github.com/PostHog/posthog/blob/master/posthog/filters.py#L61-L84

        /* **** TODO: implement! ****
           let search_extras = HashMap::<String, String>::new();
           **************************
        */


        // conditionally apply event_names filter for outer query
        //
        // NOTE: the conditional join on event props table applied
        // above applies the same filter, but it can be an INNER or
        // LEFT join, so this is still required.
        if filter_by_event_names.is_some() && filter_by_event_names.unwrap() == true {
            if let Some(names) = event_names {
                if names.len() > 0 {
                    qb.push(" AND event = ANY(");
                    for name in names.iter() {
                        qb.push_bind(name);
                    }
                    qb.push(") ");
                }
            }
        }

        // conditionally apply feature flag property filters
        if is_feature_flag.is_some() {
            if is_feature_flag.unwrap() {
                qb.push(" AND (name LIKE '$feature/%') ");
            } else {
                qb.push(" AND (name NOT LIKE '$feature/%') ");
            }
        }

        // NOTE: event_name_filter from orig Django query doesn't appear to be applied anywhere atm

        // NOTE: count query is global per project_id, so no LIMIT/OFFSET handling is applied

        qb.sql().into()
    }

    fn property_definitions_query<'a>(
        &self,
        project_id: i32,
        search: &Option<Vec<String>>,
        property_type: &Option<String>,
        group_type_index: i32,
        properties: &Option<Vec<String>>,
        excluded_properties: &Option<Vec<String>>,
        event_names: &'a Option<Vec<String>>,
        is_feature_flag: &Option<bool>,
        is_numerical: &Option<bool>,
        use_enterprise_taxonomy: &Option<bool>,
        filter_by_event_names: &Option<bool>,
        order_by_verified: bool, // TODO: where is this coming from?
        limit: i32,
        offset: i32,
    ) -> String {
        /* The original Django query we're duplicating
         * https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L262-L275

SELECT {self.property_definition_fields}, {self.event_property_field} AS is_seen_on_filtered_events
FROM {self.table}
{self._join_on_event_property()}
WHERE coalesce({self.property_definition_table}.project_id, {self.property_definition_table}.team_id) = %(project_id)s
    AND type = %(type)s
    AND coalesce(group_type_index, -1) = %(group_type_index)s
    {self.excluded_properties_filter}
    {self.name_filter} {self.numerical_filter}
    {self.search_query}
    {self.event_property_filter}
    {self.is_feature_flag_filter}
    {self.event_name_filter}
ORDER BY is_seen_on_filtered_events DESC,
         {verified_ordering}
         {self.property_definition_table}.name ASC
LIMIT {self.limit}
OFFSET {self.offset}

        * Also, the conditionally-applied join on event properties table applied above as
        * self._join_on_event_property()
        * https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L293-L305
        */

        let mut qb = QueryBuilder::<Postgres>::new(r#"
        SELECT **TODO**
        "#);

        // TODO: implement query construction!

        let verified_ordering = match order_by_verified {
            true => "verified DESC NULLS LAST,",
            _ => "",
        };

        qb.sql().into()
    }

    // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L232-L237
    // https://github.com/PostHog/posthog/blob/master/posthog/taxonomy/property_definition_api.py#L494-L499

    fn event_property_join_type(&self, filter_by_event_names: &Option<bool>) -> &str {
        if let Some(true) = filter_by_event_names {
            "INNER JOIN"
        } else {
            "LEFT JOIN"
        }
    }

    fn is_prop_type_event(&self, property_type: &Option<String>) -> bool {
        property_type.is_some() && property_type.as_ref().unwrap() == "event"
    }
}

#[derive(Serialize)]
pub struct PropDefResponse {
    count: u32,
    next: Option<String>,
    prev: Option<String>,
    results: Vec<PropDef>,
}

#[derive(Serialize)]
struct PropDef {
    id: String,
    name: String,
    description: String,
    is_numeric: bool,
    updated_at: String, // UTC ISO8601
    updated_by: Person,
    is_seen_on_filtered_events: Option<String>, // VALIDATE THIS!
    property_type: String,
    verified: bool,
    verified_at: String, // UTC ISO8601
    verified_by: Person,
    tags: Vec<String>,
}

#[derive(Serialize)]
struct Person {
    id: u32,
    uuid: String,
    distinct_id: String,
    first_name: String,
    last_name: String,
    email: String,
    is_email_verified: bool,
    hedgehog_config: HedgehogConfig,
}

#[derive(Serialize)]
struct HedgehogConfig {
    use_as_profile: bool,
    color: String,
    accessories: Vec<String>,
    role_at_organization: Option<String>,
}
