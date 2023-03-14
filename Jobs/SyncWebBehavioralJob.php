<?php

namespace App\Jobs\Bi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncWebBehavioralJob
{
    public function handle()
    {
        $maxLogActivityId = DB::connection('mysql_bi')->table('web_behavioral')->max('log_id');
        if (is_null($maxLogActivityId)) {
            $maxLogActivityId = 0;
        }

        $query = "SELECT log_id, created_at_log_activity, week(created_at_log_activity, 1),
       scroll_percentage, latest_scroll_at,  url, session_cookie, site_session_id, activity_type_id, act.weight,
       log_act.site_id, visitor_id, visitor_cookie, site_user_id, created_at_visitors, deleted_at_visitors, record_type, item_pseudo_model, referrer_type, referrer_name, search_phrase
        FROM (SELECT id AS log_id, created_at AS created_at_log_activity, scroll_percentage, latest_scroll_at, session_cookie,
                     site_session_id, visitor_id, activity_type_id, record_type, record_id, site_id, first_http_referrer_id, search_phrase, url
          FROM goldenha_cdp.log_activities
           WHERE deleted_at is null and id > ".$maxLogActivityId."
          LIMIT 1000
          ) AS log_act
        JOIN (SELECT id, visitors.visitor_cookie, site_id, site_user_id, created_at as created_at_visitors, deleted_at as deleted_at_visitors
             FROM goldenha_cdp.visitors
             ) AS v ON log_act.visitor_id=v.id AND log_act.site_id = v.site_id
        LEFT JOIN (SELECT id, site_item_id, pseudo_model as item_pseudo_model, site_id
              FROM goldenha_cdp.items
              ) AS it ON log_act.record_id = it.id AND log_act.record_type LIKE '%Item'
        LEFT JOIN (SELECT id, weight
                 FROM goldenha_cdp.activity_types
              ) AS act ON log_act.activity_type_id = act.id
        LEFT JOIN (SELECT id, referrer_type, referrer_name
              FROM goldenha_cdp.http_referrers
              ) AS http_ref ON log_act.first_http_referrer_id = http_ref.id;";

        $columns = [
            'log_id', 'created_at_log_activity', 'week_created_at_log_activity', 'scroll_percentage',
            'latest_scroll_at',  'url', 'session_cookie', 'site_session_id', 'visitor_id', 'visitor_cookie',
            'site_user_id', 'created_at_visitors', 'deleted_at_visitors', 'site_id', 'activity_type_id',
            'weight_activity', 'record_type', 'item_pseudo_model', 'referrer_type', 'referrer_name', 'search_phrase'
        ];

       DB::connection('mysql_bi')->table('web_behavioral')->insertUsing($columns, $query);

        Log::channel('jobs')->info('Sync Web Behavioral finished.');
    }
}
