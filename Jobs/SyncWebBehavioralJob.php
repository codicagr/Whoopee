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

        $query = "
        SELECT log_id, created_at_log_activity, scroll_percentage, latest_scroll_at,  url, session_cookie, site_session_id,
                activity_type_id, act.weight, log_act.site_id, visitor_id, visitor_cookie, site_user_id, created_at_visitors,
                deleted_at_visitors, record_type, item_pseudo_model, referrer_type, referrer_name, search_phrase,
                u.source, u.medium, u.campaign, u.add_group_id, u.content
        FROM (SELECT id AS log_id, created_at AS created_at_log_activity, scroll_percentage, latest_scroll_at, session_cookie,
                     site_session_id, visitor_id, activity_type_id, record_type, record_id, site_id, first_http_referrer_id, search_phrase, url, utm_id
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
            ) AS http_ref ON log_act.first_http_referrer_id = http_ref.id
        LEFT JOIN (SELECT id, source, medium, campaign, add_group_id, content
            FROM  goldenha_cdp.utms
            WHERE deleted_at is null
            ) AS u ON log_act.utm_id = u.id;
        ";

        $columns = [
            'log_id', 'created_at_log_activity', 'scroll_percentage', 'latest_scroll_at',  'url', 'session_cookie',
            'site_session_id', 'activity_type_id', 'weight_activity', 'site_id', 'visitor_id', 'visitor_cookie', 'site_user_id',
            'created_at_visitors', 'deleted_at_visitors', 'record_type', 'item_pseudo_model', 'referrer_type',
            'referrer_name', 'search_phrase', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_adgroup', 'utm_content'
        ];

       DB::connection('mysql_bi')->table('web_behavioral')->insertUsing($columns, $query);
       Log::channel('jobs')->info('[Bi]: Sync Web Behavioral finished.');
    }
}
