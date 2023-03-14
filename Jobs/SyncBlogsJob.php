<?php

namespace App\Jobs\Bi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncBlogsJob
{
    public function __construct()
    {
        //
    }

    public function handle()
    {
        $maxLogActivityId = DB::connection('mysql_bi')->table('blogs')->max('log_id');
        if(is_null($maxLogActivityId)) {
            $maxLogActivityId = 0;
        }

        $query = "SELECT blogs.log_id, blogs.site_item_id, blogs.site_id, blogs.created_at_log_activity, blogs.published_up, blogs.published_down,
         scroll_percentage, latest_scroll_at, session_cookie,
         blogs.site_session_id, blogs.visitor_id, blogs.visitor_cookie,
         gender, dob, city, blogs.state, country, activity_type_id, blogs.weight, blogs.record_type, blogs.item_pseudo_model,
         blogs.referrer_type, blogs.referrer_name, blogs.blog, shopping_categories.shopping_category
FROM
 ((SELECT log_act.log_id, it.site_item_id, log_act.site_id, log_act.created_at_log_activity, it.published_up, it.published_down,
         scroll_percentage, latest_scroll_at, session_cookie, log_act.site_session_id, log_act.visitor_id, v.visitor_cookie,
         gender, dob, city, v.state, country, activity_type_id, act.weight, record_type, it.pseudo_model AS item_pseudo_model,
         http_ref.referrer_name, http_ref.referrer_type, it.alias AS blog
    FROM (SELECT id AS log_id, created_at AS created_at_log_activity, scroll_percentage, latest_scroll_at, session_cookie,
                 site_session_id, visitor_id, activity_type_id, record_type, record_id, site_id, first_http_referrer_id, url
          FROM goldenha_cdp.log_activities WHERE log_activities.id > " . $maxLogActivityId . "  ORDER BY log_activities.id
          LIMIT 5000
        ) AS log_act
        JOIN (SELECT id, visitors.visitor_cookie, gender, dob, city, state, country
             FROM goldenha_cdp.visitors
             ) AS v ON log_act.visitor_id=v.id
        JOIN (SELECT DISTINCT id, site_item_id, pseudo_model, published_up, published_down, alias, site_id
              FROM goldenha_cdp.items
              WHERE pseudo_model='HosStory'
              ) AS it ON log_act.record_id = it.id AND log_act.record_type LIKE '%Item' AND it.site_id=log_act.site_id
        JOIN (SELECT id, weight
             FROM goldenha_cdp.activity_types
             ) AS act ON log_act.activity_type_id = act.id
        LEFT JOIN (SELECT id, referrer_type, referrer_name
                  FROM goldenha_cdp.http_referrers
                  ) AS http_ref ON log_act.first_http_referrer_id = http_ref.id
     ) AS blogs
 LEFT JOIN (SELECT log_act.id AS id, t.name AS shopping_category
            FROM (SELECT id
                         FROM goldenha_cdp.log_activities) AS log_act
                JOIN (SELECT name, log_activity_id
                     FROM goldenha_cdp.tags
                            JOIN goldenha_cdp.log_activity_tag ON log_activity_tag.tag_id = tags.id
                     WHERE tags.tag_category_id=2
                     ) AS t ON t.log_activity_id=log_act.id
            ) AS shopping_categories ON blogs.log_id=shopping_categories.id
            );";

        $columns = [
            'log_id', 'site_item_id', 'site_id', 'created_at_log_activity', 'published_up', 'published_down',
            'scroll_percentage', 'latest_scroll_at', 'session_cookie',
            'site_session_id', 'visitor_id', 'visitor_cookie',
            'gender', 'dob', 'city', 'state', 'country', 'activity_type_id', 'weight_activity',
            'record_type', 'item_pseudo_model', 'referrer_type', 'referrer_name',
            'blog', 'shopping_category'
        ];

        DB::connection('mysql_bi')->table('blogs')->insertUsing($columns, $query);

        Log::channel('jobs')->info('[Bi]: Sync Blogs finished.');
    }
}
