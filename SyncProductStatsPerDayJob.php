<?php

namespace App\Jobs\WhoopeeBi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncProductStatsPerDayJob
{
    public function handle()
    {

        $query = "
            SELECT date(log_activities.visited_at), alias as product_name, log_activities.site_id,
                   COUNT(DISTINCT session_cookie) as visits,
                   2+2*ifnull(ROUND(SUM(IF(scroll_percentage>100,100,scroll_percentage)*IF(time_to_sec(timediff(latest_scroll_at,log_activities.created_at))/60>4,4,time_to_sec(timediff(latest_scroll_at,log_activities.created_at))/60))), 0) as score
            FROM goldenha_cdp.log_activities
            JOIN goldenha_cdp.items i ON log_activities.record_id = i.id
                AND record_type = 'App\\\\Models\\\\Item'
                AND log_activities.site_id = i.site_id
            WHERE pseudo_model ='Product' and session_cookie is not null and log_activities.deleted_at is null
                  and date(log_activities.visited_at)=date_add(date(now()), INTERVAL -1 DAY)
            GROUP BY alias, log_activities.site_id
        ";

        $columns = [
            'reference_date', 'product_name', 'site_id', 'visits', 'score'
        ];

        DB::connection('mysql_bi')->table('product_stats_per_day')->insertUsing($columns, $query);
        Log::channel('jobs')->info('[Bi]: Sync Product Stats Per Day finished.');
    }
}
