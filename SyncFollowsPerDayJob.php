<?php

namespace App\Jobs\WhoopeeBi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncFollowsPerDayJob
{
    public function handle()
    {
        $query = "
            SELECT log_activities.site_id, date(log_activities.visited_at), pseudo_model,
            SUM(if(data = '{\"unfollow\":false}',1 , 0)) as follows,
            SUM(if(data = '{\"unfollow\":true}', 1, 0)) as unfollows
            FROM goldenha_cdp.log_activities
            JOIN goldenha_cdp.items i ON log_activities.record_id = i.id
                AND record_type = 'App\\\\Models\\\\Item'
                AND log_activities.site_id = i.site_id
            WHERE activity_type_id = 7 and pseudo_model in ('Product', 'Store')
                AND date(log_activities.visited_at) = date_add(date(now()), INTERVAL -1 DAY)
                AND data IS NOT NULL and session_cookie is not null
            GROUP BY log_activities.site_id, pseudo_model
        ";

        $columns = [
            'site_id', 'reference_date', 'type', 'follows', 'unfollows'
        ];

        DB::connection('mysql_bi')->table('follows_per_day')->insertUsing($columns, $query);
        Log::channel('jobs')->info('[Bi]: Sync Follows Per Day finished.');
    }
}
