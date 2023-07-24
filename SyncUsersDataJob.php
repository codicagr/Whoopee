<?php


namespace App\Jobs\WhoopeeBi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncUsersDataJob
{
    public function handle()
    {
        $query ="
        SELECT users.site_user_id, users.site_id, users.email, users.name, users.phone, users.gender, users.dob,
               users.city, users.state, users.country, users.zip_code, rfe_latest.segment, rfe_latest.sub_segment,
               users_stats.first_visit, users_stats.last_visit
        FROM (SELECT site_user_id, site_id, email, name, phone, gender, dob, city, state, country, zip_code
            FROM goldenha_cdp.visitors
            WHERE (site_user_id, site_id, updated_at) IN (SELECT site_user_id, site_id, max(updated_at)
                                                            FROM goldenha_cdp.visitors
                                                            WHERE site_user_id IS NOT NULL and email is not null and deleted_at is null
                                                            GROUP BY site_user_id, site_id)
            ) AS users
        LEFT JOIN (SELECT site_user_id, site_id, segment, sub_segment
            FROM goldenhall_bi.rfe_evolution_users
            WHERE comet_type='1 time(s),180 days ago' AND session_end=(SELECT max(session_end)
                                                                        FROM goldenhall_bi.rfe_evolution_users
                                                                        WHERE comet_type='1 time(s),180 days ago')
            ) AS rfe_latest
            ON rfe_latest.site_user_id=users.site_user_id AND rfe_latest.site_id=users.site_id
        LEFT JOIN (SELECT v.site_user_id, v.site_id, MIN(l.created_at) AS first_visit, MAX(l.created_at) AS last_visit
            FROM goldenha_cdp.log_activities l JOIN goldenha_cdp.visitors v ON l.visitor_id = v.id
            GROUP BY site_user_id, site_id) AS users_stats
            ON users_stats.site_user_id=users.site_user_id AND users_stats.site_id=users.site_id
        ON DUPLICATE KEY UPDATE email=users.email, name=users.name,  phone=users.phone,
                                gender=users.gender, dob=users.dob, city=users.city,
                                state=users.state, country=users.country, zip_code=users.zip_code,
                                segment=rfe_latest.segment, sub_segment=rfe_latest.sub_segment, last_visit_date=users_stats.last_visit,
                                updated_at=CURRENT_TIMESTAMP;
        ";

        $columns = [
            'site_user_id', 'site_id', 'email', 'name', 'phone', 'gender', 'dob', 'city', 'state', 'country',
            'zip_code', 'segment', 'sub_segment', 'first_visit_date', 'last_visit_date'
        ];

        DB::connection('mysql_bi')->table('users_data')->insertUsing($columns, $query);

        Log::channel('jobs')->info('[Bi]: Sync Users Data finished.');
    }
}
