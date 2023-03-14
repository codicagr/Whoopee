<?php

namespace App\Jobs\Bi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncUsersDataJob
{
    public function handle()
    {
        $query =" SELECT *
        FROM (SELECT site_user_id, site_id, email, name, phone, gender, dob, city, state, country, zip_code
            FROM goldenha_cdp.visitors
            WHERE (site_user_id, site_id, updated_at) IN (SELECT site_user_id, site_id, max(updated_at)
                                                        FROM goldenha_cdp.visitors
                                                        WHERE site_user_id IS NOT NULL
                                                        GROUP BY site_user_id, site_id)
        ) AS visitors_users
        ON DUPLICATE KEY UPDATE email=visitors_users.email, name=visitors_users.name,  phone=visitors_users.phone,
                            gender=visitors_users.gender,  dob=visitors_users.dob, city=visitors_users.city,
                            state=visitors_users.state, country=visitors_users.country, zip_code=visitors_users.zip_code";

        $columns = [
            'site_user_id', 'site_id', 'email', 'name', 'dob', 'gender',
            'phone', 'country', 'state', 'city', 'zip_code'
        ];

        DB::connection('mysql_bi')->table('users_data')->insertUsing($columns, $query);

        Log::channel('jobs')->info('[Bi]: Sync Users Data finished.');
    }
}
