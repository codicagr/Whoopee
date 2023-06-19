<?php

namespace App\Jobs\Bi;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class SyncAttributionInputJob
{
    public function handle()
    {
        $operator = 'blog_visit';
        $maxTouchpointId = $this->getMaxTouchPointId($operator);

        $insert = $this->getPartialInsert($operator)."
        from
            (select first_http_referrer_id, utm_id, l.session_cookie, visitor_cookie, l.created_at, max_log_id,
                    referrer_type, referrer_name, campaign, source, u.medium, u.add_group_id
                from (select log_activities.id, log_activities.session_cookie, v.visitor_cookie,
                         max(log_activities.id) as max_log_id, log_activities.created_at, first_http_referrer_id, utm_id
                    from goldenha_cdp.log_activities
                        join visitors v on log_activities.visitor_id = v.id
                    where visitor_cookie is not null and session_cookie is not null and log_activities.site_id=1
                            and log_activities.deleted_at is null and log_activities.id > " . $maxTouchpointId ."
                    group by log_activities.session_cookie) l
                    left join goldenha_cdp.http_referrers hr on l.first_http_referrer_id = hr.id
                    left join goldenha_cdp.utms u on l.utm_id = u.id
        ) as touchpoints
        left join (
            select log_activities.session_cookie, v.visitor_cookie, log_activities.id as conversion_id
            from goldenha_cdp.log_activities
                join visitors v on log_activities.visitor_id = v.id
                join items i on log_activities.record_id = i.id and log_activities.record_type LIKE '%Item'
            where visitor_cookie is not null and session_cookie is not null and i.pseudo_model='HosStory'
              and log_activities.site_id=1 and log_activities.deleted_at is null and v.deleted_at is null
            group by log_activities.session_cookie
            ) as conversions on conversions.session_cookie=touchpoints.session_cookie;";

        DB::insert($insert);
        DB::update($this->getUpdateQuery($maxTouchpointId, $operator));


        /* ---------- */


        $operator = 'engaged_visit';
        $maxTouchpointId = $this->getMaxTouchPointId($operator);

        $insert = $this->getPartialInsert($operator)."
        from
            (select first_http_referrer_id, utm_id, l.session_cookie, visitor_cookie, l.created_at, max_log_id,
                    referrer_type, referrer_name, campaign, source, u.medium, u.add_group_id
                from (select log_activities.id, log_activities.session_cookie, v.visitor_cookie,
                             max(log_activities.id) as max_log_id, log_activities.created_at, first_http_referrer_id, utm_id
                        from goldenha_cdp.log_activities
                            join visitors v on log_activities.visitor_id = v.id
                        where visitor_cookie is not null and session_cookie is not null and log_activities.site_id=1
                                and log_activities.deleted_at is null and log_activities.id > " . $maxTouchpointId ."
                        group by log_activities.session_cookie) l
                        left join goldenha_cdp.http_referrers hr on l.first_http_referrer_id = hr.id
                        left join goldenha_cdp.utms u on l.utm_id = u.id
            ) as touchpoints
            left join (
                select log_activities.session_cookie, v.visitor_cookie, log_activities.id as conversion_id
                from goldenha_cdp.log_activities
                    join visitors v on log_activities.visitor_id = v.id
                    join items i on log_activities.record_id = i.id and log_activities.record_type LIKE '%Item'
                where visitor_cookie is not null and session_cookie is not null and i.pseudo_model in ('HosStory', 'Product')
                  and log_activities.site_id=1 and log_activities.deleted_at is null and v.deleted_at is null
                group by log_activities.session_cookie
                ) as conversions on conversions.session_cookie=touchpoints.session_cookie;";

        DB::insert($insert);
        DB::update($this->getUpdateQuery($maxTouchpointId, $operator));


        /* ---------- */


        $operator = 'sign-up';
        $maxTouchpointId = $this->getMaxTouchPointId($operator);

        $insert = $this->getPartialInsert($operator)."
        from
            (select first_http_referrer_id, utm_id, l.session_cookie, visitor_cookie, l.created_at,
                    referrer_type, referrer_name, campaign, source, u.medium, u.add_group_id, max_log_id
                from (select log_activities.id, log_activities.session_cookie, v.visitor_cookie, log_activities.created_at,
                             first_http_referrer_id, utm_id, max(log_activities.id) as max_log_id
                        from goldenha_cdp.log_activities
                            join visitors v on log_activities.visitor_id = v.id
                        where visitor_cookie is not null and session_cookie is not null and log_activities.site_id=1
                            and log_activities.deleted_at is null and log_activities.id > " . $maxTouchpointId ."
                        group by log_activities.session_cookie) l
                        left join goldenha_cdp.http_referrers hr on l.first_http_referrer_id = hr.id
                        left join goldenha_cdp.utms u on l.utm_id = u.id
            ) as touchpoints
            left join (
                select log_activities.session_cookie, v.visitor_cookie, log_activities.id as conversion_id
                from goldenha_cdp.log_activities
                    join visitors v on log_activities.visitor_id = v.id
                where visitor_cookie is not null and session_cookie is not null and log_activities.site_id=1 and activity_type_id = 3 and log_activities.deleted_at is null and v.deleted_at is null
                group by session_cookie
                ) as conversions on conversions.session_cookie=touchpoints.session_cookie;";

        DB::insert($insert);
        DB::update($this->getUpdateQuery($maxTouchpointId, $operator));

        Log::channel('jobs')->info('[Bi]: Sync Attribution Input finished.');
    }

    private function getMaxTouchPointId(string $operator): int
    {
        $id = DB::connection('mysql_bi')
                ->table('attribution_input')
                ->where('conversion_type', $operator)
                ->max('touchpoint_id');

        if(is_null($id)) { return 0; }

        return $id;
    }

    private function getPartialInsert(string $operator): string
    {
        return "
        insert ignore into goldenhall_bi.attribution_input(idvisitor, idvisit, touchpoint_id, touchpoint_time, conversion_type,
                conversion_id, campaign_source, campaign_medium, campaign_name, referer_type, referer_name, campaign_adgroup)
        select
               touchpoints.visitor_cookie,
               touchpoints.session_cookie,
               touchpoints.max_log_id,
               touchpoints.created_at,
               '".$operator."' as conversion_type,
               conversions.conversion_id,
               touchpoints.source as campaign_source,
               touchpoints.medium as campaign_medium,
               touchpoints.campaign as campaign_name,
               case
                   when touchpoints.referrer_type='direct' then 1
                   when touchpoints.referrer_type='search_engine' then 2
                   when touchpoints.referrer_type='website' then 3
                   when touchpoints.referrer_type='campaign' then 6
                   when touchpoints.referrer_type='social_media' then 7
                   when touchpoints.referrer_type is null then null
                   else 8
                end as referer_type,
               touchpoints.referrer_name,
               touchpoints.add_group_id";
    }

    private function getUpdateQuery(int $maxTouchpointId, string $operator): string
    {
        return "
        UPDATE goldenhall_bi.attribution_input
        JOIN (
                select distinct site_user_id, visitors.visitor_cookie
                from goldenha_cdp.log_activities join visitors on log_activities.visitor_id = visitors.id
                where log_activities.site_id=1 and log_activities.id > " . $maxTouchpointId ."
            ) vis ON goldenhall_bi.attribution_input.idvisitor = vis.visitor_cookie
        SET goldenhall_bi.attribution_input.client_id = vis.site_user_id, updated_at=CURRENT_TIMESTAMP
        WHERE vis.site_user_id is not null and goldenhall_bi.attribution_input.client_id is null and conversion_type='".$operator."';";
    }

}
