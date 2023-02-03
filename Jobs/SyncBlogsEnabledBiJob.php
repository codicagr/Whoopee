<?php

namespace App\Jobs;

use App\Models\Item;
use Illuminate\Support\Facades\DB;

class SyncBlogsEnabledBiJob
{
    public function __construct()
    {
        //
    }

    public function handle()
    {
        $blogs = Item::where('pseudo_model', 'HosStory')->get();
        foreach($blogs as $blog) {
            DB::connection('mysql_bi')->table('blogs_enabled')->updateOrInsert(
                [
                    'site_id' => $blog->site_id,
                    'site_item_id' => $blog->site_item_id
                ],
                [
                    'site_id' => $blog->site_id,
                    'site_item_id' => $blog->site_item_id,
                    'enabled' => $blog->enabled
                ]
            );
        }
    }
}
