# Whoopee
<hr>

#### SyncBlogsEnabledBiJob
Για κάθε item του blog (HouseOfStyle) γίνεται insert στον πίνακα `blogs_enabled` (αν δεν υπάρχει ήδη)

#### SyncBlogsJob, SyncBrandsJob, SyncCategoriesJob, SyncStoresJob, SyncUsersDataJob, SyncWebBehavioralJob
Ακολουθείται ίδια λογική.<br> 
Από τον πίνακα που κοιτάει η κλάση βλέπει το μέγιστο `log_id` και βάσει αυτού <br>εκτελείται το query που συγκεντρώνει πληροφορία από διάφορους πίνακες.
- SyncBlogsJob στον πίνακα `blogs`
- SyncBrandsJob στον `brand_tags`
- SyncCategoriesJob στον `categories_tags`
- SyncStoresJob στον `store_tags`
- SyncUsersDataJob στον `users_data` (εδώ δεν κοιταμε το log_id, γίνεται καυτευθείαν inser or update στα αντίστοιχα)
- SyncWebBehavioralJob στον `web_behavioral`

#### SyncFollowsPerDayJob, SyncProductStatsPerDayJob
Εδώ δεν κοιταμε το μέγιστο `log_id`, γίνεται καυτευθείαν insert.  
- SyncUsersDataJob στον `users_data`
- SyncFollowsPerDayJob στον `follows_per_day`  
- SyncProductStatsPerDayJob στον `product_stats_per_day`  

#### SyncAttributionInputJob
Incrementally refresh table `attribution_input` for 3 different conversion types blog_visit, engaged_visit, sign-up.

<br> 

Σημ: όλα τα jobs τρέχουν 4 φορές την μέρα (07:30-08:10,  11:30-12:10,  15:30-16:10,  23:30-00:10)
