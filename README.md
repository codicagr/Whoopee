# Whoopee
<hr>

#### SyncBlogsEnabledBiJob
Για κάθε item του blog (HouseOfStyle) γίνεται insert στον πίνακα `blogs_enabled` (αν δεν υπάρχει ήδη)

#### SyncBlogsJob, SyncBrandsJob, SyncCategoriesJob, SyncStoresJob, SyncUsersDataJob, SyncWebBehavioralJob
Ακολουθείται ίδια λογική και στις 4 κλάσεις.<br> 
Από τον πίνακα που κοιτάει η κλάση βλέπει το μέγιστο `log_id` και βάσει αυτού <br>εκτελείται το query που συγκεντρώνει πληροφορία από διάφορους πίνακες.
- SyncBlogsJob στον πίνακα `blogs`
- SyncBrandsJob στον πίνακα `brand_tags`
- SyncCategoriesJob στον πίνακα `categories_tags`
- SyncStoresJob στον πίνακα `store_tags`
- SyncUsersDataJob στον πίνακα `users_data` (εδώ δεν κοιταμε το log_id, γίνεται καυτευθείαν inser or update στα αντίστοιχα)
- SyncWebBehavioralJob στον πίνακα `web_behavioral`

#### SyncAttributionInputJob
Incrementally refresh table `attribution_input` for 3 different conversion types blog_visit, engaged_visit, sign-up.

<br> 

Σημ: όλα τα jobs τρέχουν 4 φορές την μέρα (07:30-08:10,  11:30-12:10,  15:30-16:10,  23:30-00:10)
