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

<br> 

Σημ: όλα τα jobs τρέχουν 4 φορές την μέρα (07:30-08:00,  11:30-12:00,  15:30-16:00,  23:30-00:00)
