# Whoopee
<hr>

#### SyncBlogsEnabledBiJob
Για κάθε item του blog (HouseOfStyle) γίνεται insert στον πίνακα `blogs_enabled` (αν δεν υπάρχει ήδη)

#### SyncBlogsBiJob, SyncBrandsBiJob, SyncCategoriesBiJob, SyncStoresBiJob
Ακολουθείται ίδια λογική και στις 4 κλάσεις.<br> 
Από τον πίνακα που κοιτάει η κλάση βλέπει το μέγιστο `log_id` και βάσει αυτού <br>εκτελείται το query που συγκεντρώνει πληροφορία από διάφορους πίνακες.
- SyncBlogsBiJob στον πίνακα `blogs`
- SyncBrandsBiJob στον πίνακα `brand_tags`
- SyncCategoriesBiJob στον πίνακα `categories_tags`
- SyncStoresBiJob στον πίνακα `store_tags`

<br> 

Σημ: όλα τα jobs τρέχουν 4 φορές την μέρα (07:30-07:50,  11:30-11:50,  15:30-15:50,  23:30-23:50)
