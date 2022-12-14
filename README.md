# Whoopee
<hr>

#### SyncBlogsEnabledBiJob
Για κάθε item του blog (HouseOfStyle) γίνεται insert στον πίνακα `blogs_enabled` (αν δεν υπάρχει ήδη)

#### SyncBlogsBiJob, SyncBrandsBiJob, SyncCategoriesBiJob, SyncStoresBiJob
Ακολουθείται ίδια λογική και στις 4 κλάσεις.<br> 
Από τον πίνακα που κοιτάει η κλάση βλέπει το μέγιστο log_id και βάσει αυτού εκτελείται το query που συγκεντρώνει πληροφορία από διάφορους πίνακες.
- SyncBlogsBiJob στον πίνακα `blogs`
- SyncBrandsBiJob στον πίνακα `brand_tags`
- SyncCategoriesBiJob στον πίνακα `categories_tags`
- SyncStoresBiJob στον πίνακα `store_tags`
