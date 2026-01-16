
SELECT 
  TRY_TO_NUMERIC(tl.userid) AS customer_id 
  , tl.submitted_at AS survey_time
  , tl.landing_id 
  , {{ typeform_mc_parser('oTEvG2UrDSQ8') }} AS first_exposure_time
  , {{ typeform_mc_parser('fcE7YjZKDV9y') }} AS first_exposure_source
  , MAX(CASE WHEN ta.question_id = 'Yo7pgitPGW9C' THEN ta.numeric_answer END) AS looking_for_ways_to_stay_healthy_ranking
  , MAX(CASE WHEN ta.question_id = 'faRGoY6OdkhP' THEN ta.numeric_answer END) AS dont_feel_in_control_of_health_ranking
  , MAX(CASE WHEN ta.question_id = 'PcCeElogmNQo' THEN ta.numeric_answer END) AS love_cooking_at_home_ranking
  , CASE WHEN first_exposure_source = 'Ad on my mobile phone'
         THEN 'Mobile Ad'
         WHEN first_exposure_source = 'Another food company or website'
         THEN 'Other Food Company'
         WHEN {{ search_string('first_exposure_source', ('facebook','instagram')) }} THEN 'Facebook/Instagram'
         WHEN {{ search_string('first_exposure_source', ('friend','family','mother',
                                                         'father','sister','brother',
                                                         'daughter','son', 'husband',
                                                         'wife', 'aunt', 'uncle', 
                                                         'parent', 'niece', 'nephew'
                                                         'cousin', 'neighbor', 'coworker', 
                                                         'co worker', 'co-worker')) }} THEN 'Word of Mouth'
         WHEN {{ search_string('first_exposure_source', ('tv','television','commercial')) }} THEN 'TV'
         WHEN {{ search_string('first_exposure_source', ('google','search','bing')) }} THEN 'Search Engine'
         WHEN {{ search_string('first_exposure_source', ('article','blog','online')) }} THEN 'Online/Blog'
         WHEN {{ search_string('first_exposure_source', ('amazon')) }} THEN 'Amazon'
         WHEN {{ search_string('first_exposure_source', ('qvc')) }} THEN 'QVC'
         WHEN {{ search_string('first_exposure_source', ('costco')) }} THEN 'Costco'
         WHEN {{ search_string('first_exposure_source', ('youtube','you tube')) }} THEN 'YouTube'
         WHEN {{ search_string('first_exposure_source', ('tiktok','tik tok')) }} THEN 'Tik-Tok'
         WHEN {{ search_string('first_exposure_source', ('pinterest')) }} THEN 'Pinterest'
         WHEN {{ search_string('first_exposure_source', ('reddit')) }} THEN 'Reddit'
         WHEN {{ search_string('first_exposure_source', ('snapchat','snap chat')) }} THEN 'Snapchat'
         WHEN {{ search_string('first_exposure_source', ('direct mail.*')) }} THEN 'Direct Mail'
         WHEN {{ search_string('first_exposure_source', ('podcast')) }} THEN 'Podcast'
         WHEN {{ search_string('first_exposure_source', ('email','e-mail')) }} THEN 'Email'
         ELSE 'Other'
    END AS first_exposure_category
  , CASE WHEN first_exposure_source = 'Ad on my mobile phone'
         THEN 'Mobile Ad'
         WHEN first_exposure_source = 'Another food company or website'
         THEN 'Other Food Company'
         WHEN {{ search_string('first_exposure_source', ('facebook', 'face', 'book', 'insta', 'fb')) }} THEN 'Facebook/Instagram'
         WHEN {{ search_string('first_exposure_source', ('tv', 'television', 'commerical', 'msnbc', 'hallmark', 'bravo', 'the view', 'good morning america')) }} THEN 'TV'
         WHEN {{ search_string('first_exposure_source', ('article', 'blog', 'online')) }} THEN 'Online/Blog'
         WHEN {{ search_string('first_exposure_source', ('amazon')) }} THEN 'Amazon'
         WHEN {{ search_string('first_exposure_source', ('qvc')) }} THEN 'QVC'
         WHEN {{ search_string('first_exposure_source', ('costco')) }} THEN 'Costco'
         WHEN {{ search_string('first_exposure_source', ('youtube', 'you tube')) }} THEN 'YouTube'
         WHEN {{ search_string('first_exposure_source', ('tiktok', 'tik tok', 'tik-tok')) }} THEN 'Tik-Tok'
         WHEN {{ search_string('first_exposure_source', ('pinterest')) }} THEN 'Pinterest'
         WHEN {{ search_string('first_exposure_source', ('reddit')) }} THEN 'Reddit'
         WHEN {{ search_string('first_exposure_source', ('snapchat', 'snap chat')) }} THEN 'Snapchat'
         WHEN {{ search_string('first_exposure_source', ('direct mail')) }} THEN 'Direct Mail'
         WHEN {{ search_string('first_exposure_source', ('email', 'e-mail', 'gmail')) }} THEN 'Email'
         WHEN {{ search_string('first_exposure_source', ('nutritionist', 'doctor', 'nurse', 'physician', 'medical', 'therapist', 'psychiatrist')) }} THEN 'HealthCare Professional'
         WHEN {{ search_string('first_exposure_source', ('friend', 'family', 'someone', 'referral', 'work', 'dad', 'search', 'bing', 'parent', 'google', 'review',
                                                        'relative', 'brother', 'sister', 'wife', 'husband', 'father', 'grandma', 'mom', 'mother', 'daughter', 'son',
                                                        'neighbor', 'word', 'roommate', 'colleague', 'employee', 'spouse', 'niece', 'nephew', 'cousin', 'customer',
                                                        'web', 'client', 'owner', 'ceo', 'rabie', 'wilcox', 'wachs')) }} 
         THEN 'Organic'
         WHEN {{ search_string('first_exposure_source', ('oprah', 'news', 'magazine', 'blog', 'post', 'article', 'podcast', 'radio', 'bloomberg', 'pr', 'wsj', 'tmz', 
                                                        'jennifer hudson', 'wall street journal', 'show', 'illustrated', 'test kitchen', 'cnbc')) }} 
         THEN 'PR'
         WHEN first_exposure_source IS NULL THEN 'Unknown'
         ELSE 'Other'
    END AS first_exposure_category_strategic_cut
  , {{ typeform_mc_parser('NfuOPXAU6XHc') }} AS first_exposure_other_food_company
  , {{ typeform_mc_parser('KYCBSUKxT77f') }} AS household_number
  , BOOLOR_AGG(CASE WHEN ta.question_id = '2rCCMgWVa6Oi' THEN ta.boolean_answer END) AS has_used_other_meal_subscriptions
  , BOOLOR_AGG(CASE WHEN ta.question_id = 'zzxgzk6iTetq' THEN ta.boolean_answer END) AS has_seen_tovala_on_tv_raw
  , COALESCE(has_seen_tovala_on_tv_raw, FALSE) 
      OR COALESCE(first_exposure_source = 'TV commercial', FALSE) AS has_seen_tovala_on_tv
  , NULLIF(LISTAGG(CASE WHEN ta.question_id = 'JhQTY5nTkpuu' 
                        THEN ta.text_answer
                   END, '|'), '') AS purchase_reason
  , CASE WHEN COALESCE(first_exposure_source, '') = 'TV commercial'
         THEN 'tv_first'
         WHEN COALESCE(first_exposure_source, '') <> 'TV commercial' AND has_seen_tovala_on_tv
         THEN 'tv_not_first'
         WHEN NOT has_seen_tovala_on_tv
         THEN 'no_tv'
    END AS tv_customer_category
  , {{ ingredient_ranking_cleanup('2KO8P78vMeOd', 'PeYy6TjZEOSE') }} AS chicken_ranking
  , {{ ingredient_ranking_cleanup('PfjkhGgH8Vf5', 'iqi7SxTjLcc9') }} AS beef_ranking
  , {{ ingredient_ranking_cleanup('Bk7dxg3R07VQ', 'Nz1MLycjwTdB') }} AS pork_ranking
  , {{ ingredient_ranking_cleanup('o6FDcNRaPpyf', 'VQ9sEyVndCq5') }} AS fish_ranking
  , {{ ingredient_ranking_cleanup('H6G3FMRxLzim', 'TNfobyJ321Ft') }} AS shellfish_ranking
  , {{ ingredient_ranking_cleanup('7vIOqbXKxt55', 'RSWF0Lw2Nfvx') }} AS dairy_ranking
  , {{ ingredient_ranking_cleanup('EDcNBU4XjVN4', 'sQYQ6DljwZqt') }} AS eggs_ranking
  , {{ ingredient_ranking_cleanup('GI510vcfDgPv', 'Urmm580Jw6VT') }} AS gluten_ranking
  , {{ ingredient_ranking_cleanup('nccoWF4gufkb', 'et8MvBbUE2iC') }} AS tree_nuts_ranking
  , {{ ingredient_ranking_cleanup('98Z8bl6c21Fd', 'P981t5w0BUN0') }} AS spicy_foods_ranking
  , {{ ingredient_ranking_cleanup('pH74FfaSXKj7', 'sJuQgTAjlT1F') }} AS mushrooms_ranking
  , {{ ingredient_ranking_cleanup('mdVMkS2yzV4Z', 'qX7qBI4E2JFv') }} AS olives_ranking
  , {{ ingredient_ranking_cleanup('C9x5sBPel8Ak', 'av20BumANGxe') }} AS beans_ranking
  , {{ ingredient_ranking_cleanup('4OxAnzuyQd6p', 'Kd15eF9Jps4W') }} AS cilantro_ranking
  , {{ ingredient_ranking_cleanup('Ow0JT5zxLwM2', 'qNwrBAKPy7H5') }} AS soy_ranking
  , MAX(CASE WHEN ta.question_id = '2qWcF9HyYiST' THEN ta.numeric_answer END) AS meal_confidence 
  , {{ search_string('purchase_reason', ('widow','death','died','passed away')) }} AS reason_death_of_loved_one
  , {{ search_string('purchase_reason', ('empty nest','retired')) }} AS reason_empty_nester
  , {{ search_string('purchase_reason', ('single','dinner.* for 1','dinner.* for one','cook.* for 1','cook.* for one','meal.* for 1','meal.* for one','liv.* alone','liv.* by myself','cook .* for myself','cook.* for myself','liv.* on my own')) }} AS reason_single_person
  , {{ search_string('purchase_reason', ('spouse','husband','wife','partner')) }} AS reason_couple
  , {{ search_string('purchase_reason', ('new parent','baby','toddler')) }} AS reason_new_parent
  , {{ search_string('purchase_reason', ('daughter','son','kid.*','child.*','teenage.*')) }} AS reason_has_kids
  , {{ search_string('purchase_reason', ('older parent','elder.*','aging parent.*')) }} AS reason_elderly
  , {{ search_string('purchase_reason', ('surgery','cancer','chemo','blood pressure','diabetes','celiac','cholesterol','diabetic','caregiver','chronic','alzheimer.*','alzhiemer.*','disease','illness')) }} AS reason_health_issue
  , {{ search_string('purchase_reason', ('apartment.*','trailer.*',' rv ','hotel.*')) }} AS reason_not_in_a_house
  , {{ search_string('purchase_reason', ('promo.*','sale.*','discount.*','deal.*','reduced cost','lower cost','low.* price','reduced price','offer','price reduction','price break','cost reduction','price drop','cyber','49','79','99','149','50','100','199','299','349', '350','90')) }} AS reason_lower_price
  , {{ search_string('purchase_reason', ('100 day.*','trial offer','money back guarantee','return policy')) }} AS reason_100_day_trial
  , {{ search_string('purchase_reason', ('convenience','convenient','time','busy','hassle','schedule','quick','easy','ease','easier','work a.*lot','work late','home late','work long hours','low prep','no prep','fast.*','ready to eat','clean.*up','lazy','late night','simple')) }} AS reason_convenience
  , {{ search_string('purchase_reason', ('quality','fresh.*','taste','delicious.*','tasty')) }} AS reason_quality
  , {{ search_string('purchase_reason', ('health.*','diet','lose weight','fresh','weight loss','nutrition','nutritious','eat better','portion.*')) }} AS reason_health
  , {{ search_string('purchase_reason', ('hate.* .* cook.*','don.*t cook','don.*t .* cook.*','can.*t cook','decide what to cook','barely cook.*','never cook.*','tired of cook.*','difficult to cook.*','does.*t cook','terrible cook','bad cook')) }} AS reason_doesnt_cook
  , {{ search_string('purchase_reason', ('takeout','door.*dash','grubhub','delivery','quality','eat out','frozen dinner','frozen meal','fast food','restaurant.*','order out','ordering out','ordering food','order food')) }} AS reason_alternative_takeout
  , {{ search_string('purchase_reason', ('meal kit','blue apron','hello.*fresh','home chef','meal service','meal delivery service.*','meal company','freshly','factor','unity','sunbasket')) }} AS reason_alternative_meal_kit
  , {{ search_string('purchase_reason', ('toast.*')) }} AS reason_toaster
  , {{ search_string('purchase_reason', ('steam.*','moist')) }} AS reason_steam_oven
  , {{ search_string('purchase_reason', ('smart oven','automatic','wifi','connected oven')) }} AS reason_smart_oven
  , {{ search_string('purchase_reason', ('scan.*','bar.*code','auto cook','automatic')) }} AS reason_scan_to_cook
  , {{ search_string('purchase_reason', ('air fry.*','air frier')) }} AS reason_air_fry
  , {{ search_string('purchase_reason', ('oprah','favorite things')) }} AS reason_oprah
  , {{ search_string('purchase_reason', ('refer.*','endorse.*','friend','recommend.*','heard great things','heard about it')) }} AS reason_referral
  , {{ search_string('purchase_reason', ('review.*','rating.*')) }} AS reason_review
  , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY submitted_at DESC) AS nth
FROM {{ table_reference('typeform_landings') }} tl 
INNER JOIN {{ table_reference('typeform_answers') }} ta
  ON tl.landing_id = ta.landing_id
WHERE tl.form_id = 'lkCzSO'
  AND TRY_TO_NUMERIC(tl.userid) IS NOT NULL
GROUP BY 1,2,3
