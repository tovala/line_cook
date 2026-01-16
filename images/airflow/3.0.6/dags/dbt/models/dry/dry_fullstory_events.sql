{{
  config(
    alias='fullstory_events',
    materialized='incremental', 
    unique_key='row_id'
  ) 
}}

SELECT 
  __sdc_primary_key AS row_id
  , _sdc_received_at AS updated
  , DENSE_RANK() OVER (PARTITION BY sessionid ORDER BY eventstart, _sdc_sequence) AS event_sequence
  , {{ clean_string('appname') }} AS appname
  , {{ clean_string('apppackagename') }} AS apppackagename
  , {{ clean_string('eventcustomname') }} AS eventcustomname
  , eventmoddead
  , eventmoderror
  , eventmodfrustrated
  , eventstart
  , {{ clean_string('eventtargetselectortok') }} AS eventtargetselectortok
  , {{ clean_string('eventtargettext') }} AS eventtargettext
  , {{ clean_string('eventtype') }} AS eventtype
  , evt_activesubscription_bool
  , {{ clean_string('evt_addressline1_str') }} AS evt_addressline1_str
  , {{ clean_string('evt_addressline2_str') }} AS evt_addressline2_str
  , {{ clean_string('evt_amount_str') }} AS evt_amount_str
  , {{ clean_string('evt_answer_str') }} AS evt_answer_str
  , {{ clean_string('evt_availabilityzip_str') }} AS evt_availabilityzip_str
  , {{ clean_string('evt_category_str') }} AS evt_category_str
  , evt_com_tovala_tovala_notifications_cycledetails_bool
  , evt_com_tovala_tovala_notifications_mealcomplete_bool
  , evt_com_tovala_tovala_notifications_mealdelivery_bool
  , evt_com_tovala_tovala_notifications_mealwarning_bool
  , evt_com_tovala_tovala_notifications_news_bool
  , evt_com_tovala_tovala_notifications_ovenwarning_bool
  , evt_com_tovala_tovala_notifications_recipeupdates_bool
  , {{ clean_string('evt_config_headers_accept_str') }} AS evt_config_headers_accept_str
  , {{ clean_string('evt_config_headers_authorization_str') }} AS evt_config_headers_authorization_str
  , {{ clean_string('evt_config_headers_xtovalaappid_str') }} AS evt_config_headers_xtovalaappid_str
  , evt_config_maxcontentlength_real
  , {{ clean_string('evt_config_method_str') }} AS evt_config_method_str
  , evt_config_timeout_real
  , {{ clean_string('evt_config_url_str') }} AS evt_config_url_str
  , {{ clean_string('evt_config_xsrfcookiename_str') }} AS evt_config_xsrfcookiename_str
  , {{ clean_string('evt_config_xsrfheadername_str') }} AS evt_config_xsrfheadername_str
  , {{ clean_string('evt_coupon_code_str') }} AS evt_coupon_code_str
  , {{ clean_string('evt_coupon_str') }} AS evt_coupon_str
  , {{ clean_string('evt_currency_str') }} AS evt_currency_str
  , evt_cycle_real
  , {{ clean_string('evt_deliverydate_date') }} AS evt_deliverydate_date
  , {{ clean_string('evt_email_str') }} AS evt_email_str
  , {{ clean_string('evt_error_str') }} AS evt_error_str
  , {{ clean_string('evt_event_str') }} AS evt_event_str
  , {{ clean_string('evt_flow_str') }} AS evt_flow_str
  , evt_gtmuniqueeventid_real
  , evt_id_int
  , {{ clean_string('evt_installation_id_str') }} AS evt_installation_id_str
  , evt_ios_user_bool
  , {{ clean_string('evt_item_str') }} AS evt_item_str
  , evt_mealid_real
  , {{ clean_string('evt_mealid_str') }} AS evt_mealid_str
  , evt_meals_real
  , evt_meal_int
  , {{ clean_string('evt_methodname_str') }} AS evt_methodname_str
  , {{ clean_string('evt_name_str') }} AS evt_name_str
  , {{ clean_string('evt_orderid_str') }} AS evt_orderid_str
  , {{ clean_string('evt_order_id_str') }} AS evt_order_id_str
  , evt_pairedoven_bool
  , {{ clean_string('evt_phone_str') }} AS evt_phone_str
  , {{ clean_string('evt_plan_str') }} AS evt_plan_str
  , {{ clean_string('evt_postalcode_str') }} AS evt_postalcode_str
  , {{ clean_string('evt_preferredlanguage_str') }} AS evt_preferredlanguage_str
  , {{ clean_string('evt_price_str') }} AS evt_price_str
  , {{ clean_string('evt_qty_str') }} AS evt_qty_str
  , {{ clean_string('evt_quantity_str') }} AS evt_quantity_str
  , {{ clean_string('evt_query_str') }} AS evt_query_str
  , {{ clean_string('evt_referral_code_str') }} AS evt_referral_code_str
  , {{ clean_string('evt_response_config_headers_accept_str') }} AS evt_response_config_headers_accept_str
  , {{ clean_string('evt_response_config_headers_authorization_str') }} AS evt_response_config_headers_authorization_str
  , {{ clean_string('evt_response_config_headers_xtovalaappid_str') }} AS evt_response_config_headers_xtovalaappid_str
  , evt_response_config_maxcontentlength_real
  , {{ clean_string('evt_response_config_method_str') }} AS evt_response_config_method_str
  , evt_response_config_timeout_real
  , {{ clean_string('evt_response_config_url_str') }} AS evt_response_config_url_str
  , {{ clean_string('evt_response_config_xsrfcookiename_str') }} AS evt_response_config_xsrfcookiename_str
  , {{ clean_string('evt_response_config_xsrfheadername_str') }} AS evt_response_config_xsrfheadername_str
  , evt_response_data_error_bool
  , {{ clean_string('evt_response_data_message_str') }} AS evt_response_data_message_str
  , {{ clean_string('evt_response_headers_contentlength_str') }} AS evt_response_headers_contentlength_str
  , {{ clean_string('evt_response_headers_contenttype_str') }} AS evt_response_headers_contenttype_str
  , {{ clean_string('evt_response_statustext_str') }} AS evt_response_statustext_str
  , evt_response_status_real
  , {{ clean_string('evt_revenue_str') }} AS evt_revenue_str
  , {{ clean_string('evt_search_str') }} AS evt_search_str
  , {{ clean_string('evt_segmentanonymousid_str') }} AS evt_segmentanonymousid_str
  , {{ clean_string('evt_sender_str') }} AS evt_sender_str
  , {{ clean_string('evt_severity_str') }} AS evt_severity_str
  , {{ clean_string('evt_shippingcity_str') }} AS evt_shippingcity_str
  , evt_shippingoption_amount_real
  , {{ clean_string('evt_shippingoption_detail_str') }} AS evt_shippingoption_detail_str
  , {{ clean_string('evt_shippingoption_id_str') }} AS evt_shippingoption_id_str
  , {{ clean_string('evt_shippingoption_label_str') }} AS evt_shippingoption_label_str
  , {{ clean_string('evt_sku_str') }} AS evt_sku_str
  , {{ clean_string('evt_state_str') }} AS evt_state_str
  , evt_termid_real
  , evt_term_int
  , evt_term_real
  , {{ clean_string('evt_timestamp_str') }} AS evt_timestamp_str
  , {{ clean_string('evt_token_str') }} AS evt_token_str
  , evt_totalamount_real
  , evt_userid_real
  , evt_userrecipescount_int
  , {{ clean_string('evt_user_str') }} AS evt_user_str
  , evt_voiceover_bool
  , {{ clean_string('evt_walletname_str') }} AS evt_walletname_str
  , indvid
  , loaddomcontenttime
  , loadeventtime
  , loadfirstpainttime
  , pageactiveduration
  , {{ clean_string('pageagent') }} AS pageagent
  , {{ clean_string('pagebrowser') }} AS pagebrowser
  , pageclusterid
  , {{ clean_string('pagedevice') }} AS pagedevice
  , pageduration
  , pageid
  , {{ clean_string('pageip') }} AS pageip
  , {{ clean_string('pagelatlong') }} AS pagelatlong
  , pagenumerrors
  , pagenuminfos
  , pagenumwarnings
  , {{ clean_string('pageoperatingsystem') }} AS pageoperatingsystem
  , {{ clean_string('pagerefererurl') }} AS pagerefererurl
  , {{ clean_string('pageurl') }} AS pageurl
  , sessionid
  , {{ clean_string('userappkey') }} AS userappkey
  , {{ clean_string('userdisplayname') }} AS userdisplayname
  , {{ clean_string('useremail') }} AS useremail
  , userid
  , user_activesubscription_bool
  , {{ clean_string('user_addressline1_str') }} AS user_addressline1_str
  , {{ clean_string('user_addressline2_str') }} AS user_addressline2_str
  , {{ clean_string('user_availabilityzip_str') }} AS user_availabilityzip_str
  , user_com_tovala_tovala_notifications_cycledetails_bool
  , user_com_tovala_tovala_notifications_mealcomplete_bool
  , user_com_tovala_tovala_notifications_mealdelivery_bool
  , user_com_tovala_tovala_notifications_mealwarning_bool
  , user_com_tovala_tovala_notifications_news_bool
  , user_com_tovala_tovala_notifications_ovenwarning_bool
  , user_com_tovala_tovala_notifications_recipeupdates_bool
  , user_id_int
  , {{ clean_string('user_installation_id_str') }} AS user_installation_id_str
  , user_ios_user_bool
  , {{ clean_string('user_name_str') }} AS user_name_str
  , user_pairedoven_bool
  , {{ clean_string('user_phone_str') }} AS user_phone_str
  , {{ clean_string('user_postalcode_str') }} AS user_postalcode_str
  , {{ clean_string('user_preferredlanguage_str') }} AS user_preferredlanguage_str
  , {{ clean_string('user_segmentanonymousid_str') }} AS user_segmentanonymousid_str
  , {{ clean_string('user_shippingcity_str') }} AS user_shippingcity_str
  , {{ clean_string('user_state_str') }} AS user_state_str
  , user_userrecipescount_int
  , user_voiceover_bool
  , {{ clean_string('evt_pairedoven_str') }} AS evt_pairedoven_str
  , {{ clean_string('evt_config_data_str') }} AS evt_config_data_str
  , evt_line_real
  , {{ clean_string('evt_config_headers_xnonce_str') }} AS evt_config_headers_xnonce_str
  , evt_defaultshipperiod_real
  , {{ clean_string('user_pairedoven_str') }} AS user_pairedoven_str
  , {{ clean_string('evt_shared_to_str') }} AS evt_shared_to_str
  , {{ clean_string('evt_response_config_headers_xnonce_str') }} AS evt_response_config_headers_xnonce_str
  , {{ clean_string('evt_userid_str') }} AS evt_userid_str
  , evt_totalamount_real__fl
  , {{ clean_string('evt_response_config_headers_contenttype_str') }} AS evt_response_config_headers_contenttype_str
  , {{ clean_string('user_userrecipescount_str') }} AS user_userrecipescount_str
  , evt_column_real
  , {{ clean_string('evt_sourceurl_str') }} AS evt_sourceurl_str
  , {{ clean_string('evt_userrecipescount_str') }} AS evt_userrecipescount_str
  , {{ clean_string('evt_subscriptiontypeid_str') }} AS evt_subscriptiontypeid_str
  , {{ clean_string('evt_config_headers_contenttype_str') }} AS evt_config_headers_contenttype_str
  , {{ clean_string('evt_response_config_data_str') }} AS evt_response_config_data_str
  , {{ clean_string('evt_site_str') }} AS evt_site_str
  , evt_response_request_kliscorsrequest_bool
  , evt_response_request_klisrequestedwithcustomheader_bool
  , evt_request_klisrequestedwithcustomheader_bool
  , evt_totalamount_real__de
  , evt_request_kliscorsrequest_bool
  , {{ clean_string('user_site_str') }} AS user_site_str
  , {{ clean_string('evt_response_headers_cachecontrol_str') }} AS evt_response_headers_cachecontrol_str
  , {{ clean_string('evt_code_str') }} AS evt_code_str
  , user_is_order_home_screen_tester_bool
  , user_is_employee_bool
  , evt_is_employee_bool
  , {{ clean_string('evt_mealtitle_str') }} AS evt_mealtitle_str
  , evt_is_order_home_screen_tester_bool
  , {{ clean_string('evt_depth_str') }} AS evt_depth_str
  , {{ clean_string('evt_termid_str') }} AS evt_termid_str
  , {{ clean_string('evt_term_str') }} AS evt_term_str
  , {{ clean_string('evt_selectionid_str') }} AS evt_selectionid_str
  , {{ clean_string('evt_total_distance_str') }} AS evt_total_distance_str
  , evt_rating_real
  , {{ clean_string('evt_title_str') }} AS evt_title_str
  , {{ clean_string('evt_variant_str') }} AS evt_variant_str
  , {{ clean_string('evt_searched_str') }} AS evt_searched_str
  , {{ clean_string('evt_comment_str') }} AS evt_comment_str
  , evt_premium_ok_bool
  , {{ clean_string('evt_request_str') }} AS evt_request_str
  , evt_tax_real
  , {{ clean_string('evt_product_str') }} AS evt_product_str
  , {{ clean_string('evt_product_id_str') }} AS evt_product_id_str
  , {{ clean_string('user_activesubscription_str') }} AS user_activesubscription_str
  , {{ clean_string('evt_activesubscription_str') }} AS evt_activesubscription_str
  , {{ clean_string('evt_response_headers_accesscontrolallowmethods_str') }} AS evt_response_headers_accesscontrolallowmethods_str
  , {{ clean_string('evt_response_headers_xrequestid_str') }} AS evt_response_headers_xrequestid_str
  , {{ clean_string('evt_response_headers_xpoweredby_str') }} AS evt_response_headers_xpoweredby_str
  , {{ clean_string('evt_response_headers_server_str') }} AS evt_response_headers_server_str
  , {{ clean_string('evt_response_headers_contentencoding_str') }} AS evt_response_headers_contentencoding_str
  , {{ clean_string('evt_response_headers_accesscontrolallowheaders_str') }} AS evt_response_headers_accesscontrolallowheaders_str
  , {{ clean_string('evt_response_headers_date_str') }} AS evt_response_headers_date_str
  , {{ clean_string('evt_response_headers_accesscontrolalloworigin_str') }} AS evt_response_headers_accesscontrolalloworigin_str
  , {{ clean_string('evt_response_headers_vary_str') }} AS evt_response_headers_vary_str
  , {{ clean_string('evt_response_headers_connection_str') }} AS evt_response_headers_connection_str
  , evt_userinfo_aps_badge_int
  , {{ clean_string('evt_completionhandler_str') }} AS evt_completionhandler_str
  , evt_userinfo_showpopup_bool
  , {{ clean_string('evt_userinfo_aps_sound_str') }} AS evt_userinfo_aps_sound_str
  , {{ clean_string('evt_userinfo_message_str') }} AS evt_userinfo_message_str
  , {{ clean_string('evt_userinfo_aps_alert_body_str') }} AS evt_userinfo_aps_alert_body_str
  , {{ clean_string('evt_userinfo_aps_alert_title_str') }} AS evt_userinfo_aps_alert_title_str
  , {{ clean_string('evt_userinfo_diagnostics_clearreviewablequeue_str') }} AS evt_userinfo_diagnostics_clearreviewablequeue_str
  , {{ clean_string('evt_userinfo_diagnostics_addfakereviewablemeals_str') }} AS evt_userinfo_diagnostics_addfakereviewablemeals_str
  , {{ clean_string('evt_userinfo_aps_alert_str') }} AS evt_userinfo_aps_alert_str
  , evt_userinfo_data_ts_int
  , {{ clean_string('evt_userinfo_data_type_str') }} AS evt_userinfo_data_type_str
  , evt_userinfo_requiresinteraction_bool
  , {{ clean_string('evt_userinfo_aps_alert_subtitle_str') }} AS evt_userinfo_aps_alert_subtitle_str
  , {{ clean_string('evt_userinfo_id_str') }} AS evt_userinfo_id_str
  , {{ clean_string('evt_features_str') }} AS evt_features_str
  , {{ clean_string('evt_identifier_str') }} AS evt_identifier_str
  , {{ clean_string('evt_reason_str') }} AS evt_reason_str
  , {{ clean_string('evt_loadtime_str') }} AS evt_loadtime_str
  , {{ clean_string('evt_errormessage_str') }} AS evt_errormessage_str
  , {{ clean_string('evt_previouslyprompted_str') }} AS evt_previouslyprompted_str
  , {{ clean_string('evt_cookablesinneedofrating_str') }} AS evt_cookablesinneedofrating_str
  , {{ clean_string('evt_underlyingerror_str') }} AS evt_underlyingerror_str
  , {{ clean_string('evt_deviceid_str') }} AS evt_deviceid_str
  , {{ clean_string('evt_networkcount_str') }} AS evt_networkcount_str
  , {{ clean_string('evt_agent_str') }} AS evt_agent_str
  , {{ clean_string('evt_selectedmealcount_str') }} AS evt_selectedmealcount_str
  , {{ clean_string('evt_type_str') }} AS evt_type_str
  , {{ clean_string('evt_response_str') }} AS evt_response_str
  , {{ clean_string('evt_message_str') }} AS evt_message_str
  , {{ clean_string('evt_buttontitle_str') }} AS evt_buttontitle_str
  , {{ clean_string('evt_planid_str') }} AS evt_planid_str
  , {{ clean_string('evt_function_str') }} AS evt_function_str
  , {{ clean_string('evt_meal_str') }} AS evt_meal_str
  , {{ clean_string('evt_selections_str') }} AS evt_selections_str
  , {{ clean_string('evt_intention_str') }} AS evt_intention_str
  , {{ clean_string('user_pricecell_str') }} AS user_pricecell_str
  , {{ clean_string('evt_selectedday_str') }} AS evt_selectedday_str
  , {{ clean_string('evt_userinfo_barcode_str') }} AS evt_userinfo_barcode_str
  , {{ clean_string('evt_barcode_str') }} AS evt_barcode_str
  , {{ clean_string('evt_userinfo_aps_alert_sound_str') }} AS evt_userinfo_aps_alert_sound_str
  , {{ clean_string('evt_leveratorid_str') }} AS evt_leveratorid_str
  , {{ clean_string('evt_action_str') }} AS evt_action_str
  , evt_shippingcents_real
  , {{ clean_string('evt_description_str') }} AS evt_description_str
  , {{ clean_string('evt_story_str') }} AS evt_story_str
  , evt_price_cents_real
  , evt_max_selections_real
  , evt_commitment_bool
  , {{ clean_string('evt_id_str') }} AS evt_id_str
  , {{ clean_string('evt_url_str') }} AS evt_url_str
  , {{ clean_string('evt_response_request_requestmethod_str') }} AS evt_response_request_requestmethod_str
  , {{ clean_string('evt_request_requestmethod_str') }} AS evt_request_requestmethod_str
  , user_startingdefaultmealplan_real
  , {{ clean_string('user_startingselectedmealplan_str') }} AS user_startingselectedmealplan_str
  , {{ clean_string('evt_utm_variant_str') }} AS evt_utm_variant_str
  , {{ clean_string('evt_utm_popup_id_str') }} AS evt_utm_popup_id_str
  , {{ clean_string('evt_utm_source_str') }} AS evt_utm_source_str
  , {{ clean_string('evt_utm_popup_location_str') }} AS evt_utm_popup_location_str
  , {{ clean_string('evt_utm_lang_str') }} AS evt_utm_lang_str
  , {{ clean_string('evt_utm_campaign_str') }} AS evt_utm_campaign_str
  , {{ clean_string('evt_utm_cta_location_str') }} AS evt_utm_cta_location_str
  , {{ clean_string('evt_utm_quiz_name_str') }} AS evt_utm_quiz_name_str
  , {{ clean_string('evt_utm_cta_id_str') }} AS evt_utm_cta_id_str
  , {{ clean_string('evt_uid_str') }} AS evt_uid_str
  , {{ clean_string('evt_utm_quiz_cta_str') }} AS evt_utm_quiz_cta_str
  , {{ clean_string('evt_utm_medium_str') }} AS evt_utm_medium_str
  , {{ clean_string('evt_fh285_auid_str') }} AS evt_fh285_auid_str
  , evt_utm_quiz_question_order_real
  , {{ clean_string('evt_utm_quiz_question_id_str') }} AS evt_utm_quiz_question_id_str
  , evt_utm_quiz_answer_strs
  , {{ clean_string('evt_utm_quiz_question_order_str') }} AS evt_utm_quiz_question_order_str
  , evt_answer_strs
  , {{ clean_string('evt_utm_quizcta_exp_str') }} AS evt_utm_quizcta_exp_str
  , evt_request_bbi_async_bool
  , {{ clean_string('evt_request_bbi_method_str') }} AS evt_request_bbi_method_str
  , {{ clean_string('evt_request_bbi_url_str') }} AS evt_request_bbi_url_str
  , evt_request_headers_accept_strs
  , evt_request_headers_xtovalaappid_strs
  , evt_response_request_bbi_async_bool
  , {{ clean_string('evt_response_request_bbi_method_str') }} AS evt_response_request_bbi_method_str
  , {{ clean_string('evt_response_request_bbi_url_str') }} AS evt_response_request_bbi_url_str
  , evt_response_request_headers_accept_strs
  , evt_response_request_headers_xtovalaappid_strs
  , {{ clean_string('evt_utm_content_str') }} AS evt_utm_content_str
  , {{ clean_string('evt_filterid_str') }} AS evt_filterid_str
  , {{ clean_string('evt_filter_category_str') }} AS evt_filter_category_str
  , {{ clean_string('evt_filter_id_str') }} AS evt_filter_id_str
  , {{ clean_string('evt_test_str') }} AS evt_test_str
  , {{ clean_string('evt_article_title_str') }} AS evt_article_title_str
  , {{ clean_string('evt_ble_oven_registration_state_str') }} AS evt_ble_oven_registration_state_str
  , {{ clean_string('evt_ble_oven_scanner_state_str') }} AS evt_ble_oven_scanner_state_str
  , {{ clean_string('evt_bleovenregistrationstate_str') }} AS evt_bleovenregistrationstate_str
  , {{ clean_string('evt_bleovenscannerstate_str') }} AS evt_bleovenscannerstate_str
  , {{ clean_string('evt_button_id_str') }} AS evt_button_id_str
  , {{ clean_string('evt_code_type_str') }} AS evt_code_type_str
  , {{ clean_string('evt_cook_button_str') }} AS evt_cook_button_str
  , {{ clean_string('evt_cookbutton_str') }} AS evt_cookbutton_str
  , {{ clean_string('evt_delivery_day_of_week_str') }} AS evt_delivery_day_of_week_str
  , {{ clean_string('evt_deliverydayofweek_str') }} AS evt_deliverydayofweek_str
  , {{ clean_string('evt_document_url_str') }} AS evt_document_url_str
  , {{ clean_string('evt_error_description_str') }} AS evt_error_description_str
  , {{ clean_string('evt_errordescription_str') }} AS evt_errordescription_str
  , {{ clean_string('evt_gen_id_str') }} AS evt_gen_id_str
  , {{ clean_string('evt_genid_str') }} AS evt_genid_str
  , {{ clean_string('evt_location_href_str') }} AS evt_location_href_str
  , {{ clean_string('evt_meal_detail_section_id_str') }} AS evt_meal_detail_section_id_str
  , evt_meal_id_real
  , {{ clean_string('evt_meal_id_str') }} AS evt_meal_id_str
  , evt_meal_selection_count_real
  , {{ clean_string('evt_meal_selection_count_str') }} AS evt_meal_selection_count_str
  , evt_meal_selection_diff_real
  , {{ clean_string('evt_meal_selection_diff_str') }} AS evt_meal_selection_diff_str
  , evt_meal_selection_max_real
  , {{ clean_string('evt_meal_selection_max_str') }} AS evt_meal_selection_max_str
  , {{ clean_string('evt_mealdetailsectionid_str') }} AS evt_mealdetailsectionid_str
  , {{ clean_string('evt_mealselectioncount_str') }} AS evt_mealselectioncount_str
  , {{ clean_string('evt_mealselectiondiff_str') }} AS evt_mealselectiondiff_str
  , {{ clean_string('evt_mealselectionmax_str') }} AS evt_mealselectionmax_str
  , evt_plan_size_real
  , {{ clean_string('evt_plan_size_str') }} AS evt_plan_size_str
  , {{ clean_string('evt_plansize_str') }} AS evt_plansize_str
  , {{ clean_string('evt_prefix_str') }} AS evt_prefix_str
  , evt_previous_plan_size_real
  , {{ clean_string('evt_previous_plan_size_str') }} AS evt_previous_plan_size_str
  , {{ clean_string('evt_previousplansize_str') }} AS evt_previousplansize_str
  , {{ clean_string('evt_publisher_str') }} AS evt_publisher_str
  , {{ clean_string('evt_push_permissions_str') }} AS evt_push_permissions_str
  , {{ clean_string('evt_pushpermissions_str') }} AS evt_pushpermissions_str
  , {{ clean_string('evt_rating_str') }} AS evt_rating_str
  , {{ clean_string('evt_recipe_id_str') }} AS evt_recipe_id_str
  , {{ clean_string('evt_recipeid_str') }} AS evt_recipeid_str
  , {{ clean_string('evt_request_url_str') }} AS evt_request_url_str
  , {{ clean_string('evt_response_request_url_str') }} AS evt_response_request_url_str
  , {{ clean_string('evt_review_chip_id_str') }} AS evt_review_chip_id_str
  , {{ clean_string('evt_reviewchipid_str') }} AS evt_reviewchipid_str
  , {{ clean_string('evt_selection_str') }} AS evt_selection_str
  , evt_ship_period_real
  , {{ clean_string('evt_ship_period_str') }} AS evt_ship_period_str
  , {{ clean_string('evt_shipperiod_str') }} AS evt_shipperiod_str
  , {{ clean_string('evt_sub_menu_id_str') }} AS evt_sub_menu_id_str
  , {{ clean_string('evt_submenuid_str') }} AS evt_submenuid_str
  , evt_term_id_real
  , {{ clean_string('evt_term_id_str') }} AS evt_term_id_str
  , {{ clean_string('evt_time_id_str') }} AS evt_time_id_str
  , {{ clean_string('evt_timeid_str') }} AS evt_timeid_str
  , {{ clean_string('evt_tutorial_id_str') }} AS evt_tutorial_id_str
  , {{ clean_string('evt_utm_quizskipcta_exp_str') }} AS evt_utm_quizskipcta_exp_str
  , {{ clean_string('evt_experiment_id_str') }} AS evt_experiment_id_str
  , {{ clean_string('evt_variation_name_str') }} AS evt_variation_name_str
  , {{ clean_string('evt_experiment_name_str') }} AS evt_experiment_name_str
  , {{ clean_string('evt_source_id_str') }} AS evt_source_id_str
  , {{ clean_string('evt_question_str') }} AS evt_question_str
  , {{ clean_string('evt_page_slug_str') }} AS evt_page_slug_str
  , {{ clean_string('evt_navigation_item_text_str') }} AS evt_navigation_item_text_str
  , {{ clean_string('evt_button_text_str') }} AS evt_button_text_str
  , {{ clean_string('evt_meal_plan_count_str') }} AS evt_meal_plan_count_str
  , {{ clean_string('evt_oven_type_str') }} AS evt_oven_type_str
  , {{ clean_string('evt_number_str') }} AS evt_number_str
  , {{ clean_string('evt_zip_code_str') }} AS evt_zip_code_str
  , TRY_TO_DATE(evt_start_date) AS evt_start_date
FROM {{ source('fullstory_data_export', 'events') }}
WHERE sessionid IS NOT NULL
{% if is_incremental() %}
AND sessionid IN (SELECT DISTINCT sessionid 
                    FROM {{ source('fullstory_data_export', 'events') }}
                    WHERE eventstart >= 
                    (SELECT DATEADD('hour', -25, MAX(eventstart)) FROM {{this}} ))
{%- endif -%}