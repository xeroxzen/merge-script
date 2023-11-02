import pandas as pd
import os
import sys
import shutil

not_pii = {"wplu_googlesitekit_auth_scopes", "_woocommerce_persistent_cart_189", "screen_layout_sfwd-courses", "wpjo_mainsite_72_user-settings", "wpjo_mainsite_189_yoast_notifications", "wpjo_mainsite_116_yoast_promo_hide_premium_upsell_admin_block", "metaboxhidden_nav-menus", "wpik_user-settings-time", "screen_layout_product", "wpjo_mainsite_131_user-settings-time", "po_data", "wplu_googlesitekit_redirect_url", "_jupiterx_user_facebook", "gf_dimissed_23a1f38f24b6553eb5543252dc51e5b2", "wpjo_mainsite_53_meta-box-order_inc_popup", "_woocommerce_persistent_cart_175", "wpjo_mainsite_188_user-settings", "wpjo_mainsite_53_yoast_notifications", "lyrics_f7_hide_welcome_panel_on", "lyrics_googlesitekit_access_token_created_at", "wpjo_mainsite_107_meta-box-order_inc_popup", "course_5686_access_from", "wpjo_mainsite_28_yoast_notifications", "jupiterx_elementor_last_page_created_at", "wpjo_mainsite_196_user-settings-time", "_wc_google_analytics_pro_identity", "closedpostboxes_site-setup_page_acf-options-footer-call-to-action", "wp_user_level", "wpjo_mainsite_118_yoast_notifications", "wpjo_mainsite_43_yoast_notifications", "wpjo_mainsite_120_meta-box-order_inc_popup", "wpuq_googlesitekit_access_token_expires_in", "edit_comments_per_page", "twofas_light_totp_status", "wpjo_mainsite_17_wpseo-dismiss-blog-public-notice", "wp_media_library_mode", "wp_shortcode_ignore_notice", "wpseo_title", "_last_order", "wpjo_mainsite_134_user-settings", "wpik_user_level", "googleauthenticator_passwords", "_woocommerce_persistent_cart_216", "wpjo_mainsite_140_yoast_notifications", "wpjo_mainsite_80_user-settings-time", "wpseo-dismiss-configuration-notice", "wpjo_mainsite_87_dashboard_quick_press_last_post_id", "wpm8_user-settings", "metaboxhidden_wysiwyg-widget", "wpjo_mainsite_69_user-settings-time", "wpjo_mainsite_59_yoast_promo_hide_premium_upsell_admin_block", "meta-box-order_inc_popup", "closedpostboxes_pet-finder", "meta-box-order_toplevel_page_wpsso-general", "closedpostboxes_sfwd-lessonslearndash-last-login", "wpm8_googlesitekitpersistent_initial_version", "course_5361_access_from", "wpjo_mainsite_152_user-settings", "lyrics_googlesitekit_user_input_state", "wpjo_mainsite_209_yoast_notifications", "wpjo_mainsite_36_yoast_notifications", "closedpostboxes_dashboard", "wpuq_capabilities", "manageedit-postcolumnshidden_default", "meta-box-order_carts", "twofas_light_trusted_devices", "meta-box-order_wkgrecipe", "metaboxhidden_toplevel_page_affiliate-wp", "wpjo_mainsite_26_yoast_notifications", "wpjo_mainsite_6_user-settings-time", "closedpostboxes_cartflows_flow", "wpjo_mainsite_107_yoast_notifications", "lyrics_capabilities", "lyrics_googlesitekit_access_token", "googleauthenticator_description", "wpjo_mainsite_116_user-settings", "sonetel_wp_yoast_promo_hide_premium_upsell_admin_block", "SERVMASK_PREFIX_metronet_image_id", "wpjo_mainsite_112_meta-box-order_inc_popup", "wpjo_mainsite_17_meta-box-order_inc_popup", "wpjo_mainsite_172_user-settings-time", "woocommerce_admin_android_app_banner_dismissed", "wpjo_mainsite_152_user-settings-time", "wplu_googlesitekit_transient_googlesitekit_user_input_settings", "closedpostboxes_sfwd-quiz", "closedpostboxes_sfwd-certificates", "wpseo_keyword_analysis_last_run", "dismissed_maxmind_license_key_notice", "wpjo_mainsite_42_yoast_notifications", "uc_notice_dissmissed_birthday2_sale1", "_woocommerce_persistent_cart_240", "Yasseruser-settings", "_user_avatar", "wpuq_googlesitekit_refresh_token", "ame_rui_first_login_done", "wpjo_mainsite_134_user-settings-time", "screen_layout_wkgrecipe", "sonetel_wp_strings_notification", "lyrics_dashboard_quick_press_last_post_id", "wpik_wpmoly_dashboard_most_rated_movies_widget_settings", "edit_dt360_showcase_per_page", "wpjo_mainsite_94_user-settings", "edit_property-neighborhood_per_page", "wpm8_dashboard_quick_press_last_post_id", "_woocommerce_tracks_anon_id", "Yasseruser_level", "wpyi_googlesitekit_access_token_expires_in", "wpjo_mainsite_57_user-settings", "wpjo_mainsite_27_yoast_notifications", "jupiterx_user_email", "mashsb_fb_author_urlmass_mailer_user_installed", "wpjo_mainsite_149_user-settings", "edit_acf-field-group_per_page", "wpjo_mainsite_200_user-settings", "wp_capabilities", "_wc_plugin_framework_memberships_dismissed_messages", "closedpostboxes_translation", "wpjo_mainsite_204_yoast_notifications", "pmpro_consent_log", "wpjo_mainsite_127_user-settings", "wpjo_mainsite_229_yoast_notifications", "managenav-menuscolumnshidden", "_order_count", "wp_dashboard_primary", "mmt_user-settings-time", "wpm8_googlesitekit_access_token_created_at", "closedpostboxes_affiliates_page_affiliate-wp-reports", "wpjo_mainsite_101_meta-box-order_inc_popup", "wpjo_mainsite_17_user-settings", "meta-box-order_sfwd-courses", "manageedit", "wpm8_capabilities", "wplu_googlesitekit_access_token", "screen_layout_wkgtutorials", "so_panels_directory_enabled", "wplu_googlesitekitpersistent_initial_version", "wpjo_mainsite_55_yoast_notifications", "elementor_library_remote", "gf_dimissed_39452b5a831377d8deded567012cf93a", "_wc_plugin_framework_customer_order_csv_export_dismissed_messages", "dismiss_user_registration_menu", "mmt_i18nModuleTranslationAssistance", "wpjo_mainsite_77_user-settings", "wpjo_mainsite_120_user-settings-time", "wpjo_mainsite_70_yoast_notifications", "screen_layout_sfwd-lessons", "metaboxhidden_translation", "_woocommerce_load_saved_cart_after_login", "wpjo_mainsite_194_user-settings", "_itsec_primary_dashboard_widget", "wpjo_mainsite_57_yoast_notifications", "meta-box-order_site-setup_page_acf-options-custom-code", "wpjo_mainsite_101_media_library_mode", "wpjo_mainsite_149_user-settings-time", "rocketcdn_dismiss_notice", "wc_last_active", "_woocommerce_persistent_cart_1", "metaboxhidden_inc_popup", "sonetel_wp_yoast_notifications", "wprus_login_pending_async_actions", "wpseo-has-mailchimp-signup", "wpjo_mainsite_15_meta-box-order_inc_popup", "edit_shop_order_per_page", "closedpostboxes_forms_page_gf_entries", "wpjo_mainsite_3_user-settings", "my-jetpack-cache-date", "closedpostboxes_jet-smart-filters", "course_completed_19095", "closedpostboxes_wkgrecipe_page_acf-options-recipes-page", "sonetel_wp_media_library_mode", "edit_wysiwyg-widget_per_page", "wpseo_metadesc", "metaboxhidden_product", "sonetel_wp_WPML_TM_Wizard_For_Manager_Complete", "wpjo_mainsite_118_user-settings", "sonetel_wp_user-settings-time", "_money_spent", "wpjo_mainsite_user_level", "wpjo_mainsite_67_user-settings-time", "rocket_boxes", "metaboxhidden_", "wpjo_mainsite_77_user-settings-time", "edit_page_per_page", "wpik_capabilities", "metaboxhidden_dashboard", "wfls-dismiss-wfls-woocommerce-integration-notice", "wpjo_mainsite_175_user-settings-time", "wplu_user-settings", "metaboxhidden_pet-finder", "pmpro_credit_card_expiring_warning", "wpjo_mainsite_27_user-settings", "wpjo_mainsite_200_yoast_notifications", "wpmf_dashboard_quick_press_last_post_id", "course_5358_access_from", "meta-box-order_toplevel_page_acf-options-site-setup", "wpjo_mainsite_151_yoast_notifications", "SERVMASK_PREFIX_capabilities", "wpm8_googlesitekit_analytics_adsense_linked", "wpuq_elementor_editor_user_favorites", "wpjo_user_level", "pmpro_stripe_customerid", "lyrics_googlesitekit_auth_scopes", "manageedit-shop_ordercolumnshidden", "closedpostboxes_wp_automatic", "manageuserscolumnshidden", "wpjo_mainsite_235_user-settings-time", "roc_dismissed_pro_release_notice", "gform_entries_screen_options", "wpjo_mainsite_199_user-settings", "screen_layout_site-setup_page_acf-options-custom-code", "closedpostboxes_amazon_auto_links_page_aal_tools", "wpmf_user_level", "pmpro_visits", "metaboxhidden_memberships_page_pmpro-reports", "SERVMASK_PREFIX_yoast_notifications", "manageedit-categorycolumnshidden", "wpjo_mainsite_107_user-settings", "paying_customer", "wplu_googlesitekit_transient_timeout_googlesitekit_user_input_settings", "SERVMASK_PREFIX_metronet_post_id", "wpjo_mainsite_125_user-settings", "lyrics_user_level", "wpjo_mainsite_152_yoast_notifications", "metaboxhidden_property", "course_5362_access_from", "metaboxhidden_wkgrecipe", "wpjo_mainsite_175_woocommerce_pos_store", "meta-box-order_dashboard", "closedpostboxes_", "wpjo_mainsite_25_user-settings", "wpm4_user-settings-time", "metaboxhidden_post", "wpjo_mainsite_138_yoast_notifications", "wpjo_mainsite_82_yoast_notifications", "closedpostboxes_post", "wpjo_mainsite_103_user-settings", "wpm4_capabilities", "sites_network_per_page", "Yasseruser-settings-time", "wpjo_mainsite_253_user-settings-time", "wpik_wpmoly_dashboard_latest_movies_widget_settings", "my-jetpack-cache", "closedpostboxes_watermark", "wpuq_googlesitekit_access_token_created_at", "wpyi_googlesitekit_auth_scopes", "screen_layout_toplevel_page_acf-options-site-setup", "wpjo_mainsite_222_user-settings", "wpuc_dashboard_quick_press_last_post_id", "AtD_ignored_phrases", "wpjo_mainsite_9_yoast_notifications", "wpjo_mainsite_114_yoast_notifications", "wpjo_mainsite_116_user-settings-time", "wpm8_googlesitekit_analytics_ownership", "wpjo_mainsite_131_user-settings", "gf_dimissed_260ca9dd8a4577fc00b7bd5810298076", "wpjo_user-settings-time", "lyrics_googlesitekitpersistent_initial_version", "wpjo_mainsite_94_user-settings-time", "closedpostboxes_toplevel_page_wpsso-general", "metaboxhidden_sso_page_wpsso-advanced", "_woocommerce_persistent_cart_2", "wpjo_mainsite_196_user-settings", "metaboxhidden_wkgtutorials", "dismissed_wc_admin_notice", "_wpml_user_dismissed_notices", "wpjo_mainsite_31_yoast_notifications", "real_estate_package_activate_date", "googleauthenticator_relaxedmode", "wpjo_mainsite_150_user-settings", "_woocommerce_persistent_cart_182", "lyrics_user", "wpjo_mainsite_capabilities", "_wc_plugin_framework_checkout_add_ons_dismissed_messages", "wpjo_mainsite_172_user-settings", "wpjo_mainsite_154_user-settings-time", "wpml_enabled_for_translation_via_ate", "mmt_media_library_mode", "wpm8_googlesitekitpersistent_dismissed_tours", "_gform-update-entry-id", "wfls-last-login", "meta-box-order_watermark", "show_try_gutenberg_panel", "SERVMASK_PREFIX_user-settings", "wpjo_mainsite_242_yoast_notifications", "wpjo_mainsite_76_yoast_notifications", "closedpostboxes_wkgtutorials", "wpjo_mainsite_45_user-settings-time", "wpjo_mainsite_60_yoast_notifications", "wpm4_user-settings", "metaboxhidden_sfwd-lessons", "jetpack_tracks_anon_id", "gf_dimissed_4a3590d936bec5c5791537cf3e54f673", "wpjo_mainsite_46_user-settings-time", "wpjo_mainsite_229_user-settings", "wpjo_mainsite_59_user-settings-time", "wpjo_mainsite_188_wpseo-upsell-notice", "wpjo_mainsite_12_user-settings", "wpjo_mainsite_187_user-settings", "tgmpa_dismissed_notice_wp-mail-smtp", "meta-box-order_wkgtutorials", "woocommerce_admin_activity_panel_inbox_last_read", "wpjo_mainsite_175_woocommerce_product_import_mapping", "wpjo_mainsite_3_user-settings-time", "wpjo_mainsite_81_yoast_notifications", "_user_modified_gmt", "closedpostboxes_dt360_showcase", "_aioseo_settings", "real_estate_package_number_featured", "wpjo_mainsite_89_yoast_notifications", "wpcf7_hide_welcome_panel_on", "course_10392_access_from", "edit_shop_subscription_per_page", "wpjo_mainsite_15_user-settings", "course_5857_access_from", "mmt_user-settings", "wpcom_user_id", "wpjo_mainsite_dashboard_quick_press_last_post_id", "real_estate_package_key", "wpjo_mainsite_72_yoast_notifications", "sonetel_wp_user-settings", "wpjo_mainsite_150_yoast_notifications", "wpm8_googlesitekit_transient_googlesitekit_user_input_settings", "wpjo_mainsite_meta-box-order_inc_popup", "wpjo_mainsite_199_yoast_notifications", "lyrics_user-settings-time", "wpjo_mainsite_17_wpseo-upsell-notice", "bwg_photo_gallery", "wpyi_user-settings-time", "wpjo_mainsite_254_user-settings", "meta-box-order_dashboard_updated", "wpjo_mainsite_187_user-settings-time", "meta-box-order_acf-field-group", "wpyi_googlesitekit_site_verified_meta", "manageedit-pagecolumnshidden", "wpjo_mainsite_201_user-settings", "amp_dev_tools_enabled", "dismiss_paypal_menu", "wpjo_mainsite_53_user-settings", "wpjo_mainsite_12_user-settings-time", "_yoast_wpseo_profile_updated", "pmpro_stripe_dont_cancel", "wc_connect_last_box_id", "wpjo_mainsite_175_media_library_mode", "wpjo_mainsite_190_yoast_notifications", "_woocommerce_persistent_cart_133", "wpjo_mainsite_205_user-settings", "mmt_capabilities", "wplu_googlesitekit_user_input_state", "wpjo_mainsite_27_user-settings-time", "googleauthenticator_pwdenabled", "wpjo_mainsite_112_user-settings", "_woocommerce_persistent_cart", "wpjo_mainsite_174_meta-box-order_inc_popup", "closedpostboxes_product_page_acf-options-products-page", "manageedit-tribe_eventscolumnshidden", "wpjo_mainsite_204_user-settings-time", "wpjo_mainsite_35_yoast_notifications", "course_5284_access_from", "meta-box-order_product", "_woocommerce_persistent_cart_245", "wpjo_mainsite_26_user-settings-time", "metaboxhidden_forms_page_gf_entries", "wpjo_mainsite_46_user-settings", "icl_admin_language_migrated_to_wp48", "metaboxhidden_shop_coupon", "rich_editing", "use_ssl", "wplu_googlesitekit_refresh_token", "affwp_edit_payouts_per_page", "wpjo_user-settings", "wp_mail_smtp_dash_widget_lite_hide_summary_report_email_block", "wpjo_mainsite_68_yoast_notifications", "wpjo_mainsite_42_user-settings", "wpjo_mainsite_108_dashboard_quick_press_last_post_id", "wpseo-remove-upsell-notice", "course_10368_access_from", "wpjo_mainsite_188_user-settings-time", "wpjo_mainsite_user-settings-time", "wpjo_mainsite_88_yoast_notifications", "wpjo_mainsite_229_user-settings-time", "SERVMASK_PREFIX_backwpup_dinotopt_backwpup_notice_promoter", "manageedit-pagecolumnshidden_default", "wpyi_googlesitekit_access_token", "wpyi_dashboard_quick_press_last_post_id", "_aioseo_plugin_review_dismissed", "_woocommerce_persistent_cart_228", "wpjo_mainsite_171_yoast_notifications", "lyrics_googlesitekit_site_verified_meta", "_itsec_primary_dashboard_widget_dismissed", "wpjo_mainsite_125_media_library_mode", "wp__stripe_customer_id", "wpjo_mainsite_235_yoast_notifications", "SERVMASK_PREFIX_r_tru_u_x", "affwp_lc_customer_id", "affwp_promotion_method", "lyrics_googlesitekit_refresh_tokenlyrics_googlesitekit_profile", "wpjo_mainsite_17_user-settings-time", "wpjo_mainsite_194_yoast_notifications", "wpjo_mainsite_164_yoast_notifications", "wplu_user_level", "metaboxhidden_amp_podcast_page_acf-options-podcast-settings", "real_estate_favorites_property", "metaboxhidden_landing_page", "wp6b_capabilities", "wpm8_googlesitekit_profile", "sonetel_wp_language_pairs", "gdpr_consents", "wpjo_mainsite_150_meta-box-order_inc_popup", "wpjo_mainsite_69_user-settings", "wpm8_googlesitekit_auth_scopes", "wpjo_mainsite_80_yoast_notifications", "wpjo_mainsite_101_user-settings", "show_admin_bar_front", "comment_shortcuts", "closedpostboxes_family", "wpm8_googlesitekit_access_token_expires_in", "wpjo_mainsite_67_user-settings", "wpp_review_notice", "twofas_light_totp_secret", "managesites-networkcolumnshidden", "_woocommerce_persistent_cart_156", "_woocommerce_persistent_cart_226", "wplu_capabilities", "obfx_ignore_visit_dashboard_notice", "wpyi_user_level", "wpjo_mainsite_235_user-settings", "wpjo_mainsite_154_user-settings", "wpjo_mainsite_206_yoast_notifications", "admin_color", "_woocommerce_persistent_cart_104", "wp_meta-box-order_inc_popup", "wpm8_googlesitekit_access_token", "wpm8_user_level", "wpuc_capabilities", "closedpostboxes_cartflows_step",
           "advanced-ads-hide-wizard", "wpjo_mainsite_31_user-settings-time", "wpjo_mainsite_12_meta-box-order_inc_popup", "wpm8_googlesitekit_transient_timeout_googlesitekit_user_input_settings", "metaboxhidden_shop_subscription", "wp_user-settings", "_wcs_subscription_ids_cache", "wpjo_mainsite_175_user-settings", "wpjo_mainsite_115_yoast_notifications", "pmpro_stripe_updates", "meta-box-order_post", "wp_wpseo-suggested-plugin-yoast-woocommerce-seo", "wpm4_dashboard_quick_press_last_post_id", "sociallyviral_ignore_notice", "advanced-ads-subscribed", "wplu_googlesitekit_site_verified_meta", "wpjo_mainsite_yoast_notifications", "sonetel_wp_dashboard_quick_press_last_post_id", "wpyi_googlesitekit_user_input_state", "managetoplevel_page_wp_streamcolumnshidden", "_woocommerce_persistent_cart_58", "meta-box-order_sfwd-quiz", "_woocommerce_persistent_cart_100", "wpjo_mainsite_101_user-settings-time", "wpjo_mainsite_175_product_import_error_log", "wpjo_mainsite_37_user-settings", "wp_user-settings-time", "lyrics_googlesitekit_profile", "wpjo_mainsite_8_yoast_notifications", "wpjo_mainsite_44_yoast_notifications", "wprus_api_tokens", "metaboxhidden_toplevel_page_acf-options-faqs-tutorials", "wpjj_capabilities", "wpuq_googlesitekit_user_input_state", "wpjo_mainsite_112_user-settings-time", "edit_sfwd-lessons_per_page", "wpjo_mainsite_87_yoast_notifications", "ws-notice-streamtutorial", "wpuq_dashboard_quick_press_last_post_id", "wp_woocommerce_persistent_cart", "community-events-location", "screen_layout_page", "wplu_googlesitekit_profile", "wp6b_dashboard_quick_press_last_post_id", "wpjo_yoast_notifications", "wpjo_mainsite_mail_smtp_wpforms_dismissed", "wpjo_mainsite_174_user-settings", "affwp_lc_affiliate_id", "wpm8_user-settings-time", "_pum_dismissed_alerts", "course_10266_access_from", "metaboxhidden_site-setup_page_acf-options-footer-call-to-action", "wpjo_mainsite_253_yoast_notifications", "edit_shop_coupon_per_page", "gf_dimissed_ed634629e6cdb4c967062b42ca8a7ff0", "metaboxhidden_wkgrecipe_page_acf-options-recipes-page", "closedpostboxes_site-setup_page_acf-options-custom-code", "wpjo_mainsite_222_user-settings-time", "screen_layout_carts", "wplu_user-settings-time", "wp_dashboard_quick_press_last_post_id", "wpjo_mainsite_76_user-settings", "lyrics_googlesitekit_transient_timeout_googlesitekit_user_input_settings", "wpm4_media_library_mode", "dismissed_store_notice_setting_moved_notice", "wpjo_mainsite_3_yoast_notifications", "entry_id", "wpjo_mainsite_222_yoast_notifications", "wp_user_roles", "wpjo_mainsite_94_yoast_notifications", "wpjo_mainsite_31_user-settings", "screen_layout_product_page_acf-options-products-page", "wpjo_mainsite_15_yoast_notifications", "wpjo_mainsite_74_yoast_notifications", "wpm8_googlesitekit_additional_auth_scopes", "wpjo_mainsite_125_yoast_notifications", "wpjo_mainsite_86_yoast_notifications", "closedpostboxes_sso_page_wpsso-advanced", "_sfwd-course_progress", "closedpostboxes_attachment", "wpjo_mainsite_72_user-settings-time", "wpjo_mainsite_107_user-settings-time", "closedpostboxes_shop_subscription", "wplu_googlesitekit_additional_auth_scopes", "metaboxhidden_product_page_acf-options-products-page", "wpjo_mainsite_80_user-settings", "edit_product_per_page", "metaboxhidden_toplevel_page_wpmovielibrary", "wpuq_googlesitekit_tracking_optin", "lyrics_googlesitekit_transient_googlesitekit_user_input_settings", "dismiss_aweber_menu", "wplu_googlesitekit_additional_auth_scopeswplu_dashboard_quick_press_last_post_id", "wpyi_googlesitekit_profile", "wpm8_yoast_notifications", "wpjo_mainsite_118_user-settings-time", "wpjo_mainsite_162_user-settings", "wpjo_mainsite_17_yoast_notifications", "wpjo_mainsite_69_yoast_notifications", "meta-box-order_toplevel_page_acf-options-faqs-tutorials", "_wc_plugin_framework_pip_dismissed_messages", "rank_math_metabox_checklist_layout", "metaboxhidden_dt360_showcase", "wpuq_googlesitekit_survey_timeouts", "closedpostboxes_wkgrecipe", "wpjo_mainsite_127_user-settings-time", "wpjo_mainsite_129_meta-box-order_inc_popup", "wpjo_mainsite_15_user-settings-time", "wpjo_mainsite_200_user-settings-time", "edit_property-city_per_page", "screen_layout_post", "wpjo_mainsite_37_yoast_notifications", "dismissed_regenerating_thumbnails_notice", "meta-box-order_sfwd-lessons", "wpjo_capabilities", "SERVMASK_PREFIX_metronet_avatar_override", "source_domain", "wpjo_mainsite_172_yoast_notifications", "wpjo_mainsite_254_yoast_notifications", "wpjo_mainsite_7_yoast_notifications", "ignore_fbe_not_installed_notice", "metaboxhidden_family", "_redux_welcome_guide", "wp_tab_widget_ignore_notice", "wpm8_googlesitekit_site_verified_meta", "wpjo_mainsite_103_meta-box-order_inc_popup", "screen_layout_inc_popup", "closedpostboxes_nav-menus", "wpseo_linkdex", "gdpr_audit_log", "wpjo_mainsite_40_yoast_notifications", "wpjo_mainsite_55_user-settings", "wpmf_capabilities", "wpyi_googlesitekit_access_token_created_at", "closedpostboxes_acf-field-group", "screen_layout_toplevel_page_acf-options-faqs-tutorials", "edit_post_per_page", "wpjo_mainsite_254_user-settings-time", "wpuq_elementor_connect_common_data", "closedpostboxes_elementor_library", "SERVMASK_PREFIX_user_level", "mmt_dashboard_quick_press_last_post_id", "_woocommerce_persistent_cart_197", "wpjo_mainsite_33_yoast_notifications", "pmpro_views", "metaboxhidden_amazon_auto_links_page_aal_tools", "wpuq_googlesitekit_site_verified_meta", "meta-box-order_toplevel_page_aiowpsec", "edit_property-state_per_page", "wpjo_mainsite_253_user-settings", "sonetel_wp_capabilities", "course_5698_access_from", "metaboxhidden_site-setup_page_acf-options-custom-code", "lyrics_googlesitekit_refresh_token", "_woocommerce_persistent_cart_212", "SERVMASK_PREFIX_nav_menu_recently_edited", "affwp_edit_referrals_per_page", "w3tc_features_seen", "tgmpa_dismissed_notice_jupiterx", "duplicator_pro_created_format", "closedpostboxes_landing_page", "wpuq_googlesitekit_profile", "wpjo_mainsite_42_user-settings-time", "advanced-ads-admin-settings", "wpjo_mainsite_227_yoast_notifications", "wp6b_user_level", "closedpostboxes_memberships_page_pmpro-reports", "wpjo_mainsite_210_yoast_notifications", "_jupiterx_user_twitter", "wpjo_mainsite_124_yoast_notifications", "woo_dynamic_gallery-wc_dgallery_global_settings", "wpjo_mainsite_162_user-settings-time", "nav_menu_recently_edited", "lyrics_googlesitekitpersistent_dismissed_tours", "wpik_dashboard_quick_press_last_post_id", "acf_user_settings", "wplu_user-settingswplu_googlesitekit_additional_auth_scopes", "wpjo_mainsite_26_user-settings", "real_estate_package_number_listings", "sonetel_wp_user_level", "affwp_edit_visits_per_page", "woocommerce_admin_task_list_tracked_started_tasks", "wpjo_mainsite_46_yoast_notifications", "wpyi_googlesitekit_transient_timeout_googlesitekit_user_input_settings", "losedpostboxes_acf-field-group", "wpjo_mainsite_59_user-settings", "wpjo_mainsite_79_yoast_notifications", "_itsec_primary_dashboard", "wpjo_mainsite_47_user-settings-time", "syntax_highlighting", "ame_show_hints", "closedpostboxes_site-setup_page_acf-options-social-options", "_jupiterx_user_email", "closedpostboxes_advanced_ads", "wpuq_googlesitekit_access_token", "wpjo_mainsite_112_yoast_notifications", "wpjo_mainsite_45_yoast_notifications", "edit_sfwd-courses_per_page", "metaboxhidden_sfwd-certificates", "wpjo_mainsite_77_yoast_notifications", "real_estate_package_id", "wpjo_mainsite_6_yoast_notifications", "locale", "googleauthenticator_lasttimeslot", "wpjo_mainsite_126_yoast_notifications", "affwp_edit_affiliates_per_page", "manageedit-dt360_showcasecolumnshidden", "course_5757_access_from", "wpjo_meta-box-order_inc_popup", "wpjo_mainsite_3_dashboard_quick_press_last_post_id", "gf_dimissed_2065a6397034ea4ef8b8e91094464058", "manageedit-sfwd-topiccolumnshidden", "wp_yoast_notifications", "meta-box-order_wc_user_membership", "wpjo_mainsite_200_meta-box-order_inc_popup", "_wc_customer_order_csv_export_is_exported", "wpjo_mainsite_150_user-settings-time", "wpjo_mainsite_205_user-settings-time", "wpjo_mainsite_83_yoast_notifications", "wpjo_mainsite_50_yoast_notifications", "wpjo_mainsite_4_yoast_notifications", "wpjo_mainsite_171_user-settings-time", "mmt_user_level", "wpjo_mainsite_25_user-settings-time", "wpjo_mainsite_194_user-settings-time", "wplu_dashboard_quick_press_last_post_id", "wpjo_mainsite_162_meta-box-order_inc_popup", "wpjo_mainsite_18_yoast_notifications", "wpjo_mainsite_28_user-settings", "manageedit-shop_subscriptioncolumnshidden", "wpm8_googlesitekit_site_verification_file", "meta-box-order_sso_page_wpsso-advanced", "wpjo_mainsite_152_meta-box-order_inc_popup", "wpuc_user_level", "metaboxhidden_toplevel_page_wpsso-general", "wpjo_mainsite_103_media_library_mode", "metaboxhidden_elementor_library", "users_per_page", "sonetel_wp_wpseo-dismiss-blog-public-notice", "wpjo_mainsite_153_yoast_notifications", "metaboxhidden_toplevel_page_acf-options-site-setup", "course_10399_access_from", "wpuq_googlesitekit_transient_timeout_googlesitekit_user_input_settings", "wpm8_googlesitekit_user_input_state", "jetpack_tracks_wpcom_id", "wpjo_mainsite_73_yoast_notifications", "metaboxhidden_page", "wpseo_keyword_analysis-disable", "show_welcome_panel", "googleauthenticator_secret", "metaboxhidden_affiliates_page_affiliate-wp-reports", "wpjo_mainsite_25_yoast_notifications", "survey_notification_bar", "wplu_googlesitekit_access_token_created_at", "manageedit-sfwd-lessonscolumnshidden", "gettr", "metaboxhidden_wp_automatic", "edit_amp_podcast_per_page", "manageedit-sfwd-coursescolumnshidden", "edit_tribe_events_per_page", "dismissed_update_notice", "wpjo_mainsite_45_user-settings", "_wc_memberships_show_admin_restricted_content_notice", "real_estate_free_package", "_gform-entry-id", "wpjo_mainsite_103_user-settings-time", "wpjo_mainsite_155_yoast_notifications", "wpjo_mainsite_51_yoast_notifications", "wpjo_mainsite_180_yoast_notifications", "sonetel_wp_ate_activated", "wpyi_capabilities", "wpjo_mainsite_99_yoast_notifications", "wpuq_user_level", "mm_sua_attachment_id", "wpjo_mainsite_12_yoast_notifications", "mailchimp_woocommerce_is_subscribed", "closedpostboxes_toplevel_page_affiliate-wp", "metaboxhidden_cartflows_flow", "wpjo_mainsite_59_yoast_notifications", "wplu_googlesitekit_access_token_expires_in", "course_10998_access_from", "closedpostboxes_amp_podcast", "gform_forms_per_page", "wpuq_googlesitekitpersistent_initial_version", "manageuploadcolumnshidden", "_learndash_woocommerce_enrolled_courses_access_counter", "exactmetrics_user_preferences", "manageedit-productcolumnshidden", "closedpostboxes_property", "icl_admin_language", "wpjo_mainsite_34_yoast_notifications", "wpuq_googlesitekit_auth_scopes", "screen_layout_acf-field-group", "wpjo_mainsite_82_user-settings-time", "wp_backwpup_dinotopt_JVQWWZJAKVZSASDBOBYHSIDBNZSCAR3JOZSSAWLPOVZCAUTBORUW4Z34NB2HI4DTHIXS653POJSHA4TFONZS433SM4XXG5LQOBXXE5BPOBWHKZ3JNYXWEYLDNN3XA5LQF5ZGK5TJMV3XGL34", "googleauthenticator_enabled", "wpjo_mainsite_149_yoast_notifications", "itsec_user_activity_last_seen", "_affwp_no_integrations_dismissed", "wpjo_mainsite_28_user-settings-time", "wpyi_googlesitekit_refresh_token", "meta-box-order_page", "wpjo_media_library_mode", "wpjo_mainsite_155_user-settings-time", "affwp_referral_notifications", "wpuq_googlesitekit_additional_auth_scopes", "dismissed_wootenberg_notice", "screen_layout_wc_user_membership", "pmpro_last_paypalstandard_ipn_id", "wpyi_googlesitekitpersistent_initial_version", "metaboxhidden_sfwd-courses", "wpuq_user-settings", "wpjo_mainsite_75_yoast_notifications", "metaboxhidden_cartflows_step", "wplu_googlesitekitpersistent_dismissed_tours", "wpik_user-settings", "wpjo_mainsite_57_user-settings-time", "wpjo_mainsite_user-settings", "woo_dynamic_gallery-plugin_framework_global_box-opened", "edit_property_per_page", "metaboxhidden_amp_podcast", "_woocommerce_persistent_cart_257", "wpuq_googlesitekit_transient_googlesitekit_user_input_settings", "course_points", "closedpostboxes_shop_coupon", "course_completed_3093", "edit_post_tag_per_page", "metaboxhidden_attachment", "metaboxhidden_site-setup_page_acf-options-social-options", "closedpostboxes_wysiwyg-widget", "wpjo_mainsite_199_user-settings-time", "course_19095_access_from", "woo_dynamic_gallery-wc_dgallery_style_settings", "jupiterx_elementor_pages_created", "wpjo_mainsite_55_user-settings-time", "manageeditrank_math_metabox_checklist_layout", "itsec_logs_page_screen_options", "dismissed_wp_pointers", "lyrics_user-settings", "wpjo_mainsite_108_yoast_notifications", "wplu_googlesitekit_site_verification_file", "users_network_per_page", "Yasserdashboard_quick_press_last_post_id", "_user_job_position", "screen_layout_sfwd-quiz", "metaboxhidden_advanced_ads", "wpjo_mainsite_76_user-settings-time", "edit_property-feature_per_page", "wpyi_yoast_notifications", "closedpostboxes_sfwd-courses", "SERVMASK_PREFIX_user-settings-time", "edit_wkgrecipe_per_page", "_woocommerce_persistent_cart_217", "recieve_admin_emails", "gf_dimissed_5de7e8d493e43f2271673c02985f94f4", "wpyi_googlesitekit_additional_auth_scopes", "wpjo_mainsite_201_yoast_notifications", "pmpro_logins", "Yassercapabilities", "wpjo_mainsite_37_user-settings-time", "dismissed_no_secure_connection_notice", "metaboxhidden_sfwd-quiz", "closedpostboxes_inc_popup", "wpjo_mainsite_53_user-settings-time", "wpjo_mainsite_6_user-settings", "icl_admin_language_migrated_to_wp47", "course_10385_access_from", "closedpostboxes_sfwd-lessons", "gform_recent_forms", "session_tokens", "metaboxhidden_watermark", "_woocommerce_persistent_cart_179", "lyrics_googlesitekit_site_verification_file", "wpjj_user_level", "lyrics_googlesitekit_additional_auth_scopes", "meta-box-order_product_page_acf-options-products-page", "metaboxhidden_acf-field-group", "wpjo_mainsite_155_user-settings", "wp_dashboard_secondary", "elementor_introduction", "lyrics_googlesitekit_access_token_expires_in", "SERVMASK_PREFIX_dashboard_quick_press_last_post_id", "show_welcome_dialog", "metaboxhidden_jet-smart-filters", "course_10378_access_from", "_wc_plugin_framework_order_status_control_dismissed_messages", "wpyi_user-settings", "wpjo_mainsite_201_user-settings-time", "wpjo_mainsite_47_user-settings", "wpyi_googlesitekit_transient_googlesitekit_user_input_settings", "idx_user_first_login", "wpjo_mainsite_19_yoast_notifications", "upload_per_page", "closedpostboxes_toplevel_page_acf-options-faqs-tutorials", "manageedit-postcolumnshidden", "losedpostboxes_property", "edit_property-type_per_page", "twofas_light_step_token", "primary_blog", "wpjo_mainsite_82_user-settings", "wpjo_mainsite_131_yoast_notifications", "closedpostboxes_product", "wpm8_googlesitekit_refresh_token", "lyrics_statistics", "wpjo_mainsite_120_user-settings", "closedpostboxes_page", "wpuq_user-settings-time", "pmpro_expiration_notice", "_wc_customer_order_csv_export_notices", "wp_elementor_connect_common_data", "closedpostboxes_amp_podcast_page_acf-options-podcast-settings", "screen_layout_dashboard", "wpm4_user_level", "wpjo_mainsite_205_yoast_notifications", "monsterinsights_user_preferences", "wpjo_mainsite_174_user-settings-time", "wpm8_googlesitekit_analytics", "wpjo_mainsite_125_user-settings-time", "wp_mail_smtp_dash_widget_lite_hide_graph", "wpjo_mainsite_204_user-settings", "wpjo_mainsite_171_user-settings", "closedpostboxes_toplevel_page_acf-options-site-setup", "wpcom_user_data"}


colstodrop = []


def usermeta_cleaner(filename):
    """
    Clean up the usermeta table in the database
    """
    # Read the csv file passed as an argument
    df = pd.read_csv(filename)

    # Remove all rows that do not have any PII in them and meta_value is not null
    if "meta_key" in df.columns:
        df = df[~df['meta_key'].isin(not_pii) & df['meta_value'].notnull()]
        new_columns = df['meta_key'].unique()
    elif "field_id" in df.columns:
        df = df[~df['field_id'].isin(not_pii) & df['field_value'].notnull()]
        new_columns = df['field_id'].unique()
    else:
        print("No meta_key or field_id column found")

    # make unique list of meta_key

    # if there's a userid column, sort using that or use user_id column
    if 'userid' in df.columns:
        df = df.sort_values(by=['userid'])
    else:
        df = df.sort_values(by=['user_id'])

    '''
    Print the number of rows that were deleted for fun
    '''
    print(f"Number of rows deleted: {len(df)}")

    if 'umeta_id' in df.columns:
        df = df.drop('umeta_id', axis=1)
        df = pd.concat([pd.DataFrame(
            columns=new_columns),df])
    else:
        df = pd.concat([df,(pd.DataFrame(
            columns=new_columns))])

    '''
    Populate the new columns with the meta_value of the corresponding meta_key and user_id, \
    rows with NaN leave them blank.

    '''
    if 'userid' in df.columns:
        df = df[pd.to_numeric(df['userid'], errors='coerce').notnull()]
        df['userid'] = df['userid'].fillna(-1).astype(int)
        df.sort_values(by=['userid'], inplace=True)
        if "meta_key" in df.columns:
            data = df.groupby(['userid', 'meta_key'])[
                'meta_value'].first().unstack().fillna('')
        elif "field_id" in df.columns:
            data = df.groupby(['userid', 'field_id'])[
                'field_value'].first().unstack().fillna('')
        print(data.head())
    elif 'user_id' in df.columns:
        df = df[pd.to_numeric(df['user_id'], errors='coerce').notnull()]
        df['user_id'] = df['user_id'].fillna(-1).astype(int)
        df.sort_values(by=['user_id'], inplace=True)
        if "meta_key" in df.columns:
            data = df.groupby(['user_id', 'meta_key'])[
                'meta_value'].first().unstack().fillna('')
        elif "field_id" in df.columns:
            data = df.groupby(['user_id', 'field_id'])[
                'field_value'].first().unstack().fillna('')
        print(data.head())

    else:
        print("No userid or user_id column found")

    # Save the cleaned file
    cleaned_filename = os.path.splitext(filename)[0] + '_usermeta_cleaned.csv'
    data.to_csv(cleaned_filename)

    '''
    Move the original usermeta file to a folder named "originals"
    '''
    original_file_path = os.path.abspath(filename)
    original_folder_path = os.path.join(os.path.dirname(original_file_path), 'originals')
    os.makedirs(original_folder_path, exist_ok=True)
    shutil.move(original_file_path, original_folder_path)

    print(f"Original file moved to: {original_folder_path}")


if __name__ == "__main__":
    usermeta_cleaner(sys.argv[1])