/*
    mart_datto_rmm_devices.sql

    Purpose:
    Curated device dataset enriched with Windows EOL lifecycle metadata.

    Sources:
    - staging.datto_rmm.api_devices
    - staging.end_of_life_date.api_windows

    Logic:
    - LEFT JOIN on normalized OS keys (os_build, edition, lts flag, etc.)
    - Flags expired or missing EOL records via "Is EOL" boolean
    - Outputs flat, dashboard-ready dataset with patching, reboot, and audit metrics

    Destination:
    - marts.public.mart_datto_rmm_devices
*/


select dd.id                       as "id",
       dd.uid                      as "UID",
       dd.hostname                 as "Hostname",
       dd.site_name                as "Client",
       dd.category                 as "Category",

       dd.cloud_category           as "Cloud Category",
       dd.cloud_type               as "Cloud Type",


       dd.os_name                  as "OS",
       dd.os_type                  as "OS Type",
       dd.os_build                 as "OS Build Version",

       eoldw.eol_date              as "OS EOL Date",

       CASE
           WHEN eoldw.eol_date IS NULL THEN True
           WHEN eoldw.eol_date <= CURRENT_DATE THEN True
           ELSE False
           END                     as "Is EOL",

       dd.os_is_lts                as "OS is LTS",
       dd.is_server                as "Is Server",
       dd.os_release_edition       as "OS Release Edition",
       dd.release_info             as "OS Release Info",


       dd.last_reboot              as "Last Reboot",
       dd.no_reboot_last_30_days   as "No Reboot Last 30 Days",
       dd.offline_last_30_days     as "Offline Greater than 30 Days",
       dd.no_audit_last_30_days    as "No Audit Last 30 Days",


       dd.online                   as "Is Online",
       dd.adjusted_last_seen       as "Last Seen",
       dd.last_audit_date          as "Last Audit",
       dd.creation_date            as "Device Onboard Date",


       dd.local_timezone           as "Local Timezone",
       dd.web_remote_url           as "Web Remote URL",

       dd.patches_approved_pending as "Patches Pending",
       dd.patches_installed        as "Patches Installed",
       dd.patch_status             as "Patch Status",
       dd.patch_status_percentage  as "Patch Status Percentage"


from staging.datto_rmm.api_devices dd
         left join staging.end_of_life_date.api_windows as eoldw
                   on dd.os_build = eoldw.os_build
                       AND dd.is_server = eoldw.is_server
                       AND dd.release_info = eoldw.release_info
                       AND dd.os_release_edition = eoldw.os_release_edition
                       AND dd.os_is_lts = eoldw.os_is_lts

