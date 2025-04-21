/*
    mart_datto_rmm_patch_activity.sql

    Purpose:
    Builds a time-ranked view of patch install activity per device, enriched with
    device metadata and OS lifecycle info for compliance and performance tracking.

    Sources:
    - staging.datto_rmm.api_activity_logs_patch (patch event details)
    - staging.datto_rmm.api_devices (device state + patching history)
    - staging.end_of_life_date.api_windows (EOL metadata)

    Logic:
    - Joins device activity with device metadata and OS EOL lifecycle
    - Computes patch duration in minutes
    - Flags EOL systems and failed patches
    - Ranks by activity ID for deterministic row order

    Destination:
    - marts.public.mart_datto_rmm_patch_activity
*/



WITH ranked_data
         AS (SELECT ROW_NUMBER() OVER (ORDER BY dalp.id) AS "id",
                    dalp.id                              as "Activity ID",
                    dalp.action                          as "Activity Action",
                    dalp.date                            as "Activity Date",
                    dalp.site_name                       as "Client",
                    dalp.device_uid                      as "Device UID",
                    dalp.device_hostname                 as "Hostname",
                    dd.os_name                           as "OS",
                    dd.os_type                           as "OS Type",
                    dd.os_build                          as "OS Build Version",
                    eoldw.eol_date                       as "OS EOL Date",
                    CASE
                        WHEN eoldw.eol_date <= CURRENT_DATE THEN True
                        ELSE False
                        END                              as "Is EOL",
                    eoldw.os_is_lts                      as "OS is LTS",
                    dd.local_timezone                    as "Local Timezone",
                    dd.last_reboot                       as "Last Reboot",
                    dd.type                              as "Device Type",
                    dd.creation_date                     as "Device Creation Date",
                    dd.patch_status                      as "Patch Status",
                    dd.patch_status_percentage           as "Patch Status Percentage",
                    dd.patches_approved_pending          as "Patches Approved Pending",
                    dalp.patch_activity_patch_title      as "Patch Title",
                    dalp.source_forwarded_ip             as "Source IP",
                    dalp.patch_activity_hresult          as "HResult",
                    dalp.patch_activity_kb_id            as "Patch KB",
                    dalp.patch_activity_action           as "Patch Activity Action",
                    dalp.patch_activity_success          as "Patch Successful",
                    round(extract(EPOCH from (
                        dalp.patch_activity_patch_install_end -
                        dalp.patch_activity_patch_install_start
                        )) / 60, 4)                      as "Patch Install Time (in minutes)",
                    dd.last_audit_date                   as "Last Audit Date",
                    dd.no_audit_last_30_days             as "No Audit Last 30 Days",
                    dd.offline_last_30_days              as "Offline Last 30 Days",
                    dd.no_reboot_last_30_days            as "No Reboot Last 30 Days"
             FROM staging.datto_rmm.api_activity_logs_patch dalp
                      JOIN staging.datto_rmm.api_devices dd on dalp.device_uid = dd.uid
                      JOIN staging.end_of_life_date.api_windows as eoldw on dd.os_build = eoldw.os_build)
SELECT *
FROM ranked_data
ORDER BY "Patch Install Time (in minutes)" DESC;