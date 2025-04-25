-- =========================================================================
-- Query 1: Alternate Addresses from Module 1
-- =========================================================================
SELECT 
  CAST(Connect_ID AS INT64) AS Connect_ID,
  NULL AS ts_user_profile_submitted,
  CURRENT_TIMESTAMP() AS address_delivered_ts,
  '857915436' AS address_src_question_cid,
  'alternative_address' AS address_nickname,
  D_857915436_D_284580415 AS address_line_1,
  D_857915436_D_728926441 AS address_line_2,
  NULL AS street_num,      
  NULL AS street_name,      
  NULL AS apartment_num,
  D_857915436_D_907038282 AS city,
  D_857915436_D_970839481 AS state,
  D_857915436_D_379899229 AS zip_code,
  NULL AS country, 
  NULL AS cross_street_1,
  NULL AS cross_street_2
FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.module1_v2`
WHERE
  Connect_ID IS NOT NULL
  -- Include only if at least one of the address fields is populated
  AND (
      (D_857915436_D_284580415 IS NOT NULL AND D_857915436_D_284580415 != '') OR
      (D_857915436_D_728926441 IS NOT NULL AND D_857915436_D_728926441 != '') OR
      (D_857915436_D_907038282 IS NOT NULL AND D_857915436_D_907038282 != '') OR
      (D_857915436_D_970839481 IS NOT NULL AND D_857915436_D_970839481 != '') OR
      (D_857915436_D_379899229 IS NOT NULL AND D_857915436_D_379899229 != '')
  )

UNION ALL

-- =========================================================================
-- Query 2: Alternate Addresses from User Profie in the Participants table
-- =========================================================================
SELECT 
  CAST(Connect_ID AS INT64) AS Connect_ID,
  NULL AS ts_user_profile_submitted,
  CURRENT_TIMESTAMP() AS address_delivered_ts,
  '284580415' AS address_src_question_cid,
  'alternative_address' AS address_nickname,
  D_284580415 AS address_line_1,
  D_728926441 AS address_line_2,
  NULL AS street_num,      
  NULL AS street_name,      
  NULL AS apartment_num,
  D_907038282 AS city,
  D_970839481 AS state,
  D_379899229 AS zip_code,
  NULL AS country, 
  NULL AS cross_street_1,
  NULL AS cross_street_2
FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants`
WHERE 
  Connect_ID IS NOT NULL
  -- Include only if at least one of the address fields is populated
  AND (
      (D_284580415 IS NOT NULL AND D_284580415 != '') OR
      (D_728926441 IS NOT NULL AND D_728926441 != '') OR
      (D_907038282 IS NOT NULL AND D_907038282 != '') OR
      (D_970839481 IS NOT NULL AND D_970839481 != '') OR
      (D_379899229 IS NOT NULL AND D_379899229 != '')
  )
;